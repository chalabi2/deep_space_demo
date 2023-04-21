use cosmos_sdk_proto_althea::{
    cosmos::tx::v1beta1::{TxBody, TxRaw},
};

use deep_space::{
    client::Contact,
    utils::decode_any,
};
use futures::future::join_all;
use lazy_static::lazy_static;
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant}, collections::HashMap,
};

use warp::reply::{Json, WithStatus};
use base64::engine::general_purpose::URL_SAFE;
use base64::{encode, decode, Engine};
use warp::{Filter, Rejection};
use rocksdb::{Options, DB};
use serde_json::{Value, json};


lazy_static! {
    static ref COUNTER: Arc<RwLock<Counters>> = Arc::new(RwLock::new(Counters {
        blocks: 0,
        transactions: 0,
        msgs: 0
    }));
}

pub struct Counters {
    blocks: u64,
    transactions: u64,
    msgs: u64,
}

const TIMEOUT: Duration = Duration::from_secs(5);

/// finds earliest available block using binary search, keep in mind this cosmos
/// node will not have history from chain halt upgrades and could be state synced
/// and missing history before the state sync
/// Iterative implementation due to the limitations of async recursion in rust.

async fn get_earliest_block(contact: &Contact, mut start: u64, mut end: u64) -> u64 {
    while start <= end {
        let mid = start + (end - start) / 2;
        let mid_block = contact.get_block(mid).await;
        if let Ok(Some(_)) = mid_block {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }
    // off by one error correction fix bounds logic up top
    start + 1
}

async fn search(contact: &Contact, start: u64, end: u64, db: &DB) {
    let blocks = contact.get_block_range(start, end).await.unwrap();

    let mut tx_counter = 0;
    let mut msg_counter = 0;
    let blocks_len = blocks.len() as u64;
    for block in blocks {
        let block = block.unwrap();
        for tx in block.data.unwrap().txs {
            tx_counter += 1;

            let raw_tx_any = prost_types::Any {
                type_url: "/cosmos.tx.v1beta1.TxRaw".to_string(),
                value: tx,
            };
            let tx_raw: TxRaw = decode_any(raw_tx_any).unwrap();
            let tx_hash = sha256::digest_bytes(&tx_raw.body_bytes);
            let body_any = prost_types::Any {
                type_url: "/cosmos.tx.v1beta1.TxBody".to_string(),
                value: tx_raw.body_bytes,
            };
            let tx_body: TxBody = decode_any(body_any).unwrap();
            for message in tx_body.messages {
                msg_counter += 1;

                // Store the message type and value in the RocksDB
                let key = encode(&tx_hash);
                let value = base64::encode(&message.value);
                db.put(key.as_bytes(), value.as_bytes()).expect("Failed to write to database");
            }
        }
    }
    let mut c = COUNTER.write().unwrap();
    c.blocks += blocks_len;
    c.transactions += tx_counter;
    c.msgs += msg_counter;
}

fn init_db() -> DB {
    let path = "tx_db";
    let mut options = Options::default();
    options.create_if_missing(true);
    DB::open(&options, &path).expect("Failed to open the database")
}

async fn transaction_api(db: Arc<DB>) {
    // GET /transaction?tx_key=tx_{}_{} to retrieve transaction data
    let transaction_route = warp::path("transaction")
        .and(warp::get())
        .and(warp::query::<HashMap<String, String>>())
        .and(with_db(db.clone()))
        .and_then(transaction_handler);

    let routes = transaction_route;

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

fn with_db(db: Arc<DB>) -> impl Filter<Extract = (Arc<DB>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db.clone())
}


async fn transaction_handler(
    params: HashMap<String, String>,
    db: Arc<DB>,
) -> Result<WithStatus<Json>, Rejection> {
    if let Some(tx_key) = params.get("tx_key") {
        let tx_hash = decode(tx_key).map_err(|_| warp::reject::not_found())?;
        match db.get(&tx_hash) {
            Ok(Some(value_bytes)) => {
                let value_json: Value = serde_json::from_slice(&value_bytes).unwrap_or_else(|_| Value::String("Unable to deserialize message".to_string()));

                Ok(warp::reply::with_status(warp::reply::json(&value_json), warp::http::StatusCode::OK))
            }
            Ok(None) => Ok(warp::reply::with_status(
                warp::reply::json(&json!({"error": "Transaction not found"})),
                warp::http::StatusCode::NOT_FOUND,
            )),
            Err(_) => Ok(warp::reply::with_status(
                warp::reply::json(&json!({"error": "Error reading from database"})),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )),
        }
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&json!({"error": "Missing parameter: tx_key"})),
            warp::http::StatusCode::BAD_REQUEST,
        ))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let contact = Contact::new("http://gravity-grpc.polkachu.com:14290", TIMEOUT, "gravity")
        .expect("invalid url");

    let status = contact
        .get_chain_status()
        .await
        .expect("Failed to get chain status, grpc error");

    // get the latest block this node has
    let latest_block = match status {
        deep_space::client::ChainStatus::Moving { block_height } => block_height,
        _ => panic!("Node is not synced or not running"),
    };

    // now we find the earliest block this node has via binary search, we could just read it from
    // the error message you get when requesting an earlier block, but this was more fun
    let earliest_block = get_earliest_block(&contact, 0, latest_block).await;
    println!(
        "This node has {} blocks to download, starting clock now",
        latest_block - earliest_block
    );

    let db = Arc::new(init_db());
    let api_fut = transaction_api(db.clone());
    println!("Starting transaction API on port 3030");

    let processing_fut = async move {
        let start = Instant::now();

        const BATCH_SIZE: u64 = 500;
        const EXECUTE_SIZE: usize = 200;
        let mut pos = earliest_block;
        let mut futures = Vec::new();
        while pos < latest_block {
            let start = pos;
            let end = if latest_block - pos > BATCH_SIZE {
                pos += BATCH_SIZE;
                pos
            } else {
                pos = latest_block;
                latest_block
            };
            let fut = search(&contact, start, end, &db);
            futures.push(fut);
        }

        let mut futures = futures.into_iter();

        let mut buf = Vec::new();
        while let Some(fut) = futures.next() {
            if buf.len() < EXECUTE_SIZE {
                buf.push(fut);
            } else {
                let _ = join_all(buf).await;
                println!("Completed batch of {} blocks", BATCH_SIZE * EXECUTE_SIZE as u64);
                buf = Vec::new();
            }
        }
        let _ = join_all(buf).await;

        let counter = COUNTER.read().unwrap();
        println!(
            "Successfully downloaded {} blocks and {} tx containing {} messages in {} seconds",
            counter.blocks,
            counter.transactions,
            counter.msgs,
            start.elapsed().as_secs()
        )
    };

    tokio::join!(processing_fut, api_fut);
}