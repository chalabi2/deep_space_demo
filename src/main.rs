use cosmos_sdk_proto_althea::{
    cosmos::tx::v1beta1::{TxBody, TxRaw},
    ibc::applications::transfer::v1::MsgTransfer,
    tendermint::types::Block,
};

use rocksdb::{Options, DB};
use gravity_proto::gravity::MsgSendToEth;
use warp::Filter;
use serde::Serialize;

use deep_space::{
    client::Contact,
    utils::{decode_any, decode_bytes},
};
use futures::future::join_all;
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

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

#[derive(Serialize)]
pub struct ApiResponse {
    tx_hash: String,
    data: Vec<u8>,
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
        
                let send_to_eth_any = prost_types::Any {
                    type_url: "/gravity.v1.MsgSendToEth".to_string(),
                    value: message.value.clone(),
                };
                let msg_send_to_eth: Result<MsgSendToEth, _> = decode_any(send_to_eth_any);
        
                if let Ok(msg_send_to_eth) = msg_send_to_eth {
                    // Store the MsgSendToEth transaction in the database
                    let key = format!("msgSendToEth_{}", tx_hash);
                    db.put(key, &message.value).expect("Failed to store MsgSendToEth transaction");
                }
            }
        }
    let mut c = COUNTER.write().unwrap();
    c.blocks += blocks_len;
    c.transactions += tx_counter;
    c.msgs += msg_counter;
    }
}

async fn get_all_msg_send_to_eth_transactions(db: Arc<DB>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut response_data = Vec::new();

    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        match item {
            Ok((key, value)) => {
                let key_str = String::from_utf8_lossy(&key);
                if key_str.starts_with("msgSendToEth_") {
                    response_data.push(ApiResponse {
                        tx_hash: key_str[12..].to_string(),
                        data: value.to_vec(),
                    });
                }
            }
            Err(err) => {
                eprintln!("RocksDB iterator error: {}", err);
            }
        }
    }

    Ok(warp::reply::json(&response_data))
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
    let start = Instant::now();

    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    let db = DB::open(&db_options, "transactions_db").expect("Failed to open database");

    let db = Arc::new(db); 

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
    );

    // Start the API server after data is downloaded
    let api_db = db.clone();
    let api_route = warp::path("transactions")
        .and(warp::get())
        .and_then(move || get_all_msg_send_to_eth_transactions(api_db.clone()));

    println!("Starting the API server at 127.0.0.1:3030");
    warp::serve(api_route).run(([127, 0, 0, 1], 3030)).await;
}
