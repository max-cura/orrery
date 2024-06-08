use orrery_server::client::Client;
use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt().with_level(true).init();

    let net_configs = vec![
        (0, "127.0.0.1:3002"),
        // (1, "127.0.0.1:3002", "0.0.0.0:3002"),
        // (2, "127.0.0.1:3003", "0.0.0.0:3003"),
    ];

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let client = Arc::new(Client::new(0, net_configs[0].1.to_string()));

    let mut currnum = 0usize;
    let mut numbers = std::iter::repeat_with(|| {
        let c = currnum;
        currnum += 1;
        c
    });
    let mut txno = || numbers.next().unwrap();

    let tx0 = TransactionRequest {
        ir: vec![Op::Read(RowLocator::new("g", vec![12]))],
        const_buf: vec![],
        client_id: "client".to_string(),
        tx_no: txno(),
    };
    println!("TX0: {:?}", client.execute(tx0).await);

    let tx1 = TransactionRequest {
        ir: vec![Op::Insert(RowLocator::new("g", vec![12]), 0)],
        const_buf: vec![Object::Bool(true)],
        client_id: "client".to_string(),
        tx_no: txno(),
    };
    println!("TX1: {:?}", client.execute(tx1).await);

    let tx2 = TransactionRequest {
        ir: vec![Op::Read(RowLocator::new("g", vec![12]))],
        const_buf: vec![],
        client_id: "client".to_string(),
        tx_no: txno(),
    };
    println!("TX2: {:?}", client.execute(tx2).await);

    // let mut futures = JoinSet::new();
    // for i in 0..100 {
    //     let tx = TransactionRequest {
    //         ir: vec![Op::Update(RowLocator::new("g", vec![12]), 0)],
    //         const_buf: vec![Object::Int(i as i64)],
    //         client_id: "client".to_string(),
    //         tx_no: txno(),
    //     };
    //     let c = Arc::clone(&client);
    //     futures.spawn(async move { (i, c.execute(tx).await) });
    // }
    // while let Some(res) = futures.join_next().await {
    //     let (i, r) = res.unwrap();
    //     println!("TX3.{i}: {r:?}");
    // }

    let tx2 = TransactionRequest {
        ir: vec![Op::Read(RowLocator::new("g", vec![12]))],
        const_buf: vec![],
        client_id: "client".to_string(),
        tx_no: txno(),
    };
    println!("TX2: {:?}", client.execute(tx2).await);
}
