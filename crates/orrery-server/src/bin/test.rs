use clap::Parser;
use orrery_server::client::Client;
use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(clap::Parser)]
struct Args {
    #[arg(short = 'n', default_value_t = 0)]
    node_id: u64,
    ip: String,
    port: u16,
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt().with_level(true).init();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let args = Args::parse();

    let client = Arc::new(Client::new(0, format!("{}:{}", args.ip, args.port)));

    let mut rc = client.connect("test-client".into()).await;

    println!("Running TX0...");
    let r = rc
        .execute(vec![Op::Read(RowLocator::new("g", vec![12]))], vec![])
        .await;
    println!("TX0 (READ ROW 12): {:?}", r);

    println!("Running TX1...");
    let r = rc
        .execute(
            vec![Op::Insert(RowLocator::new("g", vec![12]), 0)],
            vec![Object::Bool(true)],
        )
        .await;
    println!("TX1 (INSERT ROW 12): {:?}", r);

    println!("Running TX2...");
    let r = rc
        .execute(vec![Op::Read(RowLocator::new("g", vec![12]))], vec![])
        .await;
    println!("TX2 (READ ROW 12): {:?}", r);

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
}
