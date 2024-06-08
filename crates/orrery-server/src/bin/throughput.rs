use orrery_server::client::Client;
use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
use rand;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    let net_configs = vec![
        (0, "127.0.0.1:23001"),
        (1, "127.0.0.1:23002"),
        (2, "127.0.0.1:23003"),
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

    let t1 = std::time::Instant::now();
    let mut futures = JoinSet::new();
    let mut ok_count = 0;
    for i in 0..100 {
        let c = Arc::clone(&client);
        futures.spawn(async move {
            let cs = format!("c{i}");
            for j in 0..10000 {
                let row = rand::thread_rng().gen_range(0..10000u64);
                let tx = TransactionRequest {
                    ir: vec![Op::Put(
                        RowLocator::new("g", Vec::from_iter(row.to_le_bytes().into_iter())),
                        0,
                    )],
                    const_buf: vec![Object::Int(i as i64)],
                    client_id: cs.clone(),
                    tx_no: j,
                };
                let _ = c.execute(tx).await.unwrap();
            }
        });
    }
    while let Some(res) = futures.join_next().await {
        res.unwrap();
    }
    let t2 = std::time::Instant::now();

    println!(
        "Ran 10m PUT queries in time {:?}, {ok_count} succeeded",
        t2 - t1
    );
}
