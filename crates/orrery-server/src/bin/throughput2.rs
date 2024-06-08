use orrery_server::client::Client;
use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
use rand;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[tokio::main]
async fn main() {
    let net_configs = vec![
        (0, "34.94.189.191:80"),
        // (0, "127.0.0.1:23001"),
        // (1, "127.0.0.1:23002"),
        // (2, "127.0.0.1:23003"),
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

    const FANOUT: usize = 100;
    const BLADE: usize = 100;
    const OPS: usize = 60;

    let mut times = vec![];
    for _ in 0..10 {
        println!("Building transactions...");

        let transaction_sets: Vec<Vec<TransactionRequest>> = (0..FANOUT)
            .into_par_iter()
            .map(|i| {
                let mut set = vec![];
                let cs = format!("c{i}");
                for j in 0..BLADE {
                    let mut ir = vec![];
                    for _ in 0..OPS {
                        let row = rand::thread_rng().gen_range(0..10000u64);
                        ir.push(Op::Put(
                            RowLocator::new("g", Vec::from_iter(row.to_le_bytes().into_iter())),
                            0,
                        ));
                    }
                    let tx = TransactionRequest {
                        ir,
                        const_buf: vec![Object::Int(i as i64)],
                        client_id: cs.clone(),
                        tx_no: j,
                    };
                    set.push(tx);
                }
                set
            })
            .collect();
        println!("Running transactions...");

        let t1 = std::time::Instant::now();
        let mut futures = JoinSet::new();
        let mut ok_count = 0;
        for set in transaction_sets.into_iter() {
            let c = Arc::clone(&client);
            futures.spawn(async move {
                for tx in set {
                    let _ = c.execute(tx).await.unwrap();
                }
            });
        }
        while let Some(res) = futures.join_next().await {
            res.unwrap();
        }
        let t2 = std::time::Instant::now();
        println!("Done");
        println!();

        println!(
            "Ran {}x{}PUT queries in time {:?}",
            FANOUT * BLADE,
            OPS,
            t2 - t1
        );
        times.push(t2 - t1);
    }
    println!("Average: {:?}", times.iter().sum::<Duration>() / 10);
}
