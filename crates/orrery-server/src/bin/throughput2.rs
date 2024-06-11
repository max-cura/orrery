use clap::Parser;
use futures_util::future::MaybeDone::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressIterator};
use orrery_server::client::Client;
use orrery_store::ExecutionResultFrame;
use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
use rand;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Error;

#[derive(clap::Parser)]
struct Args {
    #[arg(short = 'n', default_value_t = 0)]
    node_id: u64,
    ip: String,
    port: u16,
    #[arg(short = 'v')]
    verbose: bool,
}

// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client = Arc::new(Client::new(0, format!("{}:{}", args.ip, args.port)));

    if args.verbose {
        tracing_subscriber::fmt().with_level(true).init();
    }
    // console_subscriber::init();

    println!("CLICNT,TXCNT,TXOPS,MIN,MAX,AVG");

    let fanouts = [1, 2, 4, 8, 16, 32, 64, 128, 256];
    // // let fanouts = [32];
    let blades = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    // // let blades = [512];
    let opcounts = [1, 10, 25, 50, 75, 100];
    // // let opcounts = [75];

    // runbench(128, 8192, 100, Arc::clone(&client)).await;
    // runbench(8, 8192, 100, Arc::clone(&client)).await;

    for fanout in fanouts {
        for blade in blades {
            for ops in opcounts {
                runbench(fanout, blade, ops, Arc::clone(&client)).await;
            }
        }
    }
}

async fn runbench(fanout: usize, blade: usize, ops: usize, client: Arc<Client>) {
    let mut times = vec![];
    let pb = ProgressBar::new(10);
    for j in 0..10 {
        tracing::info!("Building transactions...");

        let transaction_sets: Vec<Vec<(Vec<Op>, Vec<Object>)>> = (0..fanout)
            .into_par_iter()
            .map(|i| {
                let mut set = vec![];
                let cs = format!("c{i}");
                for j in 0..blade {
                    let mut ir = vec![];
                    for _ in 0..ops {
                        let row = rand::thread_rng().gen_range(0..1000u64);
                        ir.push(Op::Put(
                            RowLocator::new("g", Vec::from_iter(row.to_le_bytes().into_iter())),
                            0,
                        ));
                    }
                    let tx = (ir, vec![Object::Int(i as i64)]);
                    set.push(tx);
                }
                set
            })
            .collect();
        let mut rcs = vec![];
        for i in 0..fanout {
            rcs.push(client.connect(format!("client{j}.{i}")).await);
        }
        let mut futures = JoinSet::new();

        tracing::info!("Running transactions...");
        let t1 = std::time::Instant::now();
        for (set, mut rc) in transaction_sets.into_iter().zip(rcs.into_iter()) {
            futures.spawn(async move {
                let mut f2 = FuturesUnordered::new();
                for tx in set {
                    match rc.execute(tx.0, tx.1).await {
                        Ok(f) => f2.push(f),
                        Err(e) => {
                            tracing::error!(
                                "Client {} failed to execute(): {e}",
                                rc.client_id.clone()
                            );
                        }
                    }
                }
                while !f2.is_empty() {
                    let Some(t) = f2.next().await else { break };
                    if let Err(err) = t {
                        tracing::error!("Client had error receiving piped message: {err}")
                    }
                }
                rc.finish().await;
            });
        }
        while let Some(res) = futures.join_next().await {
            res.unwrap();
        }
        let t2 = std::time::Instant::now();

        tracing::info!("Done");

        times.push(t2 - t1);
        pb.inc(1);
    }
    pb.finish_and_clear();
    println!(
        "{},{},{},{},{},{}",
        fanout,
        blade,
        ops,
        times.iter().min().unwrap().as_millis(),
        times.iter().max().unwrap().as_millis(),
        (times.iter().sum::<Duration>() / 10).as_millis(),
    );
}
