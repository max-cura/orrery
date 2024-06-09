// use clap::Parser;
// use orrery_server::client::Client;
// use orrery_wire::{Object, Op, RowLocator, TransactionRequest};
// use rand;
// use rand::Rng;
// use rayon::iter::{IntoParallelIterator, ParallelIterator};
// use std::sync::Arc;
// use std::time::Duration;
// use tokio::task::JoinSet;
//
// #[derive(clap::Parser)]
// struct Args {
//     #[arg(short = 'n', default_value_t = 0)]
//     node_id: u64,
//     ip: String,
//     port: u16,
// }
//
// // #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// #[tokio::main]
// async fn main() {
//     tokio::time::sleep(Duration::from_millis(1000)).await;
//
//     let args = Args::parse();
//
//     let client = Arc::new(Client::new(0, format!("{}:{}", args.ip, args.port)));
//
//     const FANOUT: usize = 100;
//     const BLADE: usize = 100;
//
//     for ops in [1, 10, 25, 50, 75, 100] {
//         let mut times = vec![];
//         for _ in 0..10 {
//             // println!("Building transactions...");
//
//             let transaction_sets: Vec<Vec<TransactionRequest>> = (0..FANOUT)
//                 .into_par_iter()
//                 .map(|i| {
//                     let mut set = vec![];
//                     let cs = format!("c{i}");
//                     for j in 0..BLADE {
//                         let mut ir = vec![];
//                         for _ in 0..ops {
//                             let row = rand::thread_rng().gen_range(0..10000u64);
//                             ir.push(Op::Put(
//                                 RowLocator::new("g", Vec::from_iter(row.to_le_bytes().into_iter())),
//                                 0,
//                             ));
//                         }
//                         let tx = TransactionRequest {
//                             ir,
//                             const_buf: vec![Object::Int(i as i64)],
//                             client_id: cs.clone(),
//                             tx_no: j,
//                         };
//                         set.push(tx);
//                     }
//                     set
//                 })
//                 .collect();
//             // println!("Running transactions...");
//
//             let t1 = std::time::Instant::now();
//             let mut futures = JoinSet::new();
//             let mut ok_count = 0;
//             for set in transaction_sets.into_iter() {
//                 let c = Arc::clone(&client);
//                 futures.spawn(async move {
//                     for tx in set {
//                         let _ = c.execute(tx).await.unwrap();
//                     }
//                 });
//             }
//             while let Some(res) = futures.join_next().await {
//                 res.unwrap();
//             }
//             let t2 = std::time::Instant::now();
//             // println!("Done");
//             // println!();
//
//             // println!(
//             //     "Ran {}x{}PUT queries in time {:?}",
//             //     FANOUT * BLADE,
//             //     ops,
//             //     t2 - t1
//             // );
//             times.push(t2 - t1);
//         }
//         println!("Ran {}x{}PUT queries with:", FANOUT * BLADE, ops,);
//         println!("Max: {:?}", times.iter().max());
//         println!("Average: {:?}", times.iter().sum::<Duration>() / 10);
//     }
// }

fn main() {}
