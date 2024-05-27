#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]

#![allow(dead_code)]

mod sync;
mod sets;
mod op;
mod partition;
mod sched;

use std::collections::BTreeMap;
use std::sync::Arc;
use crate::sched::TransactionFinishedInner;
use crate::sets::AccessSet;
use crate::sync::VersionLock;

struct Table {
    inner: BTreeMap<Vec<u8>, Vec<u8>>,
    whole_table_version : VersionLock,
}

#[derive(Debug)]
pub struct Transaction {
    readonly_set: AccessSet,
    write_set: AccessSet,
    intersect_set: Vec<usize>,
    number: usize,

    finished: Option<Arc<TransactionFinishedInner>>,
}
impl Transaction {
    pub fn no(&self) -> usize { self.number }
}

#[derive(Debug)]
pub enum ExecutionError {

}
type ExecutionResult = Result<Vec<u8>, ExecutionError>;

// fn main() {
//     const N: usize = 500;
//     let i1 = std::time::Instant::now();
//     let pb = DependencyGraphBuilder::empty();
//     rayon::broadcast(|c| {
//         let mut seed = [0u8; 32];
//         seed[0..8].copy_from_slice(&c.index().to_le_bytes());
//         let mut rng = rand_chacha::ChaCha12Rng::from_seed(seed);
//         for i in 0..N {
//             let mut ws = AccessSet::empty();
//             let mut rows = vec![];
//             let iv = rand::seq::index::sample(&mut rng, N * 100, 20);
//             rows.extend(iv.into_iter().map(|x| x as usize));
//             rows.sort_unstable();
//             ws.row_sets.push(RowSet {
//                 table: 0,
//                 rows,
//             });
//             pb.add_txn(Transaction {
//                 readonly_set: AccessSet::empty(),
//                 write_set: ws,
//                 intersect_set: vec![],
//                 number: c.index() * N + i,
//             });
//         }
//     });
//     let i2 = std::time::Instant::now();
//     let (am, tv) = pb.calculate_transaction_interactions();
//     println!("NUM_TXN = {}", tv.len());
//     let mut pd = PartitionDispatcher {
//         access_set: am,
//         transactions: tv,
//         defer_set: BTreeMap::new(),
//     };
//     let i3 = std::time::Instant::now();
//     println!("i1-i2: {:?}", i2.duration_since(i1));
//     println!("i2-i3: {:?}", i3.duration_since(i2));
//     let mut times = vec![];
//     while !pd.transactions.is_empty() {
//         let pd_tl = pd.transactions.len();
//         let i4 = std::time::Instant::now();
//         let q = Arc::new(Injector::new());
//         pd.partition(q.clone(), &PartitionLimits {
//             partition_max_write: 200,
//             partition_max_access: 200,
//         });
//         let i5 = std::time::Instant::now();
//         times.push((pd_tl, pd.defer_set.len(), i5.duration_since(i4)));
//         std::mem::swap(&mut pd.transactions, &mut pd.defer_set);
//     }
//     for (i, r, d) in times {
//         println!("{i} -> {r} in {d:?}");
//     }
//
//     loop {
//         match q.steal() {
//             Steal::Empty => { break }
//             Steal::Success(s) => {
//                 // println!("READ [{}] WRITE [{}] {:?}", s.readonly_set.to_string(), s.write_set.to_string(), s.txn_nos);
//             }
//             Steal::Retry => { continue }
//         }
//     }
//     println!("DEFER [{}]", pd.defer_set.iter().map(|t| format!("{}", t.no())).intersperse(" ".to_string()).collect::<String>());
// }