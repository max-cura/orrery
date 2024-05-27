use crate::sets::AccessSet;
use crate::Transaction;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct DependencyGraphBuilder {
    // (table, row) -> (write?, TxId[])
    access_set: DashMap<(usize, usize), Arc<(AtomicBool, Mutex<Vec<(usize, bool)>>)>>,
    transactions: DashMap<usize, Transaction>,
    generation: u64,
}
unsafe impl Sync for DependencyGraphBuilder {}
impl DependencyGraphBuilder {
    pub fn new(generation: u64) -> Self {
        Self {
            access_set: Default::default(),
            transactions: Default::default(),
            generation,
        }
    }

    pub fn access_set_size(&self) -> usize {
        self.access_set.len()
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    pub fn add_txn(&self, txn: Transaction) -> u64 {
        for ro in txn.readonly_set.iter_keys() {
            match self.access_set.entry(ro) {
                Entry::Occupied(mut o) => {
                    let mut g = o.get_mut().1.lock();
                    g.push((txn.no(), false));
                }
                Entry::Vacant(v) => {
                    v.insert(Arc::new((
                        AtomicBool::new(false),
                        Mutex::new(vec![(txn.no(), false)]),
                    )));
                }
            }
        }
        for w in txn.write_set.iter_keys() {
            match self.access_set.entry(w) {
                Entry::Occupied(mut o) => {
                    {
                        let mut g = o.get_mut().1.lock();
                        g.push((txn.no(), true));
                    }
                    o.get().0.store(true, Ordering::Release);
                }
                Entry::Vacant(v) => {
                    v.insert(Arc::new((
                        AtomicBool::new(true),
                        Mutex::new(vec![(txn.no(), true)]),
                    )));
                }
            }
        }

        assert!(self.transactions.insert(txn.no(), txn).is_none());

        self.generation
    }
    pub fn into_batch(self) -> Batch {
        let Self {
            access_set,
            transactions,
            generation: _,
        } = self;
        let access_set: HashMap<(usize, usize), (bool, Vec<(usize, bool)>)> = access_set
            .into_iter()
            .map(|(k, a)| {
                // PANIC: only if more than 1 strong reference exists
                let (b, v) = Arc::into_inner(a).unwrap();
                (k, (b.load(Ordering::Acquire), v.into_inner()))
            })
            .collect();
        let mut transactions: BTreeMap<usize, Transaction> = transactions.into_iter().collect();
        // intersect_set is specifically only the set of transactions who overlap in write set in
        // some way.
        transactions.par_iter_mut().for_each(|(_, f)| {
            for x in f.write_set.iter_keys() {
                let (_, v) = access_set.get(&x).unwrap();
                for &(tx, _) in v {
                    if !f.intersect_set.contains(&tx) {
                        f.intersect_set.push(tx);
                    }
                }
            }
            for x in f.readonly_set.iter_keys() {
                let (_, v) = access_set.get(&x).unwrap();
                for &(tx, w) in v {
                    if w && !f.intersect_set.contains(&tx) {
                        f.intersect_set.push(tx);
                    }
                }
            }
            f.intersect_set.sort_unstable();
        });
        Batch {
            access_set,
            transactions,
        }
    }
}

pub struct PartitionLimits {
    partition_max_write: usize,
    partition_max_access: usize,
}

/// A set of values
#[derive(Debug)]
pub struct Partition {
    readonly_set: AccessSet,
    write_set: AccessSet,
    transactions: Vec<Transaction>,
    txn_nos: Vec<usize>,
}
impl Partition {
    pub fn empty() -> Self {
        Self {
            readonly_set: AccessSet::empty(),
            write_set: AccessSet::empty(),
            transactions: Vec::new(),
            txn_nos: Vec::new(),
        }
    }
    pub fn transactions_mut(&mut self) -> &mut [Transaction] {
        &mut self.transactions
    }
    pub fn add(&mut self, txn: Transaction) {
        // println!("add {}: WRITE {}", txn.no(), txn.write_set.to_string());
        // readonly_set procedure is a little more involved since invalidation may occur
        self.readonly_set.subtract_(&txn.write_set);
        let mut ros = txn.readonly_set.clone();
        ros.subtract_(&self.write_set);
        self.readonly_set.union_(&ros);
        self.write_set.union_(&txn.write_set);
        self.txn_nos.push(txn.no());
        self.transactions.push(txn);

        let r = &mut self.write_set.row_sets[0].rows;
        let l1 = r.len();
        r.dedup();
        assert_eq!(l1, r.len());
    }
    pub fn try_add(
        &mut self,
        txn: Transaction,
        limits: &PartitionLimits,
    ) -> Result<(), Transaction> {
        // println!("try_add {}: WRITE {}", txn.no(), txn.write_set.to_string());
        // ros = R(T) \ W(P)
        // R'(P) = (R(P) \ W(T)) u ros
        // W'(P) = W(T) u W(P)
        let mut ros = txn.readonly_set.clone();
        ros.subtract_(&self.write_set);
        ros.subtract_(&self.readonly_set);
        let mut ws = txn.write_set.clone();
        ws.subtract_(&self.write_set);
        let new_read_size = self.readonly_set.len() + ros.len();
        let new_write_size = self.write_set.len() + ws.len();
        let new_access_size = new_read_size + new_write_size;
        if new_write_size <= limits.partition_max_write
            && new_access_size <= limits.partition_max_access
        {
            self.readonly_set.subtract_(&ws);
            self.readonly_set.union_nonoverlapping_unchecked_(&ros);
            self.write_set.union_nonoverlapping_unchecked_(&ws);
            self.txn_nos.push(txn.no());
            self.transactions.push(txn);

            let r = &mut self.write_set.row_sets[0].rows;
            let l1 = r.len();
            r.dedup();
            assert_eq!(l1, r.len());
            Ok(())
        } else {
            Err(txn)
        }
    }
}

#[derive(Debug)]
pub struct Batch {
    access_set: HashMap<(usize, usize), (bool, Vec<(usize, bool)>)>,
    transactions: BTreeMap<usize, Transaction>,
}

#[derive(Debug)]
pub struct PartitionDispatcher {
    access_set: HashMap<(usize, usize), (bool, Vec<(usize, bool)>)>,
    transactions: BTreeMap<usize, Transaction>,
    defer_set: BTreeMap<usize, Transaction>,
}
impl PartitionDispatcher {
    pub fn new() -> Self {
        Self {
            access_set: HashMap::new(),
            transactions: BTreeMap::new(),
            defer_set: BTreeMap::new(),
        }
    }
    pub fn install_batch(&mut self, batch: Batch) {
        assert!(self.batch_done());
        let Batch {
            access_set,
            transactions,
        } = batch;
        self.access_set = access_set;
        self.transactions = transactions;
    }

    pub fn batch_done(&self) -> bool {
        self.transactions.is_empty() && self.defer_set.is_empty()
    }
    pub fn load_deferred(&mut self) {
        assert!(self.transactions.is_empty());
        std::mem::swap(&mut self.transactions, &mut self.defer_set);
    }
    pub fn dispatch_one_round<F: Fn(Partition) -> ()>(
        &mut self,
        submit: F,
        partition_limits: &PartitionLimits,
    ) {
        let mut p = Partition::empty();
        let mut i = 0;
        'outer: loop {
            if self.transactions.is_empty() {
                break 'outer;
            }
            p.add(self.transactions.pop_first().unwrap().1);
            'inner: loop {
                if i == p.transactions.len() {
                    break 'inner;
                }
                for j in 0..p.transactions[i].intersect_set.len() {
                    let ix_txn_no = p.transactions[i].intersect_set[j];
                    if !p.txn_nos.contains(&ix_txn_no) && self.transactions.contains_key(&ix_txn_no)
                    {
                        let txn = self.transactions.remove(&ix_txn_no).unwrap();
                        match p.try_add(txn, partition_limits) {
                            Ok(()) => {}
                            Err(txn) => {
                                self.defer_set.insert(ix_txn_no, txn);
                            }
                        }
                    }
                }
                i += 1;
            }
            // no more transactions to add
            let p2 = std::mem::replace(&mut p, Partition::empty());
            submit(p2);
            i = 0;
        }
    }
}
