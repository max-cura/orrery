#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]
#![allow(dead_code)]

mod herd;
mod partition;
mod sched;
mod sets;
mod storage;
mod table;
mod transaction;

use crate::sets::AccessSet;
pub use herd::Herd;
use orrery_wire::{Op, TransactionRequest};
use parking_lot::Condvar;
use parking_lot::Mutex;
pub use partition::{
    Batch, DependencyGraphBuilder, Partition, PartitionDispatcher, PartitionLimits,
};
pub use sched::{
    new_with_watchdog, FixedConfigThreshold, IntakeStats, Threshold, TransactionFinished,
    TransactionScheduler,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub use storage::Storage;
pub use table::Table;
pub use transaction::{ResolvedOp, Transaction};

pub type DynPipe = Box<dyn Fn(Batch) + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct Config {
    /// partition limits
    pub partition_limits: PartitionLimits,
    /// batch threshold
    pub fixed_config_threshold: FixedConfigThreshold,
    /// worker count
    pub worker_count: usize,
}

pub struct Inner {
    pub storage: Mutex<Option<Storage>>,
    flag: AtomicBool,
    cv: Condvar,
    herd: Herd,
    config: Config,
}
impl Inner {
    fn submit(&self, batch: Batch) {
        self.flag.store(true, Ordering::SeqCst);
        {
            let mut storage = self.storage.lock();
            let s = storage.take().unwrap();

            let new_storage = self
                .herd
                .run_dispatcher(s, self.config.partition_limits, batch);
            let _ = storage.insert(new_storage);
        }
        self.flag.store(false, Ordering::SeqCst);
        self.cv.notify_all();
    }
}
pub struct PhaseController {
    pub inner: Arc<Inner>,
    sched: Arc<TransactionScheduler<FixedConfigThreshold, DynPipe>>,
}
impl PhaseController {
    pub fn config(&self) -> Config {
        self.inner.config.clone()
    }
    pub fn new(config: Config, storage: Storage) -> Self {
        let fct = config.fixed_config_threshold;
        let inner = Arc::new(Inner {
            storage: Mutex::new(Some(storage)),
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            herd: Herd::new(config.worker_count).unwrap(),
            config,
        });
        let inner2 = Arc::clone(&inner);
        Self {
            inner,
            sched: new_with_watchdog(fct, Box::new(move |batch| inner2.submit(batch))),
        }
    }
    /// NOTE: NEEDS TO RUN ON TRANSACTIONS IN ORDER
    pub fn add_transaction(
        &self,
        transaction_request: TransactionRequest,
    ) -> Result<TransactionFinished, ExecutionError> {
        let mut storage = self.inner.storage.lock();
        if self.inner.flag.load(Ordering::SeqCst) {
            self.inner.cv.wait(&mut storage);
        }
        let storage_mut = storage.as_mut().unwrap();
        let number = storage_mut.next_number();
        let tx = prepare_query(transaction_request, storage_mut, number)
            .map_err(ExecutionError::PreparationError)?;
        let finished = self.sched.enqueue_transaction(tx);
        Ok(finished)
    }
}

pub fn prepare_query(
    transaction_request: TransactionRequest,
    storage: &mut Storage,
    number: usize,
) -> Result<Transaction, PreparationError> {
    let TransactionRequest {
        ir,
        const_buf,
        client_id: _,
        tx_no: _,
    } = transaction_request;
    let mut resolved_ops: Vec<ResolvedOp> = vec![];
    let mut tr_write = vec![];
    let mut tr_read = vec![];
    for stmt in ir {
        match stmt {
            Op::Read(rl) => {
                let table_ref = storage
                    .resolve_required(rl)
                    .map_err(PreparationError::Read)?;
                resolved_ops.push(ResolvedOp::Read { table_ref });
                tr_read.push(table_ref);
            }
            Op::Update(rl, value) => {
                let table_ref = storage
                    .resolve_required(rl)
                    .map_err(PreparationError::Update)?;
                resolved_ops.push(ResolvedOp::Update { table_ref, value });
                tr_write.push(table_ref);
            }
            Op::UpdateConditional(rl, value, expect) => {
                let table_ref = storage
                    .resolve_required(rl)
                    .map_err(PreparationError::Update)?;
                resolved_ops.push(ResolvedOp::UpdateConditional {
                    table_ref,
                    value,
                    expect,
                });
                tr_write.push(table_ref);
            }
            Op::Delete(rl) => {
                let table_ref = storage
                    .resolve_required(rl)
                    .map_err(PreparationError::Delete)?;
                resolved_ops.push(ResolvedOp::Delete { table_ref });
                tr_write.push(table_ref);
            }
            Op::Insert(rl, value) => {
                let table_ref = storage
                    .resolve_assured(rl)
                    .map_err(PreparationError::Insert)?;
                resolved_ops.push(ResolvedOp::Insert { table_ref, value });
                tr_write.push(table_ref);
            }
            Op::Put(rl, value) => {
                let table_ref = storage.resolve_assured(rl).map_err(PreparationError::Put)?;
                resolved_ops.push(ResolvedOp::Put { table_ref, value });
                tr_write.push(table_ref);
            }
        }
    }
    let aset_write = AccessSet::from_table_refs(tr_write.into_iter());
    let mut aset_read = AccessSet::from_table_refs(tr_read.into_iter());
    aset_read.subtract_(&aset_write);

    Ok(Transaction::new(
        aset_read,
        aset_write,
        number,
        resolved_ops,
        const_buf,
    ))
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PreparationError {
    Read(ReadError),
    Update(UpdateError),
    Insert(InsertError),
    Delete(DeleteError),
    Put(PutError),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutionError {
    InputOutOfRange,
    Serialization(String),
    ReadError(ReadError),
    UpdateError(UpdateError),
    PreconditionFailed,
    InsertError(InsertError),
    PutError(PutError),
    DeleteError(DeleteError),
    PreparationError(PreparationError),
}
#[derive(Debug)]
pub struct ExecutionSuccess {
    returned_values: Vec<u8>,
}
pub type ExecutionResult = Result<ExecutionSuccess, ExecutionError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum ReadError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DeleteError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum InsertError {
    InvalidTable,
    RowExists,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum PutError {
    InvalidTable,
}
