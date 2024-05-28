#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]
#![allow(dead_code)]

mod herd;
mod partition;
mod sched;
mod sets;
mod transaction;

use crate::sched::TransactionFinishedInner;
use crate::sets::AccessSet;
use crate::transaction::Object;
use orrery_wire::TransactionIR;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::Arc;
use transaction::WriteCache;

struct TableSet {}
impl TableSet {
    pub fn get(&self, _name: &str) -> Option<&Table> {
        unimplemented!()
    }
}

#[derive(Copy, Clone)]
struct FreeListNode {
    pub next: usize,
}

union RowStorageInner {
    data: ManuallyDrop<MaybeUninit<Vec<u8>>>,
    free_list_node: FreeListNode,
}
struct RowStorage {
    inner: UnsafeCell<RowStorageInner>,
}
struct Table {
    index: BTreeMap<Vec<u8>, usize>,
    backing_rows: Vec<RowStorage>,
    free_list: usize,
}
impl Table {
    pub fn index_to_backing_row(&self, key: &[u8]) -> Option<usize> {
        self.index.get(key).copied()
    }
    pub fn id(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Transaction {
    readonly_set: AccessSet,
    write_set: AccessSet,
    intersect_set: Vec<usize>,
    number: usize,
    finished: Option<Arc<TransactionFinishedInner>>,
    ir: TransactionIR,
    //     resolved: ResolvedTransaction,
}
impl Transaction {
    pub fn no(&self) -> usize {
        self.number
    }
    pub fn execute(&mut self, db_ctx: &mut WriteCache, storage: &Storage) -> ExecutionResult {
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum ExecutionError {}
#[derive(Debug)]
pub struct ExecutionSuccess {
    returned_values: Vec<u8>,
}
type ExecutionResult = Result<ExecutionSuccess, ExecutionError>;

/// VERY DEEPLY UNSAFE
pub struct Storage {}

unsafe impl Sync for Storage {}
impl Storage {
    pub fn apply(&self, dc: WriteCache) {
        unimplemented!()
    }
    pub fn read(&self, table_ref: (usize, usize)) -> Result<Object, ReadError> {
        unimplemented!()
    }

    pub fn validate_key_update(&self, table_ref: (usize, usize)) -> Result<(), UpdateError> {
        unimplemented!()
    }
    pub fn validate_key_insert(&self, table_ref: (usize, usize)) -> Result<(), InsertError> {
        unimplemented!()
    }
    pub fn validate_key_put(&self, table_ref: (usize, usize)) -> Result<(), PutError> {
        unimplemented!()
    }
    pub fn validate_key_delete(&self, table_ref: (usize, usize)) -> Result<(), DeleteError> {
        unimplemented!()
    }
}

pub enum ReadError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
pub enum UpdateError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
pub enum InsertError {
    InvalidTable,
    AlreadyExists,
}
pub enum PutError {
    InvalidTable,
}
pub enum DeleteError {
    InvalidTable,
    InvalidRow,
}
