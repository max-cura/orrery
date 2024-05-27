#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]
#![allow(dead_code)]

mod op;
mod partition;
mod sched;
mod sets;
mod work;

use crate::sched::TransactionFinishedInner;
use crate::sets::AccessSet;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::Arc;
use crate::op::ResolvedTransaction;

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
    resolved: ResolvedTransaction,
    finished: Option<Arc<TransactionFinishedInner>>,
}
impl Transaction {
    pub fn no(&self) -> usize {
        self.number
    }
}

#[derive(Debug)]
pub enum ExecutionError {}
type ExecutionResult = Result<Vec<u8>, ExecutionError>;
