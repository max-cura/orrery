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
// mod vm;

use crate::sched::TransactionFinishedInner;
use crate::sets::AccessSet;
use crate::transaction::{CacheValue, DirtyKeyValue, ResolvedOp};
use dashmap::DashMap;
use orrery_wire::{serialize_object_set, Object, Op, RowLocator, TransactionIR};
use std::cell::UnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
const FREE_LIST_END: usize = usize::MAX;

union RowStorageInner {
    data: ManuallyDrop<MaybeUninit<Vec<u8>>>,
    free_list_node: FreeListNode,
}
struct RowStorage {
    inner: UnsafeCell<RowStorageInner>,
}
struct Table {
    index: BTreeMap<Vec<u8>, BackingRow>,
    backing_rows: Vec<RowStorage>,
    // DashMap because we may have multiple threads touching it at the same time, though we are
    // guaranteed that no two threads touch the same key.
    ephemerals: DashMap<usize, bool>,

    counter: usize,
}
#[derive(Debug, Copy, Clone)]
pub enum BackingRow {
    Backed(usize),
    Ephemeral(usize),
}
impl BackingRow {
    pub fn dissolve(self) -> usize {
        match self {
            BackingRow::Backed(i) => i,
            BackingRow::Ephemeral(i) => i,
        }
    }
}
impl Table {
    fn next_free_row(&mut self) -> Option<usize> {
        let item = self.counter;
        if self.counter == FREE_LIST_END {
            None
        } else {
            self.counter += 1;
            Some(item)
        }
    }
    fn materialize_ephemeral_index(&mut self) -> usize {
        let i = match self.next_free_row() {
            Some(i) => {
                self.backing_rows[i].inner.get_mut().data =
                    ManuallyDrop::new(MaybeUninit::uninit());
                i
            }
            None => {
                let i = self.backing_rows.len();
                self.backing_rows.push(RowStorage {
                    inner: UnsafeCell::new(RowStorageInner {
                        data: ManuallyDrop::new(MaybeUninit::uninit()),
                    }),
                });
                i
            }
        };
        self.ephemerals.insert(i, false);
        i
    }
    pub fn assure_backing_row(&mut self, key: &[u8]) -> BackingRow {
        let k = self.index.get(key);
        match k {
            None => {
                let br = BackingRow::Ephemeral(self.materialize_ephemeral_index());
                self.index.insert(key.to_vec(), br);
                br
            }
            Some(i) => *i,
        }
    }
    pub fn get_backing_row(&self, key: &[u8]) -> Option<BackingRow> {
        self.index.get(key).copied()
    }
    pub fn id(&self) -> usize {
        unimplemented!()
    }

    pub fn is_materialized(&self, row: usize) -> bool {
        self.ephemerals.get(&row).map(|r| *r).unwrap_or(false)
    }

    pub unsafe fn delete(&self, row: usize) {
        unimplemented!()
    }
    pub unsafe fn put(&self, row: usize, object: Object) {
        unimplemented!()
    }
    pub unsafe fn read(&self, row: usize) -> Result<Object, MaterializationError> {
        unimplemented!()
    }
}

pub enum MaterializationError {
    Unmaterialized,
}

#[derive(Debug)]
pub struct Transaction {
    readonly_set: AccessSet,
    write_set: AccessSet,
    intersect_set: Vec<usize>,
    number: usize,
    finished: Option<Arc<TransactionFinishedInner>>,
    ir: TransactionIR,
    resolved: Vec<ResolvedOp>,
    input_objects: Vec<Object>,
}
impl Transaction {
    pub fn no(&self) -> usize {
        self.number
    }
    fn input_object(&self, idx: usize) -> Result<&Object, ExecutionError> {
        self.input_objects
            .get(idx)
            .ok_or(ExecutionError::InputOutOfRange)
    }
    pub fn execute(&mut self, db_ctx: &mut WriteCache, storage: &Storage) -> ExecutionResult {
        let mut results = vec![];
        let items = std::mem::replace(&mut self.resolved, Vec::new());
        for op in items {
            match op {
                ResolvedOp::Read { table_ref } => {
                    results.push(
                        db_ctx
                            .read(table_ref, storage)
                            .map_err(ExecutionError::ReadError)?,
                    );
                }
                ResolvedOp::Update { table_ref, value } => {
                    db_ctx
                        .update(table_ref, self.input_object(value)?, storage)
                        .map_err(ExecutionError::UpdateError)?;
                }
                ResolvedOp::UpdateConditional {
                    table_ref,
                    value,
                    expect,
                } => {
                    if !db_ctx
                        .update_conditional(
                            table_ref,
                            self.input_object(value)?,
                            self.input_object(expect)?,
                            storage,
                        )
                        .map_err(ExecutionError::UpdateError)?
                    {
                        return Err(ExecutionError::PreconditionFailed);
                    }
                }
                ResolvedOp::Insert { table_ref, value } => {
                    db_ctx
                        .insert(table_ref, self.input_object(value)?, storage)
                        .map_err(ExecutionError::InsertError)?;
                }
                ResolvedOp::Put { table_ref, value } => {
                    db_ctx
                        .put(table_ref, self.input_object(value)?, storage)
                        .map_err(ExecutionError::PutError)?;
                }
                ResolvedOp::Delete { table_ref } => {
                    db_ctx
                        .delete(table_ref, storage)
                        .map_err(ExecutionError::DeleteError)?;
                }
            }
        }
        serialize_object_set(&results)
            .map_err(ExecutionError::Serialization)
            .map(|returned_values| ExecutionSuccess { returned_values })
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    InputOutOfRange,
    Serialization(serde_json::Error),
    ReadError(ReadError),
    UpdateError(UpdateError),
    PreconditionFailed,
    InsertError(InsertError),
    PutError(PutError),
    DeleteError(DeleteError),
}
#[derive(Debug)]
pub struct ExecutionSuccess {
    returned_values: Vec<u8>,
}
type ExecutionResult = Result<ExecutionSuccess, ExecutionError>;

/// VERY DEEPLY UNSAFE
pub struct Storage {
    tables: Vec<Table>,
    table_names: Vec<String>,
}

unsafe impl Sync for Storage {}
impl Storage {
    /// SAFETY: CALLED FROM WORKER
    pub unsafe fn apply(&self, dc: WriteCache) {
        for DirtyKeyValue { table_ref, value } in dc.into_dirty_iter() {
            match value {
                CacheValue::Deleted => {
                    self.tables[table_ref.0].delete(table_ref.1);
                }
                CacheValue::Value(v) => {
                    self.tables[table_ref.0].put(table_ref.1, v);
                }
            }
        }
    }
    pub unsafe fn read(&self, table_ref: (usize, usize)) -> Option<Object> {
        self.tables[table_ref.0].read(table_ref.1).ok()
    }
    pub fn is_materialized(&self, table_ref: (usize, usize)) -> bool {
        self.tables[table_ref.0].is_materialized(table_ref.1)
    }

    /// Succeeds if and only if the specified row exists immediately before batch execution begins
    /// OR *may* be inserted by a preceding transaction.
    ///
    /// This is used for READ, UPDATE, and DELETE.
    ///
    /// Additional verification of row existence is needed at runtime; in particular:
    ///     - that the key is not deleted by a preceding transaction
    ///     - that the key is not ephemeral at the time of the transaction
    pub fn resolve_required(
        &mut self,
        row_locator: RowLocator,
    ) -> Result<(usize, usize), ReadError> {
        let table_idx = self
            .table_names
            .iter()
            .position(|n| n == &row_locator.table)
            .ok_or(ReadError::InvalidTable)?;
        let row_idx = self.tables[table_idx]
            .get_backing_row(&row_locator.row_key)
            .map(BackingRow::dissolve)
            .ok_or(ReadError::InvalidRow)?;
        Ok((table_idx, row_idx))
    }

    /// Succeeds if and only if the specified *table* exists.
    ///
    /// This is used for both INSERT and PUT.
    ///
    /// # INSERT
    ///
    /// Note that whatever the state of the row prior to batch execution, it is possible for the
    /// row, in the intervening time, to be deleted/remain ephemeral, so we cannot use particular
    /// row state as a basis for rejecting resolution of this insert.
    pub fn resolve_assured(
        &mut self,
        row_locator: RowLocator,
    ) -> Result<(usize, usize), InsertError> {
        let table_idx = self
            .table_names
            .iter()
            .position(|n| n == &row_locator.table)
            .ok_or(InsertError::InvalidTable)?;
        // NOTE: it is not sufficient
        let row_idx = self.tables[table_idx]
            .assure_backing_row(&row_locator.row_key)
            .dissolve();
        Ok((table_idx, row_idx))
    }
}

#[derive(Debug)]
pub enum ReadError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug)]
pub enum UpdateError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug)]
pub enum DeleteError {
    InvalidTable,
    InvalidRow,
    Deleted,
}
#[derive(Debug)]
pub enum InsertError {
    InvalidTable,
    RowExists,
}
#[derive(Debug)]
pub enum PutError {
    InvalidTable,
}
