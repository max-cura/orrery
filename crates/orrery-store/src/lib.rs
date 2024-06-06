#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]
#![allow(dead_code)]

mod herd;
mod partition;
mod sched;
mod sets;
mod table;
mod transaction;

use crate::table::{BackingRow, Table};
use crate::transaction::{CacheValue, DirtyKeyValue};
use orrery_wire::{Object, RowLocator};
use transaction::WriteCache;

struct TableSet {}
impl TableSet {
    pub fn get(&self, _name: &str) -> Option<&Table> {
        unimplemented!()
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
