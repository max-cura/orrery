/// The [`Storage`] struct orchestrates accesses to the underlying storage of the system.
/// It is designed to be used via [`WriteCache`].
use crate::table::{BackingRow, Table};
use crate::transaction::{CacheValue, DirtyKeyValue, WriteCache};
use crate::{DeleteError, InsertError, PutError, ReadError, UpdateError};
use orrery_wire::{Object, RowLocator};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Storage {
    tables: Vec<Table>,
    table_names: Vec<String>,
    number: usize,
}
unsafe impl Sync for Storage {}

impl Storage {
    pub fn new_test() -> Self {
        Self {
            tables: vec![Table::new_test()],
            table_names: vec!["g".to_string()],
            number: 0,
        }
    }
    pub fn next_number(&mut self) -> usize {
        let t = self.number;
        self.number += 1;
        t
    }
}

// SAFETY:
//  CALLED FROM WORKER ONLY
impl Storage {
    /// DO NOT USE
    unsafe fn delete_unchecked(&self, table_ref: (usize, usize)) {
        self.tables[table_ref.0].delete(table_ref.1);
    }
    /// DO NOT USE
    unsafe fn put(&self, table_ref: (usize, usize), value: Object) {
        self.tables[table_ref.0].put(table_ref.1, value);
    }
    pub unsafe fn apply(&self, dc: WriteCache) {
        for DirtyKeyValue { table_ref, value } in dc.into_dirty_iter() {
            match value {
                CacheValue::Deleted => self.delete_unchecked(table_ref),
                CacheValue::Value(v) => self.put(table_ref, v),
            }
        }
    }
    /// SAFETY:
    ///  This function only verifies that `table_ref` is materialized
    ///
    /// In particular, this means that this will panic if `table_ref` is completely out of range.
    pub unsafe fn read(&self, table_ref: (usize, usize)) -> Option<Object> {
        self.tables[table_ref.0].read(table_ref.1).ok()
    }

    /// Note that this function only checks that `table_ref` is not ephemeral: it will return `true`
    /// for e.g. table_refs that are out of range.
    pub fn is_not_ephemeral(&self, table_ref: (usize, usize)) -> bool {
        self.tables[table_ref.0].is_not_ephemeral(table_ref.1)
    }
}

#[cfg(test)]
mod test_mutation {
    use crate::storage::Storage;
    use crate::InsertError;
    use orrery_wire::{Object, RowLocator};

    #[test]
    fn test_mutation() {
        let mut storage = Storage::new_test();
        let tr1 = storage
            .resolve_assured::<InsertError>(RowLocator {
                table: "g".to_string(),
                row_key: vec![0],
            })
            .unwrap();
        assert_eq!(unsafe { storage.read(tr1) }, None);
        unsafe { storage.put(tr1, Object::Bool(true)) };
        assert_eq!(unsafe { storage.read(tr1) }, Some(Object::Bool(true)));
    }
}

// RESOLVERS: these functions are run "at the gate" to determine the access sets of transactions.
impl Storage {
    /// Succeeds if and only if the specified row exists immediately before batch execution begins
    /// OR *may* be inserted by a preceding transaction.
    ///
    /// This is used for READ, UPDATE, and DELETE.
    ///
    /// Additional verification of row existence is needed at runtime; in particular:
    ///     - that the key is not deleted by a preceding transaction
    ///     - that the key is not ephemeral at the time of the transaction
    pub fn resolve_required<E: ResolutionErrorSemantics>(
        &mut self,
        row_locator: RowLocator,
    ) -> Result<(usize, usize), E> {
        let table_idx = self
            .table_names
            .iter()
            .position(|n| n == &row_locator.table)
            .ok_or(E::invalid_table())?;
        let row_idx = self.tables[table_idx]
            .get_backing_row(&row_locator.row_key)
            .map(BackingRow::dissolve)
            .ok_or(E::invalid_row())?;
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
    pub fn resolve_assured<E: ResolutionErrorSemantics>(
        &mut self,
        row_locator: RowLocator,
    ) -> Result<(usize, usize), E> {
        let table_idx = self
            .table_names
            .iter()
            .position(|n| n == &row_locator.table)
            .ok_or(E::invalid_table())?;
        // NOTE: it is not sufficient (... this sentence from the ancients remains unfinished).
        let row_idx = self.tables[table_idx]
            .assure_backing_row(&row_locator.row_key)
            .dissolve();
        Ok((table_idx, row_idx))
    }
}

pub trait ResolutionErrorSemantics {
    fn invalid_table() -> Self;
    fn invalid_row() -> Self;
}

impl ResolutionErrorSemantics for ReadError {
    fn invalid_table() -> Self {
        Self::InvalidTable
    }
    fn invalid_row() -> Self {
        Self::InvalidRow
    }
}
impl ResolutionErrorSemantics for UpdateError {
    fn invalid_table() -> Self {
        Self::InvalidTable
    }
    fn invalid_row() -> Self {
        Self::InvalidRow
    }
}
impl ResolutionErrorSemantics for DeleteError {
    fn invalid_table() -> Self {
        Self::InvalidTable
    }
    fn invalid_row() -> Self {
        Self::InvalidRow
    }
}
impl ResolutionErrorSemantics for InsertError {
    fn invalid_table() -> Self {
        Self::InvalidTable
    }
    fn invalid_row() -> Self {
        // technically, we have RowExists, but that's not implemented at resolution
        unimplemented!()
    }
}
impl ResolutionErrorSemantics for PutError {
    fn invalid_table() -> Self {
        Self::InvalidTable
    }
    fn invalid_row() -> Self {
        unimplemented!()
    }
}

#[cfg(test)]
mod test_resolution {
    use crate::storage::Storage;
    use crate::{InsertError, ReadError};
    use orrery_wire::RowLocator;

    #[test]
    fn test_resolve() {
        let mut storage = Storage::new_test();
        assert!(matches!(
            storage.resolve_required(RowLocator {
                table: "g".to_string(),
                row_key: vec![1, 2, 3],
            }),
            Err(ReadError::InvalidRow)
        ));
        let tr1 = storage
            .resolve_assured::<InsertError>(RowLocator {
                table: "g".to_string(),
                row_key: vec![0],
            })
            .unwrap();
        let tr2 = storage
            .resolve_required::<ReadError>(RowLocator {
                table: "g".to_string(),
                row_key: vec![0],
            })
            .unwrap();
        assert_eq!(tr1, tr2);
        let tr3 = storage
            .resolve_assured::<InsertError>(RowLocator {
                table: "g".to_string(),
                row_key: vec![0, 1],
            })
            .unwrap();
        assert_ne!(tr1, tr3);
    }
}
