//! [`WriteCache`] is the associated write buffer of a transaction.

use crate::storage::Storage;
use crate::{DeleteError, InsertError, PutError, ReadError, UpdateError};
use orrery_wire::Object;

trait UpdateSemanticsError: Sized {
    fn deleted() -> Self;
    fn invalid_row() -> Self;
}
impl UpdateSemanticsError for UpdateError {
    fn deleted() -> Self {
        Self::Deleted
    }
    fn invalid_row() -> Self {
        Self::InvalidRow
    }
}
impl UpdateSemanticsError for DeleteError {
    fn deleted() -> Self {
        Self::Deleted
    }
    fn invalid_row() -> Self {
        Self::InvalidRow
    }
}

#[derive(Debug, Clone)]
pub enum CacheValue {
    Deleted,
    Value(Object),
}
impl CacheValue {
    fn as_value(&self) -> Option<&Object> {
        match self {
            CacheValue::Deleted => None,
            CacheValue::Value(v) => Some(v),
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct CacheMap {
    table_ref: (usize, usize),
    dirty: bool,
}

#[derive(Debug, Clone)]
pub struct DirtyKeyValue {
    pub table_ref: (usize, usize),
    pub value: CacheValue,
}

/// Read-through, write-back cache used to run transactions. Provides rollback capability, since in
/// the case of transaction abort, the [`WriteCache`] can simply be dropped.
#[derive(Debug, Clone)]
pub struct WriteCache {
    access_cache: Vec<CacheValue>,
    access_map: Vec<CacheMap>,
}

impl WriteCache {
    pub fn new() -> Self {
        Self {
            access_cache: Vec::new(),
            access_map: Vec::new(),
        }
    }

    /// Consumes `self` to create an iterator over all the items that are written to.
    pub fn into_dirty_iter(self) -> impl Iterator<Item = DirtyKeyValue> + 'static {
        let Self {
            access_cache,
            access_map,
        } = self;
        access_cache
            .into_iter()
            .zip(access_map.into_iter())
            .filter_map(|(cv, cm)| {
                if !cm.dirty {
                    None
                } else {
                    Some(DirtyKeyValue {
                        table_ref: cm.table_ref,
                        value: cv,
                    })
                }
            })
    }

    /// Insert a single item into the cache.
    ///
    /// Preconditions:
    ///   - item must not already be in cache (this is not checked)
    #[inline]
    fn cache_insert(&mut self, table_ref: (usize, usize), value: CacheValue, dirty: bool) -> usize {
        debug_assert!(!self.access_map.iter().any(|cm| cm.table_ref == table_ref));
        let i = self.access_cache.len();
        self.access_cache.push(value);
        self.access_map.push(CacheMap { table_ref, dirty });
        i
    }
}

impl WriteCache {
    /// Perform a modification or insertion on the cache with UPDATE semantics.
    ///
    /// Preconditions:
    ///   - item must not have been deleted
    ///   - item must be fully materialized
    #[inline]
    fn cache_insert_exclusive(
        &mut self,
        table_ref: (usize, usize),
        value: CacheValue,
        storage: &Storage,
    ) -> Result<(), InsertError> {
        match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(cache_idx) => {
                if !matches!(self.access_cache[cache_idx], CacheValue::Deleted) {
                    return Err(InsertError::RowExists);
                }
                self.access_cache[cache_idx] = value;
                Ok(())
            }
            None => {
                if !storage.is_materialized(table_ref) {
                    self.cache_insert(table_ref, value, true);
                    Ok(())
                } else {
                    Err(InsertError::RowExists)
                }
            }
        }
    }

    /// Strict insert
    pub fn insert(
        &mut self,
        table_ref: (usize, usize),
        value: &Object,
        storage: &Storage,
    ) -> Result<(), InsertError> {
        self.cache_insert_exclusive(table_ref, CacheValue::Value(value.clone()), storage)
    }
}

impl WriteCache {
    /// Perform a modification or insertion on the cache with UPDATE semantics.
    ///
    /// Preconditions:
    ///   - item must not have been deleted
    ///   - item must be fully materialized
    #[inline]
    fn cache_update<E: UpdateSemanticsError>(
        &mut self,
        table_ref: (usize, usize),
        value: CacheValue,
        storage: &Storage,
    ) -> Result<(), E> {
        match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(cache_idx) => {
                if matches!(self.access_cache[cache_idx], CacheValue::Deleted) {
                    return Err(E::deleted());
                }
                self.access_cache[cache_idx] = value;
                Ok(())
            }
            None => {
                if storage.is_materialized(table_ref) {
                    self.cache_insert(table_ref, value, true);
                    Ok(())
                } else {
                    // Strictly, could be either in this case...
                    Err(E::invalid_row())
                }
            }
        }
    }
    /// Strict Update
    pub fn update(
        &mut self,
        table_ref: (usize, usize),
        value: &Object,
        storage: &Storage,
    ) -> Result<(), UpdateError> {
        // Update can fail if:
        //  - row was previously deleted
        //  - row was never materialized
        // We check here that it was materialized, and
        if storage.is_materialized(table_ref) {
            self.cache_update(table_ref, CacheValue::Value(value.clone()), storage)
        } else {
            Err(UpdateError::InvalidRow)
        }
    }
    pub fn update_conditional(
        &mut self,
        table_ref: (usize, usize),
        value: &Object,
        expect: &Object,
        storage: &Storage,
    ) -> Result<bool, UpdateError> {
        let existing = self.cache_read(table_ref, storage).unwrap();
        if &existing != expect {
            return Ok(false);
        } else {
            self.cache_update(table_ref, CacheValue::Value(value.clone()), storage)?;
            Ok(true)
        }
    }
    /// Delete
    pub fn delete(
        &mut self,
        table_ref: (usize, usize),
        storage: &Storage,
    ) -> Result<(), DeleteError> {
        self.cache_update(table_ref, CacheValue::Deleted, storage)
    }
}

impl WriteCache {
    /// Set the value corresponding to `table_ref` in the cache to `value`.
    #[inline]
    fn cache_put(&mut self, table_ref: (usize, usize), value: CacheValue, _storage: &Storage) {
        match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(i) => {
                let _ = std::mem::replace(&mut self.access_cache[i], value);
                self.access_map[i].dirty = true;
            }
            None => {
                self.cache_insert(table_ref, value, true);
            }
        }
    }
    /// Upsert
    pub fn put(
        &mut self,
        table_ref: (usize, usize),
        value: &Object,
        storage: &Storage,
    ) -> Result<(), PutError> {
        self.cache_put(table_ref, CacheValue::Value(value.clone()), storage);
        Ok(())
    }
}

impl WriteCache {
    /// Succeeds if and only if:
    ///   - row was not deleted by a prior transaction in the current partition
    ///   - row was not deleted by a transaction in a prior round
    ///   - row is fully materialized (i.e. not ephemeral)
    #[inline]
    fn cache_read(
        &mut self,
        table_ref: (usize, usize),
        storage: &Storage,
    ) -> Result<Object, ReadError> {
        match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(cache_idx) => match &self.access_cache[cache_idx] {
                CacheValue::Deleted => Err(ReadError::Deleted),
                CacheValue::Value(v) => Ok(v.clone()),
            },
            None => {
                let i = self.cache_insert(
                    table_ref,
                    CacheValue::Value(unsafe {
                        storage
                            .read(table_ref)
                            // will be None if:
                            //  deleted by partition in earlier round, or not yet materialized
                            .ok_or(ReadError::InvalidRow)?
                    }),
                    false,
                );
                Ok(self.access_cache[i].as_value().unwrap().clone())
            }
        }
    }
    /// (Consistent) Read
    pub fn read(
        &mut self,
        table_ref: (usize, usize),
        storage: &Storage,
    ) -> Result<Object, ReadError> {
        self.cache_read(table_ref, storage)
    }
}
