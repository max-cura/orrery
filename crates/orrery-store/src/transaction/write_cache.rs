use crate::transaction::Object;
use crate::{DeleteError, InsertError, PutError, ReadError, Storage, UpdateError};

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
    table_ref: (usize, usize),
    value: CacheValue,
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

    #[inline]
    fn cache_insert(&mut self, table_ref: (usize, usize), value: CacheValue, dirty: bool) -> usize {
        let i = self.access_cache.len();
        self.access_cache.push(value);
        self.access_map.push(CacheMap { table_ref, dirty });
        i
    }
    #[inline]
    fn cache_put<F: FnOnce(Object) -> ()>(
        &mut self,
        table_ref: (usize, usize),
        value: CacheValue,
        dtor: F,
        force_dirty_if_not_present: bool,
    ) {
        match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(i) => {
                let prev = std::mem::replace(&mut self.access_cache[i], value);
                if let CacheValue::Value(value) = prev {
                    dtor(value);
                }
                self.access_map[i].dirty = true;
            }
            None => {
                self.cache_insert(table_ref, value, force_dirty_if_not_present);
            }
        }
    }

    /// (Consistent) Read
    pub fn read(
        &mut self,
        table_ref: (usize, usize),
        storage: &Storage,
    ) -> Result<&Object, ReadError> {
        let cache_idx = match self
            .access_map
            .iter()
            .rposition(|tr| tr.table_ref == table_ref)
        {
            Some(i) => i,
            None => self.cache_insert(
                table_ref,
                CacheValue::Value(storage.read(table_ref)?),
                false,
            ),
        };
        Ok(self.access_cache[cache_idx].as_value().unwrap())
    }
    /// Strict Update
    pub fn update<F: FnOnce(Object) -> ()>(
        &mut self,
        table_ref: (usize, usize),
        value: Object,
        dtor: F,
        storage: &Storage,
    ) -> Result<(), UpdateError> {
        storage.validate_key_update(table_ref)?;
        self.cache_put(table_ref, CacheValue::Value(value), dtor, true);
        Ok(())
    }
    /// Strict insert
    pub fn insert(
        &mut self,
        table_ref: (usize, usize),
        value: Object,
        storage: &Storage,
    ) -> Result<(), InsertError> {
        assert!(!self.access_map.iter().any(|x| x.table_ref == table_ref));
        storage.validate_key_insert(table_ref)?;
        self.cache_insert(table_ref, CacheValue::Value(value), true);
        Ok(())
    }
    /// Upsert
    pub fn put<F: FnOnce(Object) -> ()>(
        &mut self,
        table_ref: (usize, usize),
        value: Object,
        dtor: F,
        storage: &Storage,
    ) -> Result<(), PutError> {
        storage.validate_key_put(table_ref)?;
        self.cache_put(table_ref, CacheValue::Value(value), dtor, true);
        Ok(())
    }
    /// Delete
    pub fn delete<F: FnOnce(Object) -> ()>(
        &mut self,
        table_ref: (usize, usize),
        dtor: F,
        storage: &Storage,
    ) -> Result<(), DeleteError> {
        storage.validate_key_delete(table_ref)?;
        self.cache_put(table_ref, CacheValue::Deleted, dtor, true);
        Ok(())
    }
}
