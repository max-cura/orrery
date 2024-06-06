use dashmap::DashMap;
use orrery_wire::Object;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::mem::{ManuallyDrop, MaybeUninit};

#[derive(Copy, Clone)]
struct FreeListNode {
    pub next: usize,
}
const FREE_LIST_END: usize = usize::MAX;

pub union RowStorageInner {
    data: ManuallyDrop<MaybeUninit<Object>>,
    free_list_node: FreeListNode,
}
pub struct RowStorage {
    inner: UnsafeCell<RowStorageInner>,
}
/// SAFETY:
///  WHILE TRANSACTIONS ARE EXECUTING, ONLY TRANSACTION EXECUTION CAN MODIFY THIS!
pub struct Table {
    id: usize,
    index: BTreeMap<Vec<u8>, BackingRow>,
    backing_rows: Vec<RowStorage>,
    // DashMap because we may have multiple threads touching it at the same time, though we are
    // guaranteed that no two threads touch the same key.
    ephemerals: DashMap<usize, bool>,
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
        // FIXME: actual free list system
        None
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
        self.id
    }

    pub fn is_materialized(&self, row: usize) -> bool {
        self.ephemerals.get(&row).map(|r| *r).unwrap_or(false)
    }

    pub unsafe fn delete(&self, row: usize) {
        // dematerialize!
        let md = std::mem::replace(
            &mut (&mut *self.backing_rows[row].inner.get()).data,
            ManuallyDrop::new(MaybeUninit::uninit()),
        );
        ManuallyDrop::into_inner(md).assume_init();
        self.ephemerals.insert(row, false);
    }
    pub unsafe fn put(&self, row: usize, object: Object) {
        let md = std::mem::replace(
            &mut (&mut *self.backing_rows[row].inner.get()).data,
            ManuallyDrop::new(MaybeUninit::new(object)),
        );
        ManuallyDrop::into_inner(md).assume_init();
        self.ephemerals.insert(row, true);
    }
    pub unsafe fn read(&self, row: usize) -> Result<Object, MaterializationError> {
        if self.ephemerals.get(&row).map(|x| *x).unwrap_or(true) {
            // ok
            Ok((&*self.backing_rows[row].inner.get())
                .data
                .assume_init_ref()
                .clone())
        } else {
            Err(MaterializationError::Unmaterialized)
        }
    }
}

pub enum MaterializationError {
    Unmaterialized,
}
