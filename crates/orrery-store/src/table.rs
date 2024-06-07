use dashmap::DashMap;
use orrery_wire::Object;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::mem::{ManuallyDrop, MaybeUninit};

#[derive(Copy, Clone)]
struct FreeListNode {
    pub next: usize,
}
const FREE_LIST_END: usize = usize::MAX;

pub struct RowStorageInner {
    data: ManuallyDrop<MaybeUninit<Object>>,
    // free_list_node: FreeListNode,
}
pub struct RowStorage {
    inner: UnsafeCell<RowStorageInner>,
}
/// SAFETY:
///  WHILE TRANSACTIONS ARE EXECUTING, ONLY TRANSACTION EXECUTION CAN MODIFY THIS!
#[derive(Deserialize)]
#[serde(from = "DeserializeProxy")]
pub struct Table {
    id: usize,
    index: BTreeMap<Vec<u8>, BackingRow>,
    backing_rows: Vec<RowStorage>,
    // DashMap because we may have multiple threads touching it at the same time, though we are
    // guaranteed that no two threads touch the same key.
    ephemerals: DashMap<usize, bool>,
}
impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Table", 4)?;
        s.serialize_field("id", &self.id)?;
        s.serialize_field("index", &self.index)?;
        struct T<'a> {
            i: &'a [RowStorage],
            e: &'a DashMap<usize, bool>,
        }
        impl<'a> Serialize for T<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut s = serializer.serialize_seq(Some(self.i.len()))?;
                for (j, br) in self.i.into_iter().enumerate() {
                    if self.e.get(&j).map(|x| *x).unwrap_or(true) {
                        s.serialize_element(&Some(unsafe {
                            (&*br.inner.get()).data.assume_init_ref()
                        }))?;
                    }
                }
                s.end()
            }
        }
        s.serialize_field(
            "backing_rows",
            &T {
                i: &self.backing_rows,
                e: &self.ephemerals,
            },
        )?;
        s.serialize_field("ephemerals", &self.ephemerals)?;
        s.end()
    }
}
impl From<DeserializeProxy> for Table {
    fn from(
        DeserializeProxy {
            id,
            index,
            backing_rows,
            ephemerals,
        }: DeserializeProxy,
    ) -> Self {
        Self {
            id,
            index,
            backing_rows: backing_rows
                .into_iter()
                .map(|x| match x {
                    Some(x) => RowStorage {
                        inner: UnsafeCell::new(RowStorageInner {
                            data: ManuallyDrop::new(MaybeUninit::new(x)),
                        }),
                    },
                    None => RowStorage {
                        inner: UnsafeCell::new(RowStorageInner {
                            data: ManuallyDrop::new(MaybeUninit::uninit()),
                        }),
                    },
                })
                .collect(),
            ephemerals,
        }
    }
}
#[derive(Debug, Deserialize)]
struct DeserializeProxy {
    id: usize,
    index: BTreeMap<Vec<u8>, BackingRow>,
    backing_rows: Vec<Option<Object>>,
    // DashMap because we may have multiple threads touching it at the same time, though we are
    // guaranteed that no two threads touch the same key.
    ephemerals: DashMap<usize, bool>,
}
impl Debug for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("id", &self.id)
            .field("index", &self.index)
            .field("backing_rows", &"<can't Debug>")
            .field("ephemerals", &self.ephemerals)
            .finish()
    }
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BackingRow(usize);
impl BackingRow {
    pub fn dissolve(self) -> usize {
        self.0
    }
}
impl Table {
    pub fn new_test() -> Self {
        Self {
            id: 0,
            index: BTreeMap::new(),
            backing_rows: vec![],
            ephemerals: DashMap::new(),
        }
    }
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
    pub fn clear_ephemerals(&mut self) {
        // FIXME: this is okay as long as we don't care about reusing candidates
        //   at the moment this is a lovely memory leak.
        self.ephemerals.clear();
    }
    pub fn assure_backing_row(&mut self, key: &[u8]) -> BackingRow {
        let k = self.index.get(key);
        match k {
            None => {
                let br = BackingRow(self.materialize_ephemeral_index());
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

    pub fn is_not_ephemeral(&self, row: usize) -> bool {
        self.ephemerals.get(&row).map(|r| *r).unwrap_or(true)
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
