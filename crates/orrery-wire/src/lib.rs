use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSA {
    pub inputs: Vec<usize>,
    pub operation: usize,
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Cond {
    pub cond: usize,
    pub then_branch: usize,
    pub else_branch: usize,
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Phi {
    pub cond: usize,
    pub then_value: usize,
    pub else_value: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowLocator {
    pub table: String,
    pub row_key: Vec<u8>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    // SSA(SSA),
    Read(RowLocator),
    Update(RowLocator, usize),
    UpdateConditional(RowLocator, usize, usize),
    Insert(RowLocator, usize),
    Put(RowLocator, usize),
    Delete(RowLocator),
    // Cond(Cond),
    // Phi(Phi),
    // Jump(usize),
    // Const(/* todo */),
    // Arg(usize),
    // Return(usize),
    // Abort,
    // Commit(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub ir: Vec<Op>,
    pub const_buf: Vec<Object>,

    pub client_id: String,
    pub tx_no: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Object {
    Unit,
    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    // List { item_ty: TySpec, items: Vec<Object> },
    Tuple(Vec<Object>),
    Option { inner: Option<Box<Object>> },
    Func { ssa_begin: usize },
}

pub fn serialize_object_set(input: &[Object]) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(input)
}
