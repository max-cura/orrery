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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowLocator {
    pub table: String,
    pub row_key: Vec<u8>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    SSA(SSA),
    Read(RowLocator),
    Update(RowLocator, usize),
    Insert(RowLocator, usize),
    Cond(Cond),
    Jump(usize),
    Value(/* todo */),
}
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionIR {
    pub ssa_items: Vec<Op>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub ir: TransactionIR,
}
