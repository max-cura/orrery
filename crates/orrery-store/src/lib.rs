#![feature(strict_provenance)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(iter_intersperse)]
#![allow(dead_code)]

use crate::storage::ResolutionErrorSemantics;

mod herd;
mod partition;
mod sched;
mod sets;
mod storage;
mod table;
mod transaction;

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
