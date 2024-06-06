// can't do a simple enum-as-Ty, since such a type is infinitely recursive

mod write_cache;

use crate::sched::TransactionFinishedInner;
use crate::sets::AccessSet;
use crate::{ExecutionError, ExecutionResult, ExecutionSuccess, Storage};
use orrery_wire::{serialize_object_set, Object, TransactionIR};
use std::sync::Arc;
pub use write_cache::{CacheValue, DirtyKeyValue, WriteCache};

struct TypeStore {
    compound_types: Vec<TySpec>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TyRef {
    Global(usize),
    Local(usize),
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TySpec {
    Unit,
    Bool,
    UInt,
    Int,
    Float,
    String,
    Bytes,
    List(TyRef),
    Tuple(Vec<TyRef>),
    Option(TyRef),
    Func(TyRef, TyRef),
}

#[derive(Debug, Clone)]
pub enum ResolvedOp {
    /// Run builtin operation
    // SSA(SSA),
    Read {
        table_ref: (usize, usize),
    },
    Update {
        table_ref: (usize, usize),
        value: usize,
    },
    UpdateConditional {
        table_ref: (usize, usize),
        value: usize,
        expect: usize,
    },
    // the trouble with inserts is that the row doesn't necessarily exist yet
    // so we need to optimistically allocate row numbers
    Insert {
        table_ref: (usize, usize),
        value: usize,
    },
    Put {
        table_ref: (usize, usize),
        value: usize,
    },
    Delete {
        table_ref: (usize, usize),
    },
    //
    //     /// branch
    //     Cond(Cond),
    //     /// Phi-function
    //     Phi(Phi),
    //     /// Unconditional jump
    //     Jump(usize),
    //     /// Raw value
    //     Const(usize),
    //     /// Read argument to function
    //     Arg(usize),
    //     /// Return from function
    //     Return(usize),
    //
    //     /// Abort and roll back the transaction.
    //     Abort,
    //     // Finish successfully
    //     Commit(usize),
}
// /// Assumptions:
// ///     1. Program is typed correctly.
// #[derive(Debug)]
// struct SSAProgram {
//     ops: Vec<ResolvedOp>,
//     values: Vec<Option<Object>>,
//     const_values: Vec<ObjectInner>,
// }
//
// struct UserStackFrame {
//     args: Vec<Object>,
//     ssa_begin: usize,
// }
//
// struct ExecutionState {
//     pc: usize,
//     user_call_stack: Vec<UserStackFrame>,
// }
// impl ExecutionState {
//     pub fn new(pc: usize) -> Self {
//         Self {
//             pc,
//             user_call_stack: vec![],
//         }
//     }
//     pub fn step(&mut self) {
//         self.pc += 1;
//     }
//     pub fn jump(&mut self, addr: usize) {
//         self.pc = addr;
//     }
// }
//
// enum ExecutionError {
//     Uninitialized(usize),
//     TypeError(usize, String),
//     OutOfRange(usize),
//     ArgumentOutOfRange(usize),
//     NotInFunction,
//     Abort,
// }
//
// impl SSAProgram {
//     fn read_object(&self, idx: usize) -> Result<&ObjectInner, ExecutionError> {
//         self.values
//             .get(idx)
//             .map(Option::as_ref)
//             .ok_or(ExecutionError::OutOfRange(idx))
//             .and_then(|obj| obj.ok_or(ExecutionError::Uninitialized(idx)))
//             .and_then(|obj| match obj {
//                 Object::Direct(o) => Ok(o),
//                 Object::Indirect(i) => self.read_object(*i),
//             })
//     }
//     fn take_object(&mut self, idx: usize) -> Result<ObjectInner, ExecutionError> {
//         self.values
//             .get_mut(idx)
//             .ok_or(ExecutionError::OutOfRange(idx))?
//             .take()
//             .ok_or(ExecutionError::Uninitialized(idx))
//             .and_then(|obj| match obj {
//                 Object::Direct(o) => Ok(o),
//                 Object::Indirect(i) => self.take_object(i),
//             })
//     }
//     fn object<'s, 'o: 's>(&'s self, object: &'o Object) -> Result<&ObjectInner, ExecutionError> {
//         match object {
//             Object::Direct(o) => Ok(o),
//             Object::Indirect(i) => self.read_object(*i),
//         }
//     }
//
//     fn op_read(
//         &mut self,
//         table_ref: (usize, usize),
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//     ) -> Result<ObjectInner, ExecutionError> {
//         todo!()
//     }
//     fn op_update(
//         &mut self,
//         table_ref: (usize, usize),
//         value: usize,
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//     ) -> Result<ObjectInner, ExecutionError> {
//         todo!()
//     }
//     fn op_insert(
//         &mut self,
//         table_ref: (usize, usize),
//         value: usize,
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//     ) -> Result<ObjectInner, ExecutionError> {
//         todo!()
//     }
//     fn op_put(
//         &mut self,
//         table_ref: (usize, usize),
//         value: usize,
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//     ) -> Result<ObjectInner, ExecutionError> {
//         todo!()
//     }
//     fn op_delete(
//         &mut self,
//         table_ref: (usize, usize),
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//     ) -> Result<ObjectInner, ExecutionError> {
//         todo!()
//     }
//
//     pub fn execute(
//         &mut self,
//         begin: usize,
//         write_cache: &mut WriteCache,
//         storage: &Storage,
//         execution_state: &mut ExecutionState
//     ) -> Result<ObjectInner, ExecutionError> {
//         loop {
//             let op = &self.ops[execution_state.pc];
//             // assert!(self.values[execution_state.pc].is_none());
//             match op {
//                 ResolvedOp::SSA(op) => {
//                     // self.values[execution_state.pc] =
//                     //     Some(self.run_ssa(op, write_cache, storage)?);
//                     // TODO: gather values
//                     execution_state.step();
//                 }
//                 &ResolvedOp::Read { table, row } => {
//                     self.values[execution_state.pc] = Some(Object::Direct(self.op_read(
//                         (table, row),
//                         write_cache,
//                         storage,
//                     )?));
//                     execution_state.step();
//                 }
//                 &ResolvedOp::Update { table, row, value } => {
//                     self.op_update((table, row), value, write_cache, storage)?;
//                     execution_state.step();
//                 }
//                 &ResolvedOp::Insert { table, row, value } => {
//                     self.op_insert((table, row), value, write_cache, storage)?;
//                     execution_state.step();
//                 }
//                 &ResolvedOp::Put { table, row, value } => {
//                     self.op_put((table, row), value, write_cache, storage)?;
//                     execution_state.step();
//                 }
//                 &ResolvedOp::Delete { table, row } => {
//                     self.op_delete((table, row), write_cache, storage)?;
//                     execution_state.step();
//                 }
//                 ResolvedOp::Cond(cond) => match self.read_object(cond.cond)? {
//                     ObjectInner::Bool(b) => execution_state.jump(if *b {
//                         cond.then_branch
//                     } else {
//                         cond.else_branch
//                     }),
//                     t => {
//                         let s = format!("expected type Bool got value {:?}", t);
//                         return Err(ExecutionError::TypeError(cond.cond, s));
//                     }
//                 },
//                 ResolvedOp::Phi(phi) => match self.read_object(phi.cond)? {
//                     ObjectInner::Bool(b) => {
//                         self.values[execution_state.pc] = Some(Object::Indirect(if *b {
//                             phi.then_value
//                         } else {
//                             phi.else_value
//                         }));
//                         execution_state.step();
//                     }
//                     t => {
//                         let s = format!("expected type Bool got value {:?}", t);
//                         return Err(ExecutionError::TypeError(phi.cond, s));
//                     }
//                 },
//                 ResolvedOp::Jump(addr) => {
//                     execution_state.jump(*addr);
//                 }
//                 ResolvedOp::Const(v) => {
//                     // TODO: make this not clone
//                     self.values[execution_state.pc] =
//                         Some(Object::Direct(self.const_values[*v].clone()));
//                     execution_state.step();
//                 }
//                 ResolvedOp::Arg(idx) => {
//                     // TODO: make this not clone
//                     self.values[execution_state.pc] = Some(Object::Direct(
//                         self.object(
//                             execution_state
//                                 .user_call_stack
//                                 .last()
//                                 .ok_or(ExecutionError::NotInFunction)?
//                                 .args
//                                 .get(*idx)
//                                 .ok_or(ExecutionError::ArgumentOutOfRange(*idx))?,
//                         )?
//                         .clone(),
//                     ));
//                 }
//                 ResolvedOp::Return(value) => {
//                     let _frame = execution_state
//                         .user_call_stack
//                         .pop()
//                         .ok_or(ExecutionError::NotInFunction)?;
//                     return self.take_object(*value);
//                 }
//                 ResolvedOp::Abort => {
//                     return Err(ExecutionError::Abort)
//                 }
//                 ResolvedOp::Commit(value) => {
//                     return self.take_object(*value)
//                 }
//             }
//         }
//     }
// }

#[derive(Debug)]
pub struct Transaction {
    pub(crate) readonly_set: AccessSet,
    pub(crate) write_set: AccessSet,
    pub(crate) intersect_set: Vec<usize>,
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
    pub fn set_finished(&mut self, signal: Arc<TransactionFinishedInner>) {
        self.finished = Some(signal);
    }
    pub fn get_finished(&mut self) -> Option<Arc<TransactionFinishedInner>> {
        self.finished.take()
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
