// can't do a simple enum-as-Ty, since such a type is infinitely recursive

use orrery_wire::{Cond, Phi, SSA};

mod write_cache;
use crate::Storage;
pub use write_cache::{CacheValue, WriteCache};

struct TypeStore {
    compound_types: Vec<TySpec>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum TyRef {
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
pub enum Object {
    Direct(ObjectInner),
    Indirect(usize),
}
#[derive(Debug, Clone)]
pub enum ObjectInner {
    Unit,
    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List { item_ty: TySpec, items: Vec<Object> },
    Tuple(Vec<Object>),
    Option { inner: Option<Box<Object>> },
    Func { ssa_begin: usize },
}

#[derive(Debug, Clone)]
pub enum ResolvedOp {
    /// Run builtin operation
    SSA(SSA),

    Read {
        table: usize,
        row: usize,
    },
    Update {
        table: usize,
        row: usize,
        value: usize,
    },
    // the trouble with inserts is that the row doesn't necessarily exist yet
    // so we need to optimistically allocate row numbers
    Insert {
        table: usize,
        row: usize,
        value: usize,
    },
    Put {
        table: usize,
        row: usize,
        value: usize,
    },
    Delete {
        table: usize,
        row: usize,
    },

    /// branch
    Cond(Cond),
    /// Phi-function
    Phi(Phi),
    /// Unconditional jump
    Jump(usize),
    /// Raw value
    Const(usize),
    /// Read argument to function
    Arg(usize),
    /// Return from function
    Return(usize),

    /// Abort and roll back the transaction.
    Abort,
    // Finish successfully
    Commit(usize),
}
/// Assumptions:
///     1. Program is typed correctly.
#[derive(Debug)]
struct SSAProgram {
    ops: Vec<ResolvedOp>,
    values: Vec<Option<Object>>,
    const_values: Vec<ObjectInner>,
}

struct UserStackFrame {
    args: Vec<Object>,
    ssa_begin: usize,
}

struct ExecutionState {
    pc: usize,
    user_call_stack: Vec<UserStackFrame>,
}
impl ExecutionState {
    pub fn new(pc: usize) -> Self {
        Self {
            pc,
            user_call_stack: vec![],
        }
    }
    pub fn step(&mut self) {
        self.pc += 1;
    }
    pub fn jump(&mut self, addr: usize) {
        self.pc = addr;
    }
}

enum ExecutionError {
    Uninitialized(usize),
    TypeError(usize, String),
    OutOfRange(usize),
    ArgumentOutOfRange(usize),
    NotInFunction,
}

impl SSAProgram {
    fn read_object(&self, idx: usize) -> Result<&ObjectInner, ExecutionError> {
        self.values
            .get(idx)
            .map(Option::as_ref)
            .ok_or(ExecutionError::OutOfRange(idx))
            .and_then(|obj| obj.ok_or(ExecutionError::Uninitialized(idx)))
            .and_then(|obj| match obj {
                Object::Direct(o) => Ok(o),
                Object::Indirect(i) => self.read_object(*i),
            })
    }
    fn take_object(&mut self, idx: usize) -> Result<ObjectInner, ExecutionError> {
        self.values
            .get_mut(idx)
            .ok_or(ExecutionError::OutOfRange(idx))?
            .take()
            .ok_or(ExecutionError::Uninitialized(idx))
            .and_then(|obj| match obj {
                Object::Direct(o) => Ok(o),
                Object::Indirect(i) => self.take_object(i),
            })
    }
    fn object<'s, 'o: 's>(&'s self, object: &'o Object) -> Result<&ObjectInner, ExecutionError> {
        match object {
            Object::Direct(o) => Ok(o),
            Object::Indirect(i) => self.read_object(*i),
        }
    }

    fn op_read(
        &mut self,
        table_ref: (usize, usize),
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        todo!()
    }
    fn op_update(
        &mut self,
        table_ref: (usize, usize),
        value: usize,
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        todo!()
    }
    fn op_insert(
        &mut self,
        table_ref: (usize, usize),
        value: usize,
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        todo!()
    }
    fn op_put(
        &mut self,
        table_ref: (usize, usize),
        value: usize,
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        todo!()
    }
    fn op_delete(
        &mut self,
        table_ref: (usize, usize),
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        todo!()
    }

    pub fn run(
        &mut self,
        begin: usize,
        write_cache: &mut WriteCache,
        storage: &Storage,
    ) -> Result<ObjectInner, ExecutionError> {
        let mut execution_state = ExecutionState::new(begin);
        loop {
            let op = &self.ops[execution_state.pc];
            // assert!(self.values[execution_state.pc].is_none());

            match op {
                ResolvedOp::SSA(op) => {
                    // self.values[execution_state.pc] =
                    //     Some(self.run_ssa(op, write_cache, storage)?);
                    // TODO: gather values
                    execution_state.step();
                }
                &ResolvedOp::Read { table, row } => {
                    self.values[execution_state.pc] = Some(Object::Direct(self.op_read(
                        (table, row),
                        write_cache,
                        storage,
                    )?));
                    execution_state.step();
                }
                &ResolvedOp::Update { table, row, value } => {
                    self.op_update((table, row), value, write_cache, storage)?;
                    execution_state.step();
                }
                &ResolvedOp::Insert { table, row, value } => {
                    self.op_insert((table, row), value, write_cache, storage)?;
                    execution_state.step();
                }
                &ResolvedOp::Put { table, row, value } => {
                    self.op_put((table, row), value, write_cache, storage)?;
                    execution_state.step();
                }
                &ResolvedOp::Delete { table, row } => {
                    self.op_delete((table, row), write_cache, storage)?;
                    execution_state.step();
                }
                ResolvedOp::Cond(cond) => match self.read_object(cond.cond)? {
                    ObjectInner::Bool(b) => execution_state.jump(if *b {
                        cond.then_branch
                    } else {
                        cond.else_branch
                    }),
                    t => {
                        let s = format!("expected type Bool got value {:?}", t);
                        return Err(ExecutionError::TypeError(cond.cond, s));
                    }
                },
                ResolvedOp::Phi(phi) => match self.read_object(phi.cond)? {
                    ObjectInner::Bool(b) => {
                        self.values[execution_state.pc] = Some(Object::Indirect(if *b {
                            phi.then_value
                        } else {
                            phi.else_value
                        }));
                        execution_state.step();
                    }
                    t => {
                        let s = format!("expected type Bool got value {:?}", t);
                        return Err(ExecutionError::TypeError(phi.cond, s));
                    }
                },
                ResolvedOp::Jump(addr) => {
                    execution_state.jump(*addr);
                }
                ResolvedOp::Const(v) => {
                    // TODO: make this not clone
                    self.values[execution_state.pc] =
                        Some(Object::Direct(self.const_values[*v].clone()));
                    execution_state.step();
                }
                ResolvedOp::Arg(idx) => {
                    self.values[execution_state.pc] = Some(Object::Direct(
                        self.object(
                            execution_state
                                .user_call_stack
                                .last()
                                .ok_or(ExecutionError::NotInFunction)?
                                .args
                                .get(*idx)
                                .ok_or(ExecutionError::ArgumentOutOfRange(*idx))?,
                        )?
                        .clone(),
                    ));
                }
                ResolvedOp::Return(value) => {
                    let _frame = execution_state
                        .user_call_stack
                        .pop()
                        .ok_or(ExecutionError::NotInFunction)?;
                    return self.take_object(*value);
                }
                ResolvedOp::Abort => {
                    todo!()
                }
                ResolvedOp::Commit(value) => {
                    todo!()
                }
            }
        }
    }
}
