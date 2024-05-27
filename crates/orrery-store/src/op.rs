// can't do a simple enum-as-Ty, since such a type is infinitely recursive

use std::fmt::{Debug, Formatter};
use orrery_wire::{Cond, RowLocator, SSA};
use std::mem::ManuallyDrop;

union RtValue {
    boolean: bool,
    uint: u64,
    int: i64,
    float: f32,
    string: ManuallyDrop<String>,
    bytes: ManuallyDrop<Vec<u8>>,
    // used for tuple, list, option. fn don't have values at runtime (at the moment)
    ptr: *mut u8,
}
// enum SideEffect {
//     MayWrite{ access_id: usize },
//     MayRead{ access_id: usize },
//     /* Abort */
// }
pub struct DatabaseContext {
    // side_effects: Vec<SideEffect>,
    access_cache: Vec<(usize, RtValue)>,
    // access index -> (table, index)
    access_map: Vec<(usize, (usize, usize))>,
}
impl Debug for DatabaseContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

pub enum ResolvedOp {
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
    Insert {
        table: usize,
        row: usize,
        value: usize,
    },
    Cond(Cond),
    Jump(usize),
    Value(RtValue),
    InvalidRead(RowLocator),
    InvalidWrite(RowLocator, usize),
}

struct SSAContext {
    // all user-defined "functions" are inlined
    items: Vec<ResolvedOp>,
    values: Vec<RtValue>,
}
impl Debug for SSAContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

// todo: in-place operations
// todo: JIT'ing
// ???
// much possibility

#[derive(Debug)]
pub struct ResolvedTransaction {
    ssa_ctx: SSAContext,
}

struct ExecutionOutput {
    rt_value: RtValue,
}

enum ExecutionError {
    Abort,
}
