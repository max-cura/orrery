// can't do a simple enum-as-Ty, since such a type is infinitely recursive

use std::mem::ManuallyDrop;
use orrery_wire::{Cond, Op, RowLocator, SSA};
use crate::TableSet;

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
struct DatabaseContext {
    // side_effects: Vec<SideEffect>,
    access_cache: Vec<(usize, RtValue)>,
    // access index -> (table, index)
    access_map: Vec<(usize, (usize, usize))>,
}

pub enum ResolvedOp {
    SSA(SSA),
    Read {
        table: usize,
        row: usize,
    },
    Write {
        table: usize,
        row: usize,
        value: usize,
    },
    Cond(Cond),
    Jump(usize),
    InvalidRead(RowLocator),
    InvalidWrite(RowLocator, usize),
}
impl ResolvedOp {
    pub fn try_from_wire(wire: Op, table_set: &TableSet) -> Self {
        match wire {
            Op::SSA(x) => Self::SSA(x),
            Op::Read(rl) => {
                let Some((table, row)) = table_set.get(&rl.table)
                    .and_then(|table| table.index_to_backing_row(&rl.row_key)
                        .map(|row| (table.id(), row))) else {
                    return Self::InvalidRead(rl);
                };
                Self::Read { table, row }
            }
            Op::Write(rl, v) => {
                let Some((table, row)) = table_set.get(&rl.table)
                    .and_then(|table| table.index_to_backing_row(&rl.row_key)
                        .map(|row| (table.id(), row))) else {
                    return Self::InvalidWrite(rl, v);
                };
                Self::Write { table, row, value: v }

            }
            Op::Cond(c) => Self::Cond(c),
            Op::Jump(j) => Self::Jump(j),
        }
    }
}

struct SSAContext {
    // all user-defined "functions" are inlined
    items: Vec<ResolvedOp>,
    values: Vec<RtValue>,
}

// todo: in-place operations
// todo: JIT'ing
// ???
// much possibility

struct ExecutableTransaction {
    ssa_ctx: SSAContext,
    db_ctx: DatabaseContext,
}

struct ExecutionOutput {
    rt_value: RtValue
}

enum ExecutionError {
    Abort,
}