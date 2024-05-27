// can't do a simple enum-as-Ty, since such a type is infinitely recursive

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

enum SideEffect {
    MayWrite{ access_id: usize },
    MayRead{ access_id: usize },
    /* Abort */
}
struct DatabaseContext {
    side_effects: Vec<SideEffect>,
    access_cache: Vec<(usize, RtValue)>,
    access_map: Vec<(usize, (usize, usize))>,
}

struct SSA {
    inputs: Vec<usize>,
    operation: usize,
}
struct SSAContext {
    // all user-defined "functions" are inlined
    items: Vec<SSA>,
    values: Vec<RtValue>,
}

// todo: in-place operations
// todo: JIT'ing
// ???
// much possibility

struct TransactionExecution {
    ssa_ctx: SSAContext,
    db_ctx: DatabaseContext,
}

struct ExecutionOutput {
    rt_value: RtValue
}

enum ExecutionError {
    Abort,
}

impl TransactionExecution {
    pub fn execute(&mut self) -> Result<ExecutionOutput, ExecutionError> {
        unimplemented!()
    }
}