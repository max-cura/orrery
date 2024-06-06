union VmObject {
    u64: u64,
    i64: i64,
    bool: bool,

    // complex objects exist on the VM heap
    str: *mut u8,
    bytes: (*mut u8, usize),
    list: *mut u8,
    tuple: *mut u8,
    option: *mut u8,
}

// Function
//  fixed set of typed locals x0..xn
//  fixed set of arguments y0..yn
//  heap is global
// control is provided by:
//  block (instructions...) end
//  if (instructions) else (instructions) end
//  loop (instructions) end
//  br (index)
//  br_if (index)
//  call
//  return

#[repr(u8)]
pub enum Opcode {
    // 0x00 imm16
    Abort = 0x00,
    Commit = 0x01,
    End = 0x02,
    // 0x03 (ins) 0x02
    Block = 0x03,
    // 0x04 (then) 0x02
    // 0x04 (then) 0x05 (else) 0x02
    If = 0x04,
    Else = 0x05,
    // 0x06 (body) 0x02
    Loop = 0x06,
    // 0x07 label8
    Br = 0x07,
    // 0x08 label8
    BrIf = 0x08,
    // 0x09 func16
    Call = 0x09,
    CallIndirect = 0x0a,
    Return = 0x0c,
    Nop = 0x0d,

    Drop = 0x0e,

    // 0x10 table32 row32
    DbRead = 0x10,
    // 0x11 table32 row32
    DbUpdate = 0x11,
    // 0x12 table32 row32
    DbInsert = 0x12,
    // 0x13 table32 row32
    DbPut = 0x13,
    // 0x14 table32 row32
    DbDelete = 0x14,

    // 0x20 var16
    LocalGet = 0x20,
    // 0x21 var16
    LocalSet = 0x21,
    // 0x22 var16
    LocalTee = 0x22,
    // 0x23 arg8
    ArgGet = 0x23,

    U64Eqz = 0x30,
    U64Eq = 0x31,
    U64Nez = 0x32,
    U64Ne = 0x33,
    U64Lt = 0x34,
    U64Le = 0x35,
    U64Gt = 0x36,
    U64Ge = 0x37,

    I64Eqz = 0x38,
    I64Eq = 0x39,
    I64Nez = 0x3a,
    I64Ne = 0x3b,
    I64Lt = 0x3c,
    I64Le = 0x3d,
    I64Gt = 0x3e,
    I64Ge = 0x3f,

    U64Add = 0x40,
    U64Sub = 0x41,
    U64Mul = 0x42,
    U64Div = 0x43,
    U64Rem = 0x44,
    U64And = 0x45,
    U64Or = 0x46,
    U64Xor = 0x47,
    U64Shl = 0x48,
    U64Shr = 0x49,
    U64Ror = 0x4a,
    U64Not = 0x4b,
    U64Ext = 0x4c,
    U64ToI64 = 0x4d,
    U64Const = 0x4e,

    I64Add = 0x50,
    I64Sub = 0x51,
    I64Mul = 0x52,
    I64Div = 0x53,
    I64Rem = 0x54,
    I64Neg = 0x55,
    I64Ext = 0x56,
    I64ToU64 = 0x57,
    I64Const = 0x58,

    BoolAnd = 0x60,
    BoolOr = 0x61,
    BoolNot = 0x62,
    BoolEq = 0x63,
    BoolNe = 0x64,
    BoolConst = 0x65,

    // StrLen = 0x70,
    // StrFmt = 0x71,
    // StrPush = 0x72,
    // StrContains = 0x73,
    // StrFind,
    // StrRevFind,
    // StrSubstr,
    // StrEq = 0x77,
    // StrNe = 0x78,
    // StrExt = 0x79,
    // StrConst = 0x7a,
}