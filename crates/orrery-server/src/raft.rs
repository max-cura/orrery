mod store;
mod state;

use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeBounds;
use openraft::{OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig};
use openraft::storage::{RaftLogStorage, RaftStateMachine};

pub struct Request {

}
pub struct Response {

}

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
    // TODO: figure out snapshotting
        // SnapshotData = (),
);


