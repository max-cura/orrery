// pub mod proto {
//     tonic::include_proto!("orrery_wire");
// }
// pub mod raft;

use dashmap::DashSet;
use orrery_store::PhaseController;

pub mod api;
pub mod raft;

/// The set of information that is necessary to upkeep the replicated system.
pub struct State {
    outstanding_transactions: DashSet<usize>,
    phase_controller: PhaseController,
}

fn main() {
    //
}
