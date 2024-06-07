// pub mod proto {
//     tonic::include_proto!("orrery_wire");
// }
// pub mod raft;

use dashmap::DashMap;
use orrery_store::PhaseController;
use orrery_wire::TransactionRequest;
use std::sync::Arc;

pub mod api;
mod client;
mod network;
pub mod raft;

pub struct State {
    outstanding_transactions: Arc<DashMap<usize, TransactionRequest>>,
    phase_controller: PhaseController,
}

fn main() {
    //
}
