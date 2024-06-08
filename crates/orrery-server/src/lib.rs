// pub mod proto {
//     tonic::include_proto!("orrery_wire");
// }
// pub mod raft;

use crate::api::{build_router, AppInner};
use crate::client::Client;
use crate::network::Network;
use crate::raft::state::StateMachine;
use crate::raft::store::LogStorage;
use crate::raft::{NodeId, TypeConfig};
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use openraft::storage::RaftLogStorage;
use openraft::Raft;
use orrery_store::{Config, FixedConfigThreshold, PartitionLimits, PhaseController, Storage};
use orrery_wire::TransactionRequest;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::log::{Level, LevelFilter};

pub mod api;
pub mod client;
pub mod network;
pub mod raft;

pub struct State {
    outstanding_transactions: Arc<DashMap<usize, TransactionRequest>>,
    // outstanding_transaction_by_client: Arc<DashMap<(String, usize), usize>>,
    phase_controller: PhaseController,
}

#[tracing::instrument]
pub async fn start_raft_node(
    node_id: NodeId,
    node_addr: String,
    bind_addr: String,
    worker_count: usize,
) {
    let orrery_config = Config {
        partition_limits: PartitionLimits {
            partition_max_write: 200,
            partition_max_access: 200,
        },
        fixed_config_threshold: FixedConfigThreshold {
            max_access_set_size: 3000,
            max_transaction_count: 5000,
            max_flush_interval: Duration::from_millis(15),
        },
        worker_count,
    };
    let raft_config = openraft::Config {
        cluster_name: "orrery".to_string(),
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        heartbeat_interval: 500,
        ..Default::default() // install_snapshot_timeout: 500,
                             // max_payload_entries: 0,
                             // replication_lag_threshold: 0,
                             // snapshot_policy: SnapshotPolicy::Never,
                             // snapshot_max_chunk_size: 0,
                             // max_in_snapshot_log_to_keep: 0,
                             // purge_batch_size: 0,
                             // enable_tick: false,
                             // enable_heartbeat: false,
                             // enable_elect: false,
    };
    let raft_config = Arc::new(raft_config.validate().unwrap());

    let log_store = LogStorage::<TypeConfig>::default();
    let storage = Storage::new_test();
    let state_machine = Arc::new(StateMachine::new(orrery_config, storage));
    let raft = Raft::new(
        node_id,
        raft_config,
        Network,
        log_store,
        state_machine.clone(),
    )
    .await
    .unwrap();

    let app = AppInner {
        node_id,
        node_addr,
        raft_instance: raft.clone(),
        // raft_log_store: log_store,
        raft_state_machine: state_machine,
    };

    tracing::info!("Starting Orrery node ({node_id})");

    let router = build_router(app);

    // run our app with hyper, listening globally on port 3000
    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    // axum::serve(listener, app).await.unwrap();
    // let router = Router::new().route("/", get(|| async { "Hello, World!" }));

    println!("listening at {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();

    tracing::info!("Orrery node ({node_id}) terminating");
}
