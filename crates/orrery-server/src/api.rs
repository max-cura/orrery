use crate::raft::state::StateMachine;
use crate::raft::store::LogStorage;
use crate::raft::{NodeId, Request, TypeConfig};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::Infallible;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, Raft, RaftMetrics, RaftTypeConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

pub struct AppInner {
    pub node_id: NodeId,
    pub node_addr: String,
    pub raft_instance: Raft<TypeConfig>,
    pub raft_log_store: LogStorage<TypeConfig>,
    pub raft_state_machine: Arc<StateMachine>,
    pub raft_config: Arc<openraft::Config>,
}

// Axum uses State<T: Clone>, so this is a clone-able wrapper around AppInner
pub struct App(Arc<AppInner>);
impl Deref for App {
    type Target = AppInner;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}
impl Clone for App {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

pub fn build_router(app: AppInner) -> Router {
    Router::new()
        .route("/execute", post(execute))
        .route("/add-learner", post(add_learner))
        .route("/change-membership", post(change_membership))
        .route("/init", post(init))
        .route("/metrics", get(metrics))
        .route("/raft-vote", post(vote))
        .route("/raft-append", post(append))
        .route("/raft-snapshot", post(snapshot))
        .with_state(App(Arc::new(app)))
}

async fn execute(app: State<App>, Json(req): Json<Request>) -> impl IntoResponse {
    let r = app
        .raft_instance
        .client_write(req)
        .await
        .map_err(|e| e.to_string())?;
    let d = r
        .data
        .result
        .expect("response to a transaction should be Some")
        .map_err(|e| e.to_string())?;
    match d.await {
        Ok(es) => Ok(es.returned_values),
        Err(err) => Err(err.to_string()),
    }
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
async fn add_learner(app: State<App>, req: Json<(NodeId, String)>) -> impl IntoResponse {
    let node_id = req.0 .0;
    let node = BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = app.raft_instance.add_learner(node_id, node, true).await;
    Json(res)
}

/// Changes specified learners to members, or remove members.
async fn change_membership(app: State<App>, req: Json<BTreeSet<NodeId>>) -> impl IntoResponse {
    let res = app.raft_instance.change_membership(req.0, false).await;
    Json(res)
}

/// Initialize a single-node cluster.
async fn init(app: State<App>) -> impl IntoResponse {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.node_id,
        BasicNode {
            addr: app.node_addr.clone(),
        },
    );
    let res = app.raft_instance.initialize(nodes).await;
    Json(res)
}

/// Get the latest metrics of the cluster
async fn metrics(app: State<App>) -> impl IntoResponse {
    let metrics = app.raft_instance.metrics().borrow().clone();

    let res: Result<
        RaftMetrics<<TypeConfig as RaftTypeConfig>::NodeId, <TypeConfig as RaftTypeConfig>::Node>,
        Infallible,
    > = Ok(metrics);
    Json(res)
}

async fn vote(app: State<App>, req: Json<VoteRequest<NodeId>>) -> impl IntoResponse {
    let res = app.raft_instance.vote(req.0).await;
    Json(res)
}

async fn append(app: State<App>, req: Json<AppendEntriesRequest<TypeConfig>>) -> impl IntoResponse {
    let res = app.raft_instance.append_entries(req.0).await;
    Json(res)
}

async fn snapshot(
    app: State<App>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    let res = app.raft_instance.install_snapshot(req.0).await;
    Json(res)
}
