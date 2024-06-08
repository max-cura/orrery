use crate::client::ExecuteAPIResult;
use crate::network::t;
use crate::raft::state::StateMachine;
use crate::raft::{NodeId, Request, TypeConfig};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::{ClientWriteError, Infallible, RaftError};
use openraft::raft::{
    AppendEntriesRequest, ClientWriteResponse, InstallSnapshotRequest, VoteRequest,
};
use openraft::{BasicNode, Raft, RaftMetrics, RaftTypeConfig};
use orrery_store::{ExecutionError, TransactionFinished};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

pub struct AppInner {
    pub node_id: NodeId,
    pub node_addr: String,
    pub raft_instance: Raft<TypeConfig>,
    // pub raft_log_store: LogStorage<TypeConfig>,
    pub raft_state_machine: Arc<StateMachine>,
    // pub raft_config: Arc<openraft::Config>,
}

impl Debug for AppInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppInner")
            .field("node_id", &self.node_id)
            .field("node_addr", &self.node_addr)
            .field("raft_instance", &"<no Debug impl>")
            .field("raft_state_machine", &"<no Debug impl>")
            .finish()
    }
}

// Axum uses State<T: Clone>, so this is a clone-able wrapper around AppInner
#[derive(Debug)]
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
        .route("/", get(|| async { "Hello, world" }))
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

#[tracing::instrument]
async fn execute(
    app: State<App>,
    Json(req): Json<Request>,
) -> Json<
    Result<Result<Vec<u8>, ExecutionError>, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
> {
    tracing::warn!("API: /execute");
    Json(
        match app.raft_instance.client_write(req).await.map(|r| {
            r.data
                .result
                .expect("response to a transaction should be Some")
        }) {
            Ok(o) => match o {
                Ok(f) => Ok(f.await.map(|v| v.returned_values)),
                Err(e) => Ok(Err(e)),
            },
            Err(e) => Err(e),
        },
    )
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[tracing::instrument]
async fn add_learner(
    app: State<App>,
    req: Json<(NodeId, String)>,
) -> Json<
    Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
> {
    tracing::warn!("API: /add-learner");
    let node_id = req.0 .0;
    let node = BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = app.raft_instance.add_learner(node_id, node, true).await;
    Json(res)
}

/// Changes specified learners to members, or remove members.
#[tracing::instrument]
async fn change_membership(app: State<App>, req: Json<BTreeSet<NodeId>>) -> impl IntoResponse {
    tracing::warn!("API: /change-membership");
    let res = app.raft_instance.change_membership(req.0, false).await;
    Json(res)
}

/// Initialize a single-node cluster.
#[tracing::instrument]
async fn init(app: State<App>) -> impl IntoResponse {
    tracing::warn!("API: /init");
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
#[tracing::instrument]
async fn metrics(app: State<App>) -> impl IntoResponse {
    tracing::warn!("API: /metrics");
    let metrics = app.raft_instance.metrics().borrow().clone();

    let res: Result<
        RaftMetrics<<TypeConfig as RaftTypeConfig>::NodeId, <TypeConfig as RaftTypeConfig>::Node>,
        Infallible,
    > = Ok(metrics);
    Json(res)
}

#[tracing::instrument]
async fn vote(app: State<App>, req: Json<VoteRequest<NodeId>>) -> impl IntoResponse {
    tracing::warn!("API: /vote");
    let res = app.raft_instance.vote(req.0).await;
    Json(res)
}

#[tracing::instrument]
async fn append(app: State<App>, req: Json<AppendEntriesRequest<TypeConfig>>) -> impl IntoResponse {
    tracing::debug!("API: /append");
    let res = app.raft_instance.append_entries(req.0).await;
    Json(res)
}

#[tracing::instrument]
async fn snapshot(
    app: State<App>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    tracing::warn!("API: /snapshot");
    let res = app.raft_instance.install_snapshot(req.0).await;
    Json(res)
}
