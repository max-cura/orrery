use crate::client::ExecuteAPIResult;
use crate::network::t;
use crate::raft::state::StateMachine;
use crate::raft::{NodeId, Request, TypeConfig};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Error, Json, Router};
use axum_extra::TypedHeader;
use futures_util::{SinkExt, StreamExt};
use openraft::error::{ClientWriteError, Infallible, RaftError};
use openraft::raft::{
    AppendEntriesRequest, ClientWriteResponse, InstallSnapshotRequest, VoteRequest,
};
use openraft::{BasicNode, Raft, RaftMetrics, RaftTypeConfig};
use orrery_store::{ExecutionError, ExecutionResult, ExecutionResultFrame, TransactionFinished};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
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
        .route("/connect", get(connect))
        // .route("/execute", post(execute))
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
async fn connect(
    app: State<App>,
    ws: WebSocketUpgrade,
    _user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    // let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
    //     user_agent.to_string()
    // } else {
    //     String::from("Unknown browser")
    // };
    if let Some(nid) = app.raft_instance.current_leader().await {
        if nid == app.node_id {
            tracing::info!("client at {addr} connecting to leader")
        } else {
            tracing::error!("client at {addr} connecting to non-leader node")
        }
    } else {
        tracing::error!("client at {addr} connnected when no leader is present")
    }
    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| handle_socket(app, socket, addr))
}

async fn handle_socket(app: State<App>, mut socket: WebSocket, who: SocketAddr) {
    let mut open_transaction_task_handles = vec![];

    let (outgoing_enqueue, mut outgoing_pop) = tokio::sync::mpsc::unbounded_channel();
    let outgoing_enqueue = Arc::new(outgoing_enqueue);

    loop {
        tokio::select! {
            outgoing = outgoing_pop.recv() => {
                if let Some(msg) = outgoing {
                    let response_string =
                        match serde_json::to_string(&msg) {
                            Ok(s) => s,
                            Err(err) => {
                                tracing::error!(
                                "Failed to serialize error result from transaction: {err}"
                            );
                                continue;
                            }
                        };
                    if let Err(err) = socket.send(Message::Text(response_string)).await {
                        tracing::error!("Error sending message to {who}: {err}");
                    }
                }
            }
            msg = socket.recv() => {
                let Some(msg) = msg else {
                    tracing::error!("Websocket from {who} has closed");
                    break;
                };
                let msg = match msg {
                    Ok(m) => m,
                    Err(err) => {
                        tracing::error!("Failed to read from WS {who}: {err}");
                        break;
                    }
                };
                match msg {
                    Message::Text(s) => {
                        let req = match serde_json::from_str::<Request>(&s) {
                            Ok(req) => req,
                            Err(err) => {
                                tracing::error!("Failed to deserialize request: {err}");
                                continue;
                            }
                        };
                        let client_tx_no = req.transaction.tx_no;
                        let client = req.transaction.client_id.clone();
                        tracing::info!("received message from {client}");
                        match app.raft_instance.client_write(req).await.map(|r| {
                            r.data
                                .result
                                .expect("response to a transaction should be Some")
                        }) {
                            Ok(enqueue_result) => match enqueue_result {
                                Ok(transaction_finished_future) => {
                                    let outgoing_enqueue = Arc::clone(&outgoing_enqueue);
                                    open_transaction_task_handles.push(tokio::spawn(async move {
                                        let transaction_result = transaction_finished_future.await;
                                        tracing::info!("Transaction finished with result: {transaction_result:?}");
                                        let send_result = outgoing_enqueue.send(transaction_result);
                                        if let Err(err) = send_result {
                                            tracing::error!(
                                                "Failed to send result to {who} aka {client}: {err}"
                                            );
                                        }
                                    }));
                                }
                                Err(err) => {
                                    let response_string =
                                        match serde_json::to_string(&ExecutionResultFrame {
                                            inner: Err(err),
                                            txn_id: client_tx_no,
                                        }) {
                                            Ok(s) => s,
                                            Err(err) => {
                                                tracing::error!(
                                                "Failed to serialize error result from transaction: {err}"
                                            );
                                                continue;
                                            }
                                        };
                                    if let Err(err) = socket.send(Message::Text(response_string)).await {
                                        tracing::error!("Error sending message to {who}: {err}");
                                    }
                                }
                            },
                            Err(err) => {
                                tracing::error!("Raft failure while attempting to client_write() message from {client}@{who}: {err}");
                                break;
                            }
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(_) => {
                        socket.send(Message::Pong(vec![])).await.unwrap();
                    }
                    Message::Pong(_) => {
                        tracing::error!("SERVER received PONG from CLIENT");
                        break
                    }
                    Message::Close(_) => {
                        tracing::info!("SERVER Received CLOSE from CLIENT");
                        break
                    }
                }
            }
        }
    }

    tracing::info!("RECEIVER socket handler exited, waiting for open transactions...");

    for handle in open_transaction_task_handles {
        handle.await.unwrap();
    }
    tracing::info!("RECEIVER Finished handling socket")

    // let (mut sink, mut stream) = socket.split();
    //
    // let mut sender = tokio::spawn(async move {
    //     while let Some(resp) = rx.recv().await {
    //         let res_str = match serde_json::to_string(&resp) {
    //             Ok(res_str) => res_str,
    //             Err(err) => {
    //                 tracing::error!("Failed to serialize response: {err}");
    //                 break;
    //             }
    //         };
    //         tracing::info!("Sending message to {who}: {resp:?}");
    //         if let Err(err) = sink.send(Message::Text(res_str)).await {
    //             tracing::error!("Error sending message to {who}: {err}");
    //             break;
    //         }
    //     }
    //     tracing::info!("SENDER finished");
    // });
    //
    // let mut receiver = tokio::spawn(async move {
    //     let mut handles = vec![];
    //     while let Some(msg) = stream.next().await {
    //         // tracing::info!("Received message: {msg:?}");
    //         match msg {
    //             Ok(msg) => match msg {
    //                 Message::Text(s) => {
    //                     let req = match serde_json::from_str::<Request>(&s) {
    //                         Ok(req) => req,
    //                         Err(err) => {
    //                             tracing::error!("Failed to deserialize request: {err}");
    //                             continue;
    //                         }
    //                     };
    //                     let tx_no = req.transaction.tx_no;
    //                     let client = req.transaction.client_id.clone();
    //                     tracing::info!("received message from {client}");
    //                     match app.raft_instance.client_write(req).await.map(|r| {
    //                         r.data
    //                             .result
    //                             .expect("response to a transaction should be Some")
    //                     }) {
    //                         Ok(tx_res) => match tx_res {
    //                             Ok(tf) => {
    //                                 let tx2 = Arc::clone(&tx_arc);
    //                                 tracing::info!(
    //                                     "Waiting on TransactionFinished (client {who} aka {client})..."
    //                                 );
    //                                 let (abort_tx, abort_rx) =
    //                                     tokio::sync::oneshot::channel::<()>();
    //                                 // handles.push(abort_tx);
    //                                 handles.push(tokio::spawn(async move {
    //                                     let res = tf.await;
    //                                     // tokio::select!(
    //                                     //     res = tf => {
    //                                             tracing::info!("Transaction finished with result: {res:?}");
    //                                             let r = tx2.send(res);
    //                                             if let Err(err) = r {
    //                                                 tracing::error!("Failed to send result to {who} aka {client}: {err}")
    //                                             }
    //                                     //     }
    //                                     //     _ = abort_rx => {
    //                                     //         // tracing::info!("")
    //                                     //     }
    //                                     // )
    //                                 }));
    //                             }
    //                             Err(e) => tx_arc
    //                                 .send(ExecutionResultFrame {
    //                                     inner: Err(e),
    //                                     txn_id: tx_no,
    //                                 })
    //                                 .unwrap(),
    //                         },
    //                         Err(raft_err) => {
    //                             tracing::error!("Raft failure while attempting to submit transaction: {raft_err}");
    //                             continue;
    //                         }
    //                     }
    //                 }
    //                 Message::Binary(b) => {
    //                     panic!("Received binary message from client")
    //                 }
    //                 Message::Ping(p) => {
    //                     // tracing::info!("SERVER: PING")
    //                 }
    //                 Message::Pong(_) => {
    //                     // tracing::info!("SERVER: PONG")
    //                 }
    //                 Message::Close(_) => {
    //                     tracing::info!("closing websocket from {who}");
    //                     break;
    //                 }
    //             },
    //             Err(err) => {
    //                 tracing::error!("failed to receive message from {who}: {err}");
    //                 break;
    //             }
    //         }
    //     }
    //     tracing::info!("RECEIVER aborting outstanding handles");
    //     for handle in handles {
    //         handle.await.unwrap();
    //     }
    //     tracing::info!("RECEIVER finished");
    // });
    //
    // tokio::join!(receiver, sender);
}

// #[tracing::instrument]
// async fn execute(
//     app: State<App>,
//     Json(req): Json<Request>,
// ) -> Json<
//     Result<Result<Vec<u8>, ExecutionError>, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
// > {
//     // tracing::warn!("API: /execute");
//     Json(
//         match app.raft_instance.client_write(req).await.map(|r| {
//             r.data
//                 .result
//                 .expect("response to a transaction should be Some")
//         }) {
//             Ok(o) => match o {
//                 Ok(f) => Ok(f.await.map(|v| v.returned_values)),
//                 Err(e) => Ok(Err(e)),
//             },
//             Err(e) => Err(e),
//         },
//     )
// }

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
