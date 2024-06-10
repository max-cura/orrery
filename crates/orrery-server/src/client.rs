use crate::network::t;
use crate::raft::{NodeId, Request, TypeConfig};
use dashmap::DashMap;
use futures_util::stream::SplitSink;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use openraft::error::{
    ClientWriteError, ForwardToLeader, InitializeError, NetworkError, RaftError, RemoteError,
};
use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, LeaderId, RaftMetrics, TryAsRef};
use orrery_store::{ExecutionError, ExecutionResult, ExecutionResultFrame};
use orrery_wire::{Object, Op, TransactionRequest};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{client, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::warn;

#[derive(Copy, Clone, Serialize, Deserialize)]
struct Empty {}

pub struct Client {
    pub leader: Arc<Mutex<(NodeId, String)>>,
    pub inner: reqwest::Client,
}

pub struct RemoteConnection {
    leader: (NodeId, String),
    pub client_id: String,
    next_txn_id: usize,
    deferred: Arc<DashMap<usize, oneshot::Sender<ExecutionResultFrame>>>,
    exec_queue_push: UnboundedSender<Message>,
    abort_notify: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}
impl RemoteConnection {
    pub async fn new(leader: (NodeId, String), client_id: String) -> Self {
        let remote_url = format!("ws://{}/connect", leader.1);
        let (mut websocket, _resp) = connect_async(remote_url)
            .await
            .expect("failed to connect to server (websocket)");
        let deferred: Arc<DashMap<usize, oneshot::Sender<ExecutionResultFrame>>> =
            Arc::new(DashMap::new());
        let deferred_arc = Arc::clone(&deferred);
        let client_id_clone = client_id.clone();
        let (exec_queue_push, mut exec_queue_pop) = tokio::sync::mpsc::unbounded_channel();
        let (abort_notify, mut abort_wait) = oneshot::channel();

        let join_handle = tokio::spawn(async move {
            let mut ping_timeout = tokio::time::sleep(Duration::from_millis(20));
            tokio::pin!(ping_timeout);
            loop {
                tokio::select! {
                    msg_result = websocket.next() => {
                        let Some(msg) = msg_result else { break };
                        let msg = match msg {
                            Ok(msg) => msg,
                            Err(err) => {
                                tracing::error!(
                                    "Error receiving message at {} with {} outstanding: {err}",
                                    client_id_clone,
                                    deferred_arc.len()
                                );
                                break;
                            }
                        };
                        match msg {
                            Message::Text(s) => {
                                let msg: ExecutionResultFrame = serde_json::from_str(&s)
                                    .expect("Failed to deserialize message from server");
                                if let Some(defer) = deferred_arc.remove(&msg.txn_id) {
                                    defer.1.send(msg).unwrap();
                                }
                            }
                            Message::Binary(_) => {
                                tracing::error!("CLIENT received BINARY message from SERVER")
                            }
                            Message::Ping(_) => {
                                tracing::error!("CLIENT received PING message from SERVER");
                            }
                            Message::Pong(_) => {
                                ping_timeout.as_mut().reset(Instant::now() + Duration::from_millis(20));
                            }
                            Message::Close(_) => {
                                tracing::error!("CLIENT received CLOSE from SERVER");
                                break
                            }
                            Message::Frame(_) => {}
                        }
                    }
                    _ = &mut ping_timeout => {
                        websocket.send(Message::Ping(vec![])).await.unwrap();
                    }
                    tx = exec_queue_pop.recv() => {
                        if let Some(tx) = tx {
                            if let Err(err) = websocket.send(tx).await {
                                tracing::error!("{client_id_clone}: Error sending message to WS: {err}");
                                break
                            }
                        } else {
                            tracing::info!("execution queue closed");
                        }
                    }
                    _ = &mut abort_wait => {
                        tracing::info!("receiving abort notification");
                        break
                    }
                }
            }
            if let Err(err) = websocket
                .close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: Cow::from("normal"),
                }))
                .await
            {
                tracing::info!("{client_id_clone}: failed to close websocket: {err}");
            }
        });
        Self {
            leader,
            client_id,
            next_txn_id: 0,
            deferred,
            exec_queue_push,
            abort_notify: Some(abort_notify),
            join_handle: Some(join_handle),
        }
    }
}

// pub struct RemoteConn {
//     leader: (NodeId, String),
//     pub client_id: String,
//     next_txn_id: usize,
//     sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
//     deferred: Arc<DashMap<usize, oneshot::Sender<ExecutionResultFrame>>>,
//     abort_tx: Option<oneshot::Sender<()>>,
//     recv_hdl: Option<JoinHandle<()>>,
// }
impl RemoteConnection {
    // pub async fn new(leader: (NodeId, String), client_id: String) -> Self {
    //     let remote_url = format!("ws://{}/connect", leader.1);
    //     let (conn, resp) = connect_async(remote_url)
    //         .await
    //         .expect("failed to connect to server (websocket)");
    //
    //     let (mut sender, mut receiver) = conn.split();
    //
    //     let deferred: Arc<DashMap<usize, oneshot::Sender<ExecutionResultFrame>>> =
    //         Arc::new(DashMap::new());
    //     let deferred_arc = Arc::clone(&deferred);
    //
    //     let client_id_clone = client_id.clone();
    //
    //     let (abort_tx, mut abort_rx) = oneshot::channel::<()>();
    //
    //     let handle = tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 Some(msg) = receiver.next() => match msg {
    //                     Ok(msg) => match msg {
    //                         Message::Text(s) => {
    //                             let msg: ExecutionResultFrame = serde_json::from_str(&s)
    //                                 .expect("Failed to deserialize message from server");
    //                             if let Some(defer) = deferred.remove(&msg.txn_id) {
    //                                 defer.1.send(msg).unwrap();
    //                             }
    //                         }
    //                         Message::Binary(_) => {
    //                             panic!("Received binary response from server!");
    //                         }
    //                         Message::Close(_) => {
    //                             println!("Connection closed by server, exiting.");
    //                             return;
    //                         }
    //                         Message::Ping(p) => {
    //                             tracing::info!("CLIENT: PING")
    //                         }
    //                         Message::Pong(_) => {
    //                             tracing::info!("CLIENT: PONG")
    //                         }
    //                         Message::Frame(_) => {}
    //                     },
    //                     Err(err) => {
    //                         tracing::error!(
    //                             "Error receiving message at {} with {} outstanding: {err}",
    //                             client_id_clone,
    //                             deferred.len()
    //                         );
    //                         break;
    //                     }
    //                 },
    //                 _ = &mut abort_rx => { break }
    //             }
    //         }
    //         tracing::info!("CLIENT RECEIVER FINISHED");
    //     });
    //
    //     Self {
    //         leader,
    //         client_id,
    //         sender,
    //         next_txn_id: 0,
    //         deferred: deferred_arc,
    //         abort_tx: Some(abort_tx),
    //         recv_hdl: Some(handle),
    //     }
    // }
    pub async fn execute(
        &mut self,
        ops: Vec<Op>,
        const_buf: Vec<Object>,
    ) -> Result<oneshot::Receiver<ExecutionResultFrame>, SendError<Message>> {
        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;
        let txn_req = TransactionRequest {
            ir: ops,
            const_buf,
            client_id: self.client_id.clone(),
            tx_no: txn_id,
        };

        let (tx, rx) = oneshot::channel();
        self.deferred.insert(txn_id, tx);
        if let Err(err) = self.exec_queue_push.send(Message::Text(
            serde_json::to_string(&Request {
                transaction: txn_req,
            })
            .unwrap(),
        )) {
            tracing::error!(
                "Error sending transaction no {txn_id} for execution for {}: {err}",
                self.client_id
            );
            return Err(err);
        }

        Ok(rx)
    }

    pub async fn finish(mut self) {
        tracing::info!("Dropping RemoteConn {}", self.client_id.clone());
        self.abort_notify.take().unwrap().send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap();
        tracing::info!("Dropping RemoteConn {}... done", self.client_id.clone());
    }
}

pub type ExecuteAPIResult =
    Result<Result<Vec<u8>, ExecutionError>, t::RPCError<ClientWriteError<NodeId, BasicNode>>>;

impl Client {
    pub fn new(leader_id: NodeId, leader_addr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: reqwest::Client::new(),
        }
    }

    pub async fn connect(&self, client_id: String) -> RemoteConnection {
        let leader = { self.leader.lock().unwrap().clone() };
        let rc = RemoteConnection::new(leader, client_id).await;
        rc
    }

    // pub async fn execute(&self, transaction_request: TransactionRequest) -> ExecuteAPIResult {
    //     self.send_rpc_to_leader(
    //         "execute",
    //         Some(&Request {
    //             transaction: transaction_request,
    //         }),
    //     )
    //     .await
    // }

    pub async fn init(&self) -> Result<(), t::RPCError<InitializeError<NodeId, BasicNode>>> {
        self.try_send_rpc_to_leader("init", Some(&Empty {})).await
    }
    pub async fn add_learner(
        &self,
        node: (NodeId, String),
    ) -> Result<ClientWriteResponse<TypeConfig>, t::RPCError<ClientWriteError<NodeId, BasicNode>>>
    {
        self.send_rpc_to_leader("add-learner", Some(&node)).await
    }
    pub async fn change_membership(
        &self,
        set: &BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse<TypeConfig>, t::RPCError<ClientWriteError<NodeId, BasicNode>>>
    {
        self.send_rpc_to_leader("change-membership", Some(set))
            .await
    }
    pub async fn metrics(&self) -> Result<RaftMetrics<NodeId, BasicNode>, t::RPCError> {
        self.try_send_rpc_to_leader("metrics", None::<&()>).await
    }

    async fn try_send_rpc_to_leader<RQ, RS, E>(
        &self,
        uri: &str,
        req: Option<&RQ>,
    ) -> Result<RS, t::RPCError<E>>
    where
        RQ: Serialize + 'static,
        RS: Serialize + DeserializeOwned,
        E: Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let leader = self.leader.lock().unwrap();
            (leader.0, format!("http://{}/{}", leader.1, uri))
        };
        tracing::info!("sending request to {url}");

        let f = match req {
            Some(req_body) => self.inner.post(url).json(req_body),
            None => self.inner.get(url),
        }
        .send();
        let response = match tokio::time::timeout(Duration::from_millis(10000), f).await {
            Ok(result) => result.map_err(|e| t::RPCError::Network(NetworkError::new(&e))),
            Err(timeout_err) => Err(t::RPCError::Network(NetworkError::new(&timeout_err))),
        }?;
        let t = response
            .text()
            .await
            .map_err(|e| t::RPCError::Network(NetworkError::new(&e)))?;
        tracing::info!("response: {t:?}");
        let response: Result<RS, RaftError<NodeId, E>> = serde_json::from_str(&t).unwrap();
        // let response: Result<RS, RaftError<NodeId, E>> = response
        //     .json()
        //     .await
        //     .map_err(|e| t::RPCError::Network(NetworkError::new(&e)))?;
        response.map_err(|e| t::RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }

    async fn send_rpc_to_leader<RQ, RS, E>(
        &self,
        uri: &str,
        req: Option<&RQ>,
    ) -> Result<RS, t::RPCError<E>>
    where
        RQ: Serialize + 'static,
        RS: Serialize + DeserializeOwned,
        E: Error
            + Serialize
            + DeserializeOwned
            + Clone
            + TryAsRef<ForwardToLeader<NodeId, BasicNode>>,
    {
        const MAX_RETRIES: usize = 3;

        let mut remaining_retry_count = MAX_RETRIES;

        loop {
            let res: Result<RS, t::RPCError<E>> = self.try_send_rpc_to_leader(uri, req).await;
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if let Some(ForwardToLeader {
                        leader_id: Some(leader_id),
                        leader_node: Some(leader_node),
                    }) = e.forward_to_leader()
                    {
                        {
                            let mut g = self.leader.lock().unwrap();
                            *g = (*leader_id, leader_node.addr.clone());
                        }

                        remaining_retry_count -= 1;
                        if remaining_retry_count > 0 {
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }
}
