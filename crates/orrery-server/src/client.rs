use crate::network::t;
use crate::raft::{NodeId, Request, TypeConfig};
use openraft::error::{
    ClientWriteError, ForwardToLeader, InitializeError, NetworkError, RaftError, RemoteError,
};
use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, RaftMetrics, TryAsRef};
use orrery_store::ExecutionError;
use orrery_wire::TransactionRequest;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Copy, Clone, Serialize, Deserialize)]
struct Empty {}

pub struct Client {
    pub leader: Arc<Mutex<(NodeId, String)>>,
    pub inner: reqwest::Client,
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

    pub async fn execute(&self, transaction_request: TransactionRequest) -> ExecuteAPIResult {
        self.send_rpc_to_leader(
            "execute",
            Some(&Request {
                transaction: transaction_request,
            }),
        )
        .await
    }

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
