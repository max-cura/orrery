use crate::raft::{NodeId, TypeConfig};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;

mod t {
    use crate::raft::NodeId;
    use openraft::error::{Infallible, RaftError};
    use openraft::BasicNode;

    pub type RPCError<E = Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;
}

pub struct Network;

async fn send_rpc<RQ, RS, E>(
    target: NodeId,
    target_node: &BasicNode,
    uri: &str,
    req: RQ,
) -> Result<RS, RPCError<NodeId, BasicNode, E>>
where
    RQ: Serialize,
    E: Error + DeserializeOwned,
    RS: DeserializeOwned,
{
    let url = format!("http://{}/{uri}", target_node.addr);
    let client = reqwest::Client::new();
    let resp = client.post(url).json(&req).send().await.map_err(|e| {
        if e.is_connect() {
            RPCError::Unreachable(Unreachable::new(&e))
        } else {
            RPCError::Network(NetworkError::new(&e))
        }
    })?;

    let resp: Result<RS, E> = resp
        .json()
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
    resp.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, t::RPCError> {
        send_rpc(self.target, &self.target_node, "raft-append", req).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, t::RPCError<InstallSnapshotError>> {
        send_rpc(self.target, &self.target_node, "raft-snapshot", req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, t::RPCError> {
        send_rpc(self.target, &self.target_node, "raft-vote", req).await
    }
}
