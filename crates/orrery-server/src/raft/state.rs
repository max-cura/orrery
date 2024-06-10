use crate::raft::{Response, TypeConfig};
use crate::State;
use dashmap::{DashMap, DashSet};
use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use orrery_store::{Config, ExecutionError, PhaseController, Storage, TransactionFinished};
use orrery_wire::TransactionRequest;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json_any_key::{any_key_map, any_key_vec};
use std::io::Cursor;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log;

pub struct StoredSnapshot {
    pub metadata:
        SnapshotMeta<<TypeConfig as RaftTypeConfig>::NodeId, <TypeConfig as RaftTypeConfig>::Node>,
    pub data: Vec<u8>,
}
struct Inner {
    state: State,
    last_membership: StoredMembership<
        <TypeConfig as RaftTypeConfig>::NodeId,
        <TypeConfig as RaftTypeConfig>::Node,
    >,
    last_applied_log: Option<LogId<<TypeConfig as RaftTypeConfig>::NodeId>>,
}
impl Inner {
    fn new(config: Config, storage: Storage) -> Self {
        Self {
            state: State {
                outstanding_transactions: Default::default(),
                phase_controller: PhaseController::new(config, storage),
            },
            last_membership: Default::default(),
            last_applied_log: None,
        }
    }
}
pub struct StateMachine {
    state_machine: RwLock<Inner>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}
impl StateMachine {
    pub fn new(config: Config, storage: Storage) -> Self {
        Self {
            state_machine: RwLock::new(Inner::new(config, storage)),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        }
    }
}

#[derive(Debug, Serialize)]
struct SnapshotFrame<'a> {
    storage: &'a Storage,
    #[serde(with = "any_key_vec")]
    outstanding_transactions: Vec<(usize, TransactionRequest)>,
}
#[derive(Debug, Deserialize)]
struct SnapshotFrameDeser {
    storage: Storage,
    #[serde(with = "any_key_map")]
    outstanding_transactions: DashMap<usize, TransactionRequest>,
}
impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<<TypeConfig as RaftTypeConfig>::NodeId>> {
        let sm = self.state_machine.read().await;
        let data = {
            let mut pcis = sm.state.phase_controller.inner.storage.lock();
            tracing::info!("storage locked: snapshot");
            while sm.state.phase_controller.inner.flag.load(Ordering::SeqCst) {
                sm.state.phase_controller.inner.cv.wait(&mut pcis);
            }
            let snapshot_frame = SnapshotFrame {
                storage: pcis.as_ref().unwrap(),
                outstanding_transactions: sm
                    .state
                    .outstanding_transactions
                    .iter()
                    .map(|x| {
                        let y = x.pair();
                        (*y.0, y.1.clone())
                    })
                    .collect::<Vec<_>>(),
            };
            let s = serde_json::to_vec(&snapshot_frame).map_err(|e| {
                tracing::error!("failed to build snapshot: {e}");
                StorageIOError::read_state_machine(&e)
            })?;
            tracing::info!("storage unlocked: snapshot");
            s
        };
        let last_applied_log_id = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(sm);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log_id {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let metadata = SnapshotMeta {
            last_log_id: last_applied_log_id,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            metadata: metadata.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta: metadata,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachine> {
    type SnapshotBuilder = Self;

    #[rustfmt::skip]
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<<TypeConfig as RaftTypeConfig>::NodeId>>, StoredMembership<<TypeConfig as RaftTypeConfig>::NodeId, <TypeConfig as RaftTypeConfig>::Node>, ), StorageError<<TypeConfig as RaftTypeConfig>::NodeId>> {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<
        Vec<<TypeConfig as RaftTypeConfig>::R>,
        StorageError<<TypeConfig as RaftTypeConfig>::NodeId>,
    >
    where
        I: IntoIterator<Item = <TypeConfig as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = vec![];
        let mut sm = self.state_machine.write().await;
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(Response { result: None });
                }
                EntryPayload::Normal(request) => {
                    // tracing::info!("applying transaction {:?}", request.transaction);
                    // NOTE add_transaction will block the network thread if interrupted by batch
                    // execution
                    let finished = match sm
                        .state
                        .phase_controller
                        .add_transaction(request.transaction.clone())
                    {
                        Ok((number, mut finished)) => {
                            // tracing::info!("successfully applied transaction no. {number}");
                            // TODO: deduplication
                            sm.state
                                .outstanding_transactions
                                .insert(number, request.transaction);
                            let ot = Arc::clone(&sm.state.outstanding_transactions);
                            finished.set_ext(Box::new(move || {
                                ot.remove(&number);
                            }));
                            Ok(finished)
                        }
                        Err(e) => {
                            tracing::error!("error applying transaction: {e}");
                            Err(e)
                        }
                    };
                    responses.push(Response {
                        result: Some(finished),
                    })
                }
                EntryPayload::Membership(membership) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(Response { result: None });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
        StorageError<<TypeConfig as RaftTypeConfig>::NodeId>,
    > {
        Ok(Box::new(Cursor::new(vec![])))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<
            <TypeConfig as RaftTypeConfig>::NodeId,
            <TypeConfig as RaftTypeConfig>::Node,
        >,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<<TypeConfig as RaftTypeConfig>::NodeId>> {
        let new_snapshot = StoredSnapshot {
            metadata: meta.clone(),
            data: snapshot.into_inner(),
        };
        let SnapshotFrameDeser {
            storage,
            outstanding_transactions,
        } = serde_json::from_slice(&new_snapshot.data).map_err(|e| {
            tracing::error!("Failed to deserialize snapshot: {e}");
            StorageIOError::read_snapshot(Some(new_snapshot.metadata.signature()), &e)
        })?;
        let config = {
            self.state_machine
                .read()
                .await
                .state
                .phase_controller
                .config()
        };
        let updated_state_machine = Inner {
            state: State {
                outstanding_transactions: Arc::new(outstanding_transactions),
                phase_controller: PhaseController::new(config, storage),
            },
            last_membership: meta.last_membership.clone(),
            last_applied_log: meta.last_log_id,
        };
        let mut sm = self.state_machine.write().await;
        *sm = updated_state_machine;

        let mut ds = self.current_snapshot.write().await;
        drop(sm);

        *ds = Some(new_snapshot);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<<TypeConfig as RaftTypeConfig>::NodeId>>
    {
        if let Some(snapshot) = &*self.current_snapshot.read().await {
            let data = snapshot.data.clone();
            Ok(Some(Snapshot {
                meta: snapshot.metadata.clone(),
                snapshot: Box::new(std::io::Cursor::new(data)),
            }))
        } else {
            Ok(None)
        }
    }
}
