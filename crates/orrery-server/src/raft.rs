pub mod state;
pub mod store;

use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig};
use orrery_store::{ExecutionError, TransactionFinished};
use orrery_wire::TransactionRequest;
use serde::de::{Error, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::Cursor;
use std::ops::RangeBounds;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub(crate) transaction: TransactionRequest,
}

// fn x() -> Option<Result<TransactionFinished, ExecutionError>> {}

#[derive(Debug, Deserialize)]
pub struct Response {
    #[serde(skip, default)]
    pub result: Option<Result<TransactionFinished, ExecutionError>>,
}
impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Response", 0)?;
        s.skip_field("result")?;
        s.end()
    }
}

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

#[cfg(test)]
mod test {
    use crate::raft::state::StateMachine;
    use crate::raft::store::LogStorage;
    use crate::raft::{NodeId, TypeConfig};
    use openraft::testing::{StoreBuilder, Suite};
    use openraft::StorageError;
    use orrery_store::{Config, FixedConfigThreshold, PartitionLimits, Storage};
    use std::sync::Arc;
    use std::time::Duration;

    struct MyStoreBuilder {}

    impl StoreBuilder<TypeConfig, LogStorage<TypeConfig>, Arc<StateMachine>, ()> for MyStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), LogStorage<TypeConfig>, Arc<StateMachine>), StorageError<NodeId>> {
            Ok((
                (),
                LogStorage::default(),
                Arc::new(StateMachine::new(
                    Config {
                        partition_limits: PartitionLimits {
                            partition_max_write: 200,
                            partition_max_access: 200,
                        },
                        fixed_config_threshold: FixedConfigThreshold {
                            max_access_set_size: 3000,
                            max_transaction_count: 3000,
                            max_flush_interval: Duration::from_millis(10),
                        },
                        worker_count: 0,
                    },
                    Storage::new_test(),
                )),
            ))
        }
    }

    #[test]
    pub fn test() -> Result<(), StorageError<NodeId>> {
        Suite::test_all(MyStoreBuilder {})?;
        Ok(())
    }
}
