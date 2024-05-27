use openraft::storage::RaftStateMachine;
use openraft::{
    LogId, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};

struct Inner<C: RaftTypeConfig> {
}

pub struct StateMachine<C: RaftTypeConfig> {
}

impl<C: RaftTypeConfig> RaftSnapshotBuilder<C> for StateMachine<C> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C::NodeId>> {
        todo!()
    }
}

impl<C: RaftTypeConfig> RaftStateMachine<C> for StateMachine<C> {
    type SnapshotBuilder = Self;

    #[rustfmt::skip]
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>, ), StorageError<C::NodeId>> {
        todo!()
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        todo!()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        todo!()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>> {
        todo!()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>> {
        todo!()
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>> {
        todo!()
    }
}
