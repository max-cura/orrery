//! Taken (almost) verbatim from https://github.com/datafuselabs/openraft/blob/main/examples/memstore/src/log_store.rs
//! Changes:
//!     - more performant `append()`, `truncate()`, and `purge()` methods.

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, Vote,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default, Debug, Clone)]
pub struct LogStorage<C: RaftTypeConfig>(Arc<Mutex<LogStorageInner<C>>>)
where
    C::Entry: Clone;

impl<C: RaftTypeConfig> RaftLogReader<C> for LogStorage<C>
where
    C::Entry: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.try_get_log_entries(range)
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStorage<C>
where
    C::Entry: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.get_log_state()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.save_vote(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.read_vote()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut l = self.0.lock().await;
        l.append(entries, callback)
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.truncate(log_id)
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut l = self.0.lock().await;
        l.purge(log_id)
    }
}

#[derive(Debug, Clone)]
struct LogStorageInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>,
    committed: Option<LogId<C::NodeId>>,
    vote: Option<Vote<C::NodeId>>,

    log: BTreeMap<u64, C::Entry>,
}

impl<C: RaftTypeConfig> Default for LogStorageInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            committed: None,
            vote: None,
            log: Default::default(),
        }
    }
}

impl<C: RaftTypeConfig> LogStorageInner<C> {
    fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        // TODO: may panic if range is bad
        Ok(self
            .log
            .range(range.clone())
            .map(|x| x.1)
            .cloned()
            .collect())
    }

    fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last_purged_log_id = self.last_purged_log_id;
        let last_log_id = self
            .log
            .last_key_value()
            .map(|(_, e)| *e.get_log_id())
            .or(last_purged_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.vote = Some(*vote);
        Ok(())
    }

    fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote)
    }

    fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.log.extend(
            entries
                .into_iter()
                .map(|entry| (entry.get_log_id().index, entry)),
        );
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let _ = self.log.split_off(&log_id.index);
        Ok(())
    }

    fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let last_purged = self.last_purged_log_id;
        assert!(last_purged <= Some(log_id));
        self.last_purged_log_id = Some(log_id);

        let keys = self
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
        // if let Some(ref first_unpurged_idx) = self.log.range(..=log_id.index).next().map(|x| *x.0) {
        //     self.log = self.log.split_off(first_unpurged_idx);
        // } else {
        //     self.log.clear();
        // }

        Ok(())
    }
}
