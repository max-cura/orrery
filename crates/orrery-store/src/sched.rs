use crate::partition::{Batch, DependencyGraphBuilder};
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::Instant;

mod tx_finished;
use crate::transaction::Transaction;
pub use tx_finished::TransactionFinished;
pub use tx_finished::TransactionFinishedInner;

pub struct IntakeStats {
    access_set_size: usize,
    transaction_count: usize,
}

pub trait Threshold: Send + Sync {
    fn should_flush(&self, intake_stats: IntakeStats) -> bool;
    /// Implementation note: this function is only called when the TransactionScheduler is constructed.
    /// A correct implementation of this function must therefore always return the same value.
    fn max_flush_interval(&self) -> Duration;
}

/// A simple [`Threshold`] implementation for [`TransactionScheduler`].
/// Please note that the
#[derive(Debug, Copy, Clone)]
pub struct FixedConfigThreshold {
    /// Maximum size of global access set that can be accumulated before the
    /// [`TransactionScheduler`] will flush. If this is zero, the [`TransactionScheduler`] will not
    /// flush based on the size of the global access set.
    pub max_access_set_size: usize,
    /// Maximum number of transactions that can be accumulated before the [`TransactionScheduler`]
    /// will flush. If this is zero, the [`TransactionScheduler`] will not flush based on the number
    /// of queued transactions.
    pub max_transaction_count: usize,
    /// The maximum amount of time between [`TransactionScheduler`] flushes; if this amount of time
    /// has passed without the [`TransactionScheduler`] flushing, then a flush will be run.
    pub max_flush_interval: Duration,
}
impl Threshold for FixedConfigThreshold {
    fn should_flush(&self, intake_stats: IntakeStats) -> bool {
        if self.max_access_set_size > 0 && self.max_access_set_size < intake_stats.access_set_size {
            true
        } else if self.max_transaction_count > 0
            && self.max_transaction_count < intake_stats.transaction_count
        {
            true
        } else {
            false
        }
    }

    fn max_flush_interval(&self) -> Duration {
        self.max_flush_interval
    }
}

pub struct TransactionScheduler<TH: Threshold, F: Fn(Batch) -> () + Send + Sync + 'static> {
    dependency_graph_builder: ArcSwap<DependencyGraphBuilder>,
    threshold: TH,
    pipe: F,
    // 2n = n'th generation
    // 2n+1 = waiting to update to n+1'th generation
    generation: AtomicU64,
    flush_sender: UnboundedSender<Instant>,
    last_flush: parking_lot::Mutex<Instant>,
}

pub fn new_with_watchdog<TH: Threshold + 'static, F: Fn(Batch) -> () + Send + Sync + 'static>(
    threshold: TH,
    pipe: F,
) -> Arc<TransactionScheduler<TH, F>> {
    let max_flush_interval = threshold.max_flush_interval();

    let (ts, mut flush_receiver) = TransactionScheduler::new(threshold, pipe);
    let ts_arc = Arc::new(ts);
    let ts_arc_weak = Arc::downgrade(&ts_arc);

    let watchdog: JoinHandle<()> = tokio::spawn(async move {
        tracing::debug!("watchdog entered");
        'watchdog_loop: loop {
            let r = tokio::time::timeout(max_flush_interval, async { flush_receiver.recv().await })
                .await;
            tracing::debug!("watchdog: {r:?}");
            match r {
                Ok(o) => {
                    /* okay */
                    if o.is_none() {
                        break 'watchdog_loop;
                    }
                }
                Err(_) => {
                    // force flush
                    if let Some(arc) = ts_arc_weak.upgrade() {
                        arc.watchdog_flush(max_flush_interval);
                    }
                }
            }
        }
        // if execution reaches this point, that means that the TransactionScheduler has closed
    });

    // if JoinHandle is dropped, then the task continues to run in the background
    drop(watchdog);

    tracing::info!("created watchdog");

    ts_arc
}

impl<TH: Threshold, F: Fn(Batch) -> () + Send + Sync + 'static> TransactionScheduler<TH, F> {
    /// Do not call this function directly. Use [`sched::new_with_watchdog`](new_with_watchdog)
    /// instead.
    fn new(threshold: TH, pipe: F) -> (Self, UnboundedReceiver<Instant>) {
        let (flush_sender, flush_receiver) = tokio::sync::mpsc::unbounded_channel();

        (
            Self {
                dependency_graph_builder: ArcSwap::new(Arc::new(DependencyGraphBuilder::new(0))),
                threshold,
                pipe,
                generation: AtomicU64::new(0),
                flush_sender,
                last_flush: parking_lot::Mutex::new(Instant::now()),
            },
            flush_receiver,
        )
    }

    fn intake_stats(&self, dg: &arc_swap::Guard<Arc<DependencyGraphBuilder>>) -> IntakeStats {
        IntakeStats {
            access_set_size: dg.access_set_size(),
            transaction_count: dg.transaction_count(),
        }
    }

    fn flush_internal(&self, flush_gen: u64, from_watchdog: bool) {
        // tracing::info!("flush_internal 1 ({from_watchdog}) (gen={flush_gen})");
        let old = self
            .dependency_graph_builder
            .swap(Arc::new(DependencyGraphBuilder::new(flush_gen + 2)));
        {
            let mut guard = self.last_flush.lock();
            *guard = Instant::now();
        }
        self.generation.store(flush_gen + 2, Ordering::SeqCst);
        // wait until `old` is the only reference left
        // since we swap()'ed already, the strong_count is monotonically decreasing
        if old.transaction_count() > 0 {
            // tracing::info!("flush_internal 2 ({from_watchdog}) (gen={flush_gen})");
            while Arc::strong_count(&old) > 1 {
                std::hint::spin_loop();
            }
            // tracing::info!("flush_internal 3 ({from_watchdog}) (gen={flush_gen})");
            // once `old` is the only reference, we can call Arc::into_inner on it and send it down the
            // pipe
            let dg = Arc::into_inner(old).unwrap();
            (self.pipe)(dg.into_batch());
        }
    }

    fn flush(&self, flush_gen: u64, from_watchdog: bool) -> bool {
        if self
            .generation
            .compare_exchange(flush_gen, flush_gen + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.flush_internal(flush_gen, from_watchdog);
            true
        } else {
            false
        }
    }

    fn post_flush_barrier(&self) {
        while self.generation.load(Ordering::SeqCst) % 2 == 1 {
            std::hint::spin_loop();
        }
    }

    fn watchdog_flush(&self, guard_interval: Duration) {
        let gen = self.generation.load(Ordering::SeqCst);
        if gen % 2 == 1 {
            // swap in progress, we're good
            self.post_flush_barrier()
        } else {
            // trigger a flush
            let elapsed = self.last_flush.lock().elapsed();
            if elapsed <= guard_interval {
                // swap ran, but watchdog didn't see it in time
            } else {
                // actually need to flush
                self.flush(gen, true);
            }
        }
    }

    pub fn enqueue_transaction(&self, mut transaction: Transaction) -> TransactionFinished {
        let (future, signal) = TransactionFinished::new(transaction.client_no());
        transaction.set_finished(signal);

        let (orig_gen, stats) = {
            let dg = self.dependency_graph_builder.load();

            let orig_gen = dg.add_txn(transaction);
            let stats = self.intake_stats(&dg);

            (orig_gen, stats)
        };

        if self.threshold.should_flush(stats) {
            tracing::info!("enqueue_transaction: flushing");
            if !self.flush(orig_gen, false) {
                tracing::info!("enqueue_transaction: waiting for post-flush barrier");
                self.post_flush_barrier();
            }
            tracing::info!("enqueue_transaction: done flushing");
        }

        future
    }
}
