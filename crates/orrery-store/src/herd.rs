use crate::partition::{Batch, Partition, PartitionDispatcher, PartitionLimits};
use crate::sched::TransactionFinishedInner;
use crate::storage::Storage;
use crate::transaction::WriteCache;
use crossbeam::deque::{Injector, Steal};
use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

/// Used internally for partition dispatch
struct PartitionTask {
    partition: Partition,
    storage: Arc<Storage>,
}

/// Pool of workers threads used to run transactions.
pub struct Herd {
    worker_count: usize,

    batch_queue: parking_lot::Mutex<VecDeque<Batch>>,
    batch_queue_cv: parking_lot::Condvar,

    injector: Arc<Injector<PartitionTask>>,
    herd_control: Arc<HerdControl>,
    thread_pool: ThreadPool,
}
impl Herd {
    pub fn new(worker_count: usize) -> Result<Self, ThreadPoolBuildError> {
        let thread_pool = ThreadPoolBuilder::new().num_threads(worker_count).build()?;
        let injector = Arc::new(Injector::new());
        let herd_control = Arc::new(HerdControl::new(worker_count));
        let i2 = Arc::clone(&injector);
        let h2 = Arc::clone(&herd_control);
        thread_pool.spawn_broadcast(move |_ctx| {
            let mut worker = Worker {
                herd_control: Arc::clone(&h2),
                injector: Arc::clone(&i2),
            };
            worker.run();
        });
        Ok(Self {
            worker_count,
            batch_queue: parking_lot::Mutex::new(VecDeque::new()),
            batch_queue_cv: parking_lot::Condvar::new(),
            injector,
            herd_control,
            thread_pool,
        })
    }

    /// Should be run on its own thread
    pub fn run_dispatcher(
        &self,
        storage: Storage,
        partition_limits: PartitionLimits,
        batch: Batch,
    ) -> Storage {
        // tracing::info!("running dispatcher...");
        if batch.len() == 0 {
            return storage;
        }

        // tracing::info!("building dispatcher");

        let mut partition_dispatcher = PartitionDispatcher::new();
        let arc_storage = Arc::new(storage);
        let submit = |partition: Partition| {
            self.injector.push(PartitionTask {
                partition,
                storage: Arc::clone(&arc_storage),
            });
        };

        // tracing::info!("waiting for workers to finish");

        // park the workers until we can install a batch
        self.herd_control.wait_for_workers_finished(false);

        // loop {
        // let batch = {
        //     let mut queue = self.batch_queue.lock();
        //     if queue.is_empty() {
        //         self.batch_queue_cv.wait(&mut queue);
        //     }
        //     queue.pop_front().unwrap()
        // };
        // run the dispatcher

        let sc = Arc::strong_count(&arc_storage);
        // tracing::info!("strong count: {sc}");

        partition_dispatcher.install_batch(batch);
        while !partition_dispatcher.batch_done() {
            self.herd_control.start_new_round();
            tracing::info!("dispatching round");
            partition_dispatcher.dispatch_one_round(submit, &partition_limits);
            partition_dispatcher.load_deferred();
            self.herd_control.wait_for_workers_finished(true);
        }
        // }
        // tracing::info!("strong count (after): {sc}");
        Arc::into_inner(arc_storage)
            .expect("Storage should have exactly 1 strong reference when workers are finished")
    }
    //
    // /// Send a batch of partitions on the Herd.
    // pub fn enqueue_batch(&self, batch: Batch) {
    //     {
    //         let mut queue = self.batch_queue.lock();
    //         queue.push_back(batch);
    //     }
    //     self.batch_queue_cv.notify_one();
    // }
}

/// Used internally to coordinate a herd of workers.
#[derive(Debug)]
struct HerdControl {
    batch_dispatch_finished: AtomicBool,
    wait_for_workers_to_finish: Barrier,
    new_batch: Barrier,
    workers_finished: parking_lot::Mutex<bool>,
    workers_finished_cv: parking_lot::Condvar,
}
impl HerdControl {
    /// Create a new [`HerdControl`] with the specified worker count.
    fn new(worker_count: usize) -> Self {
        Self {
            batch_dispatch_finished: AtomicBool::new(true),
            wait_for_workers_to_finish: Barrier::new(worker_count),
            new_batch: Barrier::new(worker_count + 1),
            workers_finished: parking_lot::Mutex::new(false),
            workers_finished_cv: parking_lot::Condvar::new(),
        }
    }

    /// Used by the herd controller, along with [`wait_for_workers_finished`], to drive the
    /// two-phase batch stepping.
    ///
    /// Wakes the herd, and tells them to start polling the injector.
    fn start_new_round(&self) {
        self.batch_dispatch_finished.store(false, Ordering::SeqCst);
        // tracing::info!("set batch_dispatch_finished=false");
        self.new_batch.wait();
    }
    /// Used by the herd controller, along with [`start_new_batch`] to drive the two-phase batch
    /// stepping.
    ///
    /// Indicates to the herd that no new tasks will be injected, and sleeps the thread until all
    /// tasks in the injector are completed.
    #[tracing::instrument]
    fn wait_for_workers_finished(&self, wait_on_false: bool) {
        // tracing::info!("set batch_dispatch_finished=true");
        self.batch_dispatch_finished.store(true, Ordering::SeqCst);
        let mut g = self.workers_finished.lock();
        // tracing::info!("waiting for workers to finish, finished={}", *g);
        if *g == false && wait_on_false {
            // if workers aren't finish, wait until they all finish
            self.workers_finished_cv.wait(&mut g);
        }
        *g = false;
    }

    /// Called by a single worker exactly once when both conditions are true:
    ///     - all workers in the herd have received the batch have received the batch finished
    ///       notification
    ///     - the injection queue is empty
    #[tracing::instrument]
    fn workers_finished(&self) {
        // tracing::info!("workers all finished");
        let mut g = self.workers_finished.lock();
        *g = true;
        self.workers_finished_cv.notify_one();
    }
}

#[derive(Debug)]
struct Worker {
    herd_control: Arc<HerdControl>,
    injector: Arc<Injector<PartitionTask>>,
}
impl Worker {
    fn run_partition(&mut self, mut partition: PartitionTask) {
        tracing::info!(
            "running partition of size: {}",
            partition.partition.transactions_mut().len()
        );
        for txn in partition.partition.transactions_mut() {
            let mut write_cache = WriteCache::new();
            let result = txn.execute(&mut write_cache, &partition.storage);
            if result.is_ok() {
                unsafe {
                    partition.storage.apply(write_cache);
                }
            }
            // tracing::info!("finished transaction {txn:?}, result is {result:?}");
            if let Some(fin) = txn.get_finished() {
                TransactionFinishedInner::finish(fin, result)
            }
        }
    }

    fn run_available_partitions(&mut self) {
        loop {
            match self.injector.steal() {
                Steal::Empty => break,
                Steal::Retry => continue,
                Steal::Success(partition) => {
                    self.run_partition(partition);
                }
            }
        }
    }

    #[tracing::instrument]
    fn run(&mut self) {
        // tracing::info!("running worker");
        loop {
            self.run_available_partitions();
            if self
                .herd_control
                .batch_dispatch_finished
                .load(Ordering::SeqCst)
            {
                // batch_dispatch_finished only means that no new partitions will be injected; we
                // still have to clear the queue
                self.run_available_partitions();
                if self
                    .herd_control
                    .wait_for_workers_to_finish
                    .wait()
                    .is_leader()
                {
                    self.herd_control.workers_finished();
                }
                // tracing::warn!("worker waiting for new batch");
                self.herd_control.new_batch.wait();
                // tracing::warn!("worker notified of new batch");
            }
        }
    }
}
