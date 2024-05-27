use std::collections::VecDeque;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, Ordering};
use crossbeam::deque::{Injector, Steal};
use rayon::{ThreadPool, ThreadPoolBuilder, ThreadPoolBuildError};
use crate::op::DatabaseContext;
use crate::partition::{Batch, Partition, PartitionDispatcher, PartitionLimits};

/// VERY DEEPLY UNSAFE
struct Storage {

}
unsafe impl Sync for Storage {}
impl Storage {
    pub fn apply(&self, dc: DatabaseContext) {
        unimplemented!()
    }
}

struct PartitionTask {
    partition: Partition,
    storage: Arc<Storage>,
}

pub struct WorkPool {
    worker_count: usize,
    batch_queue: parking_lot::Mutex<VecDeque<Batch>>,
    injector: Arc<Injector<PartitionTask>>,
    herd_control: Arc<HerdControl>,
    thread_pool: ThreadPool
}

impl WorkPool {
    pub fn new(worker_count: usize) -> Result<Self, ThreadPoolBuildError> {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()?;
        let injector = Arc::new(Injector::new());
        let herd_control = Arc::new(HerdControl::new(worker_count));
        let i2 = Arc::clone(&injector);
        let h2 = Arc::clone(&herd_control);
        thread_pool.spawn_broadcast(move |ctx| {
            let mut worker = Worker {
                herd_control: Arc::clone(&h2),
                injector: Arc::clone(&i2),
            };
            worker.run();
        });
        Ok(Self {
            worker_count,
            batch_queue: parking_lot::Mutex::new(VecDeque::new()),
            injector,
            herd_control,
            thread_pool
        })
    }

    /// Should be run on its own thread
    pub fn run_dispatcher(&self, storage: Arc<Storage>, partition_limits: PartitionLimits) {
        let mut partition_dispatcher = PartitionDispatcher::new();
        let submit = |partition: Partition| {
            self.injector.push(PartitionTask {
                partition,
                storage: Arc::clone(&storage),
            });
        };

        // park the workers until we can install a batch
        self.herd_control.wait_for_workers_finished();

        loop {
            let batch = {
                let mut queue = self.batch_queue.lock();
                queue.pop_front()
            };
            let Some(batch) = batch else {
                std::thread::yield_now();
                continue
            };
            partition_dispatcher.install_batch(batch);
            while !partition_dispatcher.batch_done() {
                self.herd_control.start_new_batch();
                partition_dispatcher.dispatch_one_round(submit, &partition_limits);
                self.herd_control.wait_for_workers_finished();
            }
        }
    }

    pub fn install(&self, batch: Batch) {
        let mut queue = self.batch_queue.lock();
        queue.push_back(batch);
    }
}

fn run_partition(
    mut partition: Partition,
    storage: Arc<Storage>,
) {
    let transactions = partition.consume();
    let mut db_ctx = DatabaseContext::new();
    for txn in transactions {
        /* TODO: execute the transaction on `db_ctx` with `storage` as the backing store */
        let result = Ok(vec![]);

        txn.finished.unwrap().finish(result)
    }
    storage.apply(db_ctx);
}

struct HerdControl {
    batch_dispatch_finished: AtomicBool,
    wait_for_workers_to_finish: Barrier,
    new_batch: Barrier,
    workers_finished: parking_lot::Mutex<bool>,
    workers_finished_cv: parking_lot::Condvar,
}
impl HerdControl {
    pub fn new(worker_count: usize) -> Self {
        Self {
            batch_dispatch_finished: AtomicBool::new(false),
            wait_for_workers_to_finish: Barrier::new(worker_count),
            new_batch: Barrier::new(worker_count + 1),
            workers_finished: parking_lot::Mutex::new(false),
            workers_finished_cv: parking_lot::Condvar::new(),
        }
    }
    pub fn start_new_batch(&self) {
        self.batch_dispatch_finished.store(false, Ordering::SeqCst);
        self.new_batch.wait();
    }
    pub fn wait_for_workers_finished(&self) {
        self.batch_dispatch_finished.store(true, Ordering::SeqCst);
        let mut g = self.workers_finished.lock();
        if !*g {
            self.workers_finished_cv.wait(&mut g);
            *g = false;
        }
    }

    pub fn workers_finished(&self) {
        let mut g = self.workers_finished.lock();
        *g = true;
        self.workers_finished_cv.notify_one();
    }
}

struct Worker {
    herd_control: Arc<HerdControl>,
    injector: Arc<Injector<PartitionTask>>,
}

impl Worker {
    fn run_partition(&mut self, _partition: PartitionTask) {
        unimplemented!()
    }

    fn run(&mut self) {
        loop {
            if self.herd_control.batch_dispatch_finished.load(Ordering::SeqCst) {
                if self.herd_control.wait_for_workers_to_finish.wait().is_leader() {
                    self.herd_control.workers_finished();
                }
                self.herd_control.new_batch.wait();
            }

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
    }
}
