use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::Thread;
use crossbeam::deque::{Injector, Steal, Worker as WorkerQueue};
use crate::op::DatabaseContext;
use crate::partition::{Partition, PartitionDispatcher};

pub struct WorkPool {
    partition_dispatcher: PartitionDispatcher,
}

impl WorkPool {

}

fn run_partition(
    mut partition: Partition,
    _db_ctx: &mut DatabaseContext,
    /* TODO: execution environment? */
) {
    let Partition {
        readonly_set, write_set, transactions, txn_nos
    } = partition;
    for txn in transactions {
        /* TODO: execute the transaction on db_ctx */
        let result = Ok(vec![]);

        txn.finished.unwrap().finish(result)
    }
}

enum Command {
    Barrier(Arc<Barrier>),
    Park,
    Exit,
}

struct WorkerControl {
    parked: AtomicBool,
    thread: parking_lot::Mutex<Option<Thread>>,
}
impl WorkerControl {
    pub fn unpark(&self) {
        self.parked.store(false, Ordering::SeqCst);
        let g = self.thread.lock();
        match g.as_ref() {
            None => {
                // okay to just return; parked is set to false, and the Worker hasn't gotten into
                // its loop yet, so it'll see parked correctly and won't park
            }
            Some(thread) => {
                thread.unpark()
            }
        }
    }
}

struct Worker {
    work: WorkerQueue<Command>,
    injector: Injector<Partition>,
    control: Arc<WorkerControl>,
}

impl Worker {
    fn run(&mut self) {
        {
            let mut guard = self.control.thread.lock();
            let _ = guard.insert(std::thread::current());
        }
        loop {
            while self.control.parked.load(Ordering::SeqCst) {
                std::thread::park();
            }

            loop {
                match self.injector.steal() {
                    Steal::Empty => break,
                    Steal::Retry => continue,
                    Steal::Success(_partition) => {
                        unimplemented!("Partition runner")
                    }
                }
            }

            let task = self.work.pop();
            match task {
                Some(Command::Barrier(barrier)) => {
                    barrier.wait().unwrap()
                }
                Some(Command::Exit) => {
                    return
                }
                Some(Command::Park) | None => {
                    self.control.parked.store(true, Ordering::SeqCst);
                    continue
                }
            }
        }
    }
}
