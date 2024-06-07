use crate::ExecutionResult;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct TransactionFinishedInner {
    value: parking_lot::Mutex<Option<ExecutionResult>>,
    finished: AtomicBool,
    waker: parking_lot::Mutex<Option<Waker>>,
}

#[derive(Debug)]
pub struct TransactionFinished(Arc<TransactionFinishedInner>);

impl TransactionFinishedInner {
    pub fn new() -> Self {
        TransactionFinishedInner {
            value: parking_lot::Mutex::new(None),
            finished: AtomicBool::new(false),
            waker: parking_lot::Mutex::new(None),
        }
    }
    pub fn finish(self: Arc<Self>, result: ExecutionResult) {
        {
            let mut guard = self.value.lock();
            let _ = guard.insert(result);
        }
        self.finished.store(true, Ordering::SeqCst);
        {
            let mut guard = self.waker.lock();
            if let Some(waker) = guard.take() {
                waker.wake();
            }
        }
    }
}

impl TransactionFinished {
    pub fn new() -> (Self, Arc<TransactionFinishedInner>) {
        let arc = Arc::new(TransactionFinishedInner::new());
        let this = Self(Arc::clone(&arc));
        (this, arc)
    }
}

impl Future for TransactionFinished {
    type Output = ExecutionResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.finished.load(Ordering::SeqCst) {
            let value = {
                let mut guard = self.0.value.lock();
                guard.take().unwrap()
            };
            Poll::Ready(value)
        } else {
            {
                let mut guard = self.0.waker.lock();
                let _ = guard.insert(cx.waker().clone());
            }
            // it's possible that in between finished.load() and storing the waker, a different
            // thread called finish, and set finished=true, but couldn't call a waker since we
            // hadn't stored one yet
            // At this point, the waker has been stored, so if we see don't see a finished here,
            // then any other thread setting finished will be able to read the waker we set
            if self.0.finished.load(Ordering::SeqCst) {
                let value = {
                    let mut guard = self.0.value.lock();
                    guard.take().unwrap()
                };
                Poll::Ready(value)
            } else {
                Poll::Pending
            }
        }
    }
}
