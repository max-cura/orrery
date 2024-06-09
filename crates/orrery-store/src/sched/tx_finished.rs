use crate::{ExecutionResult, ExecutionResultFrame};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub struct TransactionFinishedInner {
    value: parking_lot::Mutex<Option<ExecutionResult>>,
    finished: AtomicBool,
    waker: parking_lot::Mutex<Option<Waker>>,

    ext: parking_lot::Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
    ext_tx_no: usize,
}
impl Debug for TransactionFinishedInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionFinishedInner")
            .field("value", &self.value)
            .field("finished", &self.finished)
            .field("waker", &self.waker)
            .field(
                "ext",
                if self.ext.lock().is_some() {
                    &"Some(<box FnOnce>)"
                } else {
                    &"None"
                },
            )
            .field("ext_tx_no", &self.ext_tx_no)
            .finish()
    }
}

#[derive(Debug)]
pub struct TransactionFinished(Arc<TransactionFinishedInner>);

impl TransactionFinishedInner {
    pub fn new(tx_no: usize) -> Self {
        TransactionFinishedInner {
            value: parking_lot::Mutex::new(None),
            finished: AtomicBool::new(false),
            waker: parking_lot::Mutex::new(None),
            ext: parking_lot::Mutex::new(None),
            ext_tx_no: tx_no,
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
    pub fn set_ext(&self, ext: Box<dyn FnOnce() + Send + 'static>) {
        let mut g = self.ext.lock();
        *g = Some(ext);
    }
}

impl TransactionFinished {
    pub fn new(txn_id: usize) -> (Self, Arc<TransactionFinishedInner>) {
        let arc = Arc::new(TransactionFinishedInner::new(txn_id));
        let this = Self(Arc::clone(&arc));
        (this, arc)
    }
    pub fn set_ext(&mut self, ext: Box<dyn FnOnce() + Send + 'static>) {
        self.0.set_ext(ext);
    }
}

impl Future for TransactionFinished {
    type Output = ExecutionResultFrame;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.finished.load(Ordering::SeqCst) {
            let value = {
                let mut guard = self.0.value.lock();
                guard.take().unwrap()
            };
            if let Some(ext) = (&mut self.0.ext.lock()).take() {
                ext();
            }
            Poll::Ready(ExecutionResultFrame {
                inner: value.map(|x| x.returned_values),
                txn_id: self.0.ext_tx_no,
            })
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
                if let Some(ext) = (&mut self.0.ext.lock()).take() {
                    ext();
                }
                Poll::Ready(ExecutionResultFrame {
                    inner: value.map(|x| x.returned_values),
                    txn_id: self.0.ext_tx_no,
                })
            } else {
                Poll::Pending
            }
        }
    }
}
