use core::sync::atomic::{AtomicU32, Ordering};

use alloc::{collections::BTreeMap, sync::Arc};
use tokio::sync::{Mutex, mpsc};

use crate::queue::{QueueProducer, QueueWorker, Response, Task, WrappedTask};

#[derive(Debug, Clone)]
/// Bounded Tokio channels with an out-of-order response buffer.
///
/// Clones share channels and storage. Response waiters hold the shared buffer
/// and receiver locks until their response arrives, serializing concurrent
/// waits. A full buffer discards the smallest task ID, so a response may be lost.
/// The response-channel closure path is currently unimplemented and panics.
pub struct MemoryQueueStd {
    task_tx: mpsc::Sender<Task>,
    task_rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    response_tx: mpsc::Sender<Response>,
    response_rx: Arc<Mutex<mpsc::Receiver<Response>>>,
    buffer: Arc<Mutex<BTreeMap<u128, Response>>>,
    capacity: Arc<AtomicU32>,
}

impl Default for MemoryQueueStd {
    fn default() -> Self {
        let capacity = 100;
        let (task_tx, task_rx) = mpsc::channel(capacity);
        let (response_tx, response_rx) = mpsc::channel(capacity);
        let buffer = BTreeMap::new();
        let capacity = Arc::new(AtomicU32::new(capacity as u32));

        Self {
            task_tx,
            task_rx: Arc::new(Mutex::new(task_rx)),
            response_tx,
            response_rx: Arc::new(Mutex::new(response_rx)),
            buffer: Arc::new(Mutex::new(buffer)),
            capacity,
        }
    }
}

impl MemoryQueueStd {
    /// Sets the task channel, response channel, and response-buffer capacities.
    ///
    /// Sending waits for channel space. Use a positive capacity that fits in
    /// `u32`; the buffer limit is stored with an unchecked `usize` to `u32` cast.
    ///
    /// # Panics
    ///
    /// Panics if the capacity is zero or exceeds Tokio's channel limit.
    pub fn with_capacity(capacity: usize) -> Self {
        let (task_tx, task_rx) = mpsc::channel(capacity);
        let (response_tx, response_rx) = mpsc::channel(capacity);
        let buffer = BTreeMap::new();
        let capacity = Arc::new(AtomicU32::new(capacity as u32));

        Self {
            task_tx,
            task_rx: Arc::new(Mutex::new(task_rx)),
            response_tx,
            response_rx: Arc::new(Mutex::new(response_rx)),
            buffer: Arc::new(Mutex::new(buffer)),
            capacity,
        }
    }
}

impl QueueProducer for MemoryQueueStd {
    type Message = u128;

    async fn send_task(&self, task: Task) -> anyhow::Result<Self::Message> {
        let id = task.id;
        self.task_tx.send(task).await?;
        Ok(id)
    }

    async fn receive_response(&self, message: Self::Message) -> anyhow::Result<Option<Response>> {
        let mut buffer = self.buffer.lock().await;

        if let Some(r) = buffer.remove(&message) {
            return Ok(Some(r));
        }

        let mut response_rx = self.response_rx.lock().await;

        while let Some(r) = response_rx.recv().await {
            if r.task.id == message {
                return Ok(Some(r));
            } else {
                let c = self.capacity.fetch_max(1, Ordering::Relaxed) as usize;

                while c <= buffer.len() {
                    if let Some(r) = buffer.pop_first() {
                        tracing::debug!("discarding response `{}` for capacity `{c}`", r.0);
                    }
                }

                if let Some(r) = buffer.insert(r.task.id, r) {
                    tracing::debug!("overriding task `{}` on local buffer", r.task.id);
                }
            }
        }

        todo!()
    }
}

impl QueueWorker for MemoryQueueStd {
    type Message = ();

    async fn receive_task(&self) -> anyhow::Result<Option<WrappedTask<Self::Message>>> {
        let task = self.task_rx.lock().await.recv().await;

        Ok(task.map(|task| WrappedTask { message: (), task }))
    }

    async fn send_response(
        &self,
        _message: Self::Message,
        response: Response,
    ) -> anyhow::Result<()> {
        self.response_tx.send(response).await?;
        Ok(())
    }
}
