#![allow(dead_code)]

use core::sync::atomic::{AtomicU32, Ordering};

use alloc::{collections::VecDeque, sync::Arc};
use spin::Mutex;

use crate::queue::{QueueProducer, QueueWorker, Response, Task, WrappedTask};

#[derive(Debug, Default, Clone)]
/// Spin-locked queues that poll once without suspending or registering a waker.
///
/// Clones share storage. Empty task queues or missing responses return `None`
/// immediately; callers must arrange polling. Defaults to unbounded storage.
/// A bounded queue discards the oldest entries instead of applying backpressure.
pub struct MemoryQueueNoStd {
    tasks: Arc<Mutex<VecDeque<Task>>>,
    responses: Arc<Mutex<VecDeque<Response>>>,
    capacity: Arc<Option<AtomicU32>>,
}

impl MemoryQueueNoStd {
    /// Bounds each queue, discarding its oldest entry when a push reaches capacity.
    ///
    /// Use `1..=u32::MAX`: larger values are truncated to `u32`. A zero stored
    /// capacity can make the eviction loop run forever on the first push.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: Arc::new(Some(AtomicU32::new(capacity as u32))),
            ..Default::default()
        }
    }
}

impl QueueProducer for MemoryQueueNoStd {
    type Message = u128;

    async fn send_task(&self, task: Task) -> anyhow::Result<u128> {
        let tasks = &mut *self.tasks.lock();
        let id = task.id;

        if let Some(c) = &*self.capacity {
            let c = c.fetch_max(1, Ordering::Relaxed) as usize;

            while c <= tasks.len() {
                if let Some(t) = tasks.pop_front() {
                    tracing::debug!("discarding task `{}` for capacity `{c}`", t.id);
                }
            }
        }

        tasks.push_back(task);

        Ok(id)
    }

    async fn receive_response(&self, message: u128) -> anyhow::Result<Option<Response>> {
        let responses: &mut VecDeque<_> = &mut self.responses.lock();
        let i = match responses
            .iter()
            .enumerate()
            .find_map(|(i, r)| (r.task.id == message).then_some(i))
        {
            Some(i) => i,
            None => return Ok(None),
        };

        Ok(responses.remove(i))
    }
}

impl QueueWorker for MemoryQueueNoStd {
    type Message = ();

    async fn receive_task(&self) -> anyhow::Result<Option<WrappedTask<Self::Message>>> {
        let task = self.tasks.lock().pop_front();

        Ok(task.map(|task| WrappedTask { message: (), task }))
    }

    async fn send_response(
        &self,
        _message: Self::Message,
        response: Response,
    ) -> anyhow::Result<()> {
        let responses = &mut *self.responses.lock();

        if let Some(c) = &*self.capacity {
            let c = c.fetch_max(1, Ordering::Relaxed) as usize;

            while c <= responses.len() {
                if let Some(r) = responses.pop_front() {
                    tracing::debug!(
                        "discarding task response `{}` for capacity `{c}`",
                        r.task.id
                    );
                }
            }
        }

        responses.push_back(response);

        Ok(())
    }
}
