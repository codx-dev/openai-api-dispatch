use serde::{Deserialize, Serialize};

use crate::task::{Response, Task};

#[cfg(feature = "memory-queue")]
/// Shared in-process queues with different `std` and `no_std` behavior.
pub mod memory;

#[cfg(feature = "nats-queue")]
/// JSON request/reply over Core NATS; requires `std` and a Tokio runtime.
pub mod nats;

/// Submits tasks and retrieves their responses.
///
/// Waiting and cancellation semantics depend on the backend. Returned futures
/// do not carry a `Send` guarantee; this trait is not dyn-compatible.
pub trait QueueProducer {
    /// Backend-specific handle used to correlate a submitted task's response.
    type Message;

    /// Submits a task and returns its response handle, not its execution result.
    fn send_task(&self, task: Task) -> impl Future<Output = anyhow::Result<Self::Message>>;

    /// Consumes a handle to retrieve a response.
    ///
    /// `None` may mean no response is ready (`no_std` memory) or the response
    /// stream ended (NATS); consult the backend before treating it as terminal.
    fn receive_response(
        &self,
        message: Self::Message,
    ) -> impl Future<Output = anyhow::Result<Option<Response>>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A task paired with the backend metadata needed to route its reply.
pub struct WrappedTask<T> {
    /// Reply metadata; pass it unchanged to [`QueueWorker::send_response`].
    pub message: T,
    /// Work to execute.
    pub task: Task,
}

/// Receives queued tasks and routes their responses back to producers.
///
/// Returned futures do not carry a `Send` guarantee; this trait is not
/// dyn-compatible.
pub trait QueueWorker {
    /// Backend-specific reply metadata attached to each received task.
    type Message;

    /// Receives a task, or `None` when none is available or the stream ends.
    ///
    /// NATS and `std` memory wait for work; `no_std` memory polls once.
    fn receive_task(
        &self,
    ) -> impl Future<Output = anyhow::Result<Option<WrappedTask<Self::Message>>>>;

    /// Sends a response using the metadata from the corresponding task.
    fn send_response(
        &self,
        message: Self::Message,
        response: Response,
    ) -> impl Future<Output = anyhow::Result<()>>;
}
