use alloc::{string::String, sync::Arc};

use crate::{
    executor::Executor,
    queue::{QueueWorker, WrappedTask},
    task::{Response, Task},
};

#[cfg(test)]
mod tests;

#[cfg(any(feature = "nats-queue", feature = "jetstream-queue"))]
use crate::executor::openai::ExecutorAsyncOpenai;
#[cfg(feature = "jetstream-queue")]
use crate::queue::jetstream::JetStreamWorker;
#[cfg(feature = "nats-queue")]
use crate::queue::nats::NatsWorker;

#[cfg(feature = "nats-queue")]
/// A Core NATS consumer backed by the OpenAI-compatible chat executor.
pub type NatsOpenaiWorker = Worker<NatsWorker, ExecutorAsyncOpenai>;

#[cfg(feature = "jetstream-queue")]
/// A durable JetStream worker backed by the OpenAI-compatible executor.
pub type JetStreamOpenaiWorker = Worker<JetStreamWorker, ExecutorAsyncOpenai>;

#[derive(Debug, Clone)]
/// Connects a queue consumer to an executor, processing one task at a time.
///
/// Use [`Self::new`] for any queue/executor combination, then [`Self::run`] to
/// receive tasks, execute them, and send responses through the queue.
pub struct Worker<Q: QueueWorker, E: Executor> {
    queue: Q,
    executor: E,
}

#[cfg(feature = "jetstream-queue")]
impl JetStreamOpenaiWorker {
    /// Binds to provisioned JetStream resources and creates the API executor.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let queue = JetStreamWorker::from_env_or_default().await?;
        Ok(Self::new(queue, ExecutorAsyncOpenai::from_env_or_default()))
    }
}

#[cfg(feature = "nats-queue")]
impl NatsOpenaiWorker {
    /// Builds the NATS consumer and API executor from their environment settings.
    ///
    /// Requires `OPENAI_API_DEFAULT_MODEL` and a reachable NATS server. See
    /// [`NatsWorker::from_env_or_default`] and
    /// [`ExecutorAsyncOpenai::from_env_or_default`] for defaults.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let queue = NatsWorker::from_env_or_default().await?;
        let executor = ExecutorAsyncOpenai::from_env_or_default();

        Ok(Self { queue, executor })
    }
}

impl<Q: QueueWorker, E: Executor> Worker<Q, E> {
    /// Connects a queue to an executor without starting the processing loop.
    pub fn new(queue: Q, executor: E) -> Self {
        Self { queue, executor }
    }

    /// Processes tasks sequentially until the queue returns `None` or an error.
    ///
    /// Queue and executor errors stop the loop. An executor error leaves the
    /// task without a response; JetStream can redeliver an unacknowledged task
    /// to an active worker within its delivery and retention limits. This loop
    /// does not restart itself or retry execution errors locally.
    /// Responses with `success == false` are sent back and do not stop the loop.
    /// A polling queue returning `None` ends the loop even if more work may arrive.
    /// There is no shutdown signal; callers must arrange cancellation themselves.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("openai-api worker subscribed to queue");

        while let Some(WrappedTask { message, task }) = self.queue.receive_task().await? {
            let id = task.id;

            tracing::debug!("received task id `{id}`");

            let response = self.executor.execute(task).await?;

            tracing::debug!(
                "responding task id `{id}` with success({})",
                response.success
            );

            if !response.success {
                tracing::debug!(
                    "task id `{id}` responded with error `{}`",
                    response.contents
                );
            }

            self.queue.send_response(message, response).await?;

            tracing::debug!("task id `{id}` response sent to queue");
        }

        tracing::info!("openai-api worker terminated");

        Ok(())
    }
}

impl<Q: QueueWorker, E: Executor> QueueWorker for Worker<Q, E> {
    type Message = Q::Message;

    async fn receive_task(&self) -> anyhow::Result<Option<WrappedTask<Self::Message>>> {
        self.queue.receive_task().await
    }

    async fn send_response(
        &self,
        message: Self::Message,
        response: Response,
    ) -> anyhow::Result<()> {
        self.queue.send_response(message, response).await
    }
}

impl<Q: QueueWorker, E: Executor> Executor for Worker<Q, E> {
    fn default_model(&self) -> &Arc<Option<String>> {
        self.executor.default_model()
    }

    async fn execute(&self, task: Task) -> anyhow::Result<Response> {
        self.executor.execute(task).await
    }
}
