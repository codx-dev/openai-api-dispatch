//! JSON request/reply on Core NATS, without JetStream persistence or task retries.
//!
//! Subjects concatenate a prefix and model verbatim; no separator is inserted.
//! Each task needs a reply subject. Ensure a worker subscription is active before
//! publishing: constructors do not flush subscriptions as a readiness barrier.

use std::env;

use alloc::sync::Arc;
use async_nats::{Client, Message, Subscriber};
use tokio::sync::Mutex;
use tokio_stream::StreamExt as _;

use crate::{
    queue::{QueueProducer, QueueWorker, Response, Task, WrappedTask},
    utils,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
/// Publishes JSON tasks with a dedicated inbox for each response.
///
/// Explicit task models override the configured default. Sending fails if
/// neither exists. Receiving waits for the first inbox message and decodes it
/// as a [`Response`]; no built-in response timeout or NATS-status handling is
/// provided. Use [`Task::send_and_wait`] with a timeout to bound the reply wait.
pub struct NatsProducer {
    client: Client,
    default_model: Arc<Option<String>>,
    prefix: String,
}

#[derive(Debug, Clone)]
/// Consumes one model subject in the fixed `task_workers` queue group.
///
/// Clones share one subscription. Replies require the incoming message's reply
/// subject; malformed JSON and missing reply subjects return errors.
pub struct NatsWorker {
    client: Client,
    subscriber: Arc<Mutex<Subscriber>>,
}

fn format_subject(prefix: &str, model: &str) -> String {
    format!("{prefix}{model}")
}

impl NatsProducer {
    /// Connects using `OPENAI_API_NATS_URL` (default `nats://localhost:4222`).
    ///
    /// Snapshots `OPENAI_API_NATS_PREFIX` (default `openai-api-queue/`) and the
    /// optional `OPENAI_API_DEFAULT_MODEL`. Connection errors are returned.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let default_model = utils::get_default_model();
        let prefix =
            env::var("OPENAI_API_NATS_PREFIX").unwrap_or_else(|_| "openai-api-queue/".into());
        let url =
            env::var("OPENAI_API_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
        let client = async_nats::ConnectOptions::new()
            .request_timeout(None)
            .connect(url)
            .await?;

        tracing::info!(
            "nats client connected to `{:?}`",
            client.server_info().connect_urls
        );

        Ok(Self {
            client,
            default_model,
            prefix,
        })
    }

    fn subject(&self, task: &Task) -> anyhow::Result<String> {
        let model = task.model(&self.default_model)?;

        Ok(format_subject(&self.prefix, &model))
    }
}

impl NatsWorker {
    /// Connects and subscribes to the prefix plus `OPENAI_API_DEFAULT_MODEL`.
    ///
    /// Requires the model variable. URL/prefix defaults match
    /// [`NatsProducer::from_env_or_default`]; missing configuration, connection,
    /// and subscription errors are returned. No subscription flush is performed.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let model = utils::get_default_model()
            .as_ref()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no model provided for the nats worker."))?;
        let prefix =
            env::var("OPENAI_API_NATS_PREFIX").unwrap_or_else(|_| "openai-api-queue/".into());
        let subject = format_subject(&prefix, &model);

        let url =
            env::var("OPENAI_API_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
        let client = async_nats::ConnectOptions::new()
            .request_timeout(None)
            .connect(url)
            .await?;

        let subscriber = client
            .queue_subscribe(subject.clone(), "task_workers".to_string())
            .await?;
        let subscriber = Arc::new(Mutex::new(subscriber));

        tracing::info!(
            "nats worker connected to `{:?}` with subject `{subject}`",
            client.server_info().connect_urls
        );

        Ok(Self { client, subscriber })
    }
}

impl QueueProducer for NatsProducer {
    type Message = Subscriber;

    async fn send_task(&self, task: Task) -> anyhow::Result<Self::Message> {
        tracing::debug!("nats queue; sending task `{}`", task.id,);

        let subject = self.subject(&task)?;

        tracing::debug!("nats queue; task `{}` subject `{subject}`", task.id,);

        let payload = serde_json::to_vec(&task)?;

        let inbox = self.client.new_inbox();
        let subscriber = self.client.subscribe(inbox.clone()).await?;

        self.client
            .publish_with_reply(subject.to_string(), inbox, payload.into())
            .await?;

        tracing::debug!("nats queue; task `{}` sent", task.id,);

        Ok(subscriber)
    }

    async fn receive_response(
        &self,
        mut message: Self::Message,
    ) -> anyhow::Result<Option<Response>> {
        match message.next().await {
            Some(m) => Ok(Some(serde_json::from_slice(&m.payload)?)),
            None => Ok(None),
        }
    }
}

impl QueueWorker for NatsWorker {
    type Message = Message;

    async fn receive_task(&self) -> anyhow::Result<Option<WrappedTask<Self::Message>>> {
        let message = match self.subscriber.lock().await.next().await {
            Some(m) => m,
            None => return Ok(None),
        };
        let task = serde_json::from_slice(&message.payload)?;

        Ok(Some(WrappedTask { message, task }))
    }

    async fn send_response(
        &self,
        message: Self::Message,
        response: Response,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&response)?;
        let reply_to = message
            .reply
            .ok_or_else(|| anyhow::anyhow!("message reply recipient empty"))?;

        self.client.publish(reply_to, payload.into()).await?;

        Ok(())
    }
}
