use core::time::Duration;
use std::env;

use alloc::sync::Arc;
use async_nats::jetstream::{
    self, AckKind, Message,
    consumer::{
        AckPolicy, DeliverPolicy, ReplayPolicy,
        pull::{Config, Stream},
    },
    kv::Store,
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt as _;

use crate::{
    queue::{QueueWorker, WrappedTask, jetstream::types::TaskRecord},
    task::{Response, Task},
    utils,
};

#[derive(Clone)]
/// Receives durable tasks and saves their responses before acknowledging delivery.
///
/// Clones share one pull stream and worker ID. Receiving waits when the queue is
/// idle and marks each returned task as running in the KV bucket. A successful
/// response is stored and acknowledged; an unsuccessful response is stored and
/// terminates redelivery of that message.
///
/// Acknowledgment deadlines are not renewed during execution. Unacknowledged
/// tasks can be redelivered within the consumer's delivery and retention limits,
/// so API calls may repeat. Reaching the server's delivery limit does not itself
/// create a failed KV record or a response for the producer.
pub struct JetStreamWorker {
    id: u128,
    store: Store,
    messages: Arc<Mutex<Stream>>,
    max_deliver: i64,
    default_model: Arc<Option<String>>,
}

impl JetStreamWorker {
    /// Connects to NATS and opens or creates the queue resources and durable consumer.
    ///
    /// Reads the variables documented in the
    /// [module configuration](crate::queue::jetstream), including the consumer
    /// group, acknowledgment wait, and delivery limits. Existing resources are
    /// reused without updating their server configuration. Stream and bucket
    /// creation restrictions do not prevent creation of a missing consumer.
    ///
    /// Returns errors for connection failures, invalid configuration, or failed
    /// resource operations. The default model is optional and only supplies a
    /// fallback when this queue constructs a delivery-limit error response.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let id = utils::id();
        let default_model = utils::get_default_model();
        let client = utils::nats_client_from_env_or_default().await?;
        let js = jetstream::new(client);
        let store = TaskRecord::get_or_maybe_create_store(&js).await?.store;
        let stream = TaskRecord::get_or_maybe_create_stream(&js).await?;

        let durable_name = env::var("OPENAI_API_NATS_CONSUMER_GROUP")
            .unwrap_or_else(|_| "openai_api_dispatch_workers".into());
        let consumers_description = "OpenAI API dispatch worker consumer pool".to_string();

        let ack_wait = env::var("OPENAI_API_NATS_ACK_WAIT")
            .ok()
            .map::<Result<u64, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_ACK_WAIT: {e}"))?
            .unwrap_or(30);

        let max_deliver = env::var("OPENAI_API_NATS_MAX_DELIVER")
            .ok()
            .map::<Result<i64, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_MAX_DELIVER: {e}"))?
            .unwrap_or(3);

        let max_ack_pending = env::var("OPENAI_API_NATS_MAX_ACK_PENDING")
            .ok()
            .map::<Result<i64, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_MAX_ACK_PENDING: {e}"))?
            .unwrap_or(100);

        // TODO add suport per worker model capability
        let subject = TaskRecord::get_subject();

        let config = Config {
            durable_name: Some(durable_name.clone()),
            description: Some(consumers_description),
            filter_subject: subject,
            max_deliver,
            max_ack_pending,
            ack_wait: Duration::from_secs(ack_wait),
            ack_policy: AckPolicy::Explicit,
            deliver_policy: DeliverPolicy::All,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        };

        let consumer = stream.get_or_create_consumer(&durable_name, config).await?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await?;

        Ok(Self {
            id,
            store,
            messages: Arc::new(Mutex::new(messages)),
            max_deliver,
            default_model,
        })
    }
}

impl QueueWorker for JetStreamWorker {
    type Message = Message;

    async fn receive_task(&self) -> anyhow::Result<Option<WrappedTask<Self::Message>>> {
        let mut messages = self.messages.lock().await;

        while let Some(message) = messages.next().await {
            let message = message.map_err(|e| anyhow::anyhow!(e))?;

            let task = Task::try_from_json_bytes(&message.payload)?;
            let mut record = TaskRecord::from(&task);

            tracing::debug!("nats-jetstream queue; task `{}` received", task.id,);

            if 0 < self.max_deliver {
                let info = message.info().map_err(|e| anyhow::anyhow!(e))?;

                if self.max_deliver < info.delivered {
                    tracing::debug!(
                        "nats-jetstream queue; task `{}` rejected with delivery attempts `{}`",
                        task.id,
                        info.delivered
                    );

                    let tokens = 0;
                    let model = task.model(&self.default_model)?;
                    let error = format!(
                        "maximum deliveries `{}` for task `{}` reached",
                        info.delivered, task.id
                    );

                    let response = Response::error(task, tokens, model, error);

                    self.send_response(message, response).await?;

                    continue;
                }
            }

            tracing::debug!("nats-jetstream queue; task `{}` accepted", task.id,);

            record.running(self.id);

            self.store
                .put(&record.id, record.to_json_bytes().into())
                .await?;

            return Ok(Some(WrappedTask { message, task }));
        }

        Ok(None)
    }

    async fn send_response(
        &self,
        message: Self::Message,
        response: Response,
    ) -> anyhow::Result<()> {
        let id = response.task.id;

        tracing::debug!("nats-jetstream queue; response `{id}` received");

        let mut record = TaskRecord::from(&response.task);

        // TODO implement message retry Nack(Duration)
        let ack = if response.success {
            record.completed(response);
            AckKind::Ack
        } else {
            record.failed(response);
            AckKind::Term
        };

        self.store
            .put(&record.id, record.to_json_bytes().into())
            .await?;

        message
            .ack_with(ack)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        tracing::debug!("nats-jetstream queue; response `{id}` accepted");

        Ok(())
    }
}
