use async_nats::{
    header::{NATS_EXPECTED_STREAM, NATS_MESSAGE_ID},
    jetstream::{self, kv::Store},
};
use tokio_stream::StreamExt as _;

use crate::{
    queue::{
        QueueProducer,
        jetstream::types::{StoreStreamSubject, TaskRecord, TaskStatus},
    },
    task::{Response, Task},
    utils,
};

#[derive(Debug, Clone)]
/// Publishes durable tasks and retrieves their responses from a JetStream KV bucket.
///
/// Tasks are published to the configured subject, independently of their model.
/// An existing, valid KV record for a task ID is reused without publishing again.
/// Receiving a response waits for a completed or failed record and returns the
/// stored [`Response`], whose success flag may be false. Use [`Task::send_and_wait`]
/// to check that flag and optionally limit the response wait.
///
/// KV writes and task publication are separate operations. A publish error can
/// leave a queued record without a corresponding stream message; reusing that
/// task ID returns the record without retrying publication.
pub struct JetStreamProducer {
    js: jetstream::Context,
    store: Store,
    stream: String,
    subject: String,
}

impl JetStreamProducer {
    /// Connects to NATS and opens or creates the configured task stream and KV bucket.
    ///
    /// Reads the connection and provisioning variables documented in the
    /// [module configuration](crate::queue::jetstream). Existing resources are
    /// reused without updating their configuration. A creation-forbidden flag
    /// requires the corresponding resource to be accessible already.
    ///
    /// Returns errors for connection failures, invalid configuration, or failed
    /// resource lookup or creation. No default model is required.
    pub async fn from_env_or_default() -> anyhow::Result<Self> {
        let client = utils::nats_client_from_env_or_default().await?;
        let js = jetstream::new(client);

        TaskRecord::get_or_maybe_create_stream(&js).await?;

        let StoreStreamSubject {
            store,
            stream,
            subject,
        } = TaskRecord::get_or_maybe_create_store(&js).await?;

        Ok(Self {
            js,
            store,
            stream,
            subject,
        })
    }
}

impl QueueProducer for JetStreamProducer {
    type Message = TaskRecord;

    async fn send_task(&self, task: Task) -> anyhow::Result<Self::Message> {
        tracing::debug!("nats-jetstream queue; sending task `{}`", task.id,);

        let record = TaskRecord::from(&task);

        match self.store.entry(&record.id).await? {
            Some(entry) => match TaskRecord::try_from_json_bytes(entry.value) {
                Ok(r) => {
                    tracing::debug!("nats-jetstream queue; already queued `{}`", task.id,);
                    return Ok(r);
                }
                Err(e) => {
                    tracing::warn!(
                        "nats-jetstream queue; overwriting invalid task `{}` on store({}): {e}",
                        entry.revision,
                        task.id,
                    );
                    self.store
                        .update(&record.id, record.to_json_bytes().into(), entry.revision)
                        .await?;
                }
            },
            None => {
                self.store
                    .put(&record.id, record.to_json_bytes().into())
                    .await?;
            }
        }

        let mut headers = async_nats::HeaderMap::new();

        headers.insert(NATS_MESSAGE_ID, task.id.to_string());
        headers.insert(NATS_EXPECTED_STREAM, self.stream.as_str());

        let payload = task.to_json_bytes();
        let ack = self
            .js
            .publish_with_headers(self.subject.clone(), headers, payload.into())
            .await?
            .await?;

        if ack.duplicate {
            tracing::warn!(
                "nats-jetstream queue; duplicated task `{}` post KV check",
                task.id,
            );
            anyhow::bail!("task `{}` discarded; duplicated", task.id);
        }

        tracing::debug!("nats-jetstream queue; task `{}` sent", task.id,);

        Ok(record)
    }

    async fn receive_response(&self, record: Self::Message) -> anyhow::Result<Option<Response>> {
        let mut watcher = self.store.watch_with_history(&record.id).await?;

        while let Some(maybe_entry) = watcher.next().await {
            let entry = maybe_entry?;

            if entry.value.is_empty() {
                tracing::trace!(
                    "nats-jetstream queue; skipping empty tombstone `{}`",
                    record.id,
                );
                continue;
            }

            match TaskRecord::try_from_json_bytes(&entry.value)?.status {
                TaskStatus::Queued => {
                    tracing::trace!("nats-jetstream queue; waiting task queued `{}`", record.id,);
                }
                TaskStatus::Running { worker } => {
                    tracing::trace!(
                        "nats-jetstream queue; waiting task queued `{}` with worker `{}`",
                        record.id,
                        worker
                    );
                }
                TaskStatus::Completed { result } => {
                    anyhow::ensure!(
                        result.success,
                        "nats-jetstream queue; task `{}` completed but with failed `{}`",
                        record.id,
                        result.contents
                    );

                    tracing::debug!(
                        "nats-jetstream queue; received response on task `{}`",
                        record.id,
                    );

                    return Ok(Some(result));
                }
                TaskStatus::Failed { error } => {
                    anyhow::ensure!(
                        !error.success,
                        "nats-jetstream queue; task `{}` completed but with failed `{}`",
                        record.id,
                        error.contents
                    );

                    tracing::debug!(
                        "nats-jetstream queue; received error response on task `{}`",
                        record.id,
                    );

                    return Ok(Some(error));
                }
            }
        }

        anyhow::bail!("nats-jetstream queue; no response for task `{}`", record.id,);
    }
}
