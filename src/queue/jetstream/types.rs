use core::time::Duration;
use std::env;

use async_nats::jetstream::{
    Context,
    kv::{self, Store},
    stream::{Config, DiscardPolicy, RetentionPolicy, StorageType, Stream},
};
use serde::{Deserialize, Serialize};

use crate::task::{Response, Task};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Task lifecycle state stored in the JetStream KV bucket.
pub enum TaskStatus {
    /// Recorded as queued, awaiting a worker.
    Queued,
    /// Received by a worker for execution.
    Running {
        /// ID of the worker that most recently received the task.
        worker: u128,
    },
    /// Recorded as completed with a retained response.
    Completed {
        /// Response saved when the task completed successfully.
        result: Response,
    },
    /// Recorded as failed with a retained error response.
    Failed {
        /// Unsuccessful response saved for the task.
        error: Response,
    },
}

#[derive(Debug, Clone)]
/// KV store and task stream addressing resolved during queue construction.
pub struct StoreStreamSubject {
    /// KV bucket containing task status and retained responses.
    pub store: Store,
    /// Name of the stream that stores task messages.
    pub stream: String,
    /// Full subject used to publish and receive task messages.
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Task ID and status retained in JetStream KV and used as a producer response handle.
///
/// Converting from a task creates a queued record. Status methods update only
/// this in-memory value; they do not write to KV, acknowledge deliveries, or
/// validate that a response matches the record's ID or status.
pub struct TaskRecord {
    /// Decimal task ID, used as the KV key.
    pub id: String,
    /// Latest recorded lifecycle state and any terminal response.
    pub status: TaskStatus,
}

impl From<&Task> for TaskRecord {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id.to_string(),
            status: TaskStatus::Queued,
        }
    }
}

impl TaskRecord {
    /// Serializes the task record into JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("infallible serialization")
    }

    /// Deserializes the task record from JSON bytes.
    pub fn try_from_json_bytes<B: AsRef<[u8]>>(bytes: B) -> anyhow::Result<Self> {
        let bytes = bytes.as_ref();

        Ok(serde_json::from_slice(bytes)?)
    }

    /// Marks this record completed and retains a copy of the supplied response.
    pub fn completed(&mut self, response: Response) {
        self.status = TaskStatus::Completed {
            result: response.clone(),
        };
    }

    /// Marks this record failed and retains a copy of the supplied response.
    pub fn failed(&mut self, response: Response) {
        self.status = TaskStatus::Failed {
            error: response.clone(),
        };
    }

    /// Marks this record running under the supplied worker ID.
    pub fn running(&mut self, worker: u128) {
        self.status = TaskStatus::Running { worker };
    }

    pub(crate) fn get_max_age() -> anyhow::Result<Duration> {
        let max_age = env::var("OPENAI_API_NATS_TTL_SECS")
            .ok()
            .map::<Result<u64, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_TTL_SECS: {e}"))?
            .unwrap_or(3600);

        Ok(Duration::from_secs(max_age))
    }

    fn get_stream() -> String {
        env::var("OPENAI_API_NATS_STREAM").unwrap_or_else(|_| "OPENAI_API_DISPATCH".into())
    }

    fn get_storage_type() -> StorageType {
        if env::var("OPENAI_API_NATS_MEMORY_STORAGE").ok().is_some() {
            StorageType::Memory
        } else {
            StorageType::File
        }
    }

    pub(crate) fn get_subject() -> String {
        let stream = Self::get_stream();
        let subject =
            env::var("OPENAI_API_NATS_SUBJECT").unwrap_or_else(|_| "dispatch.task".into());

        format!("{stream}.{subject}")
    }

    pub(crate) async fn get_or_maybe_create_stream(js: &Context) -> anyhow::Result<Stream> {
        let stream = Self::get_stream();

        tracing::info!("nats-jetstream queue; using stream `{stream}`",);

        if env::var("OPENAI_API_NATS_STREAM_FORBID_CREATE")
            .ok()
            .is_some()
        {
            tracing::info!(
                "nats-jetstream queue; stream creation forbidden by OPENAI_API_NATS_STREAM_FORBID_CREATE, fetching...",
            );
            return Ok(js.get_stream(&stream).await?);
        }

        let subjects = vec![Self::get_subject()];

        let max_age = Self::get_max_age()?;
        let storage = Self::get_storage_type();

        let duplicate_window = env::var("OPENAI_API_NATS_DUPLICATE_WINDOW_SECS")
            .ok()
            .map::<Result<u64, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_DUPLICATE_WINDOW_SECS: {e}"))?
            .unwrap_or(600);

        let num_replicas = env::var("OPENAI_API_NATS_REPLICAS")
            .ok()
            .map::<Result<usize, _>, _>(|ttl| ttl.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_REPLICAS: {e}"))?
            .unwrap_or(1);

        let config = Config {
            name: stream,
            subjects,
            retention: RetentionPolicy::WorkQueue,
            discard: DiscardPolicy::New,
            duplicate_window: Duration::from_secs(duplicate_window),
            max_age,
            storage,
            num_replicas,
            ..Default::default()
        };

        Ok(js.get_or_create_stream(config).await?)
    }

    pub(crate) async fn get_or_maybe_create_store(
        js: &Context,
    ) -> anyhow::Result<StoreStreamSubject> {
        let stream = Self::get_stream();
        let subject = Self::get_subject();
        let bucket = env::var("OPENAI_API_NATS_BUCKET")
            .unwrap_or_else(|_| "openai_api_dispatch_task".into());

        tracing::info!(
            "nats-jetstream queue; using stream `{stream}`, subject `{subject}`, bucket `{bucket}`"
        );

        let store = match js.get_key_value(&bucket).await {
            Ok(s) => s,
            Err(e) => {
                tracing::info!("nats-jetstream queue; bucket `{bucket}` not present, creating...",);

                anyhow::ensure!(
                    env::var("OPENAI_API_NATS_STORE_FORBID_CREATE")
                        .ok()
                        .is_none(),
                    "nats-jetstream queue; store unavailable and OPENAI_API_NATS_STORE_FORBID_CREATE set: {e}"
                );

                let max_age = TaskRecord::get_max_age()?;
                let storage = Self::get_storage_type();
                let max_bytes = env::var("OPENAI_API_NATS_MAX_BYTES")
                    .ok()
                    .map::<Result<i64, _>, _>(|ttl| ttl.parse())
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("invalid OPENAI_API_NATS_MAX_BYTES: {e}"))?
                    .unwrap_or(0);

                js.create_key_value(kv::Config {
                    bucket,
                    max_age,
                    max_bytes,
                    description: "openai-api-dispatch tasks bucket".to_string(),
                    storage,
                    history: 1,
                    ..Default::default()
                })
                .await?
            }
        };

        Ok(StoreStreamSubject {
            store,
            stream,
            subject,
        })
    }
}
