//! Durable task delivery through NATS JetStream, with task status and responses
//! retained in a key-value (KV) bucket.
//!
//! [crate::queue::jetstream::JetStreamProducer] publishes JSON tasks to a work-queue stream and watches
//! their KV records for responses. [crate::queue::jetstream::JetStreamWorker] shares a durable pull
//! consumer with other workers in its group, waits for tasks, and records results
//! before acknowledging deliveries. This module requires the `jetstream-queue`
//! feature, a Tokio runtime, and a server with JetStream enabled.
//!
//! # Environment configuration
//!
//! [crate::queue::jetstream::JetStreamProducer::from_env_or_default] and
//! [crate::queue::jetstream::JetStreamWorker::from_env_or_default] read the following variables during
//! construction. Existing instances do not track subsequent environment changes.
//! Defaults apply when a variable is unset or contains non-Unicode data. Invalid
//! numeric text returns an error when the corresponding setting is parsed;
//! resource names and numeric limits are also subject to NATS validation.
//!
//! ## Connection and resource names
//!
//! | Variable | Default | Behavior |
//! | --- | --- | --- |
//! | `OPENAI_API_NATS_URL` | `nats://localhost:4222` | NATS connection URL used by both producers and workers. |
//! | `OPENAI_API_NATS_STREAM` | `OPENAI_API_DISPATCH` | Task stream name, also used as the prefix of the task subject. |
//! | `OPENAI_API_NATS_SUBJECT` | `dispatch.task` | Subject suffix. The full subject is `<stream>.<subject>`, with a literal dot inserted between the values. |
//! | `OPENAI_API_NATS_BUCKET` | `openai_api_dispatch_task` | KV bucket containing task records keyed by the decimal task ID. |
//!
//! The default task subject is `OPENAI_API_DISPATCH.dispatch.task`. Producers
//! publish to this subject and workers filter on it; task models do not select
//! subjects in this backend. Producers and workers must agree on the stream,
//! subject, and bucket. The bucket name is independent of the stream name, so use
//! distinct names for both when isolating queues. An existing task record causes
//! the producer to reuse that record instead of publishing the task again.
//!
//! ## Stream and KV provisioning
//!
//! Both constructors reuse existing resources and create missing ones by default.
//! Creation settings below do not update existing streams or buckets.
//!
//! | Variable | Default | Behavior |
//! | --- | --- | --- |
//! | `OPENAI_API_NATS_MEMORY_STORAGE` | Unset (file storage) | Presence selects memory storage for newly created task streams and KV buckets. |
//! | `OPENAI_API_NATS_TTL_SECS` | `3600` | Maximum age in seconds (`u64`) of task stream messages and KV entries. Each KV update has its own age; this is not a response-wait timeout. |
//! | `OPENAI_API_NATS_DUPLICATE_WINDOW_SECS` | `600` | Stream deduplication window in seconds (`u64`) for message IDs, which the producer sets to task IDs. KV record deduplication is separate. |
//! | `OPENAI_API_NATS_REPLICAS` | `1` | Replica count (`usize`) for the task stream. KV bucket replication uses the client's default configuration. |
//! | `OPENAI_API_NATS_MAX_BYTES` | `0` | Total KV bucket size limit in bytes (`i64`), passed to NATS through the KV configuration. Applies only when creating a bucket. |
//! | `OPENAI_API_NATS_STREAM_FORBID_CREATE` | Unset | Presence requires the task stream to exist; fetching it must succeed. Stream creation settings are skipped. |
//! | `OPENAI_API_NATS_STORE_FORBID_CREATE` | Unset | Presence requires the KV bucket to be accessible; a failed lookup returns an error instead of attempting creation. |
//!
//! The three presence flags (`MEMORY_STORAGE`, `STREAM_FORBID_CREATE`, and
//! `STORE_FORBID_CREATE`, each prefixed with `OPENAI_API_NATS_`) accept any Unicode
//! value, including an empty string, `0`, or `false`, as enabled. Unset a flag to
//! disable it. The creation restrictions apply to the stream and bucket only;
//! workers still create their durable consumer if it is missing.
//!
//! ## Worker settings
//!
//! These variables are read by [crate::queue::jetstream::JetStreamWorker::from_env_or_default]. Existing
//! durable consumers are reused without updating their server configuration.
//!
//! | Variable | Default | Behavior |
//! | --- | --- | --- |
//! | `OPENAI_API_NATS_CONSUMER_GROUP` | `openai_api_dispatch_workers` | Durable pull consumer name. Workers using the same stream and consumer share task deliveries. |
//! | `OPENAI_API_NATS_ACK_WAIT` | `30` | Time in seconds (`u64`) the consumer waits for an acknowledgment before a delivery becomes eligible for redelivery. |
//! | `OPENAI_API_NATS_MAX_DELIVER` | `3` | Consumer delivery limit per message (`i64`), including the initial delivery. Positive values also enable the worker's local delivery-count check. |
//! | `OPENAI_API_NATS_MAX_ACK_PENDING` | `100` | Maximum number of outstanding, unacknowledged messages (`i64`) across the entire consumer, shared by all its workers. |
//! | `OPENAI_API_DEFAULT_MODEL` | Unset | Optional fallback model when the worker constructs a delivery-limit error response for a task without an explicit model. |
//!
//! The default model is not required to construct a JetStream worker and does not
//! affect its subscription. Executing tasks may impose separate model requirements.
//!
mod producer;
mod types;
mod worker;

#[cfg(test)]
mod tests;

pub use producer::JetStreamProducer;
pub use worker::JetStreamWorker;
