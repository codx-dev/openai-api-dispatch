# openai-api-dispatch

[![crates.io](https://img.shields.io/crates/v/openai-api-dispatch?label=latest)](https://crates.io/crates/openai-api-dispatch)
[![Documentation](https://docs.rs/openai-api-dispatch/badge.svg)](https://docs.rs/openai-api-dispatch/)
[![License](https://img.shields.io/crates/l/openai-api-dispatch.svg)](#license)

OpenAI-compatible chat requests through memory, Core NATS, or durable JetStream queues.

Producers submit typed tasks, workers call the configured API, and responses return through the queue.

## Core NATS quick start

Run a NATS server, then configure the model and OpenAI-compatible endpoint:

```sh
export OPENAI_API_KEY=your-key
export OPENAI_API_URL=https://api.openai.com/v1
export OPENAI_API_DEFAULT_MODEL=gpt-4.1-mini
export OPENAI_API_NATS_URL=nats://127.0.0.1:4222
```

This example starts a worker and sends one call through NATS. Use the default crate features and enable Tokio's `macros`, `rt-multi-thread`, and `time` features.

```rust,no_run
# #[cfg(feature = "nats-queue")]
use openai_api_dispatch::{
    queue::nats::NatsProducer,
    task::TaskBuilder,
    worker::NatsOpenaiWorker,
};

# #[cfg(feature = "nats-queue")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let worker = NatsOpenaiWorker::from_env_or_default().await?;
    let _worker = tokio::spawn(worker.run());

    let producer = NatsProducer::from_env_or_default().await?;
    let task = TaskBuilder::new()
        .with_prompt("Reply with a one-line greeting")
        .build_chat()?;

    let response = task.send_and_wait(&producer, Some(30)).await?;
    println!("{}", response.contents);

    Ok(())
}
# #[cfg(not(feature = "nats-queue"))]
# fn main() {}
```

Workers subscribe to `openai-api-queue/<model>` by default. Override the prefix with `OPENAI_API_NATS_PREFIX`. In production, run workers and producers as separate processes.

## Durable JetStream queues

The `jetstream-queue` feature is enabled by default. Use
`queue::jetstream::JetStreamProducer` to submit tasks, and combine
`queue::jetstream::JetStreamWorker` with an executor through `worker::Worker::new`.
The `queue::jetstream` module documents all connection, provisioning, and worker
environment settings.

JetStream stores tasks in a work-queue stream and keeps status and responses in
a KV bucket. Workers save a response before acknowledging a successful task or
terminating redelivery of a failed task. Defaults are three total deliveries per
message, a 30-second acknowledgment wait, and a one-hour maximum age for task
messages and KV entries. KV updates start a new entry age. Use fresh task IDs for
new work: an existing KV record suppresses publication of that ID.

Unacknowledged tasks can be redelivered while an active worker is available and
delivery and retention limits permit it. The worker does not renew acknowledgment
deadlines or impose an execution deadline. API calls can repeat if execution
outlasts the acknowledgment wait or a worker crashes before acknowledgment.
Reaching the server's delivery limit does not itself produce an error response,
so callers should bound their response wait.

Producer and worker constructors open existing resources or create missing ones.
Existing resource configurations are not updated. Set
`OPENAI_API_NATS_STREAM_FORBID_CREATE` and `OPENAI_API_NATS_STORE_FORBID_CREATE` to
require an existing stream and KV bucket; a worker can still create its durable
consumer. Both flags are enabled by any value, including `0` or `false`.

## Configuration and behavior

Environment settings are read when constructing producers, workers, and executors.
The NATS connection and routing notes below describe Core NATS; JetStream uses
the resource and delivery settings documented in `queue::jetstream`.

| Variable | Default |
| --- | --- |
| `OPENAI_API_NATS_URL` | `nats://localhost:4222` |
| `OPENAI_API_NATS_WORKERS_GROUP` | `task_workers` |
| `OPENAI_API_NATS_PREFIX` | `openai-api-queue/` |
| `OPENAI_API_URL` | `http://127.0.0.1:8000/v1` |
| `OPENAI_API_DEFAULT_MODEL` | Unset; required by Core NATS workers |

- A task's explicit model overrides the executor default and selects its Core NATS subject. JetStream uses one configured subject for all task models.
- Core NATS constructors do not wait for server confirmation of subscriptions, so startup can race with publishing.
- Each worker handles one task at a time.
- Queue/API errors stop its loop without an error reply; supervise spawned workers.
- `send_and_wait` returns a `ValidatedResponse` after checking the response's success flag; an unsuccessful response returns `Err`. Its optional timeout limits only the reply wait, not submission, and expiry does not cancel execution.
- Direct calls to `Executor::execute` or `QueueProducer::receive_response` return raw responses; check their `success` flag even when the call returns `Ok`.
- Only non-streaming chat is implemented.
- The current prompt is sent as a user message; system entries in input history are ignored (use `with_system`).
- Schemas request strict JSON output without local validation.
- `payload` is caller metadata, not model input.
- `no_std` generated IDs can repeat after restart or wraparound.

## Features

Default features are `std`, `memory-queue`, `nats-queue`, and `jetstream-queue`.
Both NATS backends imply `std` and Tokio support. The API executor requires `std`.

#### no_std

Disable default features and enable `memory-queue` to use the polling memory
backend without `std`. An allocator and pointer/32-bit atomics are required.

This backend polls once, has no timeout support, and evicts old items when bounded; the `std` backend uses bounded Tokio channels and backpressure.

## Development

Run `cargo test` for local tests. `just check` also requires `cargo-hack` and the `thumbv7em-none-eabi` target. Live integration tests are ignored by default: run `just check-nats MODEL [URL]` or `just check-openai MODEL [URL]` against configured servers.
Run `just check-jetstream [MODEL] [URL]` against a JetStream-enabled NATS server.
Each invocation uses a separate stream and KV bucket with memory storage, and
each task uses a fresh ID so retained results do not suppress test submissions.

## License

Licensed under either the [MIT license](LICENSE-MIT) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option.
