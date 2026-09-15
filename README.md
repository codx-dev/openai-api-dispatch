# openai-api-dispatch

[![crates.io](https://img.shields.io/crates/v/openai-api-dispatch?label=latest)](https://crates.io/crates/openai-api-dispatch)
[![Documentation](https://docs.rs/openai-api-dispatch/badge.svg)](https://docs.rs/openai-api-dispatch/)
[![License](https://img.shields.io/crates/l/openai-api-dispatch.svg)](#license)

OpenAI-compatible chat requests through memory or NATS queues.

Producers submit typed tasks, workers call the configured API, and responses return through the queue.

## NATS quick start

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
    anyhow::ensure!(response.success, "{}", response.contents);
    println!("{}", response.contents);

    Ok(())
}
# #[cfg(not(feature = "nats-queue"))]
# fn main() {}
```

Workers subscribe to `openai-api-queue/<model>` by default. Override the prefix with `OPENAI_API_NATS_PREFIX`. In production, run workers and producers as separate processes.

## Configuration and behavior

Environment settings are read when constructing producers, workers, and executors.

| Variable | Default |
| --- | --- |
| `OPENAI_API_NATS_URL` | `nats://localhost:4222` |
| `OPENAI_API_NATS_PREFIX` | `openai-api-queue/` |
| `OPENAI_API_URL` | `http://127.0.0.1:8000/v1` |
| `OPENAI_API_DEFAULT_MODEL` | Unset; required by NATS workers |

- A task's explicit model overrides the default and selects its NATS subject. Prefix and model are concatenated verbatim. Workers share the `task_workers` queue group; this uses Core NATS without persistence or task retries. Constructors do not wait for server confirmation of subscriptions, so startup can race with publishing.
- Each worker handles one task at a time. Queue/API errors stop its loop without an error reply; supervise spawned workers. `send_and_wait` limits only the reply wait, not submission, and expiry does not cancel execution. Check `response.success` even when the call returns `Ok`.
- Only non-streaming chat is implemented. The current prompt is sent as a user message; system entries in input history are ignored (use `with_system`). Returned history currently duplicates the latest user prompt before the assistant reply. Schemas request strict JSON output without local validation. Keep `max_tokens` within `u32`; it is cast unchecked. `payload` is caller metadata, not model input.

## Features

Default features are `std`, `memory-queue`, and `nats-queue`; NATS implies `std`. The API executor requires `std`.

For `no_std` memory queues, set `default-features = false, features = ["memory-queue"]`. An allocator and pointer/32-bit atomics are required. This backend polls once, has no timeout support, and evicts old items when bounded; the `std` backend uses bounded Tokio channels and backpressure. Use positive capacities and unique task IDs: `TaskBuilder::default()` uses ID zero, and `no_std` generated IDs can repeat after restart or wraparound.

Custom queues/executors implement `QueueProducer`, `QueueWorker`, and `Executor`. Only the NATS/API pairing has a public `Worker` constructor; other combinations need a caller-managed loop.

## Development

Run `cargo test` for local tests. `just check` also requires `cargo-hack` and the `thumbv7em-none-eabi` target. Live integration tests are ignored by default: run `just check-nats MODEL [URL]` or `just check-openai MODEL [URL]` against configured servers.

## License

Licensed under either the [MIT license](LICENSE-MIT) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option.
