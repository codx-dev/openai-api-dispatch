use std::{env, time::Duration};

use tokio::{sync::Mutex, time::timeout};

use super::{JetStreamProducer, JetStreamWorker};
use crate::{
    queue::{QueueProducer as _, QueueWorker as _},
    task::{Response, Task, TaskBuilder},
    utils,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

// The default workers share a durable consumer, so tests within one invocation
// must not compete for tasks. The just recipe isolates stream and bucket names.
static NATS_TEST: Mutex<()> = Mutex::const_new(());

fn worker_model() -> String {
    utils::get_default_model()
        .as_ref()
        .clone()
        .or_else(|| env::var("OPENAI_API_DEFAULT_MODEL").ok())
        .expect("set OPENAI_API_DEFAULT_MODEL when running NATS tests")
}

fn task(model: &str) -> Task {
    TaskBuilder::new()
        .with_prompt("user prompt")
        .with_model(model)
        .build_chat()
        .unwrap()
}

async fn default_queue() -> (JetStreamProducer, JetStreamWorker) {
    let worker = JetStreamWorker::from_env_or_default().await.unwrap();
    let producer = JetStreamProducer::from_env_or_default().await.unwrap();

    (producer, worker)
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_worker_receives_tasks_from_default_producer() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;
    let expected = task(&worker_model());

    producer.send_task(expected.clone()).await.unwrap();
    let received = timeout(TEST_TIMEOUT, worker.receive_task())
        .await
        .expect("timed out waiting for the NATS task")
        .unwrap()
        .expect("the NATS worker subscription ended");

    assert_eq!(received.task, expected);

    worker
        .send_response(
            received.message,
            Response::success(expected, 0, worker_model(), "response"),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_producer_receives_responses_from_default_worker() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;
    let expected_task = task(&worker_model());
    let producer_message = producer.send_task(expected_task.clone()).await.unwrap();
    let worker_message = timeout(TEST_TIMEOUT, worker.receive_task())
        .await
        .expect("timed out waiting for the NATS task")
        .unwrap()
        .expect("the NATS worker subscription ended");
    let expected_response = Response::success(expected_task, 42, worker_model(), "response");

    worker
        .send_response(worker_message.message, expected_response.clone())
        .await
        .unwrap();

    let received = timeout(TEST_TIMEOUT, producer.receive_response(producer_message))
        .await
        .expect("timed out waiting for the NATS response")
        .unwrap();

    assert_eq!(received, Some(expected_response));
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_worker_waits_for_tasks_when_queue_is_empty() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;

    // Check both the initial idle queue and an idle period after completing work.
    for _ in 0..2 {
        let receive = worker.receive_task();
        tokio::pin!(receive);
        assert!(
            timeout(Duration::from_millis(100), &mut receive)
                .await
                .is_err(),
            "the worker stopped waiting while the queue was empty"
        );

        let expected = task(&worker_model());
        producer.send_task(expected.clone()).await.unwrap();
        let received = timeout(TEST_TIMEOUT, receive)
            .await
            .expect("timed out waiting for the JetStream task")
            .unwrap()
            .expect("the JetStream worker subscription ended");

        assert_eq!(received.task, expected);
        worker
            .send_response(
                received.message,
                Response::success(expected, 0, worker_model(), "response"),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_worker_preserves_pending_tasks_across_calls_and_clones() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;
    let cloned_worker = worker.clone();
    let tasks = [
        task(&worker_model()),
        task(&worker_model()),
        task(&worker_model()),
    ];

    for expected in &tasks {
        producer.send_task(expected.clone()).await.unwrap();
    }

    for (receiver, expected) in [&worker, &cloned_worker, &worker].into_iter().zip(tasks) {
        let received = timeout(TEST_TIMEOUT, receiver.receive_task())
            .await
            .expect("timed out waiting for a pending JetStream task")
            .unwrap()
            .expect("the JetStream worker subscription ended");

        assert_eq!(received.task, expected);
        receiver
            .send_response(
                received.message,
                Response::success(expected, 0, worker_model(), "response"),
            )
            .await
            .unwrap();
    }
}
