use std::{env, time::Duration};

use tokio::{sync::Mutex, time::timeout};

use super::{NatsProducer, NatsWorker};
use crate::{
    queue::{QueueProducer as _, QueueWorker as _},
    task::{Response, Task, TaskBuilder},
    utils,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

// The default worker instances join the same queue group and subscribe to the
// same model subject, so these integration tests must not compete for tasks.
static NATS_TEST: Mutex<()> = Mutex::const_new(());

fn worker_model() -> String {
    utils::get_default_model()
        .as_ref()
        .clone()
        .or_else(|| env::var("OPENAI_API_DEFAULT_MODEL").ok())
        .expect("set OPENAI_API_DEFAULT_MODEL when running NATS tests")
}

fn task(id: u128, model: &str) -> Task {
    TaskBuilder::new()
        .with_prompt("user prompt")
        .with_model(model)
        .build_chat()
        .unwrap()
        .with_id(id)
}

async fn default_queue() -> (NatsProducer, NatsWorker) {
    let worker = NatsWorker::from_env_or_default().await.unwrap();
    let producer = NatsProducer::from_env_or_default().await.unwrap();

    // Ensure the server has registered the worker subscription before a task
    // is published from the producer's independent connection.
    worker.client.flush().await.unwrap();

    (producer, worker)
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_worker_receives_tasks_from_default_producer() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;
    let expected = task(1, &worker_model());

    let response_message = producer.send_task(expected.clone()).await.unwrap();
    let received = timeout(TEST_TIMEOUT, worker.receive_task())
        .await
        .expect("timed out waiting for the NATS task")
        .unwrap()
        .expect("the NATS worker subscription ended");

    assert_eq!(received.task, expected);

    // Close the response subscription created by `send_task` explicitly.
    drop(response_message);
}

#[tokio::test]
#[ignore = "requires a NATS server and a configured worker model"]
async fn default_producer_receives_responses_from_default_worker() {
    let _guard = NATS_TEST.lock().await;
    let (producer, worker) = default_queue().await;
    let expected_task = task(2, &worker_model());
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
