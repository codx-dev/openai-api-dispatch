#![cfg(all(feature = "memory-queue", feature = "std"))]

use core::time::Duration;

use tokio::task::JoinHandle;

use super::Worker;
use crate::{
    executor::{DummyExecutor, Executor as _},
    queue::{QueueProducer as _, QueueWorker as _, memory::MemoryQueue},
    task::{Interaction, Response, Task, TaskBuilder},
};

const RESPONSE_TIMEOUT_SECS: u64 = 5;

fn producer_and_worker(
    executor: DummyExecutor,
) -> (MemoryQueue, Worker<MemoryQueue, DummyExecutor>) {
    let producer = MemoryQueue::default();
    let worker = Worker {
        queue: producer.clone(),
        executor,
    };

    (producer, worker)
}

fn task(id: u128, model: Option<&str>) -> Task {
    let builder = TaskBuilder::new().with_prompt("user prompt");
    let builder = match model {
        Some(model) => builder.with_model(model),
        None => builder,
    };

    builder.build_chat().unwrap().with_id(id)
}

fn start_worker(executor: DummyExecutor) -> (MemoryQueue, JoinHandle<anyhow::Result<()>>) {
    let (producer, worker) = producer_and_worker(executor);

    (producer, tokio::spawn(worker.run()))
}

async fn stop_worker(worker_handle: JoinHandle<anyhow::Result<()>>) {
    worker_handle.abort();
    let error = worker_handle
        .await
        .expect_err("the worker should run until it is cancelled");
    assert!(error.is_cancelled());
}

async fn receive_response(producer: &MemoryQueue, message: u128) -> Response {
    tokio::time::timeout(
        Duration::from_secs(RESPONSE_TIMEOUT_SECS),
        producer.receive_response(message),
    )
    .await
    .expect("the worker should return a response before the timeout")
    .expect("the memory queue should receive a response")
    .expect("the response channel should remain open")
}

async fn send_to_worker_with_executor(task: Task, executor: DummyExecutor) -> Response {
    let (producer, worker_handle) = start_worker(executor);
    let response = task
        .send_and_wait(&producer, Some(RESPONSE_TIMEOUT_SECS))
        .await;

    // A memory queue has no shutdown signal, so stop the worker once the
    // response under test has been received.
    stop_worker(worker_handle).await;

    response.expect("the worker should return a response before the timeout")
}

async fn send_to_worker(task: Task) -> Response {
    send_to_worker_with_executor(task, DummyExecutor::default()).await
}

#[tokio::test]
async fn worker_processes_a_memory_queue_task_with_the_default_model() {
    let expected_task = task(0, None);
    let expected_response = Response::success(expected_task.clone(), 100, "dummy", "0");

    assert_eq!(send_to_worker(expected_task).await, expected_response);
}

#[tokio::test]
async fn worker_processes_a_memory_queue_task_with_its_requested_model() {
    let expected_task = task(2, Some("requested-model"));
    let expected_response = Response::success(expected_task.clone(), 100, "requested-model", "2");

    assert_eq!(send_to_worker(expected_task).await, expected_response);
}

#[tokio::test]
async fn worker_sends_unsuccessful_responses_and_continues_processing() {
    let mut executor = DummyExecutor::default();
    executor.set_fail();
    let (producer, worker_handle) = start_worker(executor);
    let first_task = task(3, None);
    let second_task = task(4, Some("requested-model"));

    let first_message = producer.send_task(first_task.clone()).await.unwrap();
    let second_message = producer.send_task(second_task.clone()).await.unwrap();
    let first_response = receive_response(&producer, first_message).await;
    let second_response = receive_response(&producer, second_message).await;
    stop_worker(worker_handle).await;

    assert_eq!(
        first_response,
        Response::error(first_task, 100, "dummy", "3")
    );
    assert_eq!(
        second_response,
        Response::error(second_task, 100, "requested-model", "4")
    );
}

#[tokio::test]
async fn worker_preserves_boundary_and_optional_task_values() {
    let expected_task = TaskBuilder::new()
        .with_system("")
        .with_history([
            Interaction::System(String::new()),
            Interaction::user(""),
            Interaction::assistant(""),
        ])
        .with_schema(serde_json::json!({}))
        .with_payload(serde_json::json!([]))
        .with_prompt("")
        .with_model("")
        .with_max_tokens(0)
        .build_chat()
        .unwrap()
        .with_id(u128::MAX);
    let expected_response =
        Response::success(expected_task.clone(), 100, "", u128::MAX.to_string());

    assert_eq!(send_to_worker(expected_task).await, expected_response);
}

#[tokio::test]
async fn worker_matches_prequeued_responses_when_received_out_of_order() {
    let (producer, worker) = producer_and_worker(DummyExecutor::default());
    let tasks = [task(10, None), task(11, None), task(12, None)];
    let mut messages = Vec::new();

    for task in &tasks {
        messages.push(producer.send_task(task.clone()).await.unwrap());
    }

    let worker_handle = tokio::spawn(worker.run());
    let third_response = receive_response(&producer, messages[2]).await;
    let first_response = receive_response(&producer, messages[0]).await;
    let second_response = receive_response(&producer, messages[1]).await;
    stop_worker(worker_handle).await;

    assert_eq!(
        third_response,
        Response::success(tasks[2].clone(), 100, "dummy", "12")
    );
    assert_eq!(
        first_response,
        Response::success(tasks[0].clone(), 100, "dummy", "10")
    );
    assert_eq!(
        second_response,
        Response::success(tasks[1].clone(), 100, "dummy", "11")
    );
}

#[tokio::test]
async fn worker_handles_concurrent_producer_clones() {
    let (producer, worker_handle) = start_worker(DummyExecutor::default());
    let first_producer = producer.clone();
    let second_producer = producer.clone();
    let first_task = task(20, None);
    let second_task = task(21, Some("second-model"));
    let expected_first_task = first_task.clone();
    let expected_second_task = second_task.clone();

    let (first_response, second_response) = tokio::join!(
        first_task.send_and_wait(&first_producer, Some(RESPONSE_TIMEOUT_SECS)),
        second_task.send_and_wait(&second_producer, Some(RESPONSE_TIMEOUT_SECS)),
    );
    stop_worker(worker_handle).await;

    assert_eq!(
        first_response.unwrap(),
        Response::success(expected_first_task, 100, "dummy", "20")
    );
    assert_eq!(
        second_response.unwrap(),
        Response::success(expected_second_task, 100, "second-model", "21")
    );
}

#[tokio::test]
async fn worker_delegates_queue_worker_operations_to_the_memory_queue() {
    let (producer, worker) = producer_and_worker(DummyExecutor::default());
    let expected_task = task(30, None);
    let expected_response = Response::success(expected_task.clone(), 7, "model", "contents");
    let producer_message = producer.send_task(expected_task.clone()).await.unwrap();

    let wrapped_task = worker.receive_task().await.unwrap().unwrap();
    assert_eq!(wrapped_task.task, expected_task);

    worker
        .send_response((), expected_response.clone())
        .await
        .unwrap();
    assert_eq!(
        producer.receive_response(producer_message).await.unwrap(),
        Some(expected_response)
    );
}

#[tokio::test]
async fn worker_delegates_executor_operations_to_the_dummy_executor() {
    let (_, worker) = producer_and_worker(DummyExecutor::default());
    let expected_task = task(40, None);

    assert_eq!(worker.default_model().as_ref().as_deref(), Some("dummy"));
    assert_eq!(
        worker.execute(expected_task.clone()).await.unwrap(),
        Response::success(expected_task, 100, "dummy", "40")
    );
}

#[tokio::test]
#[cfg(feature = "std")]
async fn memory_queue_producer_times_out_without_a_running_worker() {
    let producer = MemoryQueue::default();

    let error = task(50, None)
        .send_and_wait(&producer, Some(RESPONSE_TIMEOUT_SECS))
        .await
        .expect_err("a producer without a worker should time out");

    assert!(error.is::<tokio::time::error::Elapsed>());
}
