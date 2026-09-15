use alloc::vec::Vec;

use super::{MemoryQueue, nostd::MemoryQueueNoStd};
use crate::{
    queue::{QueueProducer as _, QueueWorker as _, WrappedTask},
    task::{Interaction, Response, Task, TaskBuilder},
};

fn task(id: u128) -> Task {
    TaskBuilder::new()
        .with_system("system prompt")
        .with_prompt("user prompt")
        .with_model("test-model")
        .with_max_tokens(128)
        .with_schema(id)
        .with_payload(id)
        .build_chat()
        .unwrap()
        .with_id(id)
}

fn response(id: u128) -> Response {
    Response::success(task(id), id as u64, "test-model", "response contents")
        .with_history(Vec::from([Interaction::assistant("assistant response")]))
}

fn wrapped(task: Task) -> WrappedTask<()> {
    WrappedTask { message: (), task }
}

#[tokio::test]
async fn tasks_are_received_in_fifo_order() {
    let queue = MemoryQueue::default();
    let tasks = [task(1), task(2), task(3)];

    for task in &tasks {
        queue.send_task(task.clone()).await.unwrap();
    }

    for task in tasks {
        assert_eq!(queue.receive_task().await.unwrap(), Some(wrapped(task)));
    }
}

#[tokio::test]
async fn responses_are_received_by_message() {
    let queue = MemoryQueue::default();
    let responses = [response(1), response(2), response(3)];
    let mut messages = Vec::new();

    for response in &responses {
        messages.push(queue.send_task(response.task.clone()).await.unwrap());
        queue.send_response((), response.clone()).await.unwrap();
    }

    for index in [2, 0, 1] {
        assert_eq!(
            queue.receive_response(messages[index]).await.unwrap(),
            Some(responses[index].clone())
        );
    }
}

#[tokio::test]
async fn clones_share_tasks_and_responses() {
    let queue = MemoryQueue::default();
    let clone = queue.clone();
    let expected_task = task(1);
    let expected_response = response(1);

    let producer_message = queue.send_task(expected_task.clone()).await.unwrap();
    let wrapped_task = clone.receive_task().await.unwrap().unwrap();
    assert_eq!(wrapped_task.task, expected_task);

    clone
        .send_response((), expected_response.clone())
        .await
        .unwrap();

    assert_eq!(
        queue.receive_response(producer_message).await.unwrap(),
        Some(expected_response)
    );
}

#[tokio::test]
async fn task_and_response_channels_are_independent() {
    let queue = MemoryQueue::default();
    let expected_task = task(1);
    let expected_response = response(1);

    queue
        .send_response((), expected_response.clone())
        .await
        .unwrap();
    let producer_message = queue.send_task(expected_task.clone()).await.unwrap();

    assert_eq!(
        queue.receive_task().await.unwrap(),
        Some(wrapped(expected_task))
    );
    assert_eq!(
        queue.receive_response(producer_message).await.unwrap(),
        Some(expected_response)
    );
}

#[tokio::test]
async fn no_std_queue_returns_none_when_empty() {
    let queue = MemoryQueueNoStd::default();

    assert_eq!(queue.receive_task().await.unwrap(), None);
    assert_eq!(queue.receive_response(1).await.unwrap(), None);
}

#[tokio::test]
async fn no_std_capacity_evicts_the_oldest_items() {
    let queue = MemoryQueueNoStd::with_capacity(2);
    let second_task = task(2);
    let third_task = task(3);
    let second_response = response(2);
    let third_response = response(3);
    let mut messages = Vec::new();

    for id in 1..=3 {
        messages.push(queue.send_task(task(id)).await.unwrap());
        queue.send_response((), response(id)).await.unwrap();
    }

    assert_eq!(
        queue.receive_task().await.unwrap(),
        Some(wrapped(second_task))
    );
    assert_eq!(
        queue.receive_task().await.unwrap(),
        Some(wrapped(third_task))
    );
    assert_eq!(queue.receive_task().await.unwrap(), None);

    assert_eq!(
        queue.receive_response(messages[1]).await.unwrap(),
        Some(second_response)
    );
    assert_eq!(
        queue.receive_response(messages[2]).await.unwrap(),
        Some(third_response)
    );
    assert_eq!(queue.receive_response(messages[0]).await.unwrap(), None);
}

#[cfg(feature = "std")]
#[tokio::test]
async fn std_capacity_applies_backpressure_without_losing_items() {
    use core::time::Duration;

    use tokio::time::timeout;

    let queue = MemoryQueue::with_capacity(1);
    let first_task = task(1);
    let second_task = task(2);

    let first_message = queue.send_task(first_task.clone()).await.unwrap();

    let blocked_send = queue.send_task(second_task.clone());
    tokio::pin!(blocked_send);
    assert!(
        timeout(Duration::from_millis(20), &mut blocked_send)
            .await
            .is_err()
    );

    assert_eq!(
        queue.receive_task().await.unwrap(),
        Some(wrapped(first_task))
    );
    let second_message = timeout(Duration::from_secs(1), blocked_send)
        .await
        .expect("send should complete after capacity becomes available")
        .unwrap();
    assert_eq!(
        queue.receive_task().await.unwrap(),
        Some(wrapped(second_task))
    );

    let first_response = response(1);
    let second_response = response(2);

    queue
        .send_response((), first_response.clone())
        .await
        .unwrap();

    let blocked_send = queue.send_response((), second_response.clone());
    tokio::pin!(blocked_send);
    assert!(
        timeout(Duration::from_millis(20), &mut blocked_send)
            .await
            .is_err()
    );

    assert_eq!(
        queue.receive_response(first_message).await.unwrap(),
        Some(first_response)
    );
    timeout(Duration::from_secs(1), blocked_send)
        .await
        .expect("send should complete after capacity becomes available")
        .unwrap();
    assert_eq!(
        queue.receive_response(second_message).await.unwrap(),
        Some(second_response)
    );
}
