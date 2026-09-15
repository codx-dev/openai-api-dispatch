use std::time::Duration;

use serde_json::Value;
use tokio::time::timeout;

use super::ExecutorAsyncOpenai;
use crate::{
    executor::Executor as _,
    task::{Interaction, TaskBuilder},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

fn default_executor() -> (ExecutorAsyncOpenai, String) {
    let executor = ExecutorAsyncOpenai::from_env_or_default();
    let model = executor
        .default_model()
        .as_ref()
        .clone()
        .expect("set OPENAI_API_DEFAULT_MODEL when running OpenAI executor tests");

    (executor, model)
}

#[tokio::test]
#[ignore = "requires an OpenAI-compatible server and OPENAI_API_DEFAULT_MODEL"]
async fn default_executor_completes_a_chat_task_with_a_system_prompt() {
    let (executor, model) = default_executor();
    let task = TaskBuilder::new()
        .with_system("Reply briefly.")
        .with_prompt("Say hello.")
        .with_max_tokens(64)
        .build_chat()
        .unwrap();

    let response = timeout(TEST_TIMEOUT, executor.execute(task))
        .await
        .expect("timed out waiting for the OpenAI response")
        .unwrap();

    assert!(
        response.success,
        "executor returned an unsuccessful response: {response:?}"
    );
    assert_eq!(response.model, model);
    assert!(!response.contents.trim().is_empty());

    let chat = response.task.try_to_chat().unwrap();
    assert_eq!(chat.system.as_deref(), Some("Reply briefly."));
    assert_eq!(
        chat.history.unwrap(),
        [
            Interaction::user("Say hello."),
            Interaction::assistant(response.contents),
        ]
    );
}

#[tokio::test]
#[ignore = "requires an OpenAI-compatible server and OPENAI_API_DEFAULT_MODEL"]
async fn default_executor_returns_schema_constrained_output() {
    let (executor, model) = default_executor();
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "answer": {
                "type": "string"
            }
        },
        "required": ["answer"],
        "additionalProperties": false
    });
    let task = TaskBuilder::new()
        .with_schema(schema.clone())
        .with_prompt("Return a short greeting in the answer field.")
        .with_max_tokens(64)
        .build_chat()
        .unwrap();

    let response = timeout(TEST_TIMEOUT, executor.execute(task))
        .await
        .expect("timed out waiting for the OpenAI response")
        .unwrap();

    assert!(
        response.success,
        "executor returned an unsuccessful response: {response:?}"
    );
    assert_eq!(response.model, model);

    let output: Value = serde_json::from_str(&response.contents)
        .expect("schema-constrained response should contain valid JSON");
    let output = output
        .as_object()
        .expect("schema-constrained response should be an object");
    assert_eq!(output.len(), 1);
    assert!(output.get("answer").is_some_and(Value::is_string));

    let chat = response.task.try_to_chat().unwrap();
    assert_eq!(chat.schema, Some(schema));
}

#[tokio::test]
#[ignore = "requires an OpenAI-compatible server and OPENAI_API_DEFAULT_MODEL"]
async fn default_executor_preserves_user_and_assistant_history() {
    let (executor, model) = default_executor();
    let task = TaskBuilder::new()
        .with_history([
            Interaction::System("old system message".into()),
            Interaction::user("My name is Ada."),
            Interaction::assistant("Hello, Ada."),
        ])
        .with_prompt("What is my name?")
        .with_max_tokens(64)
        .build_chat()
        .unwrap();

    let response = timeout(TEST_TIMEOUT, executor.execute(task))
        .await
        .expect("timed out waiting for the OpenAI response")
        .unwrap();

    assert!(
        response.success,
        "executor returned an unsuccessful response: {response:?}"
    );
    assert_eq!(response.model, model);

    let chat = response.task.try_to_chat().unwrap();
    assert_eq!(
        chat.history.unwrap(),
        [
            Interaction::user("My name is Ada."),
            Interaction::assistant("Hello, Ada."),
            Interaction::user("What is my name?"),
            Interaction::assistant(response.contents),
        ]
    );
}
