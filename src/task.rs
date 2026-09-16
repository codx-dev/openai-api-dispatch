use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{queue::QueueProducer, utils};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Builds a chat task; only the prompt is required at build time.
///
/// Prefer [`Self::new`] for a generated ID: derived `Default` uses ID zero.
pub struct TaskBuilder {
    /// Correlation ID; must be unique among outstanding memory-queue tasks.
    pub id: u128,
    /// Explicit model, overriding the producer/executor default when present.
    pub model: Option<String>,
    /// Requested output limit; the API executor casts this to `u32` unchecked.
    pub max_tokens: Option<u64>,
    /// Caller metadata carried through the response, not sent to the model.
    pub payload: Option<Value>,
    /// System instruction, separate from conversation history.
    pub system: Option<String>,
    /// Earlier turns; the API executor ignores system entries here.
    pub history: Option<Vec<Interaction>>,
    /// JSON schema requested as strict structured output; not locally validated.
    pub schema: Option<Value>,
    /// Current prompt; `None` fails to build, but an empty string is accepted.
    pub prompt: Option<String>,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskBuilder {
    /// Creates an empty builder with an ID from [`utils::id`].
    pub fn new() -> Self {
        Self {
            id: utils::id(),
            model: None,
            max_tokens: None,
            payload: None,
            system: None,
            history: None,
            schema: None,
            prompt: None,
        }
    }

    /// Overrides the default model; NATS uses this model to select a subject.
    pub fn with_model<M: ToString>(mut self, model: M) -> Self {
        self.model.replace(model.to_string());
        self
    }

    /// Sets the requested output limit; keep it within `u32` for the API executor.
    pub fn with_max_tokens(mut self, max_tokens: u64) -> Self {
        self.max_tokens.replace(max_tokens);
        self
    }

    /// Attaches caller metadata without including it in the model request.
    ///
    /// # Panics
    ///
    /// Panics if the payload cannot be serialized as JSON.
    pub fn with_payload<P: Serialize>(mut self, payload: P) -> Self {
        let payload = serde_json::to_value(&payload).expect("infallible serialization");
        self.payload.replace(payload);
        self
    }

    /// Sets the system instruction prepended to the model's messages.
    pub fn with_system<S: ToString>(mut self, system: S) -> Self {
        self.system.replace(system.to_string());
        self
    }

    /// Replaces earlier turns; the API executor discards system entries.
    pub fn with_history<H: IntoIterator<Item = Interaction>>(mut self, history: H) -> Self {
        self.history.replace(history.into_iter().collect());
        self
    }

    /// Sets a strict output schema without validating schema correctness.
    ///
    /// # Panics
    ///
    /// Panics if the schema cannot be serialized as JSON.
    pub fn with_schema<S: Serialize>(mut self, schema: S) -> Self {
        let schema = serde_json::to_value(&schema).expect("infallible serialization");
        self.schema.replace(schema);
        self
    }

    /// Sets the current prompt, sent as a user message by the API executor.
    pub fn with_prompt<P: ToString>(mut self, prompt: P) -> Self {
        self.prompt.replace(prompt.to_string());
        self
    }

    /// Builds a chat task, returning an error if no prompt was supplied.
    ///
    /// Model availability, token limits, and schema correctness are not checked.
    pub fn build_chat(self) -> anyhow::Result<Task> {
        let Self {
            id,
            model,
            max_tokens,
            payload,
            system,
            history,
            schema,
            prompt,
        } = self;
        let prompt =
            prompt.ok_or_else(|| anyhow::anyhow!("the prompt is mandatory for a chat task."))?;
        let contents = serde_json::to_value(TaskChat {
            system,
            history,
            schema,
            prompt,
        })
        .expect("infallible serialization");

        Ok(Task {
            id,
            task_type: TaskType::Chat,
            contents,
            model,
            max_tokens,
            payload,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Serializable envelope for work submitted to a queue or executor.
///
/// Use [`TaskBuilder`] for chat tasks. Derived `Default` has ID zero and null
/// contents, so it is not an executable chat request.
pub struct Task {
    /// Correlation ID; memory queues require uniqueness for outstanding tasks.
    pub id: u128,
    /// Operation encoded in [`Self::contents`].
    pub task_type: TaskType,
    /// JSON input matching the task type; chat tasks contain a [`TaskChat`].
    pub contents: Value,
    /// Explicit model, or `None` to use the producer/executor default.
    pub model: Option<String>,
    /// Requested output limit; the API executor casts this to `u32` unchecked.
    pub max_tokens: Option<u64>,
    /// Opaque caller metadata preserved in the response task.
    pub payload: Option<Value>,
}

impl Task {
    /// Submits this task and retrieves its response, which may be unsuccessful.
    ///
    /// With `std`, `Some(seconds)` limits only the response wait after submission
    /// and requires a Tokio runtime with time enabled. Expiry does not cancel
    /// worker execution. `None` adds no timeout; backend polling semantics still
    /// apply. Without `std`, a supplied timeout errors before sending.
    ///
    /// Submission/retrieval failures, timeout expiry, and `None` responses return
    /// errors. Check the response's `success` field separately from the `Result`.
    pub async fn send_and_wait<Q: QueueProducer>(
        self,
        queue: &Q,
        timeout_secs: Option<u64>,
    ) -> anyhow::Result<Response> {
        match timeout_secs {
            #[cfg(feature = "std")]
            Some(t) => {
                let timeout = core::time::Duration::from_secs(t);
                let message = queue.send_task(self).await?;

                let response = queue.receive_response(message);
                let response = tokio::time::timeout(timeout, response).await??;

                response.ok_or_else(|| anyhow::anyhow!("task response timeout"))
            }

            _ => {
                anyhow::ensure!(timeout_secs.is_none(), "timeout not implemented on runtime");

                let message = queue.send_task(self).await?;
                let response = queue.receive_response(message).await?;

                response.ok_or_else(|| anyhow::anyhow!("task response not available"))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Execution outcome, distinct from queue or executor transport errors.
pub struct Response {
    /// Original task; the API executor updates its history on successful output.
    pub task: Task,
    /// Whether contents represent output (`true`) or an execution failure (`false`).
    pub success: bool,
    /// Token usage; the API executor reports total tokens, or zero if absent.
    pub tokens: u64,
    /// Resolved request model; not necessarily the model name returned by the API.
    pub model: String,
    /// Output text or failure details; structured output remains JSON text.
    pub contents: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Supported operations; only chat is currently implemented.
pub enum TaskType {
    /// A chat request with [`TaskChat`] contents.
    #[default]
    Chat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Chat inputs encoded in [`Task::contents`].
pub struct TaskChat {
    /// Optional system instruction prepended to the model's messages.
    pub system: Option<String>,
    /// Previous turns; system entries are ignored by the API executor.
    pub history: Option<Vec<Interaction>>,
    /// Strict output schema requested from the endpoint, not checked locally.
    pub schema: Option<Value>,
    /// Current prompt, sent as a user message after any prior history.
    pub prompt: String,
}

impl TaskChat {
    /// Creates chat inputs with a prompt and no system, history, or schema.
    pub fn new<P: ToString>(prompt: P) -> Self {
        Self {
            prompt: prompt.to_string(),
            system: None,
            history: None,
            schema: None,
        }
    }

    /// Sets the system instruction, independently of any history entries.
    pub fn with_system<S: ToString>(mut self, system: S) -> Self {
        self.system.replace(system.to_string());
        self
    }

    /// Sets a strict output schema without checking its validity.
    ///
    /// # Panics
    ///
    /// Panics if the schema cannot be serialized as JSON.
    pub fn with_schema<S: Serialize>(mut self, schema: S) -> Self {
        // failure serialization is considered unrecoverable from the API documentation
        let schema = serde_json::to_value(&schema).expect("failed to serialize schema");
        self.schema.replace(schema);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A text-only conversation turn.
pub enum Interaction {
    /// Text previously returned by the model.
    Assistant(String),
    /// A system turn; ignored in history by the API executor.
    System(String),
    /// Text supplied by the user.
    User(String),
}

impl Task {
    /// Replaces the correlation ID; avoid duplicates among outstanding tasks.
    pub fn with_id(mut self, id: u128) -> Self {
        self.id = id;
        self
    }

    /// Sets the output limit; the API executor casts it to `u32` unchecked.
    pub fn with_max_tokens(mut self, max_tokens: u64) -> Self {
        self.max_tokens.replace(max_tokens);
        self
    }

    /// Attaches opaque caller metadata to be returned with the task.
    ///
    /// # Panics
    ///
    /// Panics if the payload cannot be serialized as JSON.
    pub fn with_payload<P: Serialize>(mut self, payload: P) -> Self {
        // failure serialization is considered unrecoverable from the API documentation
        let payload = serde_json::to_value(&payload).expect("failed to serialize payload");
        self.payload.replace(payload);
        self
    }

    /// Overrides the model used for NATS routing and API execution.
    pub fn with_model<M: ToString>(mut self, model: M) -> Self {
        self.model.replace(model.to_string());
        self
    }

    /// Decodes contents as [`TaskChat`], returning an error for incompatible JSON.
    ///
    /// Does not inspect [`Self::task_type`] or validate schema/model settings.
    pub fn try_to_chat(&self) -> anyhow::Result<TaskChat> {
        Ok(serde_json::from_value(self.contents.clone())?)
    }

    #[cfg(feature = "std")]
    pub(crate) fn model(
        &self,
        default_model: &std::sync::Arc<Option<String>>,
    ) -> anyhow::Result<String> {
        self.model
            .clone()
            .or_else(|| default_model.as_ref().clone())
            .ok_or_else(|| anyhow::anyhow!("no model provided"))
    }
}

impl Response {
    /// Creates a successful response without changing the task or its history.
    pub fn success<M: ToString, C: ToString>(
        task: Task,
        tokens: u64,
        model: M,
        contents: C,
    ) -> Self {
        Self {
            task,
            success: true,
            tokens,
            model: model.to_string(),
            contents: contents.to_string(),
        }
    }

    /// Creates an unsuccessful response with failure details in `contents`.
    pub fn error<M: ToString, C: ToString>(task: Task, tokens: u64, model: M, error: C) -> Self {
        Self {
            task,
            success: false,
            tokens,
            model: model.to_string(),
            contents: error.to_string(),
        }
    }

    /// Replaces the history stored inside the returned task's JSON contents.
    ///
    /// # Panics
    ///
    /// Panics if task contents are neither a JSON object nor null.
    pub fn with_history(mut self, history: Vec<Interaction>) -> Self {
        self.task.contents["history"] =
            serde_json::to_value(history).expect("infallible serialization");
        self
    }

    /// Decodes returned history; missing, null, or malformed history is an error.
    /// The API executor currently includes the latest user prompt twice.
    pub fn chat_history(&self) -> anyhow::Result<Vec<Interaction>> {
        let history = self
            .task
            .contents
            .get("history")
            .ok_or_else(|| anyhow::anyhow!("no history available"))?;

        Ok(serde_json::from_value(history.clone())?)
    }

    /// Returns `tokens / max(max_tokens, 1)`, clamped to `[0, 1]`, or zero if unset.
    ///
    /// This is not a billing estimate: API usage includes input tokens, whereas
    /// `max_tokens` limits output.
    pub fn usage(&self) -> f64 {
        self.task
            .max_tokens
            .map(|t| (self.tokens as f64 / t.max(1) as f64).clamp(0.0, 1.0))
            .unwrap_or(0.0)
    }
}

impl Interaction {
    /// Creates an assistant turn from text.
    pub fn assistant<A: ToString>(prompt: A) -> Self {
        Self::Assistant(prompt.to_string())
    }

    /// Creates a user turn from text.
    pub fn user<U: ToString>(prompt: U) -> Self {
        Self::User(prompt.to_string())
    }
}
