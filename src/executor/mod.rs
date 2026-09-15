use alloc::{string::String, sync::Arc};

use crate::task::{Response, Task};

// TODO add a reqwless no_std client implementation

#[cfg(feature = "std")]
/// Non-streaming Chat Completions execution through `async-openai`.
pub mod openai;

/// Executes tasks independently of their transport.
///
/// An unsuccessful [`Response`] is distinct from an execution error. Returned
/// futures do not promise `Send`, and the trait is not dyn-compatible.
pub trait Executor {
    /// Returns the fallback model used when a task does not specify one.
    fn default_model(&self) -> &Arc<Option<String>>;

    /// Executes one task; `Err` indicates execution failed before a response.
    fn execute(&self, task: Task) -> impl Future<Output = anyhow::Result<Response>>;
}

#[derive(Debug, Clone)]
/// A network-free executor for tests.
///
/// Returns the task ID as text and reports 100 tokens. Defaults to success and
/// model `dummy`; a task's explicit model takes precedence. Failure mode returns
/// `Ok(Response { success: false, .. })`, not an execution error.
pub struct DummyExecutor {
    success: bool,
    default_model: String,
    arc_default_model: Arc<Option<String>>,
}

impl Default for DummyExecutor {
    fn default() -> Self {
        let success = true;
        let default_model = String::from("dummy");
        let arc_default_model = Arc::new(Some(default_model.clone()));

        Self {
            success,
            default_model,
            arc_default_model,
        }
    }
}

impl DummyExecutor {
    /// Makes subsequent responses successful (the default).
    pub fn set_success(&mut self) {
        self.success = true;
    }

    /// Makes subsequent responses unsuccessful without returning `Err`.
    pub fn set_fail(&mut self) {
        self.success = false;
    }
}

impl Executor for DummyExecutor {
    fn default_model(&self) -> &Arc<Option<String>> {
        &self.arc_default_model
    }

    async fn execute(&self, task: Task) -> anyhow::Result<Response> {
        let tokens = 100;
        let contents = alloc::format!("{}", task.id);
        let model = task
            .model
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.default_model.clone());

        let response = if self.success {
            Response::success(task, tokens, model, contents)
        } else {
            Response::error(task, tokens, model, contents)
        };

        Ok(response)
    }
}
