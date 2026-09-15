use std::{env, iter};

use alloc::{string::String, sync::Arc};
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::chat::{
        ChatChoice, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestMessage,
        ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
        ChatCompletionResponseMessage, CreateChatCompletionRequestArgs, ResponseFormat,
        ResponseFormatJsonSchema,
    },
};

use crate::{
    executor::Executor,
    task::{Interaction, Response, Task, TaskChat, TaskType},
    utils,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
/// Executes non-streaming Chat Completions and returns the first choice's text.
///
/// Task models override the configured default. Missing models, malformed chat
/// data, and request/API failures return `Err`; missing choices or text produce
/// an unsuccessful [`Response`]. Reported usage is total tokens, or zero if absent.
///
/// Sends the optional system instruction, prior user/assistant turns, and the
/// current prompt as a user message, in that order.
///
/// # Current limitations
///
/// - System entries in input history are discarded; use [`TaskChat::system`]
///   for a persistent system instruction.
/// - Returned history currently duplicates the latest user prompt: it is kept
///   from the request messages and appended again before the assistant reply.
/// - Schemas request strict JSON output named `response`; returned text is not
///   locally parsed or validated against the schema.
/// - `max_tokens` is cast from `u64` to `u32` without range checking. Payload
///   metadata is preserved in the task but is not sent to the model.
pub struct ExecutorAsyncOpenai {
    client: Client<OpenAIConfig>,
    default_model: Arc<Option<String>>,
}

impl ExecutorAsyncOpenai {
    /// Creates a client without contacting the endpoint.
    ///
    /// Reads `OPENAI_API_URL` (default `http://127.0.0.1:8000/v1`) and snapshots
    /// `OPENAI_API_DEFAULT_MODEL`. Authentication uses `async-openai`'s default
    /// configuration. Endpoint/model validity is checked only when executing.
    pub fn from_env_or_default() -> Self {
        let default_model = utils::get_default_model();
        let url = env::var("OPENAI_API_URL").unwrap_or_else(|_| "http://127.0.0.1:8000/v1".into());
        let config = OpenAIConfig::new().with_api_base(&url);
        let client = Client::with_config(config);

        tracing::info!("openai api connected to `{url}`");

        Self {
            client,
            default_model,
        }
    }

    async fn execute_chat(
        &self,
        model: String,
        task: Task,
        chat: TaskChat,
    ) -> anyhow::Result<Response> {
        let TaskChat {
            system,
            history,
            schema,
            prompt,
        } = chat;

        let history = history
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter_map(|h| match h {
                Interaction::System(_) => None,
                Interaction::Assistant(m) => {
                    Some(ChatCompletionRequestMessage::Assistant(m.as_str().into()))
                }
                Interaction::User(m) => Some(ChatCompletionRequestMessage::User(m.as_str().into())),
            })
            .map(Some);

        let history: Vec<_> = iter::once(
            system
                .as_ref()
                .map(|s| ChatCompletionRequestMessage::System(s.as_str().into())),
        )
        .chain(history)
        .chain(iter::once(Some(ChatCompletionRequestMessage::User(
            prompt.as_str().into(),
        ))))
        .flatten()
        .collect();

        let mut request = CreateChatCompletionRequestArgs::default();

        request.model(&model);
        request.messages(history.clone());

        if let Some(t) = task.max_tokens {
            request.max_tokens(t as u32);
        }

        if let Some(schema) = schema.as_ref().cloned() {
            request.response_format(ResponseFormat::JsonSchema {
                json_schema: ResponseFormatJsonSchema {
                    name: "response".to_string(),
                    description: None,
                    schema,
                    strict: Some(true),
                },
            });
        }

        let request = request.build()?;

        tracing::debug!("task `{}` submitting", task.id);

        let response = self.client.chat().create(request).await?;
        let tokens = response
            .usage
            .as_ref()
            .map(|u| u.total_tokens as u64)
            .unwrap_or(0);

        tracing::debug!("task `{}` returned; consumed `{tokens}` tokens", task.id);

        let response = match response.choices.first() {
            Some(r) => r,
            None => {
                tracing::debug!("task `{}` error; no response provided", task.id);
                return Ok(Response::error(task, tokens, model, "no response provided"));
            }
        };

        match &response {
            ChatChoice {
                message:
                    ChatCompletionResponseMessage {
                        content: None,
                        refusal: r,
                        ..
                    },
                ..
            } => {
                let r = r
                    .as_ref()
                    .map(|r| r.as_str())
                    .unwrap_or("no reason provided");
                let m = format!("response refused: `{r}`");
                tracing::debug!("task `{}` error; `{m}`", task.id,);

                Ok(Response::error(task, tokens, model, m))
            }

            ChatChoice {
                message:
                    ChatCompletionResponseMessage {
                        content: Some(m), ..
                    },
                ..
            } => {
                let history: Vec<_> =
                    history
                        .into_iter()
                        .filter_map(|h| match h {
                            ChatCompletionRequestMessage::User(
                                ChatCompletionRequestUserMessage {
                                    content: ChatCompletionRequestUserMessageContent::Text(m),
                                    ..
                                },
                            ) => Some(Interaction::User(m)),
                            ChatCompletionRequestMessage::Assistant(
                                ChatCompletionRequestAssistantMessage {
                                    content:
                                        Some(ChatCompletionRequestAssistantMessageContent::Text(m)),
                                    ..
                                },
                            ) => Some(Interaction::Assistant(m)),
                            _ => {
                                tracing::debug!("skipping model reply {h:?}");
                                None
                            }
                        })
                        .chain(iter::once(Interaction::User(prompt)))
                        .chain(iter::once(Interaction::Assistant(m.clone())))
                        .collect();

                tracing::debug!("task `{}` served", task.id);

                Ok(Response::success(task, tokens, model, m).with_history(history))
            }
        }
    }
}

impl Executor for ExecutorAsyncOpenai {
    fn default_model(&self) -> &Arc<Option<String>> {
        &self.default_model
    }

    async fn execute(&self, task: Task) -> anyhow::Result<Response> {
        tracing::debug!("received task id `{}`", task.id);

        let model = task.model(self.default_model())?;

        tracing::debug!("task `{}` using model `{model}`", task.id);

        match &task.task_type {
            TaskType::Chat => {
                let chat = task.try_to_chat()?;

                self.execute_chat(model, task, chat).await
            }
        }
    }
}
