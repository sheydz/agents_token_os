use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMessage {
    pub role: String,
    pub content: String,
}

impl ProviderMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".to_string(),
            content: content.into(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_string(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderChunk {
    pub delta: String,
    pub is_final: bool,
}

impl ProviderChunk {
    pub fn delta(delta: impl Into<String>) -> Self {
        Self {
            delta: delta.into(),
            is_final: false,
        }
    }

    pub fn final_chunk() -> Self {
        Self {
            delta: String::new(),
            is_final: true,
        }
    }
}

#[async_trait]
pub trait StreamingProvider: Send + Sync {
    async fn stream_text(
        &self,
        messages: Vec<ProviderMessage>,
        chunk_tx: mpsc::UnboundedSender<ProviderChunk>,
    ) -> Result<String>;
}

#[async_trait]
pub trait TaskShotProvider: Send + Sync {
    async fn complete_text(&self, messages: Vec<ProviderMessage>) -> Result<String>;
}

#[derive(Clone)]
struct OpenAiCompatibleBackend {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
    temperature: f64,
}

struct OpenAiCompatibleEnvSpec {
    api_key_vars: &'static [&'static str],
    base_url_var: &'static str,
    model_var: &'static str,
    temperature_var: &'static str,
    default_base_url: &'static str,
    default_model: &'static str,
}

impl OpenAiCompatibleBackend {
    fn from_env_spec(spec: OpenAiCompatibleEnvSpec) -> Result<Option<Self>> {
        let api_key = spec
            .api_key_vars
            .iter()
            .find_map(|var| std::env::var(var).ok())
            .filter(|value| !value.trim().is_empty());
        let Some(api_key) = api_key else {
            return Ok(None);
        };

        let base_url = std::env::var(spec.base_url_var)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| spec.default_base_url.to_string());
        let model = std::env::var(spec.model_var)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| spec.default_model.to_string());
        let temperature = std::env::var(spec.temperature_var)
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.2);

        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("failed to build OpenAI-compatible reqwest client")?;

        Ok(Some(Self {
            client,
            base_url,
            api_key,
            model,
            temperature,
        }))
    }

    async fn stream_text(
        &self,
        messages: Vec<ProviderMessage>,
        chunk_tx: mpsc::UnboundedSender<ProviderChunk>,
    ) -> Result<String> {
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.model,
                "stream": true,
                "temperature": self.temperature,
                "messages": messages.iter().map(|message| {
                    json!({
                        "role": message.role,
                        "content": message.content,
                    })
                }).collect::<Vec<_>>(),
            }))
            .send()
            .await
            .context("failed to call OpenAI-compatible streaming endpoint")?
            .error_for_status()
            .context("provider returned non-success status")?;

        let mut full_text = String::new();
        let mut buffer = String::new();
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed to read provider stream bytes")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(line_end) = buffer.find('\n') {
                let mut line = buffer.drain(..=line_end).collect::<String>();
                if line.ends_with('\n') {
                    line.pop();
                }
                if line.ends_with('\r') {
                    line.pop();
                }
                let line = line.trim();
                if line.is_empty() || !line.starts_with("data:") {
                    continue;
                }

                let payload = line.trim_start_matches("data:").trim();
                if payload == "[DONE]" {
                    let _ = chunk_tx.send(ProviderChunk::final_chunk());
                    return Ok(full_text);
                }

                let value: Value = serde_json::from_str(payload)
                    .with_context(|| format!("invalid provider stream payload: {payload}"))?;
                let delta = value
                    .get("choices")
                    .and_then(Value::as_array)
                    .and_then(|choices| choices.first())
                    .and_then(|choice| choice.get("delta"))
                    .and_then(|delta| delta.get("content"))
                    .and_then(Value::as_str)
                    .unwrap_or("");

                if !delta.is_empty() {
                    full_text.push_str(delta);
                    let _ = chunk_tx.send(ProviderChunk::delta(delta.to_string()));
                }
            }
        }

        let _ = chunk_tx.send(ProviderChunk::final_chunk());
        Ok(full_text)
    }

    async fn complete_text(&self, messages: Vec<ProviderMessage>) -> Result<String> {
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.model,
                "stream": false,
                "temperature": self.temperature,
                "messages": messages.iter().map(|message| {
                    json!({
                        "role": message.role,
                        "content": message.content,
                    })
                }).collect::<Vec<_>>(),
            }))
            .send()
            .await
            .context("failed to call OpenAI-compatible completion endpoint")?
            .error_for_status()
            .context("provider2 returned non-success status")?;

        let value: Value = response
            .json()
            .await
            .context("failed to decode provider2 response body as JSON")?;
        extract_chat_completion_text(&value)
    }
}

fn extract_chat_completion_text(value: &Value) -> Result<String> {
    let Some(message) = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
    else {
        bail!("provider2 response missing choices[0].message");
    };

    if let Some(content) = message.get("content").and_then(Value::as_str) {
        return Ok(content.to_string());
    }

    if let Some(parts) = message.get("content").and_then(Value::as_array) {
        let text = parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.get("content").and_then(Value::as_str))
            })
            .collect::<String>();
        if !text.is_empty() {
            return Ok(text);
        }
    }

    bail!("provider2 response did not contain text content")
}

#[derive(Clone)]
pub struct OpenAiCompatibleProvider {
    backend: OpenAiCompatibleBackend,
}

impl OpenAiCompatibleProvider {
    pub fn from_env() -> Result<Option<Self>> {
        Ok(
            OpenAiCompatibleBackend::from_env_spec(OpenAiCompatibleEnvSpec {
                api_key_vars: &["FORM_ZERO_PROVIDER_API_KEY", "OPENAI_API_KEY"],
                base_url_var: "FORM_ZERO_PROVIDER_BASE_URL",
                model_var: "FORM_ZERO_PROVIDER_MODEL",
                temperature_var: "FORM_ZERO_PROVIDER_TEMPERATURE",
                default_base_url: "https://api.openai.com/v1",
                default_model: "gpt-4o-mini",
            })?
            .map(|backend| Self { backend }),
        )
    }
}

#[async_trait]
impl StreamingProvider for OpenAiCompatibleProvider {
    async fn stream_text(
        &self,
        messages: Vec<ProviderMessage>,
        chunk_tx: mpsc::UnboundedSender<ProviderChunk>,
    ) -> Result<String> {
        self.backend.stream_text(messages, chunk_tx).await
    }
}

pub fn default_provider_from_env() -> Result<Option<Arc<dyn StreamingProvider>>> {
    Ok(OpenAiCompatibleProvider::from_env()?
        .map(|provider| Arc::new(provider) as Arc<dyn StreamingProvider>))
}

#[derive(Clone)]
pub struct OpenAiCompatibleTaskShotProvider {
    backend: OpenAiCompatibleBackend,
}

impl OpenAiCompatibleTaskShotProvider {
    pub fn from_env() -> Result<Option<Self>> {
        Ok(
            OpenAiCompatibleBackend::from_env_spec(OpenAiCompatibleEnvSpec {
                api_key_vars: &["FORM_ZERO_PROVIDER2_API_KEY"],
                base_url_var: "FORM_ZERO_PROVIDER2_BASE_URL",
                model_var: "FORM_ZERO_PROVIDER2_MODEL",
                temperature_var: "FORM_ZERO_PROVIDER2_TEMPERATURE",
                default_base_url: "https://api.openai.com/v1",
                default_model: "gpt-4o-mini",
            })?
            .map(|backend| Self { backend }),
        )
    }
}

#[async_trait]
impl TaskShotProvider for OpenAiCompatibleTaskShotProvider {
    async fn complete_text(&self, messages: Vec<ProviderMessage>) -> Result<String> {
        self.backend.complete_text(messages).await
    }
}

pub fn default_task_shot_provider_from_env() -> Result<Option<Arc<dyn TaskShotProvider>>> {
    Ok(OpenAiCompatibleTaskShotProvider::from_env()?
        .map(|provider| Arc::new(provider) as Arc<dyn TaskShotProvider>))
}

#[derive(Clone)]
pub struct StaticStreamingProvider {
    response_text: String,
    chunk_size: usize,
}

impl StaticStreamingProvider {
    pub fn new(response_text: impl Into<String>) -> Self {
        Self {
            response_text: response_text.into(),
            chunk_size: 8,
        }
    }
}

#[async_trait]
impl StreamingProvider for StaticStreamingProvider {
    async fn stream_text(
        &self,
        _messages: Vec<ProviderMessage>,
        chunk_tx: mpsc::UnboundedSender<ProviderChunk>,
    ) -> Result<String> {
        if self.chunk_size == 0 {
            return Err(anyhow!("StaticStreamingProvider.chunk_size must be > 0"));
        }

        for chunk in self.response_text.as_bytes().chunks(self.chunk_size) {
            let delta = String::from_utf8_lossy(chunk).to_string();
            let _ = chunk_tx.send(ProviderChunk::delta(delta));
        }
        let _ = chunk_tx.send(ProviderChunk::final_chunk());
        Ok(self.response_text.clone())
    }
}
