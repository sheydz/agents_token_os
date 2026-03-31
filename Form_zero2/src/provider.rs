use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
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

#[derive(Clone)]
pub struct OpenAiCompatibleProvider {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
    temperature: f64,
}

impl OpenAiCompatibleProvider {
    pub fn from_env() -> Result<Option<Self>> {
        let api_key = match std::env::var("FORM_ZERO_PROVIDER_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
        {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return Ok(None),
        };

        let base_url = std::env::var("FORM_ZERO_PROVIDER_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let model =
            std::env::var("FORM_ZERO_PROVIDER_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        let temperature = std::env::var("FORM_ZERO_PROVIDER_TEMPERATURE")
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
}

#[async_trait]
impl StreamingProvider for OpenAiCompatibleProvider {
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
}

pub fn default_provider_from_env() -> Result<Option<Arc<dyn StreamingProvider>>> {
    Ok(OpenAiCompatibleProvider::from_env()?
        .map(|provider| Arc::new(provider) as Arc<dyn StreamingProvider>))
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
