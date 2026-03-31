use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Clone)]
pub struct LiveChatProvider {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
    temperature: f64,
}

impl LiveChatProvider {
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("GEPA_ZERO_PROVIDER_API_KEY")
            .or_else(|_| std::env::var("DASHSCOPE_API_KEY"))
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .context(
                "missing API key: set GEPA_ZERO_PROVIDER_API_KEY, DASHSCOPE_API_KEY, or OPENAI_API_KEY",
            )?;
        let base_url = std::env::var("GEPA_ZERO_PROVIDER_BASE_URL")
            .unwrap_or_else(|_| "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string());
        let model = std::env::var("GEPA_ZERO_PROVIDER_MODEL")
            .unwrap_or_else(|_| "qwen3-max-2026-01-23".to_string());
        let temperature = std::env::var("GEPA_ZERO_TEMPERATURE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.1);

        let client = Client::builder()
            .timeout(Duration::from_secs(180))
            .build()
            .context("failed to build reqwest client")?;

        Ok(Self {
            client,
            base_url,
            api_key,
            model,
            temperature,
        })
    }

    pub fn model_name(&self) -> &str {
        &self.model
    }

    pub async fn complete_text(&self, messages: &[ProviderMessage]) -> Result<String> {
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.model,
                "temperature": self.temperature,
                "messages": messages,
            }))
            .send()
            .await
            .context("failed to call provider chat/completions")?
            .error_for_status()
            .context("provider returned non-success status")?;

        let value: Value = response
            .json()
            .await
            .context("failed to decode provider response json")?;

        value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("provider response missing choices[0].message.content"))
    }
}
