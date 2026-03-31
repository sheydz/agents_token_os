use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalToolResult {
    pub success: bool,
    pub output: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[async_trait]
pub trait ExternalTool: Send + Sync {
    fn name(&self) -> &str;

    fn description(&self) -> &str;

    fn parameters_schema(&self) -> Value;

    async fn execute(&self, args: Value) -> Result<ExternalToolResult>;

    fn spec(&self) -> ExternalToolSpec {
        ExternalToolSpec {
            name: self.name().to_string(),
            description: self.description().to_string(),
            parameters: self.parameters_schema(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ExternalToolRegistry {
    tools: Arc<HashMap<String, Arc<dyn ExternalTool>>>,
}

impl ExternalToolRegistry {
    pub fn new(tools: Vec<Arc<dyn ExternalTool>>) -> Self {
        let tools = tools
            .into_iter()
            .map(|tool| (tool.name().to_string(), tool))
            .collect::<HashMap<_, _>>();
        Self {
            tools: Arc::new(tools),
        }
    }

    pub fn contains(&self, tool_name: &str) -> bool {
        self.tools.contains_key(tool_name)
    }

    pub fn get(&self, tool_name: &str) -> Option<Arc<dyn ExternalTool>> {
        self.tools.get(tool_name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self.tools.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn specs(&self) -> Vec<ExternalToolSpec> {
        let mut specs = self
            .tools
            .values()
            .map(|tool| tool.spec())
            .collect::<Vec<_>>();
        specs.sort_by(|left, right| left.name.cmp(&right.name));
        specs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoTool;

    #[async_trait]
    impl ExternalTool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }

        fn description(&self) -> &str {
            "Echo input text"
        }

        fn parameters_schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string" }
                }
            })
        }

        async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
            Ok(ExternalToolResult {
                success: true,
                output: args
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                error: None,
            })
        }
    }

    #[test]
    fn registry_contains_registered_tool() {
        let registry = ExternalToolRegistry::new(vec![Arc::new(EchoTool)]);
        assert!(registry.contains("echo"));
        assert_eq!(registry.names(), vec!["echo".to_string()]);
    }

    #[tokio::test]
    async fn tool_executes_through_registry() {
        let registry = ExternalToolRegistry::new(vec![Arc::new(EchoTool)]);
        let tool = registry.get("echo").unwrap();
        let result = tool
            .execute(serde_json::json!({ "text": "hello" }))
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.output, "hello");
    }
}
