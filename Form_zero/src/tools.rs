use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{control::TargetSelector, models::MessageSourceKind};

pub const TOOL_CREATE_PROGRAM: &str = "create_program";
pub const TOOL_SPAWN_REAL_PROCESS: &str = "spawn_real_process";
pub const TOOL_TERMINATE_PROCESS: &str = "terminate_process";
pub const TOOL_INVISIBLE_PROCESS: &str = "invisible_process";
pub const TOOL_SPAWN_BRANCH_PROCESS: &str = "spawn_branch_process";
pub const TOOL_MONITOR_EVENT_SEQUENCE: &str = "monitor_event_sequence";
pub const TOOL_HISTORY_QUERY: &str = "history_query";
pub const TOOL_HISTORY_ANNOTATE: &str = "history_annotate";
pub const TOOL_INSPECT_PROCESS_TABLE: &str = "inspect_process_table";
pub const TOOL_MAKE_PLAN: &str = "make_plan";
pub const TOOL_REPAIR_PLAN: &str = "repair_plan";
pub const TOOL_APPEND_OUTPUT_REF: &str = "append_output_ref";
pub const TOOL_DELETE_RESULT_ARTIFACT: &str = "delete_result_artifact";

pub const HARDWARE_OP_BOARD_INFO: &str = "board_info";
pub const HARDWARE_OP_DEVICE_CAPABILITIES: &str = "device_capabilities";
pub const HARDWARE_OP_GPIO_READ: &str = "gpio_read";
pub const HARDWARE_OP_GPIO_WRITE: &str = "gpio_write";
pub const HARDWARE_OP_SERIAL_QUERY: &str = "serial_query";
pub const HARDWARE_OP_SERIAL_WRITE: &str = "serial_write";
pub const HARDWARE_OP_PROBE_READ_MEMORY: &str = "probe_read_memory";
pub const HARDWARE_OP_FLASH_FIRMWARE: &str = "flash_firmware";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunStatus {
    Running,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolRunRecord {
    pub tool_run_id: Uuid,
    pub tool_name: String,
    pub host_process_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub accumulated_output: String,
    pub status: ToolRunStatus,
    pub process_scoped: bool,
}

#[derive(Clone, Default)]
pub struct ToolRunRegistry {
    inner: Arc<RwLock<HashMap<Uuid, ToolRunRecord>>>,
}

impl ToolRunRegistry {
    pub async fn start(
        &self,
        tool_name: &str,
        host_process_id: Uuid,
        deadline_ms: Option<u64>,
    ) -> ToolRunRecord {
        self.start_with_scope(tool_name, host_process_id, deadline_ms, true)
            .await
    }

    pub async fn start_ephemeral(
        &self,
        tool_name: &str,
        host_process_id: Uuid,
        deadline_ms: Option<u64>,
    ) -> ToolRunRecord {
        self.start_with_scope(tool_name, host_process_id, deadline_ms, false)
            .await
    }

    async fn start_with_scope(
        &self,
        tool_name: &str,
        host_process_id: Uuid,
        deadline_ms: Option<u64>,
        process_scoped: bool,
    ) -> ToolRunRecord {
        let created_at = Utc::now();
        let deadline_at = deadline_ms
            .and_then(|value| i64::try_from(value).ok())
            .map(|value| created_at + Duration::milliseconds(value));
        let record = ToolRunRecord {
            tool_run_id: Uuid::new_v4(),
            tool_name: tool_name.to_string(),
            host_process_id,
            created_at,
            deadline_at,
            accumulated_output: String::new(),
            status: ToolRunStatus::Running,
            process_scoped,
        };
        self.inner
            .write()
            .await
            .insert(record.tool_run_id, record.clone());
        record
    }

    pub async fn append_delta(&self, tool_run_id: Uuid, delta: &str) -> Option<ToolRunRecord> {
        let mut inner = self.inner.write().await;
        let record = inner.get_mut(&tool_run_id)?;
        record.accumulated_output.push_str(delta);
        Some(record.clone())
    }

    pub async fn finish(
        &self,
        tool_run_id: Uuid,
        final_output: Option<&str>,
    ) -> Option<ToolRunRecord> {
        let mut inner = self.inner.write().await;
        let record = inner.get_mut(&tool_run_id)?;
        if let Some(output) = final_output {
            record.accumulated_output = output.to_string();
        }
        record.status = ToolRunStatus::Finished;
        Some(record.clone())
    }

    pub async fn fail(&self, tool_run_id: Uuid) -> Option<ToolRunRecord> {
        let mut inner = self.inner.write().await;
        let record = inner.get_mut(&tool_run_id)?;
        record.status = ToolRunStatus::Failed;
        Some(record.clone())
    }

    pub async fn remove(&self, tool_run_id: Uuid) -> Option<ToolRunRecord> {
        self.inner.write().await.remove(&tool_run_id)
    }

    pub async fn get(&self, tool_run_id: Uuid) -> Option<ToolRunRecord> {
        self.inner.read().await.get(&tool_run_id).cloned()
    }

    pub async fn has_active_for_process(&self, process_id: Uuid) -> bool {
        self.inner.read().await.values().any(|record| {
            record.host_process_id == process_id
                && record.process_scoped
                && record.status == ToolRunStatus::Running
        })
    }
}

#[derive(Clone)]
pub struct ToolRegistry {
    names: Arc<HashSet<&'static str>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        let names = [
            TOOL_CREATE_PROGRAM,
            TOOL_SPAWN_REAL_PROCESS,
            TOOL_TERMINATE_PROCESS,
            TOOL_INVISIBLE_PROCESS,
            TOOL_SPAWN_BRANCH_PROCESS,
            TOOL_MONITOR_EVENT_SEQUENCE,
            TOOL_HISTORY_QUERY,
            TOOL_HISTORY_ANNOTATE,
            TOOL_INSPECT_PROCESS_TABLE,
            TOOL_MAKE_PLAN,
            TOOL_REPAIR_PLAN,
            TOOL_APPEND_OUTPUT_REF,
            TOOL_DELETE_RESULT_ARTIFACT,
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        Self {
            names: Arc::new(names),
        }
    }
}

impl ToolRegistry {
    pub fn contains(&self, tool_name: &str) -> bool {
        self.names.contains(tool_name)
    }
}

#[derive(Clone)]
pub struct HardwareOperationRegistry {
    names: Arc<HashSet<&'static str>>,
}

impl Default for HardwareOperationRegistry {
    fn default() -> Self {
        let names = [
            HARDWARE_OP_BOARD_INFO,
            HARDWARE_OP_DEVICE_CAPABILITIES,
            HARDWARE_OP_GPIO_READ,
            HARDWARE_OP_GPIO_WRITE,
            HARDWARE_OP_SERIAL_QUERY,
            HARDWARE_OP_SERIAL_WRITE,
            HARDWARE_OP_PROBE_READ_MEMORY,
            HARDWARE_OP_FLASH_FIRMWARE,
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        Self {
            names: Arc::new(names),
        }
    }
}

impl HardwareOperationRegistry {
    pub fn contains(&self, operation_name: &str) -> bool {
        self.names.contains(operation_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlacklistEntry {
    pub process_id: Uuid,
    pub block_mode: String,
}

pub fn parse_blacklist(policy_json: Option<&Value>) -> Vec<BlacklistEntry> {
    policy_json
        .and_then(|value| value.get("blacklist"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| serde_json::from_value::<BlacklistEntry>(item.clone()).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn upsert_blacklist_entry(policy_json: Option<Value>, entry: BlacklistEntry) -> Option<Value> {
    let existing_blacklist = parse_blacklist(policy_json.as_ref());
    let mut map = policy_json
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut blacklist = existing_blacklist
        .into_iter()
        .filter(|existing| existing.process_id != entry.process_id)
        .collect::<Vec<_>>();
    blacklist.push(entry);
    map.insert(
        "blacklist".to_string(),
        Value::Array(
            blacklist
                .into_iter()
                .map(|item| serde_json::to_value(item).unwrap_or(Value::Null))
                .collect(),
        ),
    );
    Some(Value::Object(map))
}

pub fn blacklist_blocks_message(
    policy_json: Option<&Value>,
    sender_process_id: Uuid,
    source_kind: MessageSourceKind,
    _metadata: Option<&Value>,
) -> bool {
    parse_blacklist(policy_json).into_iter().any(|entry| {
        if entry.process_id != sender_process_id {
            return false;
        }

        match entry.block_mode.as_str() {
            "all" => true,
            "explicit_only" => source_kind == MessageSourceKind::AdHoc,
            "ad_hoc_only" => source_kind == MessageSourceKind::AdHoc,
            _ => false,
        }
    })
}

pub fn tool_requires_provider(tool_name: &str, _tool_args: &Value) -> bool {
    match tool_name {
        TOOL_SPAWN_BRANCH_PROCESS | TOOL_MAKE_PLAN | TOOL_REPAIR_PLAN => true,
        _ => false,
    }
}

pub fn tool_requires_guide(tool_name: &str, tool_args: &Value) -> bool {
    match tool_name {
        TOOL_SPAWN_BRANCH_PROCESS => true,
        TOOL_MONITOR_EVENT_SEQUENCE => tool_args
            .get("mode")
            .and_then(Value::as_str)
            .map(|mode| matches!(mode, "watch" | "background_monitor"))
            .unwrap_or(false),
        _ => false,
    }
}

pub fn extract_tool_guide(tool_args: &Value) -> Option<String> {
    tool_args
        .get("输入子进程用途引导")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            tool_args
                .get("guide")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

pub fn target_selector_from_value(value: &Value) -> Option<TargetSelector> {
    if let Some(process_id) = value
        .get("process_id")
        .or_else(|| value.get("target_process_id"))
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
    {
        return Some(TargetSelector::ProcessId(process_id));
    }

    let program_run_id = value
        .get("program_run_id")
        .or_else(|| value.get("target_program_run_id"))
        .and_then(Value::as_str)?;
    let slot_name = value
        .get("program_slot_name")
        .or_else(|| value.get("slot_name"))
        .or_else(|| value.get("target_program_slot_name"))
        .or_else(|| value.get("target_slot"))
        .and_then(Value::as_str)?;
    Some(TargetSelector::ProgramSlot {
        program_run_id: program_run_id.to_string(),
        program_slot_name: slot_name.to_string(),
    })
}

pub fn tool_result_patch(
    tool_name: &str,
    tool_run_id: Uuid,
    host_process_id: Uuid,
    extra: Option<Value>,
) -> Option<Value> {
    let mut patch = serde_json::Map::new();
    patch.insert(
        "tool_name".to_string(),
        Value::String(tool_name.to_string()),
    );
    patch.insert(
        "tool_run_id".to_string(),
        Value::String(tool_run_id.to_string()),
    );
    patch.insert(
        "host_process_id".to_string(),
        Value::String(host_process_id.to_string()),
    );

    if let Some(Value::Object(extra_map)) = extra {
        for (key, value) in extra_map {
            patch.insert(key, value);
        }
    }

    Some(Value::Object(patch))
}

pub fn default_deadline_ms(tool_name: &str) -> Option<u64> {
    match tool_name {
        TOOL_MONITOR_EVENT_SEQUENCE => Some(30_000),
        TOOL_SPAWN_BRANCH_PROCESS | TOOL_MAKE_PLAN | TOOL_REPAIR_PLAN => Some(60_000),
        _ => Some(15_000),
    }
}

pub fn tool_finished_metadata(tool_name: &str, extra: Option<Value>) -> Option<Value> {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "tool_name".to_string(),
        Value::String(tool_name.to_string()),
    );
    if let Some(Value::Object(extra_map)) = extra {
        for (key, value) in extra_map {
            metadata.insert(key, value);
        }
    }
    Some(Value::Object(metadata))
}

pub fn provider_fallback_text(tool_name: &str, guide: Option<&str>, task: Option<&str>) -> String {
    let guide = guide.unwrap_or("no-guide");
    let task = task.unwrap_or("no-task");
    match tool_name {
        TOOL_SPAWN_BRANCH_PROCESS => format!("branch_result: {task} | guide={guide}"),
        TOOL_MAKE_PLAN => format!("plan_proposal: {task} | guide={guide}"),
        TOOL_REPAIR_PLAN => format!("repair_plan: {task} | guide={guide}"),
        _ => format!("tool_result: {task} | guide={guide}"),
    }
}
