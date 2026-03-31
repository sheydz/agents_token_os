use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnerKind {
    PrefixSuffixDefinite,
    Process,
}

impl OwnerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PrefixSuffixDefinite => "prefix_suffix_definite",
            Self::Process => "process",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionPart {
    Prefix,
    Suffix,
    BuiltAll,
}

impl DefinitionPart {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prefix => "prefix",
            Self::Suffix => "suffix",
            Self::BuiltAll => "built_all",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefiniteRow {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_json: Value,
    pub version: i64,
    pub status: String,
    pub metadata_json: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixSuffixDefiniteRow {
    pub id: Uuid,
    pub prefix_name: String,
    pub suffix_name: String,
    pub compile_version: i64,
    pub compile_status: String,
    pub metadata_json: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramRow {
    pub program_run_id: String,
    pub workflow_definite_id: Uuid,
    pub status: String,
    pub plan_state_json: ProgramPlanState,
    pub metadata_json: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramProcessBindingRow {
    pub program_run_id: String,
    pub program_slot_name: String,
    pub process_id: Uuid,
    pub attached_at: DateTime<Utc>,
    pub metadata_json: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundProcessRow {
    pub binding: ProgramProcessBindingRow,
    pub process: ProcessInstanceRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRow {
    pub id: Uuid,
    pub owner_kind: OwnerKind,
    pub owner_id: Uuid,
    pub owner_seq: i64,
    pub definition_part: Option<DefinitionPart>,
    pub segment_kind: String,
    pub content: String,
    pub token_count: i32,
    pub tokenizer: Option<String>,
    pub patch: Option<Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInstanceRow {
    pub id: Uuid,
    pub external_slot_name: String,
    pub prefix_suffix_definite_id: Uuid,
    pub status: String,
    pub policy_json: Option<Value>,
    pub last_segment_seq: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessPromptRow {
    pub process_id: Uuid,
    pub prompt_text: String,
    pub prompt_version: i64,
    pub prompt_parent_process_id: Option<Uuid>,
    pub adopted_mean_score: Option<f64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const HIDDEN_OPTIMIZER_SLOT_NAME: &str = "__optimizer__";
pub const HIDDEN_SCORE_JUDGE_SLOT_NAME: &str = "__score_judge__";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRuntimeBinding {
    pub process: ProcessInstanceRow,
    pub binding: Option<ProgramProcessBindingRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledPrefixSuffixDefinite {
    pub prefix_suffix_definite_id: Uuid,
    pub compile_version: i64,
    pub built_all_text: String,
    pub prefix_segments: Vec<SegmentRow>,
    pub suffix_segments: Vec<SegmentRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeHead {
    pub process_id: Uuid,
    pub full_text: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Plan,
    ForcedEvent,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::ForcedEvent => "forced_event",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    NormalMessage,
    TaskMessage,
    HardwareMessage,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NormalMessage => "normal_message",
            Self::TaskMessage => "task_message",
            Self::HardwareMessage => "hardware_message",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryAction {
    InterruptDeliver,
    PersistentAsyncClone,
    SegmentBoundaryDeliver,
    EphemeralAsyncClone,
    BounceWithHint,
    Ignore,
    BlacklistSender,
}

impl DeliveryAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InterruptDeliver => "interrupt_deliver",
            Self::PersistentAsyncClone => "persistent_async_clone",
            Self::SegmentBoundaryDeliver => "segment_boundary_deliver",
            Self::EphemeralAsyncClone => "ephemeral_async_clone",
            Self::BounceWithHint => "bounce_with_hint",
            Self::Ignore => "ignore",
            Self::BlacklistSender => "blacklist_sender",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResultTarget {
    Sender,
    Target,
    ExplicitTarget,
}

impl ResultTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sender => "sender",
            Self::Target => "target",
            Self::ExplicitTarget => "explicit_target",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepDeliveryPolicy {
    SegmentBoundary,
}

impl StepDeliveryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SegmentBoundary => "segment_boundary",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepResultPolicy {
    AppendContextAfterSegment,
    None,
}

impl StepResultPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppendContextAfterSegment => "append_context_after_segment",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskStep {
    pub action_name: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub action_args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardwareStep {
    pub operation_name: String,
    pub device_selector: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub operation_args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default = "default_hardware_step_exclusive")]
    pub exclusive: bool,
}

fn default_hardware_step_exclusive() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepEntry {
    pub step_id: String,
    pub step_kind: StepKind,
    pub message_kind: MessageKind,
    pub text: String,
    pub target_slot: String,
    pub base_priority: u8,
    pub requested_delivery_action: DeliveryAction,
    pub delivery_policy: StepDeliveryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    pub result_target: ResultTarget,
    pub result_policy: StepResultPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_template: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_sequence: Vec<TaskStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardware_sequence: Vec<HardwareStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MonitorTriggerEvent {
    SendMessageCompleted,
}

impl MonitorTriggerEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SendMessageCompleted => "send_message_completed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MonitorTriggerEntry {
    pub trigger_id: String,
    pub trigger_event: MonitorTriggerEvent,
    pub emitted_message_kind: MessageKind,
    pub text: String,
    pub target_slot: String,
    pub base_priority: u8,
    pub requested_delivery_action: DeliveryAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    pub result_target: ResultTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_template: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_sequence: Vec<TaskStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardware_sequence: Vec<HardwareStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(default = "default_monitor_trigger_enabled")]
    pub enabled: bool,
}

fn default_monitor_trigger_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotPlanState {
    pub slot_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_suffix_definite_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_slot: Option<String>,
    pub upstream_group: String,
    pub downstream_group: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitor_triggers: Vec<MonitorTriggerEntry>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgramPlanState {
    pub plan_version: i64,
    pub global_phase: String,
    pub slots: HashMap<String, SlotPlanState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowSlotSpec {
    pub slot_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_suffix_definite_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_slot: Option<String>,
    pub upstream_group: String,
    pub downstream_group: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitor_triggers: Vec<MonitorTriggerEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowTemplateSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<WorkflowSlotSpec>,
}

impl WorkflowTemplateSpec {
    pub fn to_workflow_json(&self) -> Value {
        serde_json::to_value(self).expect("workflow template spec should serialize")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowSeedSlot {
    pub slot_name: String,
    pub prefix_suffix_definite_id: Option<Uuid>,
    pub parent_slot: Option<String>,
    pub upstream_group: String,
    pub downstream_group: String,
    pub monitor_triggers: Vec<MonitorTriggerEntry>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageSourceKind {
    System,
    AdHoc,
    Monitor,
}

impl MessageSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::AdHoc => "ad_hoc",
            Self::Monitor => "monitor",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessMessageEnvelope {
    pub envelope_id: Uuid,
    pub message_id: Uuid,
    pub message_kind: MessageKind,
    pub source_kind: MessageSourceKind,
    pub sender_process_id: Uuid,
    pub sender_external_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_program_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_program_slot: Option<String>,
    pub target_process_id: Uuid,
    pub target_external_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_program_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_program_slot: Option<String>,
    pub message_type: String,
    pub base_priority: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_priority: Option<u8>,
    pub requested_delivery_action: DeliveryAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_delivery_action: Option<DeliveryAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_mark: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    pub result_target: ResultTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_result_process_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_result_program_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_result_program_slot: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_sequence: Vec<TaskStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardware_sequence: Vec<HardwareStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageBatchInfo {
    pub batch_id: Uuid,
    pub target_process_id: Uuid,
    pub target_slot: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_program_run_id: Option<String>,
    pub priority_mark: String,
    pub delivery_policy: String,
    pub envelope_ids: Vec<Uuid>,
    pub final_priority: u8,
    pub final_delivery_action: DeliveryAction,
    pub message_ids: Vec<Uuid>,
    pub first_enqueued_at: DateTime<Utc>,
    pub last_enqueued_at: DateTime<Utc>,
    pub frozen: bool,
}

pub type QueueEnvelope = ProcessMessageEnvelope;
pub type QueueBatchInfo = MessageBatchInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistenceCompensationRecord {
    pub operation_id: Uuid,
    pub operation_kind: String,
    pub program_run_id: String,
    pub process_id: Uuid,
    pub segment_id: Uuid,
    pub segment_kind: String,
    pub status: String,
    pub error: String,
    pub attempts: i64,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub fn approximate_token_count(content: &str) -> i32 {
    content.split_whitespace().count() as i32
}

pub fn concatenate_segment_contents(segments: &[SegmentRow]) -> String {
    let mut out = String::with_capacity(segments.iter().map(|segment| segment.content.len()).sum());
    for segment in segments {
        out.push_str(&segment.content);
    }
    out
}

pub fn assemble_runtime_text(
    built_all_text: &str,
    process_prompt_text: &str,
    process_segments: &[SegmentRow],
    open_live_segment: Option<&SegmentRow>,
) -> String {
    // Runtime head is the provider-facing head text: compiled prompt, then the mutable
    // process prompt layer, then the visible process stream.
    let process_text = concatenate_segment_contents(process_segments);
    let open_text_len = open_live_segment
        .map(|segment| segment.content.len())
        .unwrap_or_default();
    let mut combined = String::with_capacity(
        built_all_text.len() + process_prompt_text.len() + process_text.len() + open_text_len + 80,
    );
    combined.push_str(built_all_text);
    combined.push_str("\n\n[process_prompt]\n");
    combined.push_str(process_prompt_text);
    if !process_text.is_empty() || open_live_segment.is_some() {
        combined.push_str("\n\n[process_stream]\n");
        combined.push_str(&process_text);
    }
    if let Some(segment) = open_live_segment {
        combined.push_str(&segment.content);
    }
    combined
}

pub fn compile_version_from_patch(patch: Option<&Value>) -> Option<i64> {
    patch
        .and_then(|value| value.get("compile_version"))
        .and_then(Value::as_i64)
}

pub fn runtime_state_from_patch(patch: Option<&Value>) -> Option<&str> {
    patch
        .and_then(|value| value.get("runtime_state"))
        .and_then(Value::as_str)
}

pub fn merge_patch(base: Option<Value>, overlay: Option<Value>) -> Option<Value> {
    match (base, overlay) {
        (Some(Value::Object(mut base_map)), Some(Value::Object(overlay_map))) => {
            for (key, value) in overlay_map {
                base_map.insert(key, value);
            }
            Some(Value::Object(base_map))
        }
        (_, Some(overlay)) => Some(overlay),
        (base, None) => base,
    }
}

pub fn with_runtime_state(patch: Option<Value>, runtime_state: &str) -> Option<Value> {
    merge_patch(
        patch,
        Some(Value::Object(
            [(
                "runtime_state".to_string(),
                Value::String(runtime_state.to_string()),
            )]
            .into_iter()
            .collect(),
        )),
    )
}

pub fn workflow_template_spec_from_json(workflow_json: &Value) -> WorkflowTemplateSpec {
    serde_json::from_value::<WorkflowTemplateSpec>(workflow_json.clone()).unwrap_or_else(|_| {
        let slots = workflow_json
            .get("slots")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(workflow_slot_spec_from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        WorkflowTemplateSpec {
            template_kind: workflow_json
                .get("template_kind")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            slots,
        }
    })
}

pub fn workflow_seed_slots(workflow_json: &Value) -> Vec<WorkflowSeedSlot> {
    workflow_template_spec_from_json(workflow_json)
        .slots
        .into_iter()
        .map(|slot| WorkflowSeedSlot {
            slot_name: slot.slot_name.clone(),
            prefix_suffix_definite_id: slot.prefix_suffix_definite_id,
            parent_slot: slot.parent_slot.clone(),
            upstream_group: slot.upstream_group,
            downstream_group: slot.downstream_group,
            monitor_triggers: slot.monitor_triggers,
            metadata: slot.metadata,
        })
        .collect()
}

pub fn default_program_plan_state(workflow_json: &Value) -> ProgramPlanState {
    let slots = workflow_seed_slots(workflow_json)
        .into_iter()
        .map(|seed| {
            let state = default_program_slot_state(
                &seed.slot_name,
                seed.prefix_suffix_definite_id,
                seed.parent_slot.clone(),
                seed.upstream_group.clone(),
                seed.downstream_group.clone(),
                seed.monitor_triggers.clone(),
            );
            (seed.slot_name, state)
        })
        .collect();
    ProgramPlanState {
        plan_version: 1,
        global_phase: "running".to_string(),
        slots,
    }
}

pub fn default_program_slot_state(
    slot_name: &str,
    prefix_suffix_definite_id: Option<Uuid>,
    parent_slot: Option<String>,
    upstream_group: String,
    downstream_group: String,
    monitor_triggers: Vec<MonitorTriggerEntry>,
) -> SlotPlanState {
    SlotPlanState {
        slot_name: slot_name.to_string(),
        prefix_suffix_definite_id,
        parent_slot,
        upstream_group,
        downstream_group,
        monitor_triggers,
        updated_at: Utc::now(),
    }
}

pub fn monitor_trigger_entries_from_value(
    value: Option<&Value>,
    slot_name: &str,
) -> Vec<MonitorTriggerEntry> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| monitor_trigger_entry_from_value(item, slot_name))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn step_entries_from_value(
    value: Option<&Value>,
    slot_name: &str,
    default_kind: StepKind,
) -> Vec<StepEntry> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| step_entry_from_value(item, slot_name, default_kind))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn default_step_entry(
    slot_name: &str,
    step_kind: StepKind,
    target_slot: Option<&str>,
    text: Option<&str>,
) -> StepEntry {
    StepEntry {
        step_id: format!("{slot_name}.{}.default", step_kind.as_str()),
        step_kind,
        message_kind: MessageKind::NormalMessage,
        text: text
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| match step_kind {
                StepKind::Plan => {
                    format!("Continue the next plan cycle for slot `{slot_name}`.")
                }
                StepKind::ForcedEvent => {
                    format!("Execute the next forced_event cycle for slot `{slot_name}`.")
                }
            }),
        target_slot: target_slot.unwrap_or(slot_name).to_string(),
        base_priority: 4,
        requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
        delivery_policy: StepDeliveryPolicy::SegmentBoundary,
        delay_ms: None,
        result_target: ResultTarget::Sender,
        result_policy: match step_kind {
            StepKind::Plan => StepResultPolicy::None,
            StepKind::ForcedEvent => StepResultPolicy::AppendContextAfterSegment,
        },
        content_template: Some(
            text.map(ToOwned::to_owned)
                .unwrap_or_else(|| match step_kind {
                    StepKind::Plan => {
                        format!("Continue the next plan cycle for slot `{slot_name}`.")
                    }
                    StepKind::ForcedEvent => {
                        format!("Execute the next forced_event cycle for slot `{slot_name}`.")
                    }
                }),
        ),
        task_sequence: Vec::new(),
        hardware_sequence: Vec::new(),
        metadata: None,
    }
}

pub fn compensation_records_from_metadata(
    metadata_json: Option<&Value>,
) -> Vec<PersistenceCompensationRecord> {
    metadata_json
        .and_then(|value| value.get("persistence_compensation_records"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    serde_json::from_value::<PersistenceCompensationRecord>(item.clone()).ok()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn metadata_with_compensation_records(
    metadata_json: Option<Value>,
    records: &[PersistenceCompensationRecord],
) -> Option<Value> {
    let mut map = metadata_json
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    map.insert(
        "persistence_compensation_records".to_string(),
        Value::Array(
            records
                .iter()
                .map(|record| serde_json::to_value(record).unwrap_or(Value::Null))
                .collect(),
        ),
    );
    Some(Value::Object(map))
}

fn workflow_slot_spec_from_value(value: &Value) -> Option<WorkflowSlotSpec> {
    match value {
        Value::String(slot_name) => Some(WorkflowSlotSpec {
            slot_name: slot_name.clone(),
            prefix_suffix_definite_id: None,
            parent_slot: None,
            upstream_group: slot_name.clone(),
            downstream_group: slot_name.clone(),
            monitor_triggers: Vec::new(),
            metadata: None,
        }),
        Value::Object(map) => {
            let slot_name = map
                .get("slot_name")
                .and_then(Value::as_str)
                .or_else(|| map.get("name").and_then(Value::as_str))?
                .to_string();
            let monitor_triggers = if map.get("monitor_triggers").is_some() {
                monitor_trigger_entries_from_value(map.get("monitor_triggers"), &slot_name)
            } else {
                let mut entries = Vec::new();
                entries.extend(
                    step_entries_from_value(map.get("plan_loop"), &slot_name, StepKind::Plan)
                        .into_iter()
                        .map(|entry| monitor_trigger_from_step_entry(entry, &slot_name)),
                );
                entries.extend(
                    step_entries_from_value(
                        map.get("forced_event_loop"),
                        &slot_name,
                        StepKind::ForcedEvent,
                    )
                    .into_iter()
                    .map(|entry| monitor_trigger_from_step_entry(entry, &slot_name)),
                );
                entries
            };
            Some(WorkflowSlotSpec {
                slot_name: slot_name.clone(),
                prefix_suffix_definite_id: map
                    .get("prefix_suffix_definite_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok()),
                parent_slot: map
                    .get("parent_slot")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                upstream_group: map
                    .get("upstream_group")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| slot_name.clone()),
                downstream_group: map
                    .get("downstream_group")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| slot_name.clone()),
                monitor_triggers,
                metadata: map.get("metadata").cloned(),
            })
        }
        _ => None,
    }
}

fn monitor_trigger_from_step_entry(step: StepEntry, slot_name: &str) -> MonitorTriggerEntry {
    MonitorTriggerEntry {
        trigger_id: format!(
            "{}.{}",
            step.step_id,
            MonitorTriggerEvent::SendMessageCompleted.as_str()
        ),
        trigger_event: MonitorTriggerEvent::SendMessageCompleted,
        emitted_message_kind: step.message_kind,
        text: step.text,
        target_slot: if step.target_slot.is_empty() {
            slot_name.to_string()
        } else {
            step.target_slot
        },
        base_priority: step.base_priority,
        requested_delivery_action: step.requested_delivery_action,
        delay_ms: step.delay_ms,
        result_target: step.result_target,
        content_template: step.content_template,
        task_sequence: step.task_sequence,
        hardware_sequence: step.hardware_sequence,
        metadata: step.metadata,
        enabled: true,
    }
}

fn monitor_trigger_entry_from_value(value: &Value, slot_name: &str) -> Option<MonitorTriggerEntry> {
    let map = value.as_object()?;
    let emitted_message_kind = match map
        .get("emitted_message_kind")
        .or_else(|| map.get("message_kind"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| infer_message_kind_from_sequences(map).as_str())
    {
        "task_message" => MessageKind::TaskMessage,
        "hardware_message" => MessageKind::HardwareMessage,
        _ => MessageKind::NormalMessage,
    };
    Some(MonitorTriggerEntry {
        trigger_id: map
            .get("trigger_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{slot_name}.trigger.{}", Uuid::new_v4())),
        trigger_event: match map
            .get("trigger_event")
            .and_then(Value::as_str)
            .unwrap_or(MonitorTriggerEvent::SendMessageCompleted.as_str())
        {
            "send_message_completed" => MonitorTriggerEvent::SendMessageCompleted,
            _ => MonitorTriggerEvent::SendMessageCompleted,
        },
        emitted_message_kind,
        text: map
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                map.get("content_template")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| format!("monitor trigger for slot `{slot_name}`")),
        target_slot: map
            .get("target_slot")
            .and_then(Value::as_str)
            .unwrap_or(slot_name)
            .to_string(),
        base_priority: map
            .get("base_priority")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .filter(|value| (1..=8).contains(value))
            .unwrap_or(4),
        requested_delivery_action: match map
            .get("requested_delivery_action")
            .and_then(Value::as_str)
            .unwrap_or(DeliveryAction::SegmentBoundaryDeliver.as_str())
        {
            "interrupt_deliver" => DeliveryAction::InterruptDeliver,
            "persistent_async_clone" => DeliveryAction::PersistentAsyncClone,
            "ephemeral_async_clone" => DeliveryAction::EphemeralAsyncClone,
            "bounce_with_hint" => DeliveryAction::BounceWithHint,
            "ignore" => DeliveryAction::Ignore,
            "blacklist_sender" => DeliveryAction::BlacklistSender,
            _ => DeliveryAction::SegmentBoundaryDeliver,
        },
        delay_ms: map.get("delay_ms").and_then(Value::as_u64),
        result_target: match map
            .get("result_target")
            .and_then(Value::as_str)
            .unwrap_or(ResultTarget::Sender.as_str())
        {
            "target" => ResultTarget::Target,
            "explicit_target" => ResultTarget::ExplicitTarget,
            _ => ResultTarget::Sender,
        },
        content_template: map
            .get("content_template")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        task_sequence: map
            .get("task_sequence")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(task_step_from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        hardware_sequence: map
            .get("hardware_sequence")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(hardware_step_from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        metadata: map.get("metadata").cloned(),
        enabled: map.get("enabled").and_then(Value::as_bool).unwrap_or(true),
    })
}

fn step_entry_from_value(
    value: &Value,
    slot_name: &str,
    default_kind: StepKind,
) -> Option<StepEntry> {
    let map = value.as_object()?;
    let step_kind = match map
        .get("step_kind")
        .and_then(Value::as_str)
        .unwrap_or(default_kind.as_str())
    {
        "plan" => StepKind::Plan,
        "forced_event" => StepKind::ForcedEvent,
        _ => default_kind,
    };
    Some(StepEntry {
        step_id: map
            .get("step_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{slot_name}.{}.{}", step_kind.as_str(), Uuid::new_v4())),
        step_kind,
        message_kind: match map
            .get("message_kind")
            .and_then(Value::as_str)
            .unwrap_or_else(|| infer_message_kind_from_sequences(map).as_str())
        {
            "task_message" => MessageKind::TaskMessage,
            "hardware_message" => MessageKind::HardwareMessage,
            _ => MessageKind::NormalMessage,
        },
        text: map
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                map.get("content_template")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| format!("{} step for slot `{slot_name}`", step_kind.as_str())),
        target_slot: map
            .get("target_slot")
            .and_then(Value::as_str)
            .unwrap_or(slot_name)
            .to_string(),
        base_priority: map
            .get("base_priority")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .filter(|value| (1..=8).contains(value))
            .unwrap_or(4),
        requested_delivery_action: match map
            .get("requested_delivery_action")
            .and_then(Value::as_str)
            .unwrap_or(DeliveryAction::SegmentBoundaryDeliver.as_str())
        {
            "interrupt_deliver" => DeliveryAction::InterruptDeliver,
            "persistent_async_clone" => DeliveryAction::PersistentAsyncClone,
            "ephemeral_async_clone" => DeliveryAction::EphemeralAsyncClone,
            "bounce_with_hint" => DeliveryAction::BounceWithHint,
            "ignore" => DeliveryAction::Ignore,
            "blacklist_sender" => DeliveryAction::BlacklistSender,
            _ => DeliveryAction::SegmentBoundaryDeliver,
        },
        delivery_policy: StepDeliveryPolicy::SegmentBoundary,
        delay_ms: map.get("delay_ms").and_then(Value::as_u64),
        result_target: match map
            .get("result_target")
            .and_then(Value::as_str)
            .unwrap_or(ResultTarget::Sender.as_str())
        {
            "target" => ResultTarget::Target,
            "explicit_target" => ResultTarget::ExplicitTarget,
            _ => ResultTarget::Sender,
        },
        result_policy: match map.get("result_policy").and_then(Value::as_str).unwrap_or(
            match step_kind {
                StepKind::Plan => "none",
                StepKind::ForcedEvent => "append_context_after_segment",
            },
        ) {
            "append_context_after_segment" => StepResultPolicy::AppendContextAfterSegment,
            _ => StepResultPolicy::None,
        },
        content_template: map
            .get("content_template")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                map.get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            }),
        task_sequence: map
            .get("task_sequence")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(task_step_from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        hardware_sequence: map
            .get("hardware_sequence")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(hardware_step_from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        metadata: map.get("metadata").cloned(),
    })
}

fn infer_message_kind_from_sequences(map: &serde_json::Map<String, Value>) -> MessageKind {
    if map.get("hardware_sequence").is_some() {
        MessageKind::HardwareMessage
    } else if map.get("task_sequence").is_some() {
        MessageKind::TaskMessage
    } else {
        MessageKind::NormalMessage
    }
}

fn task_step_from_value(value: &Value) -> Option<TaskStep> {
    let map = value.as_object()?;
    Some(TaskStep {
        action_name: map.get("action_name").and_then(Value::as_str)?.to_string(),
        action_args: map
            .get("action_args")
            .cloned()
            .unwrap_or(Value::Object(Default::default())),
    })
}

fn hardware_step_from_value(value: &Value) -> Option<HardwareStep> {
    let map = value.as_object()?;
    Some(HardwareStep {
        operation_name: map
            .get("operation_name")
            .and_then(Value::as_str)?
            .to_string(),
        device_selector: map
            .get("device_selector")
            .and_then(Value::as_str)?
            .to_string(),
        transport: map
            .get("transport")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        operation_args: map
            .get("operation_args")
            .cloned()
            .unwrap_or(Value::Object(Default::default())),
        timeout_ms: map.get("timeout_ms").and_then(Value::as_u64),
        exclusive: map
            .get("exclusive")
            .and_then(Value::as_bool)
            .unwrap_or_else(default_hardware_step_exclusive),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn segment(
        owner_kind: OwnerKind,
        owner_seq: i64,
        definition_part: Option<DefinitionPart>,
        content: &str,
    ) -> SegmentRow {
        SegmentRow {
            id: Uuid::new_v4(),
            owner_kind,
            owner_id: Uuid::new_v4(),
            owner_seq,
            definition_part,
            segment_kind: "text".to_string(),
            content: content.to_string(),
            token_count: approximate_token_count(content),
            tokenizer: Some("whitespace".to_string()),
            patch: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn concatenation_keeps_exact_segment_order_without_inserting_separators() {
        let prefix = vec![
            segment(
                OwnerKind::PrefixSuffixDefinite,
                1,
                Some(DefinitionPart::Prefix),
                "A",
            ),
            segment(
                OwnerKind::PrefixSuffixDefinite,
                2,
                Some(DefinitionPart::Prefix),
                "B",
            ),
        ];
        let suffix = vec![segment(
            OwnerKind::PrefixSuffixDefinite,
            1,
            Some(DefinitionPart::Suffix),
            "C",
        )];
        let mut all = prefix;
        all.extend(suffix);
        assert_eq!(concatenate_segment_contents(&all), "ABC");
    }

    #[test]
    fn runtime_head_includes_process_prompt_before_process_stream() {
        let process_segments = vec![
            segment(OwnerKind::Process, 1, None, "hello"),
            segment(OwnerKind::Process, 2, None, " world"),
        ];
        assert_eq!(
            assemble_runtime_text("PREFIX", "REMINDER", &process_segments, None),
            "PREFIX\n\n[process_prompt]\nREMINDER\n\n[process_stream]\nhello world"
        );
    }

    #[test]
    fn workflow_seed_slots_preserve_monitor_triggers_and_groups() {
        let workflow_json = json!({
            "template_kind": "writer_planner",
            "slots": [
                {
                    "slot_name": "planner1",
                    "prefix_suffix_definite_id": "11111111-1111-1111-1111-111111111111",
                    "upstream_group": "planning",
                    "downstream_group": "writing",
                    "monitor_triggers": [
                        {
                            "trigger_event": "send_message_completed",
                            "message_type": "plan",
                            "text": "Plan the next writing step.",
                            "target_slot": "planner1"
                        },
                        {
                            "trigger_event": "send_message_completed",
                            "emitted_message_kind": "task_message",
                            "message_type": "forced_event",
                            "text": "Send the next system writing request.",
                            "target_slot": "writer1",
                            "task_sequence": [
                                {
                                    "action_name": "append_output_ref",
                                    "action_args": {
                                        "output_ref": "artifact://writer"
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    "slot_name": "writer1",
                    "prefix_suffix_definite_id": "22222222-2222-2222-2222-222222222222",
                    "parent_slot": "planner1",
                    "upstream_group": "writing",
                    "downstream_group": "planning"
                }
            ]
        });

        let seeds = workflow_seed_slots(&workflow_json);
        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].slot_name, "planner1");
        assert_eq!(
            seeds[0].prefix_suffix_definite_id,
            Some(Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap())
        );
        assert_eq!(seeds[0].upstream_group, "planning");
        assert_eq!(seeds[0].downstream_group, "writing");
        assert_eq!(seeds[0].monitor_triggers.len(), 2);
        assert_eq!(seeds[1].parent_slot.as_deref(), Some("planner1"));
    }

    #[test]
    fn default_program_plan_state_seeds_all_slots() {
        let workflow_json = json!({
            "slots": [
                {"slot_name": "writer1"},
                {
                    "slot_name": "planner1",
                    "upstream_group": "planning",
                    "downstream_group": "writing"
                }
            ]
        });
        let state = default_program_plan_state(&workflow_json);
        assert_eq!(state.plan_version, 1);
        assert!(state.slots.contains_key("writer1"));
        assert!(state.slots.contains_key("planner1"));
        assert_eq!(
            state
                .slots
                .get("planner1")
                .and_then(|slot| slot.prefix_suffix_definite_id),
            None
        );
        assert_eq!(
            state
                .slots
                .get("planner1")
                .map(|slot| slot.downstream_group.as_str()),
            Some("writing")
        );
        assert!(state
            .slots
            .get("planner1")
            .map(|slot| slot.monitor_triggers.is_empty())
            .unwrap_or(false));
    }

    #[test]
    fn step_entries_infer_hardware_message_from_hardware_sequence() {
        let entries = step_entries_from_value(
            Some(&json!([
                {
                    "text": "Read device status",
                    "target_slot": "worker",
                    "hardware_sequence": [
                        {
                            "operation_name": "board_info",
                            "device_selector": "board:nucleo"
                        }
                    ]
                }
            ])),
            "worker",
            StepKind::ForcedEvent,
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message_kind, MessageKind::HardwareMessage);
        assert_eq!(entries[0].hardware_sequence.len(), 1);
        assert!(entries[0].task_sequence.is_empty());
        assert!(entries[0].hardware_sequence[0].exclusive);
    }

    #[test]
    fn monitor_triggers_infer_hardware_message_from_hardware_sequence() {
        let triggers = monitor_trigger_entries_from_value(
            Some(&json!([
                {
                    "text": "Flash firmware",
                    "target_slot": "operator",
                    "hardware_sequence": [
                        {
                            "operation_name": "flash_firmware",
                            "device_selector": "board:uno",
                            "transport": "serial",
                            "operation_args": {
                                "image_ref": "artifact://firmware/latest"
                            }
                        }
                    ]
                }
            ])),
            "operator",
        );

        assert_eq!(triggers.len(), 1);
        assert_eq!(
            triggers[0].emitted_message_kind,
            MessageKind::HardwareMessage
        );
        assert_eq!(triggers[0].hardware_sequence.len(), 1);
        assert_eq!(
            triggers[0].hardware_sequence[0].transport.as_deref(),
            Some("serial")
        );
    }
}
