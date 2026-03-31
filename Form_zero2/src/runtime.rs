use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, RwLock, Semaphore},
    time,
};
use uuid::Uuid;

use crate::{
    control::{RuntimeControlCommand, RuntimeControlQueue, SendMessageRequest, TargetSelector},
    db::Database,
    external_tools::ExternalToolRegistry,
    models::{
        approximate_token_count, default_program_slot_state, merge_patch,
        monitor_trigger_entries_from_value, with_runtime_state, DeliveryAction, HardwareStep,
        MessageKind, MessageSourceKind, MonitorTriggerEntry, MonitorTriggerEvent, OwnerKind,
        ProcessInstanceRow, ProcessPromptRow, ProcessRuntimeBinding, ProgramProcessBindingRow,
        QueueBatchInfo, QueueEnvelope, ResultTarget, RuntimeHead, SegmentRow, StepKind, TaskStep,
        HIDDEN_OPTIMIZER_SLOT_NAME, HIDDEN_SCORE_JUDGE_SLOT_NAME,
    },
    peripherals::{build_external_tool_registry, peripherals_config_from_env, PeripheralsConfig},
    provider::{default_provider_from_env, ProviderChunk, ProviderMessage, StreamingProvider},
    tools::{
        blacklist_blocks_message, default_deadline_ms, extract_tool_guide,
        target_selector_from_value, tool_finished_metadata, tool_requires_guide,
        tool_requires_provider, tool_result_patch, upsert_blacklist_entry, BlacklistEntry,
        HardwareOperationRegistry, ToolRegistry, ToolRunRegistry, HARDWARE_OP_BOARD_INFO,
        HARDWARE_OP_DEVICE_CAPABILITIES, HARDWARE_OP_FLASH_FIRMWARE, HARDWARE_OP_GPIO_READ,
        HARDWARE_OP_GPIO_WRITE, HARDWARE_OP_PROBE_READ_MEMORY, HARDWARE_OP_SERIAL_QUERY,
        HARDWARE_OP_SERIAL_WRITE, TOOL_APPEND_OUTPUT_REF, TOOL_CREATE_PROGRAM,
        TOOL_DELETE_RESULT_ARTIFACT, TOOL_HISTORY_ANNOTATE, TOOL_HISTORY_QUERY,
        TOOL_INSPECT_PROCESS_TABLE, TOOL_INVISIBLE_PROCESS, TOOL_MAKE_PLAN,
        TOOL_MONITOR_EVENT_SEQUENCE, TOOL_REPAIR_PLAN, TOOL_SPAWN_BRANCH_PROCESS,
        TOOL_SPAWN_REAL_PROCESS, TOOL_TERMINATE_PROCESS,
    },
};

const DELIVERY_ACTION_ORDER: [DeliveryAction; 2] = [
    DeliveryAction::InterruptDeliver,
    DeliveryAction::SegmentBoundaryDeliver,
];

type EphemeralStreamId = Uuid;
const SEND_SEGMENT_KIND: &str = "send_segment";
const RECEIVE_SEGMENT_KIND: &str = "receive_segment";
const PROVIDER_STREAM_SEGMENT_KIND: &str = SEND_SEGMENT_KIND;
const PROVIDER_STREAM_TOKENIZER: &str = "whitespace";
const PERSISTED_FLUSH_INTERVAL_MS: u64 = 500;
const HIDDEN_SCORE_JUDGE_TOOL_NAME: &str = "__score_judge_call__";
const HIDDEN_OPTIMIZER_TOOL_NAME: &str = "__optimizer_call__";
const HIDDEN_REPROMPT_TOOL_NAME: &str = "__optimizer_reprompt__";

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub error_log_path: PathBuf,
    pub provider_input_budget: usize,
    pub compaction_trigger_ratio: f32,
    pub peripherals_config: PeripheralsConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            error_log_path: std::env::var("FORM_ZERO_RUNTIME_ERROR_LOG")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("./form_zero_runtime_errors.jsonl")),
            provider_input_budget: std::env::var("FORM_ZERO_PROVIDER_INPUT_BUDGET")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(8_000),
            compaction_trigger_ratio: std::env::var("FORM_ZERO_COMPACTION_TRIGGER_RATIO")
                .ok()
                .and_then(|value| value.parse::<f32>().ok())
                .filter(|value| *value > 0.0 && *value <= 1.0)
                .unwrap_or(0.7),
            peripherals_config: peripherals_config_from_env(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenLiveSegment {
    pub segment_id: Uuid,
    pub owner_kind: OwnerKind,
    pub owner_id: Uuid,
    pub owner_seq: i64,
    pub segment_kind: String,
    pub content: String,
    pub token_count: i32,
    pub tokenizer: Option<String>,
    pub patch: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl OpenLiveSegment {
    fn new(
        owner_id: Uuid,
        owner_seq: i64,
        segment_kind: String,
        tokenizer: Option<String>,
        patch: Option<Value>,
    ) -> Self {
        Self {
            segment_id: Uuid::new_v4(),
            owner_kind: OwnerKind::Process,
            owner_id,
            owner_seq,
            segment_kind,
            content: String::new(),
            token_count: 0,
            tokenizer,
            patch: with_runtime_state(patch, "open"),
            created_at: Utc::now(),
        }
    }

    fn apply_delta(
        &mut self,
        content_delta: &str,
        segment_kind: Option<String>,
        tokenizer: Option<String>,
        patch: Option<Value>,
    ) -> Result<()> {
        // if let Some(segment_kind) = segment_kind {
        //     if self.segment_kind != segment_kind {
        //         bail!(
        //             "segment_kind mismatch for owner {}: current={}, incoming={}",
        //             self.owner_id,
        //             self.segment_kind,
        //             segment_kind
        //         );
        //     }
        // }

        if let Some(tokenizer) = tokenizer {
            if self.content.is_empty() && self.tokenizer.is_none() {
                self.tokenizer = Some(tokenizer);
                // } else if self.tokenizer.as_deref() != Some(tokenizer.as_str()) {
                //     bail!(
                //         "tokenizer mismatch for owner {}: current={:?}, incoming={}",
                //         self.owner_id,
                //         self.tokenizer,
                //         tokenizer
                //     );
            }
        }
        let _ = segment_kind;
        self.content.push_str(content_delta);
        self.token_count = approximate_token_count(&self.content);
        self.patch = with_runtime_state(merge_patch(self.patch.clone(), patch), "open");
        Ok(())
    }

    fn mark_sealed(&mut self) {
        self.patch = with_runtime_state(self.patch.clone(), "sealed");
    }

    fn to_segment_row(&self) -> SegmentRow {
        SegmentRow {
            id: self.segment_id,
            owner_kind: self.owner_kind,
            owner_id: self.owner_id,
            owner_seq: self.owner_seq,
            definition_part: None,
            segment_kind: self.segment_kind.clone(),
            content: self.content.clone(),
            token_count: self.token_count,
            tokenizer: self.tokenizer.clone(),
            patch: self.patch.clone(),
            created_at: self.created_at,
        }
    }
}

#[derive(Clone, Default)]
pub struct LiveBusRegistry {
    open_inner: Arc<RwLock<HashMap<Uuid, OpenLiveSegment>>>,
    sealed_pending_inner: Arc<RwLock<HashMap<Uuid, Vec<OpenLiveSegment>>>>,
}

impl LiveBusRegistry {
    pub async fn get(&self, process_id: Uuid) -> Option<OpenLiveSegment> {
        self.open_inner.read().await.get(&process_id).cloned()
    }

    pub async fn upsert(&self, segment: OpenLiveSegment) {
        self.open_inner
            .write()
            .await
            .insert(segment.owner_id, segment);
    }

    pub async fn remove(&self, process_id: Uuid) -> Option<OpenLiveSegment> {
        self.open_inner.write().await.remove(&process_id)
    }

    pub async fn push_pending(&self, process_id: Uuid, segment: OpenLiveSegment) {
        let mut inner = self.sealed_pending_inner.write().await;
        let pending = inner.entry(process_id).or_default();
        pending.push(segment);
        pending.sort_by_key(|item| item.owner_seq);
    }

    pub async fn pending_segments(&self, process_id: Uuid) -> Vec<OpenLiveSegment> {
        self.sealed_pending_inner
            .read()
            .await
            .get(&process_id)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn drain_all_pending(&self) -> Vec<(Uuid, Vec<OpenLiveSegment>)> {
        self.sealed_pending_inner
            .write()
            .await
            .drain()
            .collect::<Vec<_>>()
    }

    pub async fn open_owner_ids(&self) -> Vec<Uuid> {
        self.open_inner.read().await.keys().copied().collect()
    }

    pub async fn last_pending_owner_seq(&self, process_id: Uuid) -> Option<i64> {
        self.sealed_pending_inner
            .read()
            .await
            .get(&process_id)
            .and_then(|segments| segments.last().map(|segment| segment.owner_seq))
    }
}

#[derive(Clone, Default)]
struct EphemeralLiveBusRegistry {
    open_inner: Arc<RwLock<HashMap<EphemeralStreamId, OpenLiveSegment>>>,
    sealed_pending_inner: Arc<RwLock<HashMap<EphemeralStreamId, Vec<OpenLiveSegment>>>>,
}

impl EphemeralLiveBusRegistry {
    async fn get(&self, stream_id: EphemeralStreamId) -> Option<OpenLiveSegment> {
        self.open_inner.read().await.get(&stream_id).cloned()
    }

    async fn upsert(&self, stream_id: EphemeralStreamId, segment: OpenLiveSegment) {
        self.open_inner.write().await.insert(stream_id, segment);
    }

    async fn clear(&self, stream_id: EphemeralStreamId) {
        self.open_inner.write().await.remove(&stream_id);
        self.sealed_pending_inner.write().await.remove(&stream_id);
    }
}

#[derive(Clone, Default)]
struct RuntimeHeadRegistry {
    inner: Arc<RwLock<HashMap<Uuid, ResidentPromptHeadState>>>,
}

#[derive(Clone)]
struct ResidentPromptHeadState {
    owner_id: Uuid,
    full_text: String,
    built_all_len: usize,
    stream_started: bool,
    dirty: bool,
    last_bootstrap_segment_seq: i64,
}

impl RuntimeHeadRegistry {
    async fn get(&self, process_id: Uuid) -> Option<ResidentPromptHeadState> {
        self.inner.read().await.get(&process_id).cloned()
    }

    async fn upsert(&self, runtime_head: ResidentPromptHeadState) {
        self.inner
            .write()
            .await
            .insert(runtime_head.owner_id, runtime_head);
    }

    async fn remove(&self, process_id: Uuid) -> Option<ResidentPromptHeadState> {
        self.inner.write().await.remove(&process_id)
    }
}

#[derive(Clone, Default)]
struct EphemeralRuntimeHeadRegistry {
    inner: Arc<RwLock<HashMap<EphemeralStreamId, ResidentPromptHeadState>>>,
}

impl EphemeralRuntimeHeadRegistry {
    async fn get(&self, stream_id: EphemeralStreamId) -> Option<ResidentPromptHeadState> {
        self.inner.read().await.get(&stream_id).cloned()
    }

    async fn upsert(&self, runtime_head: ResidentPromptHeadState) {
        self.inner
            .write()
            .await
            .insert(runtime_head.owner_id, runtime_head);
    }

    async fn remove(&self, stream_id: EphemeralStreamId) -> Option<ResidentPromptHeadState> {
        self.inner.write().await.remove(&stream_id)
    }
}

impl ResidentPromptHeadState {
    fn as_runtime_head(&self, process_id: Uuid) -> RuntimeHead {
        RuntimeHead {
            process_id,
            full_text: self.full_text.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct ProcessStreamGateRegistry {
    inner: Arc<Mutex<HashMap<Uuid, Arc<Semaphore>>>>,
}

impl ProcessStreamGateRegistry {
    async fn acquire(&self, process_id: Uuid) -> Result<OwnedSemaphorePermit> {
        let semaphore = {
            let mut inner = self.inner.lock().await;
            inner
                .entry(process_id)
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("process stream gate closed for process {process_id}"))
    }
}

#[derive(Clone, Default)]
struct HardwareDeviceGateRegistry {
    inner: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl HardwareDeviceGateRegistry {
    async fn acquire(&self, device_selector: &str) -> Result<OwnedSemaphorePermit> {
        let semaphore = {
            let mut inner = self.inner.lock().await;
            inner
                .entry(device_selector.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(1)))
                .clone()
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("hardware device gate closed for `{device_selector}`"))
    }
}

fn append_prompt_stream_text(prompt_head: &mut ResidentPromptHeadState, content: &str) {
    if content.is_empty() {
        return;
    }

    if !prompt_head.stream_started {
        prompt_head.full_text.push_str("\n\n[process_stream]\n");
        prompt_head.stream_started = true;
    }
    prompt_head.full_text.push_str(content);
}

fn prompt_head_stream_started(full_text: &str) -> bool {
    full_text.contains("\n\n[process_stream]\n")
}

fn history_query_value(
    process_id: Uuid,
    sealed_segments: &[SegmentRow],
    open_live_segment: Option<&SegmentRow>,
    tool_args: &Value,
) -> Value {
    let keyword = tool_args
        .get("keyword")
        .or_else(|| tool_args.get("query"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let segment_kind = tool_args
        .get("segment_kind")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let limit = tool_args
        .get("limit")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok());

    let mut matched_segments = sealed_segments
        .iter()
        .filter(|segment| {
            segment_kind
                .as_deref()
                .map(|kind| segment.segment_kind == kind)
                .unwrap_or(true)
                && keyword
                    .as_deref()
                    .map(|needle| segment.content.contains(needle))
                    .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();

    if let Some(open_live) = open_live_segment.filter(|segment| {
        segment_kind
            .as_deref()
            .map(|kind| segment.segment_kind == kind)
            .unwrap_or(true)
            && keyword
                .as_deref()
                .map(|needle| segment.content.contains(needle))
                .unwrap_or(true)
    }) {
        matched_segments.push(open_live.clone());
    }

    if let Some(limit) = limit {
        let keep_from = matched_segments.len().saturating_sub(limit);
        matched_segments = matched_segments.split_off(keep_from);
    }

    json!({
        "process_id": process_id,
        "query": {
            "keyword": keyword,
            "segment_kind": segment_kind,
            "limit": limit,
        },
        "sealed_segments": sealed_segments,
        "open_live_segment": open_live_segment,
        "matched_segments": matched_segments,
    })
}

async fn load_history_segments(
    db: &Database,
    live_bus_registry: &LiveBusRegistry,
    process_id: Uuid,
) -> Result<Vec<SegmentRow>> {
    let mut segments = db.list_sealed_process_segments(process_id).await?;
    segments.extend(
        live_bus_registry
            .pending_segments(process_id)
            .await
            .into_iter()
            .map(|segment| segment.to_segment_row()),
    );
    segments.sort_by_key(|segment| segment.owner_seq);
    Ok(segments)
}

async fn append_ephemeral_mailbox_batch_to_live(
    live_bus_registry: &EphemeralLiveBusRegistry,
    runtime_head_registry: &EphemeralRuntimeHeadRegistry,
    stream_id: EphemeralStreamId,
    content: &str,
    patch: Option<Value>,
) -> Result<SegmentRow> {
    if content.is_empty() {
        bail!("cannot append empty content to ephemeral live bus for stream {stream_id}");
    }

    let mut runtime_head = runtime_head_registry
        .get(stream_id)
        .await
        .ok_or_else(|| anyhow!("missing ephemeral runtime head for stream {stream_id}"))?;
    append_prompt_stream_text(&mut runtime_head, content);
    runtime_head_registry.upsert(runtime_head).await;

    let live_update = async {
        let mut open_live = match live_bus_registry.get(stream_id).await {
            Some(segment) => segment,
            None => OpenLiveSegment::new(
                stream_id,
                1,
                RECEIVE_SEGMENT_KIND.to_string(),
                Some("whitespace".to_string()),
                patch.clone(),
            ),
        };
        open_live.apply_delta(
            content,
            Some(RECEIVE_SEGMENT_KIND.to_string()),
            Some("whitespace".to_string()),
            patch.clone(),
        )?;
        live_bus_registry.upsert(stream_id, open_live.clone()).await;
        Ok::<OpenLiveSegment, anyhow::Error>(open_live)
    }
    .await;

    let open_live = match live_update {
        Ok(open_live) => open_live,
        Err(error) => {
            mark_ephemeral_prompt_head_dirty(runtime_head_registry, stream_id).await;
            return Err(error);
        }
    };

    Ok(open_live.to_segment_row())
}

async fn bootstrap_prompt_head_state(
    db: &Database,
    live_bus_registry: &LiveBusRegistry,
    process_id: Uuid,
) -> Result<ResidentPromptHeadState> {
    let sealed_head = db.load_runtime_head(process_id).await?;
    let process = db.get_process_instance(process_id).await?;
    let open_live_segment = live_bus_registry
        .get(process_id)
        .await
        .map(|segment| segment.to_segment_row());
    let pending_segments = live_bus_registry
        .pending_segments(process_id)
        .await
        .into_iter()
        .map(|segment| segment.to_segment_row())
        .collect::<Vec<_>>();
    let mut full_text = sealed_head.full_text;
    for segment in &pending_segments {
        if !prompt_head_stream_started(&full_text) {
            full_text.push_str("\n\n[process_stream]\n");
        }
        full_text.push_str(&segment.content);
    }
    if let Some(segment) = open_live_segment.as_ref() {
        if !prompt_head_stream_started(&full_text) {
            full_text.push_str("\n\n[process_stream]\n");
        }
        full_text.push_str(&segment.content);
    }

    Ok(ResidentPromptHeadState {
        owner_id: process_id,
        built_all_len: full_text
            .find("\n\n[process_stream]\n")
            .unwrap_or(full_text.len()),
        stream_started: prompt_head_stream_started(&full_text),
        full_text,
        dirty: false,
        last_bootstrap_segment_seq: open_live_segment
            .as_ref()
            .map(|segment| segment.owner_seq)
            .or_else(|| pending_segments.last().map(|segment| segment.owner_seq))
            .unwrap_or(process.last_segment_seq),
    })
}

async fn resident_prompt_head_state(
    db: &Database,
    live_bus_registry: &LiveBusRegistry,
    runtime_head_registry: &RuntimeHeadRegistry,
    process_id: Uuid,
) -> Result<(ResidentPromptHeadState, bool)> {
    if let Some(runtime_head) = runtime_head_registry.get(process_id).await {
        if !runtime_head.dirty {
            return Ok((runtime_head, false));
        }
    }

    let runtime_head = bootstrap_prompt_head_state(db, live_bus_registry, process_id).await?;
    runtime_head_registry.upsert(runtime_head.clone()).await;
    Ok((runtime_head, true))
}

async fn resident_runtime_head(
    db: &Database,
    live_bus_registry: &LiveBusRegistry,
    runtime_head_registry: &RuntimeHeadRegistry,
    process_id: Uuid,
) -> Result<RuntimeHead> {
    let (runtime_head, _) =
        resident_prompt_head_state(db, live_bus_registry, runtime_head_registry, process_id)
            .await?;
    Ok(runtime_head.as_runtime_head(process_id))
}

async fn seal_resident_open_live(
    runtime_head_registry: &RuntimeHeadRegistry,
    process_id: Uuid,
    owner_seq: i64,
) {
    let Some(mut runtime_head) = runtime_head_registry.get(process_id).await else {
        return;
    };

    runtime_head.last_bootstrap_segment_seq =
        runtime_head.last_bootstrap_segment_seq.max(owner_seq);
    runtime_head_registry.upsert(runtime_head).await;
}

#[allow(dead_code)]
async fn mark_resident_prompt_head_dirty(
    runtime_head_registry: &RuntimeHeadRegistry,
    process_id: Uuid,
) {
    let Some(mut runtime_head) = runtime_head_registry.get(process_id).await else {
        return;
    };

    runtime_head.dirty = true;
    runtime_head_registry.upsert(runtime_head).await;
}

async fn mark_ephemeral_prompt_head_dirty(
    runtime_head_registry: &EphemeralRuntimeHeadRegistry,
    stream_id: EphemeralStreamId,
) {
    let Some(mut runtime_head) = runtime_head_registry.get(stream_id).await else {
        return;
    };

    runtime_head.dirty = true;
    runtime_head_registry.upsert(runtime_head).await;
}

async fn next_live_owner_seq(
    db: &Database,
    runtime_head_registry: &RuntimeHeadRegistry,
    live_bus_registry: &LiveBusRegistry,
    process_id: Uuid,
) -> Result<i64> {
    if let Some(open_live) = live_bus_registry.get(process_id).await {
        return Ok(open_live.owner_seq);
    }
    if let Some(owner_seq) = live_bus_registry.last_pending_owner_seq(process_id).await {
        return Ok(owner_seq + 1);
    }
    if let Some(runtime_head) = runtime_head_registry.get(process_id).await {
        return Ok(runtime_head.last_bootstrap_segment_seq + 1);
    }

    Ok(db.get_process_instance(process_id).await?.last_segment_seq + 1)
}

fn provider_stream_patch(
    tool_name: &str,
    tool_run_id: Uuid,
    host_process_id: Uuid,
    stream_id: Option<EphemeralStreamId>,
) -> Option<Value> {
    tool_result_patch(
        tool_name,
        tool_run_id,
        host_process_id,
        Some(json!({
            "provider_stream": true,
            "stream_id": stream_id.map(|value| value.to_string()),
        })),
    )
}

async fn seal_persisted_live_to_pending(
    live_bus_registry: &LiveBusRegistry,
    runtime_head_registry: &RuntimeHeadRegistry,
    sequence_engine: &MonitorEventSequenceEngine,
    process: &ProcessInstanceRow,
    binding: Option<&ProgramProcessBindingRow>,
    executor_process_id: Option<Uuid>,
    tool_name: Option<&str>,
    tool_run_id: Option<Uuid>,
) -> Result<Option<OpenLiveSegment>> {
    let Some(mut open_live) = live_bus_registry.remove(process.id).await else {
        return Ok(None);
    };
    open_live.mark_sealed();
    live_bus_registry
        .push_pending(process.id, open_live.clone())
        .await;
    seal_resident_open_live(runtime_head_registry, process.id, open_live.owner_seq).await;
    let _ = sequence_engine
        .ingest(RuntimeSignal {
            kind: RuntimeSignalKind::SegmentSealed,
            observed_process_id: process.id,
            observed_stream_id: None,
            observed_slot: binding.map(|value| value.program_slot_name.clone()),
            executor_process_id,
            segment_kind: Some(open_live.segment_kind.clone()),
            content: Some(open_live.content.clone()),
            tool_name: tool_name.map(ToOwned::to_owned),
            tool_run_id,
            metadata: open_live.patch.clone(),
            observed_at: Utc::now(),
        })
        .await;
    Ok(Some(open_live))
}

fn prompt_head_from_runtime_head(
    owner_id: Uuid,
    runtime_head: &RuntimeHead,
    last_bootstrap_segment_seq: i64,
) -> ResidentPromptHeadState {
    let full_text = runtime_head.full_text.clone();
    ResidentPromptHeadState {
        owner_id,
        built_all_len: full_text
            .find("\n\n[process_stream]\n")
            .unwrap_or(full_text.len()),
        stream_started: prompt_head_stream_started(&full_text),
        full_text,
        dirty: false,
        last_bootstrap_segment_seq,
    }
}

async fn seed_ephemeral_runtime_head_if_missing(
    runtime_head_registry: &EphemeralRuntimeHeadRegistry,
    stream_id: EphemeralStreamId,
    snapshot_runtime_head: &RuntimeHead,
) {
    if runtime_head_registry.get(stream_id).await.is_some() {
        return;
    }

    runtime_head_registry
        .upsert(prompt_head_from_runtime_head(
            stream_id,
            snapshot_runtime_head,
            0,
        ))
        .await;
}

async fn ephemeral_runtime_head(
    runtime_head_registry: &EphemeralRuntimeHeadRegistry,
    stream_id: EphemeralStreamId,
    process_id: Uuid,
) -> Result<RuntimeHead> {
    let runtime_head = runtime_head_registry
        .get(stream_id)
        .await
        .ok_or_else(|| anyhow!("missing ephemeral runtime head for stream {stream_id}"))?;
    Ok(runtime_head.as_runtime_head(process_id))
}

#[derive(Debug, Clone, Copy)]
enum LiveOwnerRef {
    Process(Uuid),
    Ephemeral(EphemeralStreamId),
}
fn boundary_decision_from_signal(signal: &RuntimeSignal) -> BoundaryDecision {
    // fn metadata_uuid(signal: &RuntimeSignal, key: &str) -> Option<Uuid> {
    //     signal
    //         .metadata
    //         .as_ref()
    //         .and_then(|value| value.get(key))
    //         .and_then(Value::as_str)
    //         .and_then(|value| Uuid::parse_str(value).ok())
    // }

    // fn sequence_actions_from_signal(signal: &RuntimeSignal) -> Vec<SequenceEngineAction> {
    match signal.kind {
        RuntimeSignalKind::LiveFrame => BoundaryDecision::Continue,
        RuntimeSignalKind::ForceBoundary
        | RuntimeSignalKind::ProviderCompleted
        | RuntimeSignalKind::SendMessageCompleted => {
            BoundaryDecision::SealNow
            // RuntimeSignalKind::SendStarted => vec![SequenceEngineAction::SealCurrentSegment {
            //     process_id: signal.observed_process_id,
            // }],
            // RuntimeSignalKind::SendCompleted => {
            //     let mut actions = vec![SequenceEngineAction::SealCurrentSegment {
            //         process_id: signal.observed_process_id,
            //     }];
            //     if let Some(target_process_id) = metadata_uuid(signal, "release_target_process_id") {
            //         actions.push(SequenceEngineAction::ReleaseMailbox { process_id: target_process_id });
            //     }
            //     actions
            // }
            // RuntimeSignalKind::ReceiveCompleted
            // | RuntimeSignalKind::OutputCompleted
            // | RuntimeSignalKind::ForceInterrupted => {
            //     let mut actions = vec![SequenceEngineAction::SealCurrentSegment {
            //         process_id: signal.observed_process_id,
            //     }];
            //     if signal.kind == RuntimeSignalKind::OutputCompleted && signal.observed_stream_id.is_none()
            //     {
            //         actions.push(SequenceEngineAction::ReleaseMailbox {
            //             process_id: signal.observed_process_id,
            //         });
            //     }
            //     actions
        }
        RuntimeSignalKind::FlushTick => BoundaryDecision::Ignore,
        _ => BoundaryDecision::Ignore,
        // RuntimeSignalKind::LiveFrame
        // | RuntimeSignalKind::QueueDelivered
        // | RuntimeSignalKind::ToolFinished
        // | RuntimeSignalKind::ToolMaterialized
        // | RuntimeSignalKind::ProviderCompleted
        // | RuntimeSignalKind::ForceBoundary
        // | RuntimeSignalKind::SendMessageCompleted
        // | RuntimeSignalKind::ReceiveStarted
        // | RuntimeSignalKind::SegmentSealed
        // | RuntimeSignalKind::FlushTick => Vec::new(),
    }
}

async fn append_process_segment_to_live_bus(
    db: &Database,
    live_bus_registry: &LiveBusRegistry,
    runtime_head_registry: &RuntimeHeadRegistry,
    sequence_engine: &MonitorEventSequenceEngine,
    process: &ProcessInstanceRow,
    binding: Option<&ProgramProcessBindingRow>,
    segment_kind: &str,
    content: &str,
    tokenizer: Option<&str>,
    patch: Option<Value>,
    executor_process_id: Option<Uuid>,
    tool_name: Option<&str>,
    tool_run_id: Option<Uuid>,
) -> Result<SegmentRow> {
    if content.is_empty() {
        bail!(
            "cannot append empty content to live bus for process {}",
            process.id
        );
    }

    let (mut runtime_head, _) =
        resident_prompt_head_state(db, live_bus_registry, runtime_head_registry, process.id)
            .await?;
    append_prompt_stream_text(&mut runtime_head, content);
    runtime_head_registry.upsert(runtime_head).await;

    let live_update = async {
        let mut open_live = match live_bus_registry.get(process.id).await {
            Some(segment) => segment,
            None => OpenLiveSegment::new(
                process.id,
                next_live_owner_seq(db, runtime_head_registry, live_bus_registry, process.id)
                    .await?,
                segment_kind.to_string(),
                tokenizer.map(ToOwned::to_owned),
                patch.clone(),
            ),
        };
        open_live.apply_delta(
            content,
            Some(segment_kind.to_string()),
            tokenizer.map(ToOwned::to_owned),
            patch.clone(),
        )?;
        live_bus_registry.upsert(open_live.clone()).await;
        Ok::<OpenLiveSegment, anyhow::Error>(open_live)
    }
    .await;

    let open_live = match live_update {
        Ok(open_live) => open_live,
        Err(error) => {
            mark_resident_prompt_head_dirty(runtime_head_registry, process.id).await;
            return Err(error);
        }
    };

    let _ = sequence_engine
        .ingest(RuntimeSignal {
            kind: RuntimeSignalKind::LiveFrame,
            observed_process_id: process.id,
            observed_stream_id: None,
            observed_slot: binding.map(|value| value.program_slot_name.clone()),
            executor_process_id,
            segment_kind: Some(open_live.segment_kind.clone()),
            content: Some(content.to_string()),
            tool_name: tool_name.map(ToOwned::to_owned),
            tool_run_id,
            metadata: open_live.patch.clone(),
            observed_at: Utc::now(),
        })
        .await;

    Ok(open_live.to_segment_row())
}

// fn process_body_patch(body_subkind: &str, patch: Option<Value>) -> Option<Value> {
//     merge_patch(
//         patch,
//         Some(json!({
//             "body_subkind": body_subkind,
//         })),
//     )
// }

// async fn append_process_body_to_live_bus(
//     db: &Database,
//     live_bus_registry: &LiveBusRegistry,
//     runtime_head_registry: &RuntimeHeadRegistry,
//     sequence_engine: &MonitorEventSequenceEngine,
//     process: &ProcessInstanceRow,
//     binding: Option<&ProgramProcessBindingRow>,
//     body_subkind: &str,
//     content: &str,
//     tokenizer: Option<&str>,
//     patch: Option<Value>,
//     executor_process_id: Option<Uuid>,
//     tool_name: Option<&str>,
//     tool_run_id: Option<Uuid>,
// ) -> Result<SegmentRow> {
//     append_process_segment_to_live_bus(
//         db,
//         live_bus_registry,
//         runtime_head_registry,
//         sequence_engine,
//         process,
//         binding,
//         PROCESS_BODY_SEGMENT_KIND,
//         content,
//         tokenizer,
//         process_body_patch(body_subkind, patch),
//         executor_process_id,
//         tool_name,
//         tool_run_id,
//     )
//     .await
// }

#[derive(Debug, Clone)]
struct ProviderStreamTarget {
    live_owner: LiveOwnerRef,
    observed_process_id: Uuid,
    observed_slot: Option<String>,
    executor_process_id: Option<Uuid>,
}

struct ProviderGeneration {
    host: ProcessRuntimeBinding,
    runtime_head: RuntimeHead,
    provider_stream_target: ProviderStreamTarget,
    output: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptAdoptionDecision {
    Promote,
    Rollback,
}

fn render_provider_user_prompt(
    guide: Option<&str>,
    runtime_head: &RuntimeHead,
    task: &str,
) -> String {
    format!(
        "[guide]\n{}\n\n[runtime_head]\n{}\n\n[task]\n{}",
        guide.unwrap_or(""),
        runtime_head.full_text,
        task
    )
}

fn parse_mean_score(output: &str) -> Result<f64> {
    output
        .lines()
        .find_map(|line| {
            line.strip_prefix("mean_score:")
                .map(str::trim)
                .and_then(|value| value.parse::<f64>().ok())
        })
        .ok_or_else(|| anyhow!("score_judge output missing parseable `mean_score:` line"))
}

fn decide_prompt_adoption(prompt: &ProcessPromptRow, mean_score: f64) -> PromptAdoptionDecision {
    match prompt.adopted_mean_score {
        None => PromptAdoptionDecision::Promote,
        Some(adopted) if mean_score > adopted => PromptAdoptionDecision::Promote,
        Some(_) => PromptAdoptionDecision::Rollback,
    }
}

#[derive(Clone, Default)]
pub struct SlotMappingCache {
    inner: Arc<RwLock<HashMap<(String, String), ProcessInstanceRow>>>,
}

impl SlotMappingCache {
    pub async fn get(
        &self,
        program_run_id: &str,
        program_slot_name: &str,
    ) -> Option<ProcessInstanceRow> {
        self.inner
            .read()
            .await
            .get(&(program_run_id.to_string(), program_slot_name.to_string()))
            .cloned()
    }

    pub async fn insert(
        &self,
        program_run_id: &str,
        program_slot_name: &str,
        process: ProcessInstanceRow,
    ) {
        self.inner.write().await.insert(
            (program_run_id.to_string(), program_slot_name.to_string()),
            process,
        );
    }

    pub async fn remove(&self, program_run_id: &str, program_slot_name: &str) {
        self.inner
            .write()
            .await
            .remove(&(program_run_id.to_string(), program_slot_name.to_string()));
    }
}

#[derive(Clone, Default)]
struct ActivityRegistry {
    inner: Arc<RwLock<HashMap<Uuid, DateTime<Utc>>>>,
}

impl ActivityRegistry {
    async fn touch(&self, process_id: Uuid) {
        self.inner.write().await.insert(process_id, Utc::now());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSignalKind {
    SegmentSealed,
    QueueDelivered,
    ToolFinished,
    ToolMaterialized,
    LiveFrame,
    ProviderCompleted,
    ForceBoundary,
    SendMessageCompleted,
    // SendStarted,
    // SendCompleted,
    // ReceiveStarted,
    // ReceiveCompleted,
    // OutputCompleted,
    // ForceInterrupted,
    FlushTick,
}

impl RuntimeSignalKind {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "segment_sealed" => Some(Self::SegmentSealed),
            "queue_delivered" => Some(Self::QueueDelivered),
            "tool_finished" => Some(Self::ToolFinished),
            "tool_materialized" => Some(Self::ToolMaterialized),
            "live_frame" => Some(Self::LiveFrame),
            "provider_completed" => Some(Self::ProviderCompleted),
            "force_boundary" => Some(Self::ForceBoundary),
            "send_message_completed" => Some(Self::SendMessageCompleted),
            // "send_started" => Some(Self::SendStarted),
            // "send_completed" => Some(Self::SendCompleted),
            // "receive_started" => Some(Self::ReceiveStarted),
            // "receive_completed" => Some(Self::ReceiveCompleted),
            // "output_completed" => Some(Self::OutputCompleted),
            // "force_interrupted" => Some(Self::ForceInterrupted),
            "flush_tick" => Some(Self::FlushTick),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentSealed => "segment_sealed",
            Self::QueueDelivered => "queue_delivered",
            Self::ToolFinished => "tool_finished",
            Self::ToolMaterialized => "tool_materialized",
            Self::LiveFrame => "live_frame",
            Self::ProviderCompleted => "provider_completed",
            Self::ForceBoundary => "force_boundary",
            Self::SendMessageCompleted => "send_message_completed",
            // Self::SendStarted => "send_started",
            // Self::SendCompleted => "send_completed",
            // Self::ReceiveStarted => "receive_started",
            // Self::ReceiveCompleted => "receive_completed",
            // Self::OutputCompleted => "output_completed",
            // Self::ForceInterrupted => "force_interrupted",
            Self::FlushTick => "flush_tick",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSignal {
    pub kind: RuntimeSignalKind,
    pub observed_process_id: Uuid,
    pub observed_stream_id: Option<EphemeralStreamId>,
    pub observed_slot: Option<String>,
    pub executor_process_id: Option<Uuid>,
    pub segment_kind: Option<String>,
    pub content: Option<String>,
    pub tool_name: Option<String>,
    pub tool_run_id: Option<Uuid>,
    pub metadata: Option<Value>,
    pub observed_at: DateTime<Utc>,
}

// #[allow(dead_code)]
#[derive(Debug, Clone)]
enum BoundaryDecision {
    Continue,
    SealNow,
    Ignore,
    // enum SequenceEngineAction {
    //     SealCurrentSegment { process_id: Uuid },
    //     OpenSegment { process_id: Uuid, segment_kind: String },
    //     AppendToCurrent {
    //         process_id: Uuid,
    //         segment_kind: String,
    //         content: String,
    //         patch: Option<Value>,
    //     },
    //     ReleaseMailbox { process_id: Uuid },
    //     MarkMailboxReleasePending { process_id: Uuid, cause: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceFilter {
    pub watched_process_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watched_stream_id: Option<EphemeralStreamId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_kinds: Vec<RuntimeSignalKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "matcher", rename_all = "snake_case")]
pub enum SequenceMatcher {
    AnyOfAllowed,
    OrderedSubsequence { sequence: Vec<RuntimeSignalKind> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceBuffer {
    pub next_index: usize,
    pub matched_signals: Vec<RuntimeSignal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceOutcome {
    pub registration_id: Uuid,
    pub host_process_id: Uuid,
    pub watched_process_id: Uuid,
    pub matched_event_kinds: Vec<String>,
    pub matched_sequence: Vec<Value>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: String,
    pub output_text: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone)]
struct QueueSequenceTarget {
    process: ProcessInstanceRow,
    binding: Option<ProgramProcessBindingRow>,
}

#[derive(Debug, Clone)]
struct SequenceRegistration {
    registration_id: Uuid,
    host_process: ProcessInstanceRow,
    host_binding: Option<ProgramProcessBindingRow>,
    watched_process: ProcessInstanceRow,
    filter: SequenceFilter,
    matcher: SequenceMatcher,
    buffer: SequenceBuffer,
    started_at: DateTime<Utc>,
    deadline_at: Option<DateTime<Utc>>,
    mode: String,
    return_mode: String,
    message_type: String,
    priority_mark: Option<String>,
    queue_target: Option<QueueSequenceTarget>,
    guide: Option<String>,
}

struct SequenceRegistrationHandle {
    registration: SequenceRegistration,
    outcome_tx: Option<oneshot::Sender<SequenceOutcome>>,
}

#[derive(Debug, Clone)]
enum RemoteExecutionHost {
    PersistedProcess {
        host: ProcessRuntimeBinding,
    },
    EphemeralClone {
        host: ProcessRuntimeBinding,
        stream_id: EphemeralStreamId,
        snapshot_runtime_head: RuntimeHead,
    },
}

#[derive(Debug, Clone)]
struct RemoteTaskExecutionRequest {
    execution_host: RemoteExecutionHost,
    envelope: QueueEnvelope,
}

#[derive(Debug, Clone)]
struct HardwareExecutionRequest {
    host: ProcessRuntimeBinding,
    envelope: QueueEnvelope,
}

#[derive(Clone, Default)]
pub struct MonitorEventSequenceEngine {
    inner: Arc<Mutex<HashMap<Uuid, SequenceRegistrationHandle>>>,
}

impl MonitorEventSequenceEngine {
    async fn register(
        &self,
        registration: SequenceRegistration,
    ) -> oneshot::Receiver<SequenceOutcome> {
        let registration_id = registration.registration_id;
        let deadline_at = registration.deadline_at;
        let (outcome_tx, outcome_rx) = oneshot::channel();
        self.inner.lock().await.insert(
            registration_id,
            SequenceRegistrationHandle {
                registration,
                outcome_tx: Some(outcome_tx),
            },
        );

        if let Some(deadline_at) = deadline_at {
            let engine = self.clone();
            tokio::spawn(async move {
                let now = Utc::now();
                let sleep_for = if deadline_at > now {
                    (deadline_at - now)
                        .to_std()
                        .unwrap_or_else(|_| Duration::from_millis(0))
                } else {
                    Duration::from_millis(0)
                };
                time::sleep(sleep_for).await;
                engine.finish_with_status(registration_id, "timeout").await;
            });
        }

        outcome_rx
    }

    async fn cancel_for_process(&self, process_id: Uuid) {
        let registration_ids = {
            let inner = self.inner.lock().await;
            inner
                .iter()
                .filter_map(|(registration_id, handle)| {
                    let registration = &handle.registration;
                    let queue_target_matches = registration
                        .queue_target
                        .as_ref()
                        .map(|target| target.process.id == process_id)
                        .unwrap_or(false);
                    if registration.host_process.id == process_id
                        || registration.watched_process.id == process_id
                        || queue_target_matches
                    {
                        Some(*registration_id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        for registration_id in registration_ids {
            self.finish_with_status(registration_id, "cancelled").await;
        }
    }
    async fn ingest(&self, signal: RuntimeSignal) -> BoundaryDecision {
        // async fn ingest(&self, signal: RuntimeSignal) -> Vec<SequenceEngineAction> {
        let completed = {
            let mut inner = self.inner.lock().await;
            let mut completed = Vec::new();

            for handle in inner.values_mut() {
                let registration = &mut handle.registration;
                if registration.filter.watched_process_id != signal.observed_process_id {
                    continue;
                }
                if registration.filter.watched_stream_id.is_some()
                    && registration.filter.watched_stream_id != signal.observed_stream_id
                {
                    continue;
                }
                if registration.filter.event_kinds.is_empty() {
                    continue;
                }
                if !registration.filter.event_kinds.contains(&signal.kind) {
                    continue;
                }

                let matched = match &registration.matcher {
                    SequenceMatcher::AnyOfAllowed => true,
                    SequenceMatcher::OrderedSubsequence { sequence } => sequence
                        .get(registration.buffer.next_index)
                        .map(|expected| expected == &signal.kind)
                        .unwrap_or(false),
                };

                if !matched {
                    continue;
                }

                registration.buffer.matched_signals.push(signal.clone());
                registration.buffer.next_index += 1;

                let done = match &registration.matcher {
                    SequenceMatcher::AnyOfAllowed => true,
                    SequenceMatcher::OrderedSubsequence { sequence } => {
                        registration.buffer.next_index >= sequence.len()
                    }
                };
                if done {
                    completed.push(registration.registration_id);
                }
            }

            completed
        };

        for registration_id in completed {
            self.finish_with_status(registration_id, "matched").await;
        }
        boundary_decision_from_signal(&signal)
        // sequence_actions_from_signal(&signal)
    }

    async fn finish_with_status(&self, registration_id: Uuid, status: &str) {
        let finished = {
            let mut inner = self.inner.lock().await;
            inner.remove(&registration_id)
        };

        let Some(mut handle) = finished else {
            return;
        };

        let outcome = registration_outcome(&handle.registration, status);
        if let Some(outcome_tx) = handle.outcome_tx.take() {
            let _ = outcome_tx.send(outcome);
        }
    }
}

#[derive(Clone)]
pub struct GlobalMessageQueue {
    tx: mpsc::UnboundedSender<QueueEnvelope>,
}

impl GlobalMessageQueue {
    fn new() -> (Self, mpsc::UnboundedReceiver<QueueEnvelope>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    fn enqueue(&self, envelope: QueueEnvelope) -> Result<()> {
        if let Some(delay_ms) = envelope.delay_ms.filter(|value| *value > 0) {
            let tx = self.tx.clone();
            tokio::spawn(async move {
                time::sleep(Duration::from_millis(delay_ms)).await;
                let _ = tx.send(envelope);
            });
            return Ok(());
        }

        self.tx
            .send(envelope)
            .map_err(|error| anyhow!("failed to enqueue queue envelope: {error}"))
    }
}

#[derive(Debug, Clone)]
struct MailboxBatch {
    info: QueueBatchInfo,
    envelopes: Vec<QueueEnvelope>,
    rerouted_from_process_id: Option<Uuid>,
    rerouted_to_process_id: Option<Uuid>,
}

impl MailboxBatch {
    fn new(envelope: QueueEnvelope) -> Self {
        let final_priority = envelope.final_priority.unwrap_or(envelope.base_priority);
        let final_delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);
        let created_at = envelope.created_at;
        let target_slot = envelope
            .target_program_slot
            .clone()
            .unwrap_or_else(|| envelope.target_external_name.clone());
        Self {
            info: QueueBatchInfo {
                batch_id: Uuid::new_v4(),
                target_process_id: envelope.target_process_id,
                target_slot,
                target_program_run_id: envelope.target_program_run_id.clone(),
                priority_mark: priority_label(final_priority),
                delivery_policy: final_delivery_action.as_str().to_string(),
                envelope_ids: vec![envelope.envelope_id],
                final_priority,
                final_delivery_action,
                message_ids: vec![envelope.message_id],
                first_enqueued_at: created_at,
                last_enqueued_at: created_at,
                frozen: false,
            },
            envelopes: vec![envelope],
            rerouted_from_process_id: None,
            rerouted_to_process_id: None,
        }
    }

    fn append(&mut self, envelope: QueueEnvelope) {
        self.info.envelope_ids.push(envelope.envelope_id);
        self.info.message_ids.push(envelope.message_id);
        self.info.last_enqueued_at = envelope.created_at;
        self.envelopes.push(envelope);
    }
}

#[derive(Debug, Clone)]
struct ProcessMailbox {
    lanes: HashMap<(u8, DeliveryAction), VecDeque<MailboxBatch>>,
    // pending_release_causes: Vec<String>,
}

impl ProcessMailbox {
    fn new() -> Self {
        Self {
            lanes: HashMap::new(),
            // pending_release_causes: Vec::new(),
        }
    }

    fn enqueue(&mut self, envelope: QueueEnvelope) -> QueueBatchInfo {
        let key = (
            envelope.final_priority.unwrap_or(envelope.base_priority),
            envelope
                .final_delivery_action
                .unwrap_or(envelope.requested_delivery_action),
        );
        let lane = self.lanes.entry(key).or_default();
        if let Some(batch) = lane.back_mut().filter(|batch| !batch.info.frozen) {
            batch.append(envelope);
            return batch.info.clone();
        }

        let batch = MailboxBatch::new(envelope);
        let info = batch.info.clone();
        lane.push_back(batch);
        info
    }

    fn drain_release_batches(&mut self) -> Vec<MailboxBatch> {
        let mut released = Vec::new();
        for final_priority in 1..=8 {
            for delivery_action in DELIVERY_ACTION_ORDER {
                let Some(lane) = self.lanes.get_mut(&(final_priority, delivery_action)) else {
                    continue;
                };
                while let Some(batch) = lane.pop_front() {
                    released.push(batch);
                }
            }
        }
        self.lanes.retain(|_, lane| !lane.is_empty());
        released
    }

    fn is_empty(&self) -> bool {
        self.lanes.is_empty()
        //     self.lanes.is_empty() && self.pending_release_causes.is_empty()
        // }

        // fn has_pending_release(&self) -> bool {
        //     !self.pending_release_causes.is_empty()
        // }

        // fn mark_release_pending(&mut self, cause: impl Into<String>) {
        //     let cause = cause.into();
        //     if !cause.is_empty() && !self.pending_release_causes.iter().any(|item| item == &cause) {
        //         self.pending_release_causes.push(cause);
        //     }
        // }

        // fn clear_pending_release(&mut self) {
        //     self.pending_release_causes.clear();
    }
}

#[derive(Clone, Default)]
struct MailboxRegistry {
    inner: Arc<Mutex<HashMap<Uuid, ProcessMailbox>>>,
}

#[derive(Clone)]
pub struct QueueDispatcher {
    db: Database,
    live_bus_registry: LiveBusRegistry,
    ephemeral_live_bus_registry: EphemeralLiveBusRegistry,
    runtime_head_registry: RuntimeHeadRegistry,
    ephemeral_runtime_head_registry: EphemeralRuntimeHeadRegistry,
    activity_registry: ActivityRegistry,
    global_message_queue: GlobalMessageQueue,
    sequence_engine: MonitorEventSequenceEngine,
    mailboxes: MailboxRegistry,
    task_exec_tx: mpsc::UnboundedSender<RemoteTaskExecutionRequest>,
}

impl QueueDispatcher {
    fn new(
        db: Database,
        live_bus_registry: LiveBusRegistry,
        ephemeral_live_bus_registry: EphemeralLiveBusRegistry,
        runtime_head_registry: RuntimeHeadRegistry,
        ephemeral_runtime_head_registry: EphemeralRuntimeHeadRegistry,
        activity_registry: ActivityRegistry,
        global_message_queue: GlobalMessageQueue,
        sequence_engine: MonitorEventSequenceEngine,
        task_exec_tx: mpsc::UnboundedSender<RemoteTaskExecutionRequest>,
    ) -> Self {
        Self {
            db,
            live_bus_registry,
            ephemeral_live_bus_registry,
            runtime_head_registry,
            ephemeral_runtime_head_registry,
            activity_registry,
            global_message_queue,
            sequence_engine,
            mailboxes: MailboxRegistry::default(),
            task_exec_tx,
        }
    }

    fn start(self, mut rx: mpsc::UnboundedReceiver<QueueEnvelope>) {
        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                self.accept_envelope(envelope).await;
            }
        });
    }

    async fn retarget_envelope_to_current_slot(
        &self,
        mut envelope: QueueEnvelope,
    ) -> QueueEnvelope {
        let Some(program_run_id) = envelope.target_program_run_id.clone() else {
            return envelope;
        };
        let Some(program_slot_name) = envelope.target_program_slot.clone() else {
            return envelope;
        };
        let Ok(Some(current_process)) = self
            .db
            .resolve_process_by_slot(&program_run_id, &program_slot_name)
            .await
        else {
            return envelope;
        };
        if current_process.id == envelope.target_process_id {
            return envelope;
        }

        envelope.target_process_id = current_process.id;
        envelope.target_external_name = current_process.external_slot_name;
        envelope
    }

    async fn accept_envelope(&self, envelope: QueueEnvelope) {
        let envelope = self.retarget_envelope_to_current_slot(envelope).await;
        let final_delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);

        match final_delivery_action {
            DeliveryAction::InterruptDeliver => {
                let batch = MailboxBatch::new(envelope);
                self.deliver_batch(batch).await;
            }
            DeliveryAction::SegmentBoundaryDeliver => {
                let mut inner = self.mailboxes.inner.lock().await;
                let mailbox = inner
                    .entry(envelope.target_process_id)
                    .or_insert_with(ProcessMailbox::new);
                mailbox.enqueue(envelope);
                // let target_process_id = envelope.target_process_id;
                // let should_try_release = {
                //     let mut inner = self.mailboxes.inner.lock().await;
                //     let mailbox = inner
                //         .entry(target_process_id)
                //         .or_insert_with(ProcessMailbox::new);
                //     mailbox.enqueue(envelope);
                //     mailbox.has_pending_release()
                // };
                // if should_try_release {
                //     self.request_release_for_process(
                //         target_process_id,
                //         "pending_release_after_enqueue",
                //     )
                //     .await;
                // }
            }
            DeliveryAction::PersistentAsyncClone | DeliveryAction::EphemeralAsyncClone => {
                if let Err(error) = self.deliver_via_async_clone(envelope).await {
                    self.emit_delivery_failure(
                        Uuid::new_v4(),
                        None,
                        "async_clone_target".to_string(),
                        Vec::new(),
                        format!("failed async clone delivery: {error}"),
                    );
                }
            }
            DeliveryAction::BounceWithHint => {
                let _ = self.bounce_message(envelope).await;
            }
            DeliveryAction::Ignore => {}
            DeliveryAction::BlacklistSender => {
                let _ = self.blacklist_sender_and_drop(envelope).await;
            }
        }
    }

    async fn note_runtime_activity(&self, process_id: Uuid) {
        self.activity_registry.touch(process_id).await;
    }
    async fn release_for_process(&self, process_id: Uuid) {
        let batches = {
            let mut inner = self.mailboxes.inner.lock().await;
            let Some(mailbox) = inner.get_mut(&process_id) else {
                return;
            };
            let batches = mailbox.drain_release_batches();
            if mailbox.is_empty() {
                inner.remove(&process_id);
            }
            batches
        };

        for batch in batches {
            self.deliver_batch(batch).await;
        }
    }

    async fn move_mailbox_target(&self, from_process_id: Uuid, target: &ProcessRuntimeBinding) {
        let target_slot = target
            .binding
            .as_ref()
            .map(|binding| binding.program_slot_name.clone())
            .unwrap_or_else(|| target.process.external_slot_name.clone());
        let target_program_run_id = target
            .binding
            .as_ref()
            .map(|binding| binding.program_run_id.clone());

        let mut inner = self.mailboxes.inner.lock().await;
        let Some(mut mailbox) = inner.remove(&from_process_id) else {
            return;
        };

        for lane in mailbox.lanes.values_mut() {
            for batch in lane.iter_mut() {
                batch.info.target_process_id = target.process.id;
                batch.info.target_slot = target_slot.clone();
                batch.info.target_program_run_id = target_program_run_id.clone();
                for envelope in batch.envelopes.iter_mut() {
                    envelope.target_process_id = target.process.id;
                    envelope.target_external_name = target.process.external_slot_name.clone();
                    envelope.target_program_run_id = target_program_run_id.clone();
                    envelope.target_program_slot = target
                        .binding
                        .as_ref()
                        .map(|binding| binding.program_slot_name.clone());
                }
            }
        }

        let target_mailbox = inner
            .entry(target.process.id)
            .or_insert_with(ProcessMailbox::new);
        for (key, mut batches) in mailbox.lanes.drain() {
            target_mailbox
                .lanes
                .entry(key)
                .or_default()
                .append(&mut batches);
        }
    }

    async fn deliver_batch(&self, batch: MailboxBatch) {
        let delivery_action = batch.info.final_delivery_action;
        let target = match self
            .db
            .get_process_runtime_binding(batch.info.target_process_id)
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                self.emit_delivery_failure(
                    batch.info.batch_id,
                    Some(batch.info.target_process_id),
                    batch.info.target_slot.clone(),
                    batch.info.envelope_ids.clone(),
                    format!("failed to load target process: {error}"),
                );
                return;
            }
        };

        if delivery_action == DeliveryAction::InterruptDeliver {
            let _ = self.force_boundary_for_process(&target).await;
            // let _ = self.force_interrupt_for_process(&target).await;
        }

        let filtered = batch
            .envelopes
            .into_iter()
            .filter(|envelope| {
                !blacklist_blocks_message(
                    target.process.policy_json.as_ref(),
                    envelope.sender_process_id,
                    envelope.source_kind,
                    envelope.metadata.as_ref(),
                )
            })
            .collect::<Vec<_>>();

        if filtered.is_empty() {
            self.emit_delivery_failure(
                batch.info.batch_id,
                Some(target.process.id),
                target
                    .binding
                    .as_ref()
                    .map(|binding| binding.program_slot_name.clone())
                    .unwrap_or_else(|| target.process.external_slot_name.clone()),
                batch.info.envelope_ids.clone(),
                "all envelopes were blocked by invisible_process policy".to_string(),
            );
            return;
        }

        let content = aggregate_batch_content(&batch.info, &filtered);
        let patch = queue_batch_patch(
            &batch.info,
            &filtered,
            target.binding.as_ref(),
            batch.rerouted_from_process_id,
            batch.rerouted_to_process_id,
        );

        match self
            .append_mailbox_batch_to_live(&target, &content, patch)
            .await
        {
            Ok(segment) => {
                let _ = self
                    .sequence_engine
                    .ingest(RuntimeSignal {
                        kind: RuntimeSignalKind::QueueDelivered,
                        observed_process_id: target.process.id,
                        observed_stream_id: None,
                        observed_slot: target
                            .binding
                            .as_ref()
                            .map(|binding| binding.program_slot_name.clone()),
                        executor_process_id: None,
                        segment_kind: Some(segment.segment_kind.clone()),
                        content: Some(segment.content.clone()),
                        tool_name: None,
                        tool_run_id: None,
                        metadata: segment.patch.clone(),
                        observed_at: Utc::now(),
                    })
                    .await;

                let _ = self
                    .record_forced_event_results(batch.info.batch_id, &filtered, &target)
                    .await;

                for envelope in filtered
                    .iter()
                    .filter(|envelope| envelope.message_kind == MessageKind::TaskMessage)
                {
                    let _ = self.task_exec_tx.send(RemoteTaskExecutionRequest {
                        execution_host: RemoteExecutionHost::PersistedProcess {
                            host: target.clone(),
                        },
                        envelope: envelope.clone(),
                    });
                }
            }
            Err(error) => {
                self.emit_delivery_failure(
                    batch.info.batch_id,
                    Some(target.process.id),
                    batch.info.target_slot.clone(),
                    batch.info.envelope_ids.clone(),
                    format!("failed to materialize mailbox batch: {error}"),
                );
            }
        }
    }

    async fn append_mailbox_batch_to_live(
        &self,
        target: &ProcessRuntimeBinding,
        content: &str,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        append_process_segment_to_live_bus(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            &self.sequence_engine,
            &target.process,
            target.binding.as_ref(),
            RECEIVE_SEGMENT_KIND,
            content,
            Some("whitespace"),
            patch,
            None,
            None,
            None,
        )
        .await
    }

    async fn materialize_ephemeral_batch(
        &self,
        stream_id: EphemeralStreamId,
        target: &ProcessRuntimeBinding,
        batch: MailboxBatch,
    ) -> Result<Vec<QueueEnvelope>> {
        let filtered = batch
            .envelopes
            .into_iter()
            .filter(|envelope| {
                !blacklist_blocks_message(
                    target.process.policy_json.as_ref(),
                    envelope.sender_process_id,
                    envelope.source_kind,
                    envelope.metadata.as_ref(),
                )
            })
            .collect::<Vec<_>>();

        if filtered.is_empty() {
            self.emit_delivery_failure(
                batch.info.batch_id,
                Some(target.process.id),
                target
                    .binding
                    .as_ref()
                    .map(|binding| binding.program_slot_name.clone())
                    .unwrap_or_else(|| target.process.external_slot_name.clone()),
                batch.info.envelope_ids.clone(),
                "all envelopes were blocked by invisible_process policy".to_string(),
            );
            return Ok(filtered);
        }

        let content = aggregate_batch_content(&batch.info, &filtered);
        let patch = queue_batch_patch(
            &batch.info,
            &filtered,
            target.binding.as_ref(),
            batch.rerouted_from_process_id,
            batch.rerouted_to_process_id,
        );

        match append_ephemeral_mailbox_batch_to_live(
            &self.ephemeral_live_bus_registry,
            &self.ephemeral_runtime_head_registry,
            stream_id,
            &content,
            patch,
        )
        .await
        {
            Ok(segment) => {
                let _ = self
                    .sequence_engine
                    .ingest(RuntimeSignal {
                        kind: RuntimeSignalKind::QueueDelivered,
                        observed_process_id: target.process.id,
                        observed_stream_id: Some(stream_id),
                        observed_slot: target
                            .binding
                            .as_ref()
                            .map(|binding| binding.program_slot_name.clone()),
                        executor_process_id: None,
                        segment_kind: Some(segment.segment_kind.clone()),
                        content: Some(segment.content.clone()),
                        tool_name: None,
                        tool_run_id: None,
                        metadata: segment.patch.clone(),
                        observed_at: Utc::now(),
                    })
                    .await;

                let _ = self
                    .record_forced_event_results(batch.info.batch_id, &filtered, target)
                    .await;
            }
            Err(error) => {
                self.emit_delivery_failure(
                    batch.info.batch_id,
                    Some(target.process.id),
                    batch.info.target_slot.clone(),
                    batch.info.envelope_ids.clone(),
                    format!("failed to materialize ephemeral mailbox batch: {error}"),
                );
                return Err(error);
            }
        }

        Ok(filtered)
    }

    async fn force_boundary_for_process(&self, target: &ProcessRuntimeBinding) -> Result<()> {
        let Some(open_live) = self.live_bus_registry.get(target.process.id).await else {
            return Ok(());
        };
        let decision = self
            .sequence_engine
            .ingest(RuntimeSignal {
                kind: RuntimeSignalKind::ForceBoundary,
                observed_process_id: target.process.id,
                observed_stream_id: None,
                observed_slot: target
                    .binding
                    .as_ref()
                    .map(|binding| binding.program_slot_name.clone()),
                executor_process_id: None,
                segment_kind: Some(open_live.segment_kind.clone()),
                content: Some(open_live.content.clone()),
                tool_name: None,
                tool_run_id: None,
                metadata: open_live.patch.clone(),
                observed_at: Utc::now(),
            })
            .await;
        if matches!(decision, BoundaryDecision::SealNow) {
            seal_persisted_live_to_pending(
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine,
                &target.process,
                target.binding.as_ref(),
                None,
                None,
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn clone_target_process_for_async_action(
        &self,
        original_target: &ProcessRuntimeBinding,
        delivery_action: DeliveryAction,
    ) -> Result<ProcessRuntimeBinding> {
        let is_ephemeral = delivery_action == DeliveryAction::EphemeralAsyncClone;

        let clone_process = self
            .db
            .spawn_free_process(
                original_target.process.prefix_suffix_definite_id,
                if is_ephemeral {
                    "ephemeral_running"
                } else {
                    "running"
                },
            )
            .await?;
        let clone_binding = if let Some(original_binding) = original_target.binding.as_ref() {
            let clone_slot_name = format!(
                "{}__{}__{}",
                original_binding.program_slot_name,
                delivery_action.as_str(),
                &Uuid::new_v4().simple().to_string()[..8]
            );

            let mut program = self
                .db
                .get_program(&original_binding.program_run_id)
                .await?;
            let inherited_slot = program
                .plan_state_json
                .slots
                .get(&original_binding.program_slot_name)
                .cloned();
            let inherited_prefix_suffix_definite_id = inherited_slot
                .as_ref()
                .and_then(|slot| slot.prefix_suffix_definite_id)
                .or(Some(original_target.process.prefix_suffix_definite_id));
            let upstream_group = inherited_slot
                .as_ref()
                .map(|slot| slot.upstream_group.clone())
                .unwrap_or_else(|| clone_slot_name.clone());
            let downstream_group = inherited_slot
                .as_ref()
                .map(|slot| slot.downstream_group.clone())
                .unwrap_or_else(|| clone_slot_name.clone());

            program.plan_state_json.slots.insert(
                clone_slot_name.clone(),
                default_program_slot_state(
                    &clone_slot_name,
                    inherited_prefix_suffix_definite_id,
                    Some(original_binding.program_slot_name.clone()),
                    upstream_group,
                    downstream_group,
                    Vec::new(),
                ),
            );
            self.db
                .update_program_plan_state(
                    &original_binding.program_run_id,
                    &program.plan_state_json,
                )
                .await?;

            Some(
                self.db
                    .bind_process_to_program(
                        &original_binding.program_run_id,
                        &clone_slot_name,
                        clone_process.id,
                        None,
                    )
                    .await?,
            )
        } else {
            None
        };

        let original_runtime_head = resident_runtime_head(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            original_target.process.id,
        )
        .await?;
        if !is_ephemeral {
            let sealed_segments = load_history_segments(
                &self.db,
                &self.live_bus_registry,
                original_target.process.id,
            )
            .await?;
            let open_live_segment = self
                .live_bus_registry
                .get(original_target.process.id)
                .await
                .map(|segment| segment.to_segment_row());
            let mut last_cloned_seq = 0_i64;
            for source_segment in &sealed_segments {
                let segment = self
                    .db
                    .append_process_segment(
                        clone_process.id,
                        &source_segment.segment_kind,
                        &source_segment.content,
                        Some(source_segment.token_count),
                        source_segment.tokenizer.as_deref(),
                        source_segment.patch.clone(),
                    )
                    .await?;
                last_cloned_seq = segment.owner_seq;
            }
            if let Some(open_live) = open_live_segment {
                let mut cloned_open_live = OpenLiveSegment::new(
                    clone_process.id,
                    last_cloned_seq + 1,
                    open_live.segment_kind.clone(),
                    open_live.tokenizer.clone(),
                    open_live.patch.clone(),
                );
                cloned_open_live.apply_delta(
                    &open_live.content,
                    Some(open_live.segment_kind),
                    open_live.tokenizer,
                    open_live.patch,
                )?;
                self.live_bus_registry.upsert(cloned_open_live).await;
            }
            let (source_prompt_head, _) = resident_prompt_head_state(
                &self.db,
                &self.live_bus_registry,
                &self.runtime_head_registry,
                original_target.process.id,
            )
            .await?;
            self.runtime_head_registry
                .upsert(ResidentPromptHeadState {
                    owner_id: clone_process.id,
                    full_text: original_runtime_head.full_text.clone(),
                    built_all_len: source_prompt_head.built_all_len,
                    stream_started: source_prompt_head.stream_started,
                    dirty: false,
                    last_bootstrap_segment_seq: source_prompt_head.last_bootstrap_segment_seq,
                })
                .await;
        }

        Ok(ProcessRuntimeBinding {
            process: clone_process,
            binding: clone_binding,
        })
    }

    async fn deliver_via_async_clone(&self, mut envelope: QueueEnvelope) -> Result<()> {
        let original_target = self
            .db
            .get_process_runtime_binding(envelope.target_process_id)
            .await?;
        let delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);

        if delivery_action == DeliveryAction::EphemeralAsyncClone {
            let snapshot_runtime_head = resident_runtime_head(
                &self.db,
                &self.live_bus_registry,
                &self.runtime_head_registry,
                original_target.process.id,
            )
            .await?;
            let stream_id = Uuid::new_v4();
            seed_ephemeral_runtime_head_if_missing(
                &self.ephemeral_runtime_head_registry,
                stream_id,
                &snapshot_runtime_head,
            )
            .await;
            let filtered = self
                .materialize_ephemeral_batch(
                    stream_id,
                    &original_target,
                    MailboxBatch::new(envelope.clone()),
                )
                .await?;
            if envelope.message_kind == MessageKind::TaskMessage && !filtered.is_empty() {
                let _ = self.task_exec_tx.send(RemoteTaskExecutionRequest {
                    execution_host: RemoteExecutionHost::EphemeralClone {
                        host: original_target,
                        stream_id,
                        snapshot_runtime_head,
                    },
                    envelope,
                });
            } else {
                self.ephemeral_live_bus_registry.clear(stream_id).await;
                self.ephemeral_runtime_head_registry.remove(stream_id).await;
            }
            return Ok(());
        }

        let cloned_target = self
            .clone_target_process_for_async_action(&original_target, delivery_action)
            .await?;

        envelope.target_process_id = cloned_target.process.id;
        envelope.target_external_name = cloned_target.process.external_slot_name.clone();
        envelope.target_program_run_id = cloned_target
            .binding
            .as_ref()
            .map(|binding| binding.program_run_id.clone());
        envelope.target_program_slot = cloned_target
            .binding
            .as_ref()
            .map(|binding| binding.program_slot_name.clone());

        let mut batch = MailboxBatch::new(envelope);
        batch.rerouted_from_process_id = Some(original_target.process.id);
        batch.rerouted_to_process_id = Some(cloned_target.process.id);
        batch.info.target_process_id = cloned_target.process.id;
        batch.info.target_program_run_id = cloned_target
            .binding
            .as_ref()
            .map(|binding| binding.program_run_id.clone());
        batch.info.target_slot = cloned_target
            .binding
            .as_ref()
            .map(|binding| binding.program_slot_name.clone())
            .unwrap_or_else(|| cloned_target.process.external_slot_name.clone());

        self.deliver_batch(batch).await;
        Ok(())
    }

    async fn bounce_message(&self, envelope: QueueEnvelope) -> Result<()> {
        let sender = self
            .db
            .get_process_runtime_binding(envelope.sender_process_id)
            .await?;
        let bounce_content = envelope
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("bounce_hint"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "message was bounced by delivery judge".to_string());
        let bounced = QueueEnvelope {
            envelope_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            message_kind: MessageKind::NormalMessage,
            source_kind: MessageSourceKind::System,
            sender_process_id: envelope.target_process_id,
            sender_external_name: envelope.target_external_name.clone(),
            sender_program_run_id: envelope.target_program_run_id.clone(),
            sender_program_slot: envelope.target_program_slot.clone(),
            target_process_id: sender.process.id,
            target_external_name: sender.process.external_slot_name.clone(),
            target_program_run_id: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone()),
            target_program_slot: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            message_type: "bounce_hint".to_string(),
            base_priority: envelope.final_priority.unwrap_or(envelope.base_priority),
            final_priority: Some(envelope.final_priority.unwrap_or(envelope.base_priority)),
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
            priority_mark: Some(priority_label(
                envelope.final_priority.unwrap_or(envelope.base_priority),
            )),
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_process_id: None,
            explicit_result_program_run_id: None,
            explicit_result_program_slot: None,
            content: Some(bounce_content),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: Some(json!({
                "normal_subkind": "bounce_hint",
                "original_message_id": envelope.message_id.to_string(),
                "original_requested_delivery_action": envelope.requested_delivery_action.as_str(),
                "original_final_delivery_action": envelope.final_delivery_action.map(|action| action.as_str().to_string()),
            })),
            reply_to: None,
            created_at: Utc::now(),
        };
        self.global_message_queue.enqueue(bounced)?;
        Ok(())
    }

    async fn blacklist_sender_and_drop(&self, envelope: QueueEnvelope) -> Result<()> {
        let target = self
            .db
            .get_process_runtime_binding(envelope.target_process_id)
            .await?;
        self.db
            .update_process_policy(
                target.process.id,
                upsert_blacklist_entry(
                    target.process.policy_json.clone(),
                    BlacklistEntry {
                        process_id: envelope.sender_process_id,
                        block_mode: "all".to_string(),
                    },
                ),
            )
            .await?;
        Ok(())
    }

    fn emit_delivery_failure(
        &self,
        _queue_batch_id: Uuid,
        _target_process_id: Option<Uuid>,
        _target_slot: String,
        _envelope_ids: Vec<Uuid>,
        _reason: String,
    ) {
    }

    async fn record_forced_event_results(
        &self,
        queue_batch_id: Uuid,
        envelopes: &[QueueEnvelope],
        delivered_target: &ProcessRuntimeBinding,
    ) -> Result<()> {
        for envelope in envelopes.iter().filter(|envelope| {
            envelope.source_kind == MessageSourceKind::System
                && envelope.message_type == StepKind::ForcedEvent.as_str()
        }) {
            let metadata = envelope.metadata.as_ref();
            let forced_event_step_id = metadata
                .and_then(|value| value.get("step_id"))
                .and_then(Value::as_str)
                .unwrap_or("unknown_forced_event_step");
            let target_slot = envelope
                .target_program_slot
                .clone()
                .unwrap_or_else(|| envelope.target_external_name.clone());
            let result_content = format!(
                "forced_event `{}` executed successfully.\naction_type: mailbox_delivery\nstatus: delivered\ntarget_slot: {}\ntarget_process_id: {}",
                forced_event_step_id,
                target_slot,
                envelope.target_process_id
            );
            let result_patch = Some(json!({
                "forced_event_step_id": forced_event_step_id,
                "executor_process_id": envelope.sender_process_id.to_string(),
                "action_type": "mailbox_delivery",
                "status": "delivered",
                "queue_batch_id": queue_batch_id.to_string(),
                "queue_envelope_id": envelope.envelope_id.to_string(),
                "target_process_id": envelope.target_process_id.to_string(),
                "target_program_run_id": envelope.target_program_run_id,
                "target_program_slot": envelope.target_program_slot,
                "message_type": envelope.message_type,
            }));

            let sender_binding = self
                .db
                .get_process_runtime_binding(envelope.sender_process_id)
                .await?;
            let _ = append_process_segment_to_live_bus(
                &self.db,
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine,
                &sender_binding.process,
                sender_binding.binding.as_ref(),
                SEND_SEGMENT_KIND,
                &result_content,
                Some("whitespace"),
                result_patch,
                Some(envelope.sender_process_id),
                None,
                None,
            )
            .await?;

            let _ = delivered_target;
        }

        Ok(())
    }
}

fn priority_label(priority: u8) -> String {
    format!("p{priority}")
}

fn priority_value_from_legacy_mark(mark: &str) -> Option<u8> {
    match mark {
        "critical" => Some(1),
        "high" => Some(2),
        "normal" => Some(4),
        "low" => Some(8),
        _ => None,
    }
}

fn delivery_action_from_str(value: &str) -> Option<DeliveryAction> {
    match value {
        "interrupt_deliver" => Some(DeliveryAction::InterruptDeliver),
        "persistent_async_clone" => Some(DeliveryAction::PersistentAsyncClone),
        "segment_boundary_deliver" => Some(DeliveryAction::SegmentBoundaryDeliver),
        "ephemeral_async_clone" => Some(DeliveryAction::EphemeralAsyncClone),
        "bounce_with_hint" => Some(DeliveryAction::BounceWithHint),
        "ignore" => Some(DeliveryAction::Ignore),
        "blacklist_sender" => Some(DeliveryAction::BlacklistSender),
        _ => None,
    }
}

fn aggregate_batch_content(info: &QueueBatchInfo, envelopes: &[QueueEnvelope]) -> String {
    let mut combined = String::new();
    combined.push_str("[mailbox_batch]\n");
    combined.push_str("target_slot: ");
    combined.push_str(&info.target_slot);
    combined.push('\n');
    combined.push_str("final_priority: ");
    combined.push_str(&info.final_priority.to_string());
    combined.push('\n');
    combined.push_str("final_delivery_action: ");
    combined.push_str(info.final_delivery_action.as_str());
    combined.push('\n');
    combined.push_str("entry_count: ");
    combined.push_str(&envelopes.len().to_string());

    for (index, envelope) in envelopes.iter().enumerate() {
        combined.push_str("\n\n[entry ");
        combined.push_str(&(index + 1).to_string());
        combined.push_str("]\n");
        combined.push_str("message_kind: ");
        combined.push_str(envelope.message_kind.as_str());
        combined.push('\n');
        combined.push_str("source_slot: ");
        combined.push_str(
            envelope
                .sender_program_slot
                .as_deref()
                .unwrap_or(&envelope.sender_external_name),
        );
        combined.push('\n');
        combined.push_str("source_process_id: ");
        combined.push_str(&envelope.sender_process_id.to_string());
        combined.push('\n');
        combined.push_str("message_type: ");
        combined.push_str(&envelope.message_type);
        combined.push('\n');
        if let Some(normal_subkind) = envelope
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("normal_subkind"))
            .and_then(Value::as_str)
        {
            combined.push_str("normal_subkind: ");
            combined.push_str(normal_subkind);
            combined.push('\n');
        }
        combined.push_str("base_priority: ");
        combined.push_str(&envelope.base_priority.to_string());
        combined.push('\n');
        combined.push_str("final_priority: ");
        combined.push_str(
            &envelope
                .final_priority
                .unwrap_or(envelope.base_priority)
                .to_string(),
        );
        combined.push('\n');
        combined.push_str("requested_delivery_action: ");
        combined.push_str(envelope.requested_delivery_action.as_str());
        combined.push('\n');
        combined.push_str("final_delivery_action: ");
        combined.push_str(
            envelope
                .final_delivery_action
                .unwrap_or(envelope.requested_delivery_action)
                .as_str(),
        );
        combined.push('\n');
        match envelope.message_kind {
            MessageKind::NormalMessage => {
                combined.push_str("content:\n");
                combined.push_str(mailbox_entry_content(envelope).as_ref());
            }
            MessageKind::TaskMessage => {
                combined.push_str("task_sequence:\n");
                combined.push_str(&task_sequence_summary(&envelope.task_sequence));
            }
            MessageKind::HardwareMessage => {
                if let Some(content) = envelope.content.as_deref() {
                    combined.push_str("content:\n");
                    combined.push_str(content);
                    combined.push('\n');
                }
                combined.push_str("hardware_sequence:\n");
                combined.push_str(&hardware_sequence_summary(&envelope.hardware_sequence));
            }
        }
    }

    combined
}

fn mailbox_entry_content(envelope: &QueueEnvelope) -> Cow<'_, str> {
    if let Some(content) = envelope.content.as_deref() {
        Cow::Borrowed(content)
    } else if let Some(content_ref) = envelope.content_ref.as_deref() {
        Cow::Owned(format!("[content_ref:{content_ref}]"))
    } else {
        Cow::Borrowed("")
    }
}

fn queue_batch_patch(
    info: &QueueBatchInfo,
    envelopes: &[QueueEnvelope],
    target_binding: Option<&ProgramProcessBindingRow>,
    rerouted_from_process_id: Option<Uuid>,
    rerouted_to_process_id: Option<Uuid>,
) -> Option<Value> {
    let message_source_kinds = envelopes
        .iter()
        .map(|envelope| envelope.source_kind.as_str().to_string())
        .collect::<Vec<_>>();
    // Queue/runtime metadata stays on patch and does not become prompt text.
    let mut patch = json!({
        "queue_batch_id": info.batch_id.to_string(),
        "queue_envelope_ids": info.envelope_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        "mailbox_priority_mark": info.priority_mark,
        "mailbox_delivery_policy": info.delivery_policy,
        "mailbox_final_priority": info.final_priority,
        "mailbox_final_delivery_action": info.final_delivery_action.as_str(),
        "queue_message_ids": info.message_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        "workflow_target_slot": target_binding.map(|binding| binding.program_slot_name.clone()),
        "target_process_id": info.target_process_id.to_string(),
        "message_source_kinds": message_source_kinds,
    });

    if let Some(object) = patch.as_object_mut() {
        if let Some(from_process_id) = rerouted_from_process_id {
            object.insert(
                "rerouted_from_process_id".to_string(),
                Value::String(from_process_id.to_string()),
            );
        }
        if let Some(to_process_id) = rerouted_to_process_id {
            object.insert(
                "rerouted_to_process_id".to_string(),
                Value::String(to_process_id.to_string()),
            );
        }
    }

    Some(patch)
}

fn task_sequence_summary(task_sequence: &[TaskStep]) -> String {
    if task_sequence.is_empty() {
        return "[empty_task_sequence]".to_string();
    }

    task_sequence
        .iter()
        .enumerate()
        .map(|(index, step)| {
            format!(
                "{}. {} {}",
                index + 1,
                step.action_name,
                summarize_action_args(&step.action_args)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hardware_sequence_summary(hardware_sequence: &[HardwareStep]) -> String {
    if hardware_sequence.is_empty() {
        return "[empty_hardware_sequence]".to_string();
    }

    hardware_sequence
        .iter()
        .enumerate()
        .map(|(index, step)| {
            format!(
                "{}. {} device={} transport={} exclusive={} {}",
                index + 1,
                step.operation_name,
                step.device_selector,
                step.transport.as_deref().unwrap_or("unspecified"),
                step.exclusive,
                summarize_action_args(&step.operation_args)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_message_payload(
    message_kind: MessageKind,
    _content: Option<&str>,
    _content_ref: Option<&str>,
    task_sequence: &[TaskStep],
    hardware_sequence: &[HardwareStep],
) -> Result<()> {
    match message_kind {
        MessageKind::NormalMessage => {
            if !task_sequence.is_empty() || !hardware_sequence.is_empty() {
                bail!("normal_message cannot carry task_sequence or hardware_sequence");
            }
        }
        MessageKind::TaskMessage => {
            if task_sequence.is_empty() {
                bail!("task_message requires a non-empty task_sequence");
            }
            if !hardware_sequence.is_empty() {
                bail!("task_message cannot carry hardware_sequence");
            }
        }
        MessageKind::HardwareMessage => {
            if hardware_sequence.is_empty() {
                bail!("hardware_message requires a non-empty hardware_sequence");
            }
            if !task_sequence.is_empty() {
                bail!("hardware_message cannot carry task_sequence");
            }
        }
    }
    Ok(())
}

fn default_hardware_deadline_ms(operation_name: &str) -> u64 {
    match operation_name {
        HARDWARE_OP_FLASH_FIRMWARE => 120_000,
        HARDWARE_OP_PROBE_READ_MEMORY => 30_000,
        _ => 5_000,
    }
}

fn summarize_action_args(action_args: &Value) -> String {
    match action_args {
        Value::Null => "{}".to_string(),
        Value::Object(map) if map.is_empty() => "{}".to_string(),
        Value::Object(map) => {
            let keys = map.keys().cloned().collect::<Vec<_>>();
            format!("{{keys:{}}}", keys.join(","))
        }
        _ => action_args.to_string(),
    }
}

fn render_monitor_trigger_message_type(trigger: &MonitorTriggerEntry) -> String {
    trigger
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("message_type"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "monitor_trigger".to_string())
}

fn render_send_message_trace_started(envelope: &QueueEnvelope) -> String {
    let mut content = format!(
        "[send_message]\nstatus: started\nmessage_id: {}\nmessage_kind: {}\ntarget_process_id: {}\nbase_priority: {}\nrequested_delivery_action: {}\n",
        envelope.message_id,
        envelope.message_kind.as_str(),
        envelope.target_process_id,
        envelope.base_priority,
        envelope.requested_delivery_action.as_str(),
    );
    if let Some(target_slot) = envelope.target_program_slot.as_deref() {
        content.push_str("target_slot: ");
        content.push_str(target_slot);
        content.push('\n');
    }
    if let Some(content_text) = envelope.content.as_deref() {
        content.push_str("content:\n");
        content.push_str(content_text);
        content.push('\n');
    } else if let Some(content_ref) = envelope.content_ref.as_deref() {
        content.push_str("content:\n[content_ref:");
        content.push_str(content_ref);
        content.push_str("]\n");
    }
    if !envelope.task_sequence.is_empty() {
        content.push_str("task_sequence:\n");
        content.push_str(&task_sequence_summary(&envelope.task_sequence));
        content.push('\n');
    }
    if !envelope.hardware_sequence.is_empty() {
        content.push_str("hardware_sequence:\n");
        content.push_str(&hardware_sequence_summary(&envelope.hardware_sequence));
        content.push('\n');
    }
    content.push('\n');
    content
}

fn render_send_message_trace_completion(envelope: &QueueEnvelope) -> String {
    let mut content = String::new();
    content.push_str("status: completed\nfinal_priority: ");
    content.push_str(
        &envelope
            .final_priority
            .unwrap_or(envelope.base_priority)
            .to_string(),
    );
    content.push_str("\nfinal_delivery_action: ");
    content.push_str(
        envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action)
            .as_str(),
    );
    content
}

fn parse_reprompt_blocks(output: &str) -> Result<Vec<(Uuid, String)>> {
    let mut current_process_id = None;
    let mut current_lines = Vec::new();
    let mut blocks = Vec::new();

    for line in output.lines() {
        if let Some(raw_process_id) = line.strip_prefix("@@process ") {
            if current_process_id.is_some() {
                bail!("reprompt output started a new block before closing the previous one");
            }
            current_process_id = Some(Uuid::parse_str(raw_process_id.trim()).map_err(|error| {
                anyhow!("invalid reprompt process id `{raw_process_id}`: {error}")
            })?);
            continue;
        }
        if line.trim() == "@@end" {
            let process_id = current_process_id
                .take()
                .ok_or_else(|| anyhow!("reprompt output closed a block before opening one"))?;
            blocks.push((process_id, current_lines.join("\n").trim().to_string()));
            current_lines.clear();
            continue;
        }
        if current_process_id.is_some() {
            current_lines.push(line.to_string());
        }
    }

    if current_process_id.is_some() {
        bail!("reprompt output ended before closing a block");
    }
    if blocks.is_empty() {
        bail!("reprompt output did not contain any `@@process` blocks");
    }
    Ok(blocks)
}

fn registration_outcome(registration: &SequenceRegistration, status: &str) -> SequenceOutcome {
    let matched_event_kinds = registration
        .buffer
        .matched_signals
        .iter()
        .map(|signal| signal.kind.as_str().to_string())
        .collect::<Vec<_>>();
    let matched_sequence = registration
        .buffer
        .matched_signals
        .iter()
        .map(|signal| {
            json!({
                "kind": signal.kind.as_str(),
                "observed_process_id": signal.observed_process_id.to_string(),
                "observed_stream_id": signal.observed_stream_id.map(|stream_id| stream_id.to_string()),
                "observed_slot": signal.observed_slot,
                "executor_process_id": signal.executor_process_id.map(|process_id| process_id.to_string()),
                "segment_kind": signal.segment_kind,
                "content": signal.content,
                "tool_name": signal.tool_name,
                "tool_run_id": signal.tool_run_id.map(|tool_run_id| tool_run_id.to_string()),
                "metadata": signal.metadata,
                "observed_at": signal.observed_at,
            })
        })
        .collect::<Vec<_>>();
    let output_text = if matched_sequence.is_empty() {
        format!(
            "monitor_event_sequence {} for watched process {}",
            status, registration.watched_process.id
        )
    } else {
        matched_sequence
            .iter()
            .map(|item| serde_json::to_string(item).unwrap_or_else(|_| "{}".to_string()))
            .collect::<Vec<_>>()
            .join("\n")
    };

    SequenceOutcome {
        registration_id: registration.registration_id,
        host_process_id: registration.host_process.id,
        watched_process_id: registration.watched_process.id,
        matched_event_kinds,
        matched_sequence,
        started_at: registration.started_at,
        finished_at: Utc::now(),
        status: status.to_string(),
        output_text,
        metadata: Some(json!({
            "mode": registration.mode,
            "return_mode": registration.return_mode,
            "message_type": registration.message_type,
            "priority_mark": registration.priority_mark,
            "guide": registration.guide,
        })),
    }
}

#[derive(Clone)]
pub struct RuntimeEngine {
    db: Database,
    live_bus_registry: LiveBusRegistry,
    ephemeral_live_bus_registry: EphemeralLiveBusRegistry,
    runtime_head_registry: RuntimeHeadRegistry,
    ephemeral_runtime_head_registry: EphemeralRuntimeHeadRegistry,
    slot_mapping_cache: SlotMappingCache,
    control_queue: RuntimeControlQueue,
    global_message_queue: GlobalMessageQueue,
    queue_dispatcher: QueueDispatcher,
    sequence_engine: MonitorEventSequenceEngine,
    activity_registry: ActivityRegistry,
    tool_registry: ToolRegistry,
    external_tool_registry: ExternalToolRegistry,
    hardware_operation_registry: HardwareOperationRegistry,
    tool_run_registry: ToolRunRegistry,
    process_stream_gates: ProcessStreamGateRegistry,
    hardware_device_gates: HardwareDeviceGateRegistry,
    hardware_exec_tx: mpsc::UnboundedSender<HardwareExecutionRequest>,
    provider: Arc<dyn StreamingProvider>,
    provider_input_budget: usize,
    compaction_trigger_ratio: f32,
    error_log_path: PathBuf,
}

impl RuntimeEngine {
    pub async fn new(db: Database, config: RuntimeConfig) -> Result<Self> {
        let provider = default_provider_from_env()?
            .ok_or_else(|| anyhow!("FORM_ZERO provider is required for runtime startup"))?;
        Self::new_with_provider(db, config, provider).await
    }

    pub async fn new_with_provider(
        db: Database,
        config: RuntimeConfig,
        provider: Arc<dyn StreamingProvider>,
    ) -> Result<Self> {
        let live_bus_registry = LiveBusRegistry::default();
        let ephemeral_live_bus_registry = EphemeralLiveBusRegistry::default();
        let runtime_head_registry = RuntimeHeadRegistry::default();
        let ephemeral_runtime_head_registry = EphemeralRuntimeHeadRegistry::default();
        let slot_mapping_cache = SlotMappingCache::default();
        let (control_queue, control_rx) = RuntimeControlQueue::new();
        let (global_message_queue, queue_rx) = GlobalMessageQueue::new();
        let (task_exec_tx, task_exec_rx) = mpsc::unbounded_channel();
        let (hardware_exec_tx, hardware_exec_rx) = mpsc::unbounded_channel();
        let tool_registry = ToolRegistry::default();
        let external_tool_registry =
            build_external_tool_registry(config.peripherals_config.clone()).await?;
        let hardware_operation_registry = HardwareOperationRegistry::default();
        let tool_run_registry = ToolRunRegistry::default();
        let activity_registry = ActivityRegistry::default();
        let sequence_engine = MonitorEventSequenceEngine::default();
        let process_stream_gates = ProcessStreamGateRegistry::default();
        let hardware_device_gates = HardwareDeviceGateRegistry::default();
        let queue_dispatcher = QueueDispatcher::new(
            db.clone(),
            live_bus_registry.clone(),
            ephemeral_live_bus_registry.clone(),
            runtime_head_registry.clone(),
            ephemeral_runtime_head_registry.clone(),
            activity_registry.clone(),
            global_message_queue.clone(),
            sequence_engine.clone(),
            task_exec_tx,
        );

        let engine = Self {
            db: db.clone(),
            live_bus_registry: live_bus_registry.clone(),
            ephemeral_live_bus_registry,
            runtime_head_registry,
            ephemeral_runtime_head_registry,
            slot_mapping_cache,
            control_queue: control_queue.clone(),
            global_message_queue: global_message_queue.clone(),
            queue_dispatcher: queue_dispatcher.clone(),
            sequence_engine,
            activity_registry,
            tool_registry,
            external_tool_registry,
            hardware_operation_registry,
            tool_run_registry,
            process_stream_gates,
            hardware_device_gates,
            hardware_exec_tx,
            provider,
            provider_input_budget: config.provider_input_budget,
            compaction_trigger_ratio: config.compaction_trigger_ratio,
            error_log_path: config.error_log_path,
        };

        engine.start_control_worker(control_rx);
        queue_dispatcher.start(queue_rx);
        engine.start_persisted_flush_worker();
        engine.start_remote_task_executor(task_exec_rx);
        engine.start_hardware_executor(hardware_exec_rx);
        Ok(engine)
    }

    pub async fn load_runtime_head(&self, process_id: Uuid) -> Result<RuntimeHead> {
        resident_runtime_head(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            process_id,
        )
        .await
    }

    pub async fn send_message(&self, request: SendMessageRequest) -> Result<QueueEnvelope> {
        self.control_queue.send_message(request).await
    }

    pub async fn resolve_slot(&self, target: TargetSelector) -> Result<ProcessRuntimeBinding> {
        self.control_queue.resolve_slot(target).await
    }

    fn start_control_worker(&self, mut rx: mpsc::UnboundedReceiver<RuntimeControlCommand>) {
        let runtime = self.clone();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    RuntimeControlCommand::SendMessage {
                        request,
                        response_tx,
                    } => {
                        let _ =
                            response_tx.send(runtime.handle_send_message_request(request).await);
                    }
                    RuntimeControlCommand::ResolveSlot {
                        target,
                        response_tx,
                    } => {
                        let _ = response_tx.send(runtime.resolve_target_process(&target).await);
                    }
                }
            }
        });
    }

    fn start_persisted_flush_worker(&self) {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(PERSISTED_FLUSH_INTERVAL_MS));
            loop {
                ticker.tick().await;
                let _ = runtime.flush_pending_persisted_segments().await;
            }
        });
    }

    fn start_hardware_executor(&self, mut rx: mpsc::UnboundedReceiver<HardwareExecutionRequest>) {
        let runtime = self.clone();
        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                let _ = runtime.execute_hardware_message(request).await;
            }
        });
    }

    fn dispatch_prepared_envelope(
        &self,
        target: &ProcessRuntimeBinding,
        envelope: QueueEnvelope,
    ) -> Result<()> {
        self.global_message_queue.enqueue(envelope.clone())?;
        if envelope.message_kind == MessageKind::HardwareMessage {
            self.hardware_exec_tx
                .send(HardwareExecutionRequest {
                    host: target.clone(),
                    envelope,
                })
                .map_err(|error| anyhow!("failed to schedule hardware execution: {error}"))?;
        }
        Ok(())
    }

    async fn handle_send_message_request(
        &self,
        request: SendMessageRequest,
    ) -> Result<QueueEnvelope> {
        let sender_binding = self.resolve_target_process(&request.sender).await?;
        let _process_stream_permit = self
            .acquire_process_stream_slot(sender_binding.process.id)
            .await?;
        let target_binding = self.resolve_target_process(&request.target).await?;
        let explicit_result_binding = match request.explicit_result_target.as_ref() {
            Some(selector) => Some(self.resolve_target_process(selector).await?),
            None => None,
        };
        let envelope = self
            .prepare_message_envelope(
                &sender_binding,
                &target_binding,
                MessageSourceKind::AdHoc,
                request.message_kind,
                request.message_kind.as_str().to_string(),
                request.base_priority,
                request.requested_delivery_action,
                request.delay_ms,
                request.result_target,
                explicit_result_binding.as_ref(),
                request.content,
                request.content_ref,
                request.task_sequence,
                request.hardware_sequence,
                request.metadata,
            )
            .await?;
        self.dispatch_prepared_envelope(&target_binding, envelope.clone())?;
        self.maybe_emit_optimizer_event(&sender_binding, &target_binding, &envelope)
            .await?;
        self.handle_send_message_completed(&sender_binding, &envelope)
            .await?;
        Ok(envelope)
    }

    async fn handle_send_message_completed(
        &self,
        sender: &ProcessRuntimeBinding,
        envelope: &QueueEnvelope,
    ) -> Result<()> {
        let trace_content = render_send_message_trace_started(envelope);
        let _ = append_process_segment_to_live_bus(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            &self.sequence_engine,
            &sender.process,
            sender.binding.as_ref(),
            SEND_SEGMENT_KIND,
            &trace_content,
            Some("whitespace"),
            Some(json!({
                "message_id": envelope.message_id.to_string(),
                "message_kind": envelope.message_kind.as_str(),
                "requested_delivery_action": envelope.requested_delivery_action.as_str(),
            })),
            Some(sender.process.id),
            Some("send_message"),
            None,
        )
        .await?;

        let trace_content = render_send_message_trace_completion(envelope);
        let trace_segment = append_process_segment_to_live_bus(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            &self.sequence_engine,
            &sender.process,
            sender.binding.as_ref(),
            SEND_SEGMENT_KIND,
            &trace_content,
            Some("whitespace"),
            Some(json!({
                "message_id": envelope.message_id.to_string(),
                "message_kind": envelope.message_kind.as_str(),
                "requested_delivery_action": envelope.requested_delivery_action.as_str(),
                "final_delivery_action": envelope
                    .final_delivery_action
                    .map(|action| action.as_str().to_string()),
                "final_priority": envelope.final_priority,
            })),
            Some(sender.process.id),
            Some("send_message"),
            None,
        )
        .await?;

        let signal = RuntimeSignal {
            kind: RuntimeSignalKind::SendMessageCompleted,
            observed_process_id: sender.process.id,
            observed_stream_id: None,
            observed_slot: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            executor_process_id: Some(sender.process.id),
            segment_kind: Some(trace_segment.segment_kind.clone()),
            content: Some(trace_content),
            tool_name: Some("send_message".to_string()),
            tool_run_id: None,
            metadata: trace_segment.patch.clone(),
            observed_at: Utc::now(),
        };
        let decision = self.sequence_engine.ingest(signal).await;

        self.enqueue_slot_monitor_triggers(sender).await?;

        if matches!(decision, BoundaryDecision::SealNow) {
            let _ = seal_persisted_live_to_pending(
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine,
                &sender.process,
                sender.binding.as_ref(),
                Some(sender.process.id),
                Some("send_message"),
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn enqueue_slot_monitor_triggers(&self, sender: &ProcessRuntimeBinding) -> Result<()> {
        let Some(binding) = sender.binding.as_ref() else {
            return Ok(());
        };
        let program = self.db.get_program(&binding.program_run_id).await?;
        let Some(slot_state) = program
            .plan_state_json
            .slots
            .get(&binding.program_slot_name)
        else {
            return Ok(());
        };

        for trigger in slot_state
            .monitor_triggers
            .iter()
            .filter(|trigger| trigger.enabled)
            .filter(|trigger| trigger.trigger_event == MonitorTriggerEvent::SendMessageCompleted)
        {
            let target = self
                .resolve_target_process(&TargetSelector::ProgramSlot {
                    program_run_id: binding.program_run_id.clone(),
                    program_slot_name: trigger.target_slot.clone(),
                })
                .await?;
            let envelope = self
                .prepare_message_envelope(
                    sender,
                    &target,
                    MessageSourceKind::System,
                    trigger.emitted_message_kind,
                    render_monitor_trigger_message_type(trigger),
                    trigger.base_priority,
                    trigger.requested_delivery_action,
                    trigger.delay_ms,
                    trigger.result_target,
                    None,
                    if trigger.emitted_message_kind == MessageKind::TaskMessage {
                        None
                    } else {
                        trigger
                            .content_template
                            .clone()
                            .or_else(|| Some(trigger.text.clone()))
                    },
                    None,
                    trigger.task_sequence.clone(),
                    trigger.hardware_sequence.clone(),
                    merge_patch(
                        trigger.metadata.clone(),
                        Some(json!({
                            "trigger_id": trigger.trigger_id,
                            "trigger_event": trigger.trigger_event.as_str(),
                        })),
                    ),
                )
                .await?;
            self.dispatch_prepared_envelope(&target, envelope)?;
        }

        Ok(())
    }

    async fn flush_pending_persisted_segments(&self) -> Result<()> {
        for process_id in self.live_bus_registry.open_owner_ids().await {
            if let Some(open_live) = self.live_bus_registry.get(process_id).await {
                let _ = self
                    .sequence_engine
                    .ingest(RuntimeSignal {
                        kind: RuntimeSignalKind::FlushTick,
                        observed_process_id: process_id,
                        observed_stream_id: None,
                        observed_slot: self
                            .db
                            .get_process_binding(process_id)
                            .await?
                            .as_ref()
                            .map(|binding| binding.program_slot_name.clone()),
                        executor_process_id: None,
                        segment_kind: Some(open_live.segment_kind.clone()),
                        content: Some(open_live.content.clone()),
                        tool_name: None,
                        tool_run_id: None,
                        metadata: open_live.patch.clone(),
                        observed_at: Utc::now(),
                    })
                    .await;
            }
        }

        let pending = self.live_bus_registry.drain_all_pending().await;
        for (process_id, segments) in pending {
            let mut retry_segments = Vec::new();
            for segment in segments {
                let persisted = self
                    .db
                    .persist_sealed_live_segment(
                        process_id,
                        segment.segment_id,
                        &segment.segment_kind,
                        &segment.content,
                        segment.token_count,
                        segment.tokenizer.as_deref(),
                        segment.patch.clone(),
                    )
                    .await;
                if let Ok(persisted) = persisted {
                    seal_resident_open_live(
                        &self.runtime_head_registry,
                        process_id,
                        persisted.owner_seq,
                    )
                    .await;
                } else {
                    retry_segments.push(segment);
                    mark_resident_prompt_head_dirty(&self.runtime_head_registry, process_id).await;
                }
            }
            for segment in retry_segments {
                self.live_bus_registry
                    .push_pending(process_id, segment)
                    .await;
            }
        }
        Ok(())
    }

    fn start_remote_task_executor(
        &self,
        mut rx: mpsc::UnboundedReceiver<RemoteTaskExecutionRequest>,
    ) {
        let runtime = self.clone();
        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                let _ = runtime.execute_remote_task_message(request).await;
            }
        });
    }

    async fn acquire_process_stream_slot(&self, process_id: Uuid) -> Result<OwnedSemaphorePermit> {
        self.process_stream_gates.acquire(process_id).await
    }

    async fn seed_ephemeral_stream_head(
        &self,
        stream_id: EphemeralStreamId,
        snapshot_runtime_head: &RuntimeHead,
    ) {
        seed_ephemeral_runtime_head_if_missing(
            &self.ephemeral_runtime_head_registry,
            stream_id,
            snapshot_runtime_head,
        )
        .await;
    }

    async fn clear_ephemeral_stream(&self, stream_id: EphemeralStreamId) {
        self.ephemeral_live_bus_registry.clear(stream_id).await;
        self.ephemeral_runtime_head_registry.remove(stream_id).await;
    }

    async fn append_ephemeral_step_result(
        &self,
        stream_id: EphemeralStreamId,
        label: &str,
        content: &str,
    ) -> Result<()> {
        let mut runtime_head = self
            .ephemeral_runtime_head_registry
            .get(stream_id)
            .await
            .ok_or_else(|| anyhow!("missing ephemeral runtime head for stream {stream_id}"))?;
        append_prompt_stream_text(&mut runtime_head, &format!("\n\n[{label}]\n{content}"));
        self.ephemeral_runtime_head_registry
            .upsert(runtime_head)
            .await;
        Ok(())
    }

    async fn append_provider_chunk(
        &self,
        stream_target: &ProviderStreamTarget,
        tool_name: &str,
        tool_run_id: Uuid,
        content_delta: &str,
    ) -> Result<()> {
        if content_delta.is_empty() {
            return Ok(());
        }

        match stream_target.live_owner {
            LiveOwnerRef::Process(process_id) => {
                let (mut runtime_head, _) = resident_prompt_head_state(
                    &self.db,
                    &self.live_bus_registry,
                    &self.runtime_head_registry,
                    process_id,
                )
                .await?;
                append_prompt_stream_text(&mut runtime_head, content_delta);
                self.runtime_head_registry.upsert(runtime_head).await;

                let live_update = async {
                    let mut open_live = match self.live_bus_registry.get(process_id).await {
                        Some(segment) => segment,
                        None => OpenLiveSegment::new(
                            process_id,
                            next_live_owner_seq(
                                &self.db,
                                &self.runtime_head_registry,
                                &self.live_bus_registry,
                                process_id,
                            )
                            .await?,
                            PROVIDER_STREAM_SEGMENT_KIND.to_string(),
                            Some(PROVIDER_STREAM_TOKENIZER.to_string()),
                            provider_stream_patch(tool_name, tool_run_id, process_id, None),
                        ),
                    };
                    open_live.apply_delta(
                        content_delta,
                        Some(PROVIDER_STREAM_SEGMENT_KIND.to_string()),
                        Some(PROVIDER_STREAM_TOKENIZER.to_string()),
                        provider_stream_patch(tool_name, tool_run_id, process_id, None),
                    )?;
                    self.live_bus_registry.upsert(open_live.clone()).await;
                    Ok::<OpenLiveSegment, anyhow::Error>(open_live)
                }
                .await;

                let open_live = match live_update {
                    Ok(open_live) => open_live,
                    Err(error) => {
                        mark_resident_prompt_head_dirty(&self.runtime_head_registry, process_id)
                            .await;
                        return Err(error);
                    }
                };

                self.activity_registry.touch(process_id).await;
                self.queue_dispatcher
                    .note_runtime_activity(process_id)
                    .await;
                self.sequence_engine
                    .ingest(RuntimeSignal {
                        kind: RuntimeSignalKind::LiveFrame,
                        observed_process_id: stream_target.observed_process_id,
                        observed_stream_id: None,
                        observed_slot: stream_target.observed_slot.clone(),
                        executor_process_id: None,
                        segment_kind: Some(open_live.segment_kind.clone()),
                        content: Some(content_delta.to_string()),
                        tool_name: Some(tool_name.to_string()),
                        tool_run_id: Some(tool_run_id),
                        metadata: open_live.patch.clone(),
                        observed_at: Utc::now(),
                    })
                    .await;
            }
            LiveOwnerRef::Ephemeral(stream_id) => {
                let mut runtime_head = self
                    .ephemeral_runtime_head_registry
                    .get(stream_id)
                    .await
                    .ok_or_else(|| {
                        anyhow!("missing ephemeral runtime head for stream {stream_id}")
                    })?;
                append_prompt_stream_text(&mut runtime_head, content_delta);
                self.ephemeral_runtime_head_registry
                    .upsert(runtime_head)
                    .await;

                let live_update = async {
                    let mut open_live = match self.ephemeral_live_bus_registry.get(stream_id).await
                    {
                        Some(segment) => segment,
                        None => OpenLiveSegment::new(
                            stream_id,
                            1,
                            PROVIDER_STREAM_SEGMENT_KIND.to_string(),
                            Some(PROVIDER_STREAM_TOKENIZER.to_string()),
                            provider_stream_patch(
                                tool_name,
                                tool_run_id,
                                stream_target.observed_process_id,
                                Some(stream_id),
                            ),
                        ),
                    };
                    open_live.apply_delta(
                        content_delta,
                        Some(PROVIDER_STREAM_SEGMENT_KIND.to_string()),
                        Some(PROVIDER_STREAM_TOKENIZER.to_string()),
                        provider_stream_patch(
                            tool_name,
                            tool_run_id,
                            stream_target.observed_process_id,
                            Some(stream_id),
                        ),
                    )?;
                    self.ephemeral_live_bus_registry
                        .upsert(stream_id, open_live.clone())
                        .await;
                    Ok::<OpenLiveSegment, anyhow::Error>(open_live)
                }
                .await;

                let open_live = match live_update {
                    Ok(open_live) => open_live,
                    Err(error) => {
                        mark_ephemeral_prompt_head_dirty(
                            &self.ephemeral_runtime_head_registry,
                            stream_id,
                        )
                        .await;
                        return Err(error);
                    }
                };

                self.sequence_engine
                    .ingest(RuntimeSignal {
                        kind: RuntimeSignalKind::LiveFrame,
                        observed_process_id: stream_target.observed_process_id,
                        observed_stream_id: Some(stream_id),
                        observed_slot: stream_target.observed_slot.clone(),
                        executor_process_id: stream_target.executor_process_id,
                        segment_kind: Some(open_live.segment_kind.clone()),
                        content: Some(content_delta.to_string()),
                        tool_name: Some(tool_name.to_string()),
                        tool_run_id: Some(tool_run_id),
                        metadata: open_live.patch.clone(),
                        observed_at: Utc::now(),
                    })
                    .await;
            }
        }

        Ok(())
    }

    async fn complete_provider_stream(
        &self,
        host: &ProcessRuntimeBinding,
        provider_stream_target: &ProviderStreamTarget,
        tool_name: &str,
        tool_run_id: Uuid,
    ) -> Result<()> {
        let maybe_open = match provider_stream_target.live_owner {
            LiveOwnerRef::Process(process_id) => self.live_bus_registry.get(process_id).await,
            LiveOwnerRef::Ephemeral(stream_id) => {
                self.ephemeral_live_bus_registry.get(stream_id).await
            }
        };
        let Some(open_live) = maybe_open else {
            return Ok(());
        };
        let decision = self
            .sequence_engine
            .ingest(RuntimeSignal {
                kind: RuntimeSignalKind::ProviderCompleted,
                observed_process_id: provider_stream_target.observed_process_id,
                observed_stream_id: match provider_stream_target.live_owner {
                    LiveOwnerRef::Process(_) => None,
                    LiveOwnerRef::Ephemeral(stream_id) => Some(stream_id),
                },
                observed_slot: provider_stream_target.observed_slot.clone(),
                executor_process_id: provider_stream_target.executor_process_id,
                segment_kind: Some(open_live.segment_kind.clone()),
                content: Some(open_live.content.clone()),
                tool_name: Some(tool_name.to_string()),
                tool_run_id: Some(tool_run_id),
                metadata: open_live.patch.clone(),
                observed_at: Utc::now(),
            })
            .await;
        if matches!(decision, BoundaryDecision::SealNow)
            && matches!(
                provider_stream_target.live_owner,
                LiveOwnerRef::Process(process_id) if process_id == host.process.id
            )
        {
            let _ = seal_persisted_live_to_pending(
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine,
                &host.process,
                host.binding.as_ref(),
                Some(host.process.id),
                Some(tool_name),
                Some(tool_run_id),
            )
            .await?;
        }
        Ok(())
    }

    fn is_hidden_slot_name(slot_name: &str) -> bool {
        matches!(
            slot_name,
            HIDDEN_OPTIMIZER_SLOT_NAME | HIDDEN_SCORE_JUDGE_SLOT_NAME
        )
    }

    fn is_hidden_binding(binding: Option<&ProgramProcessBindingRow>) -> bool {
        binding
            .map(|value| Self::is_hidden_slot_name(&value.program_slot_name))
            .unwrap_or(false)
    }

    fn should_trigger_optimizer(final_delivery_action: DeliveryAction) -> bool {
        matches!(
            final_delivery_action,
            DeliveryAction::PersistentAsyncClone
                | DeliveryAction::EphemeralAsyncClone
                | DeliveryAction::BounceWithHint
                | DeliveryAction::Ignore
                | DeliveryAction::BlacklistSender
        )
    }

    async fn resolve_hidden_process(
        &self,
        program_run_id: &str,
        slot_name: &str,
    ) -> Result<ProcessRuntimeBinding> {
        self.resolve_target_process(&TargetSelector::ProgramSlot {
            program_run_id: program_run_id.to_string(),
            program_slot_name: slot_name.to_string(),
        })
        .await
    }

    fn retarget_provider_stream_target(
        &self,
        host: &ProcessRuntimeBinding,
        provider_stream_target: &ProviderStreamTarget,
    ) -> ProviderStreamTarget {
        ProviderStreamTarget {
            live_owner: LiveOwnerRef::Process(host.process.id),
            observed_process_id: host.process.id,
            observed_slot: host
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            executor_process_id: provider_stream_target
                .executor_process_id
                .map(|_| host.process.id),
        }
    }

    async fn dispatch_system_message_direct(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
        message_type: &str,
        content: String,
    ) -> Result<QueueEnvelope> {
        let envelope = QueueEnvelope {
            envelope_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            message_kind: MessageKind::NormalMessage,
            source_kind: MessageSourceKind::System,
            sender_process_id: sender.process.id,
            sender_external_name: sender.process.external_slot_name.clone(),
            sender_program_run_id: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone()),
            sender_program_slot: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            target_process_id: target.process.id,
            target_external_name: target.process.external_slot_name.clone(),
            target_program_run_id: target
                .binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone()),
            target_program_slot: target
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            message_type: message_type.to_string(),
            base_priority: 4,
            final_priority: Some(4),
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
            priority_mark: Some(priority_label(4)),
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_process_id: None,
            explicit_result_program_run_id: None,
            explicit_result_program_slot: None,
            content: Some(content),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: None,
            reply_to: None,
            created_at: Utc::now(),
        };
        self.queue_dispatcher
            .accept_envelope(envelope.clone())
            .await;
        self.queue_dispatcher
            .release_for_process(target.process.id)
            .await;
        Ok(envelope)
    }

    async fn send_process_prompt_snapshot(
        &self,
        sender: &ProcessRuntimeBinding,
        optimizer: &ProcessRuntimeBinding,
        subject: &ProcessRuntimeBinding,
    ) -> Result<()> {
        let prompt = self.db.get_process_prompt(subject.process.id).await?;
        let content = format!(
            "@@process_prompt {}\nslot: {}\nversion: {}\n{}\n@@end",
            prompt.process_id,
            subject
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.as_str())
                .unwrap_or(subject.process.external_slot_name.as_str()),
            prompt.prompt_version,
            prompt.prompt_text
        );
        self.dispatch_system_message_direct(sender, optimizer, "process_prompt_snapshot", content)
            .await?;
        Ok(())
    }

    async fn run_hidden_process_generation(
        &self,
        hidden: &ProcessRuntimeBinding,
        tool_name: &str,
        task: &str,
    ) -> Result<String> {
        let runtime_head = resident_runtime_head(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            hidden.process.id,
        )
        .await?;
        let provider_stream_target = ProviderStreamTarget {
            live_owner: LiveOwnerRef::Process(hidden.process.id),
            observed_process_id: hidden.process.id,
            observed_slot: hidden
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            executor_process_id: Some(hidden.process.id),
        };
        let tool_run = self
            .tool_run_registry
            .start(tool_name, hidden.process.id, Some(60_000))
            .await;
        match self
            .generate_text_with_provider_core(
                tool_name,
                hidden,
                &runtime_head,
                &provider_stream_target,
                None,
                task,
                tool_run.tool_run_id,
            )
            .await
        {
            Ok(output) => {
                self.tool_run_registry
                    .finish(tool_run.tool_run_id, Some(&output))
                    .await;
                Ok(output)
            }
            Err(error) => {
                self.tool_run_registry.fail(tool_run.tool_run_id).await;
                Err(error)
            }
        }
    }

    async fn maybe_emit_optimizer_event(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
        envelope: &QueueEnvelope,
    ) -> Result<()> {
        let final_delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);
        let Some(sender_binding) = sender.binding.as_ref() else {
            return Ok(());
        };
        if Self::is_hidden_binding(sender.binding.as_ref())
            || !Self::should_trigger_optimizer(final_delivery_action)
        {
            return Ok(());
        }

        let anomaly_text = format!(
            "[delivery_event]\naction: {}\nsender_process_id: {}\nsender_slot: {}\ntarget_process_id: {}\ntarget_slot: {}\nmessage_kind: {}\nmessage:\n{}",
            final_delivery_action.as_str(),
            sender.process.id,
            sender_binding.program_slot_name,
            target.process.id,
            target
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.as_str())
                .unwrap_or(target.process.external_slot_name.as_str()),
            envelope.message_kind.as_str(),
            envelope.content.as_deref().unwrap_or("")
        );
        let score_judge = self
            .resolve_hidden_process(&sender_binding.program_run_id, HIDDEN_SCORE_JUDGE_SLOT_NAME)
            .await?;
        let optimizer = self
            .resolve_hidden_process(&sender_binding.program_run_id, HIDDEN_OPTIMIZER_SLOT_NAME)
            .await?;

        self.dispatch_system_message_direct(
            sender,
            &score_judge,
            "delivery_event",
            anomaly_text.clone(),
        )
        .await?;
        self.dispatch_system_message_direct(sender, &optimizer, "delivery_event", anomaly_text)
            .await?;
        self.send_process_prompt_snapshot(sender, &optimizer, sender)
            .await?;

        let score_text = self
            .run_hidden_process_generation(
                &score_judge,
                HIDDEN_SCORE_JUDGE_TOOL_NAME,
                "Update the rolling coordination mean from the latest delivery event now visible in your runtime head. Output only two lines: `mean_score: <number>` and `note: <short text>`.",
            )
            .await?;
        self.dispatch_system_message_direct(&score_judge, &optimizer, "score_update", score_text)
            .await?;

        let hint = self
            .run_hidden_process_generation(
                &optimizer,
                HIDDEN_OPTIMIZER_TOOL_NAME,
                &format!(
                    "Write one short reminder sentence for process {} based on the latest delivery event, the latest score update, and the visible prompt snapshots in your runtime head. Output only the sentence.",
                    sender.process.id
                ),
            )
            .await?;
        let hint = hint.trim().to_string();
        if !hint.is_empty() {
            self.dispatch_system_message_direct(&optimizer, sender, "optimizer_hint", hint)
                .await?;
        }
        Ok(())
    }

    async fn rotate_process_for_reprompt(
        &self,
        binding: &ProgramProcessBindingRow,
        process: &ProcessInstanceRow,
        occupying_prompt_text: &str,
        prompt_parent_process_id: Option<Uuid>,
        adopted_mean_score: Option<f64>,
        demoted_prompt_text: Option<&str>,
    ) -> Result<ProcessRuntimeBinding> {
        let successor = self
            .db
            .spawn_free_process(process.prefix_suffix_definite_id, &process.status)
            .await?;
        self.db
            .rewrite_process_prompt_state(
                successor.id,
                occupying_prompt_text,
                prompt_parent_process_id,
                adopted_mean_score,
            )
            .await?;

        let mut last_cloned_seq = 0_i64;
        if let Some(demoted_prompt_text) = demoted_prompt_text {
            let demoted_segment = self
                .db
                .append_process_segment(
                    successor.id,
                    RECEIVE_SEGMENT_KIND,
                    demoted_prompt_text,
                    None,
                    Some(PROVIDER_STREAM_TOKENIZER),
                    None,
                )
                .await?;
            last_cloned_seq = demoted_segment.owner_seq;
        }

        let sealed_history =
            load_history_segments(&self.db, &self.live_bus_registry, process.id).await?;
        let keep_from = sealed_history.len().saturating_sub(3);
        for source_segment in sealed_history.iter().skip(keep_from) {
            let segment = self
                .db
                .append_process_segment(
                    successor.id,
                    &source_segment.segment_kind,
                    &source_segment.content,
                    Some(source_segment.token_count),
                    source_segment.tokenizer.as_deref(),
                    source_segment.patch.clone(),
                )
                .await?;
            last_cloned_seq = segment.owner_seq;
        }

        if let Some(open_live) = self.live_bus_registry.remove(process.id).await {
            let mut cloned_open_live = OpenLiveSegment::new(
                successor.id,
                last_cloned_seq + 1,
                open_live.segment_kind.clone(),
                open_live.tokenizer.clone(),
                open_live.patch.clone(),
            );
            cloned_open_live.apply_delta(
                &open_live.content,
                Some(open_live.segment_kind.clone()),
                open_live.tokenizer.clone(),
                open_live.patch.clone(),
            )?;
            self.live_bus_registry.upsert(cloned_open_live).await;
        }

        self.db
            .unbind_process_from_program(process.id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "process {} was not bound during reprompt rotation",
                    process.id
                )
            })?;
        let rebound = self
            .db
            .bind_process_to_program(
                &binding.program_run_id,
                &binding.program_slot_name,
                successor.id,
                binding.metadata_json.clone(),
            )
            .await?;
        let rotated = ProcessRuntimeBinding {
            process: successor.clone(),
            binding: Some(rebound.clone()),
        };

        self.queue_dispatcher
            .move_mailbox_target(process.id, &rotated)
            .await;
        self.slot_mapping_cache
            .insert(
                &binding.program_run_id,
                &binding.program_slot_name,
                successor,
            )
            .await;
        self.runtime_head_registry.remove(process.id).await;
        self.sequence_engine.cancel_for_process(process.id).await;

        Ok(rotated)
    }

    async fn promote_process_for_reprompt(
        &self,
        binding: &ProgramProcessBindingRow,
        process: &ProcessInstanceRow,
        prompt_text: &str,
        mean_score: f64,
    ) -> Result<ProcessRuntimeBinding> {
        self.rotate_process_for_reprompt(
            binding,
            process,
            prompt_text,
            Some(process.id),
            Some(mean_score),
            None,
        )
        .await
    }

    async fn rollback_process_for_reprompt(
        &self,
        binding: &ProgramProcessBindingRow,
        process: &ProcessInstanceRow,
        current_prompt: &ProcessPromptRow,
        demoted_prompt_text: &str,
    ) -> Result<ProcessRuntimeBinding> {
        let fallback_prompt = match current_prompt.prompt_parent_process_id {
            Some(parent_process_id) => self.db.get_process_prompt(parent_process_id).await?,
            None => current_prompt.clone(),
        };

        self.rotate_process_for_reprompt(
            binding,
            process,
            &fallback_prompt.prompt_text,
            fallback_prompt.prompt_parent_process_id,
            fallback_prompt.adopted_mean_score,
            Some(demoted_prompt_text),
        )
        .await
    }

    async fn maybe_run_global_reprompt(
        &self,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        guide: Option<&str>,
        task: &str,
    ) -> Result<Option<ProcessRuntimeBinding>> {
        let Some(binding) = host.binding.as_ref() else {
            return Ok(None);
        };
        if Self::is_hidden_binding(host.binding.as_ref()) {
            return Ok(None);
        }

        let estimated_tokens =
            approximate_token_count(&render_provider_user_prompt(guide, runtime_head, task))
                as usize;
        let threshold =
            (self.provider_input_budget as f32 * self.compaction_trigger_ratio).ceil() as usize;
        if estimated_tokens < threshold {
            return Ok(None);
        }

        let active_prompts = self
            .db
            .list_process_prompts_for_program_run(&binding.program_run_id)
            .await?
            .into_iter()
            .filter(|(prompt_binding, _)| {
                !Self::is_hidden_slot_name(&prompt_binding.program_slot_name)
            })
            .collect::<Vec<_>>();
        if active_prompts.is_empty() {
            return Ok(None);
        }

        let score_judge = self
            .resolve_hidden_process(&binding.program_run_id, HIDDEN_SCORE_JUDGE_SLOT_NAME)
            .await?;
        let optimizer = self
            .resolve_hidden_process(&binding.program_run_id, HIDDEN_OPTIMIZER_SLOT_NAME)
            .await?;
        let mean_score_text = self
            .run_hidden_process_generation(
                &score_judge,
                HIDDEN_SCORE_JUDGE_TOOL_NAME,
                "Report the current rolling coordination mean for this program from the runtime head you already have. Output only two lines: `mean_score: <number>` and `note: <short text>`.",
            )
            .await?;
        let mean_score = parse_mean_score(&mean_score_text)?;
        self.dispatch_system_message_direct(
            &score_judge,
            &optimizer,
            "score_update",
            mean_score_text,
        )
        .await?;

        for (prompt_binding, prompt) in &active_prompts {
            let content = format!(
                "@@process_prompt {}\nslot: {}\nversion: {}\n{}\n@@end",
                prompt.process_id,
                prompt_binding.program_slot_name,
                prompt.prompt_version,
                prompt.prompt_text
            );
            self.dispatch_system_message_direct(
                host,
                &optimizer,
                "process_prompt_snapshot",
                content,
            )
            .await?;
        }
        self.dispatch_system_message_direct(
            host,
            &optimizer,
            "reprompt_request",
            format!(
                "[reprompt_request]\nprogram_run_id: {}\ntrigger_process_id: {}\ntrigger_slot: {}\ninput_tokens: {}\nthreshold: {}\nmean_score: {}\n",
                binding.program_run_id,
                host.process.id,
                binding.program_slot_name,
                estimated_tokens,
                threshold
                ,
                mean_score
            ),
        )
        .await?;

        let output = self
            .run_hidden_process_generation(
                &optimizer,
                HIDDEN_REPROMPT_TOOL_NAME,
                "Rewrite the current process prompts for this program. Output only blocks in this exact format: `@@process <process_id>` on its own line, then the full prompt text, then `@@end` on its own line. Do not output anything else.",
            )
            .await?;
        let reprompt_blocks = parse_reprompt_blocks(&output)?;
        if reprompt_blocks.len() != active_prompts.len() {
            bail!(
                "reprompt output returned {} prompt blocks for {} active slots",
                reprompt_blocks.len(),
                active_prompts.len()
            );
        }
        let reprompt_map = reprompt_blocks.into_iter().collect::<HashMap<_, _>>();
        if reprompt_map.len() != active_prompts.len() {
            bail!("reprompt output contained duplicate process ids");
        }

        let mut current_host = None;
        for (process_binding, current_prompt) in active_prompts {
            let process_id = process_binding.process_id;
            let prompt_text = reprompt_map
                .get(&process_id)
                .ok_or_else(|| anyhow!("reprompt output missing active process {}", process_id))?;
            let process = self.db.get_process_instance(process_id).await?;
            let rotated = match decide_prompt_adoption(&current_prompt, mean_score) {
                PromptAdoptionDecision::Promote => {
                    self.promote_process_for_reprompt(
                        &process_binding,
                        &process,
                        prompt_text,
                        mean_score,
                    )
                    .await?
                }
                PromptAdoptionDecision::Rollback => {
                    self.rollback_process_for_reprompt(
                        &process_binding,
                        &process,
                        &current_prompt,
                        prompt_text,
                    )
                    .await?
                }
            };
            if process_id == host.process.id {
                current_host = Some(rotated);
            }
        }
        Ok(current_host)
    }

    async fn prepare_message_envelope(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
        source_kind: MessageSourceKind,
        message_kind: MessageKind,
        message_type: String,
        base_priority: u8,
        requested_delivery_action: DeliveryAction,
        delay_ms: Option<u64>,
        result_target: ResultTarget,
        explicit_result_target: Option<&ProcessRuntimeBinding>,
        content: Option<String>,
        content_ref: Option<String>,
        task_sequence: Vec<TaskStep>,
        hardware_sequence: Vec<HardwareStep>,
        metadata: Option<Value>,
    ) -> Result<QueueEnvelope> {
        validate_message_payload(
            message_kind,
            content.as_deref(),
            content_ref.as_deref(),
            &task_sequence,
            &hardware_sequence,
        )?;

        let (final_priority, raw_final_delivery_action, bounce_hint) = self
            .finalize_message_delivery(
                sender,
                target,
                message_kind,
                base_priority,
                requested_delivery_action,
                content.as_deref(),
                &task_sequence,
                &hardware_sequence,
            )
            .await?;
        let final_delivery_action = raw_final_delivery_action;

        Ok(QueueEnvelope {
            envelope_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            message_kind,
            source_kind,
            sender_process_id: sender.process.id,
            sender_external_name: sender.process.external_slot_name.clone(),
            sender_program_run_id: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone()),
            sender_program_slot: sender
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            target_process_id: target.process.id,
            target_external_name: target.process.external_slot_name.clone(),
            target_program_run_id: target
                .binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone()),
            target_program_slot: target
                .binding
                .as_ref()
                .map(|binding| binding.program_slot_name.clone()),
            message_type,
            base_priority,
            final_priority: Some(final_priority),
            requested_delivery_action,
            final_delivery_action: Some(final_delivery_action),
            priority_mark: Some(priority_label(final_priority)),
            delay_ms,
            result_target,
            explicit_result_process_id: explicit_result_target.map(|binding| binding.process.id),
            explicit_result_program_run_id: explicit_result_target
                .and_then(|binding| binding.binding.as_ref())
                .map(|binding| binding.program_run_id.clone()),
            explicit_result_program_slot: explicit_result_target
                .and_then(|binding| binding.binding.as_ref())
                .map(|binding| binding.program_slot_name.clone()),
            content,
            content_ref,
            task_sequence,
            hardware_sequence,
            metadata: merge_patch(
                metadata,
                bounce_hint.map(|bounce_hint| json!({ "bounce_hint": bounce_hint })),
            ),
            reply_to: None,
            created_at: Utc::now(),
        })
    }

    async fn finalize_message_delivery(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
        message_kind: MessageKind,
        base_priority: u8,
        requested_delivery_action: DeliveryAction,
        content: Option<&str>,
        task_sequence: &[TaskStep],
        hardware_sequence: &[HardwareStep],
    ) -> Result<(u8, DeliveryAction, Option<String>)> {
        if self.is_direct_downstream(sender, target).await? {
            return Ok((base_priority, requested_delivery_action, None));
        }

        self.judge_delivery_decision(
            sender,
            target,
            message_kind,
            base_priority,
            requested_delivery_action,
            content,
            task_sequence,
            hardware_sequence,
        )
        .await
    }

    async fn is_direct_downstream(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
    ) -> Result<bool> {
        let (Some(sender_binding), Some(target_binding)) = (&sender.binding, &target.binding)
        else {
            return Ok(false);
        };
        if sender_binding.program_run_id != target_binding.program_run_id {
            return Ok(false);
        }

        let program = self.db.get_program(&sender_binding.program_run_id).await?;
        let Some(sender_slot) = program
            .plan_state_json
            .slots
            .get(&sender_binding.program_slot_name)
        else {
            return Ok(false);
        };
        let Some(target_slot) = program
            .plan_state_json
            .slots
            .get(&target_binding.program_slot_name)
        else {
            return Ok(false);
        };

        Ok(sender_slot.downstream_group == target_slot.upstream_group)
    }

    async fn judge_delivery_decision(
        &self,
        sender: &ProcessRuntimeBinding,
        target: &ProcessRuntimeBinding,
        message_kind: MessageKind,
        base_priority: u8,
        requested_delivery_action: DeliveryAction,
        content: Option<&str>,
        task_sequence: &[TaskStep],
        hardware_sequence: &[HardwareStep],
    ) -> Result<(u8, DeliveryAction, Option<String>)> {
        let sender_head = resident_runtime_head(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            sender.process.id,
        )
        .await
        .map(|head| head.full_text)
        .unwrap_or_default();
        let task_summary = if task_sequence.is_empty() {
            String::new()
        } else {
            task_sequence_summary(task_sequence)
        };
        let hardware_summary = if hardware_sequence.is_empty() {
            String::new()
        } else {
            hardware_sequence_summary(hardware_sequence)
        };
        let relation_summary = json!({
            "sender_process_id": sender.process.id.to_string(),
            "sender_slot": sender.binding.as_ref().map(|binding| binding.program_slot_name.clone()),
            "target_process_id": target.process.id.to_string(),
            "target_slot": target.binding.as_ref().map(|binding| binding.program_slot_name.clone()),
            "same_program": sender.binding.as_ref().zip(target.binding.as_ref()).map(|(left, right)| left.program_run_id == right.program_run_id).unwrap_or(false),
        });
        let messages = vec![
            ProviderMessage::system(
                "You are a delivery judge. Return JSON only with keys `final_priority`, `final_delivery_action`, and optional `bounce_hint`.",
            ),
            ProviderMessage::system(
                "Allowed actions: interrupt_deliver, persistent_async_clone, segment_boundary_deliver, ephemeral_async_clone, bounce_with_hint, ignore, blacklist_sender.",
            ),
            ProviderMessage::user(format!(
                "[relation]\n{}\n\n[sender_runtime_head]\n{}\n\n[message]\nmessage_kind: {}\nbase_priority: {}\nrequested_delivery_action: {}\ncontent: {}\ntask_sequence:\n{}\n\nhardware_sequence:\n{}",
                serde_json::to_string_pretty(&relation_summary).unwrap_or_else(|_| "{}".to_string()),
                sender_head,
                message_kind.as_str(),
                base_priority,
                requested_delivery_action.as_str(),
                content.unwrap_or(""),
                task_summary,
                hardware_summary,
            )),
        ];
        let (chunk_tx, _chunk_rx) = mpsc::unbounded_channel();
        let output = self.provider.stream_text(messages, chunk_tx).await?;
        let parsed: Value =
            serde_json::from_str(&output).context("delivery judge must return valid JSON")?;
        let final_priority = parsed
            .get("final_priority")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .filter(|value| (1..=8).contains(value))
            .unwrap_or(base_priority);
        let final_delivery_action = parsed
            .get("final_delivery_action")
            .and_then(Value::as_str)
            .and_then(delivery_action_from_str)
            .unwrap_or(requested_delivery_action);
        let bounce_hint = parsed
            .get("bounce_hint")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok((final_priority, final_delivery_action, bounce_hint))
    }

    async fn execute_remote_task_message(&self, request: RemoteTaskExecutionRequest) -> Result<()> {
        let (host, mut runtime_head, provider_stream_target, ephemeral_stream_id) =
            match request.execution_host {
                RemoteExecutionHost::PersistedProcess { host } => (
                    host.clone(),
                    resident_runtime_head(
                        &self.db,
                        &self.live_bus_registry,
                        &self.runtime_head_registry,
                        host.process.id,
                    )
                    .await?,
                    ProviderStreamTarget {
                        live_owner: LiveOwnerRef::Process(host.process.id),
                        observed_process_id: host.process.id,
                        observed_slot: host
                            .binding
                            .as_ref()
                            .map(|binding| binding.program_slot_name.clone()),
                        executor_process_id: None,
                    },
                    None,
                ),
                RemoteExecutionHost::EphemeralClone {
                    host,
                    stream_id,
                    snapshot_runtime_head,
                } => {
                    self.seed_ephemeral_stream_head(stream_id, &snapshot_runtime_head)
                        .await;
                    (
                        host.clone(),
                        snapshot_runtime_head,
                        ProviderStreamTarget {
                            live_owner: LiveOwnerRef::Ephemeral(stream_id),
                            observed_process_id: host.process.id,
                            observed_slot: host
                                .binding
                                .as_ref()
                                .map(|binding| binding.program_slot_name.clone()),
                            executor_process_id: Some(host.process.id),
                        },
                        Some(stream_id),
                    )
                }
            };

        for (task_step_index, task_step) in request.envelope.task_sequence.iter().enumerate() {
            let deadline_ms = default_deadline_ms(&task_step.action_name);
            let uses_provider =
                tool_requires_provider(&task_step.action_name, &task_step.action_args);
            let tool_run = if ephemeral_stream_id.is_some() {
                self.tool_run_registry
                    .start_ephemeral(&task_step.action_name, host.process.id, deadline_ms)
                    .await
            } else {
                self.tool_run_registry
                    .start(&task_step.action_name, host.process.id, deadline_ms)
                    .await
            };
            let result = self
                .run_action_core(
                    &host,
                    &runtime_head,
                    &provider_stream_target,
                    &task_step.action_name,
                    tool_run.tool_run_id,
                    &task_step.action_args,
                )
                .await;

            match result {
                Ok((output, metadata, materialized)) => {
                    self.tool_run_registry
                        .finish(tool_run.tool_run_id, Some(&output))
                        .await;
                    if let Some(segment) = materialized.clone() {
                        self.sequence_engine
                            .ingest(RuntimeSignal {
                                kind: RuntimeSignalKind::ToolMaterialized,
                                observed_process_id: host.process.id,
                                observed_stream_id: ephemeral_stream_id,
                                observed_slot: host
                                    .binding
                                    .as_ref()
                                    .map(|binding| binding.program_slot_name.clone()),
                                executor_process_id: ephemeral_stream_id.map(|_| host.process.id),
                                segment_kind: Some(segment.segment_kind.clone()),
                                content: Some(segment.content.clone()),
                                tool_name: Some(task_step.action_name.clone()),
                                tool_run_id: Some(tool_run.tool_run_id),
                                metadata: segment.patch.clone(),
                                observed_at: Utc::now(),
                            })
                            .await;
                    }
                    self.sequence_engine
                        .ingest(RuntimeSignal {
                            kind: RuntimeSignalKind::ToolFinished,
                            observed_process_id: host.process.id,
                            observed_stream_id: ephemeral_stream_id,
                            observed_slot: host
                                .binding
                                .as_ref()
                                .map(|binding| binding.program_slot_name.clone()),
                            executor_process_id: ephemeral_stream_id.map(|_| host.process.id),
                            segment_kind: None,
                            content: None,
                            tool_name: Some(task_step.action_name.clone()),
                            tool_run_id: Some(tool_run.tool_run_id),
                            metadata: metadata.clone(),
                            observed_at: Utc::now(),
                        })
                        .await;
                    self.send_remote_action_result(
                        &host,
                        &request.envelope,
                        &task_step.action_name,
                        task_step_index,
                        "finished",
                        &output,
                        metadata,
                        ephemeral_stream_id.is_some(),
                    )
                    .await?;
                    if let Some(stream_id) = ephemeral_stream_id {
                        if !uses_provider {
                            self.append_ephemeral_step_result(
                                stream_id,
                                "ephemeral_step_output",
                                &output,
                            )
                            .await?;
                        }
                        runtime_head = ephemeral_runtime_head(
                            &self.ephemeral_runtime_head_registry,
                            stream_id,
                            host.process.id,
                        )
                        .await?;
                    } else {
                        runtime_head = resident_runtime_head(
                            &self.db,
                            &self.live_bus_registry,
                            &self.runtime_head_registry,
                            host.process.id,
                        )
                        .await?;
                    }
                }
                Err(error) => {
                    self.tool_run_registry.fail(tool_run.tool_run_id).await;
                    self.send_remote_action_result(
                        &host,
                        &request.envelope,
                        &task_step.action_name,
                        task_step_index,
                        "failed",
                        &error.to_string(),
                        Some(json!({ "failure": true })),
                        ephemeral_stream_id.is_some(),
                    )
                    .await?;
                    if let Some(stream_id) = ephemeral_stream_id {
                        self.append_ephemeral_step_result(
                            stream_id,
                            "ephemeral_step_failure",
                            &error.to_string(),
                        )
                        .await?;
                        runtime_head = ephemeral_runtime_head(
                            &self.ephemeral_runtime_head_registry,
                            stream_id,
                            host.process.id,
                        )
                        .await?;
                    }
                }
            }
        }

        if let Some(stream_id) = ephemeral_stream_id {
            self.clear_ephemeral_stream(stream_id).await;
        }

        Ok(())
    }

    async fn execute_hardware_message(&self, request: HardwareExecutionRequest) -> Result<()> {
        let mut outputs = Vec::new();
        let mut failed_step_index = None;
        let mut failure_text = None;

        for (index, step) in request.envelope.hardware_sequence.iter().enumerate() {
            let exclusive_permit = if step.exclusive {
                Some(
                    self.hardware_device_gates
                        .acquire(&step.device_selector)
                        .await?,
                )
            } else {
                None
            };

            let deadline_ms = step
                .timeout_ms
                .unwrap_or_else(|| default_hardware_deadline_ms(&step.operation_name));
            let step_result = time::timeout(
                Duration::from_millis(deadline_ms),
                self.run_hardware_operation(&request.host, step),
            )
            .await;

            drop(exclusive_permit);

            match step_result {
                Ok(Ok(output)) => outputs.push(format!("step {}: {}", index, output)),
                Ok(Err(error)) => {
                    failed_step_index = Some(index);
                    failure_text = Some(error.to_string());
                    break;
                }
                Err(_) => {
                    failed_step_index = Some(index);
                    failure_text = Some(format!(
                        "hardware operation `{}` timed out after {} ms",
                        step.operation_name, deadline_ms
                    ));
                    break;
                }
            }
        }

        let status = if failure_text.is_some() {
            "failed"
        } else {
            "finished"
        };
        let output = if let Some(failure_text) = failure_text.as_deref() {
            if outputs.is_empty() {
                failure_text.to_string()
            } else {
                format!("{}\n{}", outputs.join("\n"), failure_text)
            }
        } else if outputs.is_empty() {
            "hardware sequence completed with no output".to_string()
        } else {
            outputs.join("\n")
        };

        self.send_hardware_action_result(
            &request.host,
            &request.envelope,
            status,
            &output,
            failed_step_index,
        )
        .await?;

        Ok(())
    }

    async fn run_hardware_operation(
        &self,
        _host: &ProcessRuntimeBinding,
        step: &HardwareStep,
    ) -> Result<String> {
        if !self
            .hardware_operation_registry
            .contains(&step.operation_name)
        {
            bail!("unsupported hardware operation {}", step.operation_name);
        }

        let tool_name = Self::external_tool_name_for_hardware_operation(&step.operation_name)
            .ok_or_else(|| anyhow!("no external tool mapping for {}", step.operation_name))?;
        let tool_args = Self::external_tool_args_for_hardware_step(step);
        let Some(tool) = self.external_tool_registry.get(tool_name) else {
            bail!("missing external tool mapping for {}", step.operation_name);
        };
        let result = tool.execute(tool_args).await?;
        if result.success {
            Ok(result.output)
        } else {
            bail!(
                "{}",
                result.error.as_deref().unwrap_or(result.output.as_str())
            )
        }
    }

    fn external_tool_name_for_hardware_operation(operation_name: &str) -> Option<&'static str> {
        match operation_name {
            HARDWARE_OP_BOARD_INFO => Some("hardware_board_info"),
            HARDWARE_OP_DEVICE_CAPABILITIES => Some("hardware_capabilities"),
            HARDWARE_OP_GPIO_READ => Some("gpio_read"),
            HARDWARE_OP_GPIO_WRITE => Some("gpio_write"),
            HARDWARE_OP_SERIAL_QUERY => Some("serial_query"),
            HARDWARE_OP_SERIAL_WRITE => Some("serial_write"),
            HARDWARE_OP_PROBE_READ_MEMORY => Some("hardware_memory_read"),
            HARDWARE_OP_FLASH_FIRMWARE => Some("flash_firmware"),
            _ => None,
        }
    }

    fn external_tool_args_for_hardware_step(step: &HardwareStep) -> Value {
        let mut args = step.operation_args.as_object().cloned().unwrap_or_default();
        args.entry("device_selector".to_string())
            .or_insert_with(|| Value::String(step.device_selector.clone()));
        if let Some(transport) = step.transport.as_ref() {
            args.entry("transport".to_string())
                .or_insert_with(|| Value::String(transport.clone()));
        }
        if let Some(board) = Self::hardware_board_hint_from_device_selector(&step.device_selector) {
            args.entry("board".to_string())
                .or_insert_with(|| Value::String(board));
        }
        Value::Object(args)
    }

    fn hardware_board_hint_from_device_selector(device_selector: &str) -> Option<String> {
        let selector = device_selector
            .strip_prefix("board:")
            .unwrap_or(device_selector);
        let board = selector.split('/').next().unwrap_or(selector).trim();
        if board.is_empty() {
            None
        } else {
            Some(board.to_string())
        }
    }

    async fn send_hardware_action_result(
        &self,
        host: &ProcessRuntimeBinding,
        request_envelope: &QueueEnvelope,
        status: &str,
        output: &str,
        failed_step_index: Option<usize>,
    ) -> Result<()> {
        let target = self.resolve_result_target_binding(request_envelope).await?;
        let result_content = format!(
            "[hardware_executor_result]\nstatus: {status}\nhardware_message_id: {}\noutput:\n{output}",
            request_envelope.message_id
        );
        let result_metadata = Some(json!({
            "normal_subkind": "hardware_executor_result",
            "system_executor": true,
            "host_process_id": host.process.id.to_string(),
            "hardware_message_id": request_envelope.message_id.to_string(),
            "status": status,
            "failed_step_index": failed_step_index,
        }));
        let envelope = self
            .prepare_message_envelope(
                host,
                &target,
                MessageSourceKind::System,
                MessageKind::NormalMessage,
                "hardware_executor_result".to_string(),
                request_envelope
                    .final_priority
                    .unwrap_or(request_envelope.base_priority),
                DeliveryAction::SegmentBoundaryDeliver,
                None,
                ResultTarget::Sender,
                None,
                Some(result_content),
                None,
                Vec::new(),
                Vec::new(),
                result_metadata,
            )
            .await?;
        self.dispatch_prepared_envelope(&target, envelope)?;
        Ok(())
    }

    async fn send_remote_action_result(
        &self,
        executor: &ProcessRuntimeBinding,
        request_envelope: &QueueEnvelope,
        action_name: &str,
        task_step_index: usize,
        status: &str,
        output: &str,
        metadata: Option<Value>,
        is_ephemeral_clone: bool,
    ) -> Result<()> {
        let target = self.resolve_result_target_binding(request_envelope).await?;
        let result_content = format!(
            "[tool_executor_result]\nstatus: {status}\nexecuted_action_name: {action_name}\ntask_message_id: {}\ntask_step_index: {task_step_index}\noutput:\n{output}",
            request_envelope.message_id
        );
        let result_metadata = merge_patch(
            Some(json!({
                "normal_subkind": "tool_executor_result",
                "executor_process_id": executor.process.id.to_string(),
                "executed_action_name": action_name,
                "task_message_id": request_envelope.message_id.to_string(),
                "task_step_index": task_step_index,
                "status": status,
                "ephemeral_clone": is_ephemeral_clone,
            })),
            metadata,
        );
        let envelope = self
            .prepare_message_envelope(
                executor,
                &target,
                MessageSourceKind::AdHoc,
                MessageKind::NormalMessage,
                "tool_executor_result".to_string(),
                request_envelope
                    .final_priority
                    .unwrap_or(request_envelope.base_priority),
                DeliveryAction::SegmentBoundaryDeliver,
                None,
                ResultTarget::Sender,
                None,
                Some(result_content),
                None,
                Vec::new(),
                Vec::new(),
                result_metadata,
            )
            .await?;
        self.global_message_queue.enqueue(envelope)?;
        Ok(())
    }

    async fn resolve_result_target_binding(
        &self,
        request_envelope: &QueueEnvelope,
    ) -> Result<ProcessRuntimeBinding> {
        match request_envelope.result_target {
            ResultTarget::Sender => {
                self.db
                    .get_process_runtime_binding(request_envelope.sender_process_id)
                    .await
            }
            ResultTarget::Target => {
                self.db
                    .get_process_runtime_binding(request_envelope.target_process_id)
                    .await
            }
            ResultTarget::ExplicitTarget => {
                if let Some(process_id) = request_envelope.explicit_result_process_id {
                    self.db.get_process_runtime_binding(process_id).await
                } else if let (Some(program_run_id), Some(program_slot_name)) = (
                    request_envelope.explicit_result_program_run_id.as_deref(),
                    request_envelope.explicit_result_program_slot.as_deref(),
                ) {
                    self.resolve_target_process(&TargetSelector::ProgramSlot {
                        program_run_id: program_run_id.to_string(),
                        program_slot_name: program_slot_name.to_string(),
                    })
                    .await
                } else {
                    bail!("explicit_target result missing explicit target binding")
                }
            }
        }
    }

    async fn run_action_core(
        &self,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        tool_name: &str,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let is_builtin_tool = self.tool_registry.contains(tool_name);
        let is_external_tool = self.external_tool_registry.contains(tool_name);
        if !is_builtin_tool && !is_external_tool {
            bail!("unsupported tool_name {}", tool_name);
        }
        if is_builtin_tool
            && tool_requires_guide(tool_name, tool_args)
            && extract_tool_guide(tool_args).is_none()
        {
            bail!("{tool_name} requires `输入子进程用途引导` or `guide`");
        }

        match tool_name {
            TOOL_CREATE_PROGRAM => self.tool_create_program(host, tool_run_id, tool_args).await,
            TOOL_SPAWN_REAL_PROCESS => {
                self.tool_spawn_real_process(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_TERMINATE_PROCESS => {
                self.tool_terminate_process(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_INVISIBLE_PROCESS => {
                self.tool_invisible_process(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_SPAWN_BRANCH_PROCESS => {
                self.tool_spawn_branch_process(
                    host,
                    runtime_head,
                    provider_stream_target,
                    tool_run_id,
                    tool_args,
                )
                .await
            }
            TOOL_MONITOR_EVENT_SEQUENCE => self.tool_monitor_event_sequence(host, tool_args).await,
            TOOL_HISTORY_QUERY => {
                self.tool_history_query(host, runtime_head, tool_run_id, tool_args)
                    .await
            }
            TOOL_HISTORY_ANNOTATE => {
                self.tool_history_annotate(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_INSPECT_PROCESS_TABLE => {
                self.tool_inspect_process_table(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_MAKE_PLAN => {
                self.tool_make_plan(
                    host,
                    runtime_head,
                    provider_stream_target,
                    tool_run_id,
                    tool_args,
                )
                .await
            }
            TOOL_REPAIR_PLAN => {
                self.tool_repair_plan(
                    host,
                    runtime_head,
                    provider_stream_target,
                    tool_run_id,
                    tool_args,
                )
                .await
            }
            TOOL_APPEND_OUTPUT_REF => {
                self.tool_append_output_ref(host, tool_run_id, tool_args)
                    .await
            }
            TOOL_DELETE_RESULT_ARTIFACT => {
                self.tool_delete_result_artifact(host, tool_run_id, tool_args)
                    .await
            }
            _ => self.run_external_tool(tool_name, tool_args).await,
        }
    }

    async fn run_external_tool(
        &self,
        tool_name: &str,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let Some(tool) = self.external_tool_registry.get(tool_name) else {
            bail!("unsupported tool_name {}", tool_name);
        };
        let result = tool.execute(tool_args.clone()).await?;
        if result.success {
            Ok((
                result.output,
                Some(json!({
                    "tool_name": tool_name,
                    "external_tool": true,
                })),
                None,
            ))
        } else {
            bail!(
                "{}",
                result.error.as_deref().unwrap_or(result.output.as_str())
            )
        }
    }

    async fn tool_create_program(
        &self,
        _host: &ProcessRuntimeBinding,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let workflow_definite_id = required_uuid(tool_args, "workflow_definite_id")?;
        let program_run_id = required_string(tool_args, "program_run_id")?;
        let status = optional_string(tool_args, "status").unwrap_or_else(|| "running".to_string());
        let bindings_value = tool_args
            .get("bindings")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("create_program requires object field `bindings`"))?;
        let mut bindings = HashMap::new();
        for (slot, process_id_value) in bindings_value {
            let process_id = process_id_value
                .as_str()
                .and_then(|raw| Uuid::parse_str(raw).ok())
                .ok_or_else(|| anyhow!("binding for slot {slot} must be a UUID string"))?;
            bindings.insert(slot.clone(), process_id);
        }

        let program = self
            .db
            .create_program(
                &program_run_id,
                workflow_definite_id,
                &bindings,
                &status,
                None,
            )
            .await?;
        let bound_processes = self
            .db
            .list_process_instances_for_program_run(&program.program_run_id)
            .await?;

        Ok((
            serde_json::to_string(&json!({
                "program": program,
                "bindings": bound_processes,
            }))?,
            tool_finished_metadata(
                TOOL_CREATE_PROGRAM,
                Some(json!({
                    "tool_run_id": tool_run_id.to_string(),
                })),
            ),
            None,
        ))
    }

    async fn tool_spawn_real_process(
        &self,
        host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let host_binding = host.binding.as_ref().ok_or_else(|| {
            anyhow!("spawn_real_process requires a host process already bound inside a program")
        })?;
        let program_slot_name = required_string(tool_args, "program_slot_name")
            .or_else(|_| required_string(tool_args, "slot_name"))?;
        let explicit_prefix_suffix_definite_id = optional_uuid(tool_args, "prefix_suffix_definite_id");
        let status = optional_string(tool_args, "status").unwrap_or_else(|| "running".to_string());
        let parent_slot = optional_string(tool_args, "parent_slot")
            .or_else(|| Some(host_binding.program_slot_name.clone()));

        let mut program = self.db.get_program(&host_binding.program_run_id).await?;
        let existing_slot_prefix_suffix_definite_id = program
            .plan_state_json
            .slots
            .get(&program_slot_name)
            .and_then(|slot| slot.prefix_suffix_definite_id);
        let inherited_prefix_suffix_definite_id = parent_slot
            .as_ref()
            .and_then(|slot_name| program.plan_state_json.slots.get(slot_name))
            .and_then(|slot| slot.prefix_suffix_definite_id);
        let prefix_suffix_definite_id = if let Some(existing_slot_prefix_suffix_definite_id) =
            existing_slot_prefix_suffix_definite_id
        {
            if let Some(explicit_prefix_suffix_definite_id) = explicit_prefix_suffix_definite_id {
                if explicit_prefix_suffix_definite_id != existing_slot_prefix_suffix_definite_id {
                    bail!(
                        "program slot {} expects prefix_suffix_definite {} but got {}",
                        program_slot_name,
                        existing_slot_prefix_suffix_definite_id,
                        explicit_prefix_suffix_definite_id
                    );
                }
            }
            existing_slot_prefix_suffix_definite_id
        } else {
            let slot_prefix_suffix_definite_id = match (
                inherited_prefix_suffix_definite_id,
                explicit_prefix_suffix_definite_id,
            ) {
                (Some(inherited), Some(explicit)) if explicit != inherited => {
                    bail!(
                        "new program slot {} inherits prefix_suffix_definite {} from parent {} but got explicit {}",
                        program_slot_name,
                        inherited,
                        parent_slot.clone().unwrap_or_default(),
                        explicit
                    );
                }
                (Some(inherited), _) => inherited,
                (None, Some(explicit)) => explicit,
                (None, None) => bail!(
                    "new program slot {} requires prefix_suffix_definite_id or an inheritable parent slot",
                    program_slot_name
                ),
            };
            let upstream_group =
                optional_string(tool_args, "upstream_group").unwrap_or_else(|| {
                    parent_slot
                        .clone()
                        .unwrap_or_else(|| program_slot_name.clone())
                });
            let downstream_group = optional_string(tool_args, "downstream_group")
                .unwrap_or_else(|| program_slot_name.clone());
            program.plan_state_json.slots.insert(
                program_slot_name.clone(),
                default_program_slot_state(
                    &program_slot_name,
                    Some(slot_prefix_suffix_definite_id),
                    parent_slot.clone(),
                    upstream_group,
                    downstream_group,
                    monitor_triggers_from_args(tool_args, &program_slot_name),
                ),
            );
            self.db
                .update_program_plan_state(&program.program_run_id, &program.plan_state_json)
                .await?;
            slot_prefix_suffix_definite_id
        };

        let process = self
            .db
            .spawn_free_process(prefix_suffix_definite_id, &status)
            .await?;

        let binding = self
            .db
            .bind_process_to_program(
                &host_binding.program_run_id,
                &program_slot_name,
                process.id,
                None,
            )
            .await?;
        self.slot_mapping_cache
            .insert(
                &binding.program_run_id,
                &binding.program_slot_name,
                process.clone(),
            )
            .await;

        Ok((
            serde_json::to_string(&json!({
                "process": process,
                "binding": binding,
            }))?,
            Some(json!({
                "program_slot_name": program_slot_name,
            })),
            None,
        ))
    }

    async fn tool_terminate_process(
        &self,
        _host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let target_process_id = required_uuid(tool_args, "target_process_id")?;
        let removed_binding = self
            .db
            .unbind_process_from_program(target_process_id)
            .await?;
        let terminated = self
            .db
            .terminate_process_instance(target_process_id)
            .await?;

        if let Some(binding) = &removed_binding {
            self.slot_mapping_cache
                .remove(&binding.program_run_id, &binding.program_slot_name)
                .await;
            let mut program = self.db.get_program(&binding.program_run_id).await?;
            program
                .plan_state_json
                .slots
                .remove(&binding.program_slot_name);
            self.db
                .update_program_plan_state(&binding.program_run_id, &program.plan_state_json)
                .await?;
        }

        self.live_bus_registry.remove(target_process_id).await;
        self.runtime_head_registry.remove(target_process_id).await;
        self.sequence_engine
            .cancel_for_process(target_process_id)
            .await;

        Ok((
            serde_json::to_string(&json!({
                "terminated_process": terminated,
                "removed_binding": removed_binding,
            }))?,
            None,
            None,
        ))
    }

    async fn tool_invisible_process(
        &self,
        host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let target = match target_selector_from_value(tool_args) {
            Some(target) => self.resolve_target_process(&target).await?,
            None => {
                bail!("invisible_process requires process_id or program_run_id + program_slot_name")
            }
        };
        let block_mode =
            optional_string(tool_args, "block_mode").unwrap_or_else(|| "all".to_string());
        let updated = self
            .db
            .update_process_policy(
                host.process.id,
                upsert_blacklist_entry(
                    host.process.policy_json.clone(),
                    BlacklistEntry {
                        process_id: target.process.id,
                        block_mode: block_mode.clone(),
                    },
                ),
            )
            .await?;
        Ok((
            serde_json::to_string(&json!({
                "host_process_id": updated.id,
                "blocked_process_id": target.process.id,
                "block_mode": block_mode,
                "policy_json": updated.policy_json,
            }))?,
            None,
            None,
        ))
    }

    async fn tool_spawn_branch_process(
        &self,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let guide = extract_tool_guide(tool_args);
        let task = optional_string(tool_args, "task").unwrap_or_else(|| "branch task".to_string());
        let generated = self
            .generate_text_with_provider(
                TOOL_SPAWN_BRANCH_PROCESS,
                host,
                runtime_head,
                provider_stream_target,
                guide.as_deref(),
                &task,
                tool_run_id,
            )
            .await?;
        let output = generated.output.clone();

        let materialize_to_host = tool_args
            .get("materialize_to_host")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let segment = if materialize_to_host {
            Some(
                append_process_segment_to_live_bus(
                    &self.db,
                    &self.live_bus_registry,
                    &self.runtime_head_registry,
                    &self.sequence_engine,
                    &generated.host.process,
                    generated.host.binding.as_ref(),
                    SEND_SEGMENT_KIND,
                    &output,
                    Some("whitespace"),
                    tool_result_patch(
                        TOOL_SPAWN_BRANCH_PROCESS,
                        tool_run_id,
                        generated.host.process.id,
                        None,
                    ),
                    Some(generated.host.process.id),
                    Some(TOOL_SPAWN_BRANCH_PROCESS),
                    Some(tool_run_id),
                )
                .await?,
            )
        } else {
            None
        };

        Ok((output, None, segment))
    }

    async fn tool_monitor_event_sequence(
        &self,
        host: &ProcessRuntimeBinding,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let mode = optional_string(tool_args, "mode").unwrap_or_else(|| "await_once".to_string());
        let watched_selector =
            target_selector_from_value(tool_args.get("watched").unwrap_or(tool_args))
                .or_else(|| {
                    if let Some(process_id) = tool_args
                        .get("watched_process_id")
                        .and_then(Value::as_str)
                        .and_then(|raw| Uuid::parse_str(raw).ok())
                    {
                        Some(TargetSelector::ProcessId(process_id))
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    anyhow!("monitor_event_sequence requires watched process selector")
                })?;
        let watched = self.resolve_target_process(&watched_selector).await?;
        let event_kinds = tool_args
            .get("event_kinds")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(RuntimeSignalKind::from_str)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let watched_stream_id = tool_args
            .get("watched_stream_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok());

        let matcher = if let Some(sequence) = tool_args.get("sequence").and_then(Value::as_array) {
            let sequence = sequence
                .iter()
                .filter_map(Value::as_str)
                .filter_map(RuntimeSignalKind::from_str)
                .collect::<Vec<_>>();
            if sequence.is_empty() {
                SequenceMatcher::AnyOfAllowed
            } else {
                SequenceMatcher::OrderedSubsequence { sequence }
            }
        } else {
            SequenceMatcher::AnyOfAllowed
        };
        let deadline_ms = tool_args
            .get("deadline_ms")
            .and_then(Value::as_u64)
            .or_else(|| default_deadline_ms(TOOL_MONITOR_EVENT_SEQUENCE));
        let return_mode =
            optional_string(tool_args, "return_mode").unwrap_or_else(|| match mode.as_str() {
                "background_monitor" => "queue".to_string(),
                _ => "direct".to_string(),
            });
        let queue_target = if return_mode == "queue" {
            let selector =
                target_selector_from_value(tool_args.get("queue_target").unwrap_or(tool_args))
                    .or_else(|| {
                        if let Some(process_id) = tool_args
                            .get("reply_target_process_id")
                            .and_then(Value::as_str)
                            .and_then(|raw| Uuid::parse_str(raw).ok())
                        {
                            Some(TargetSelector::ProcessId(process_id))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        anyhow!("monitor_event_sequence queue mode requires queue_target")
                    })?;
            Some({
                let target = self.resolve_target_process(&selector).await?;
                QueueSequenceTarget {
                    process: target.process,
                    binding: target.binding,
                }
            })
        } else {
            None
        };

        let registration = SequenceRegistration {
            registration_id: Uuid::new_v4(),
            host_process: host.process.clone(),
            host_binding: host.binding.clone(),
            watched_process: watched.process.clone(),
            filter: SequenceFilter {
                watched_process_id: watched.process.id,
                watched_stream_id,
                event_kinds,
            },
            matcher,
            buffer: SequenceBuffer {
                next_index: 0,
                matched_signals: Vec::new(),
            },
            started_at: Utc::now(),
            deadline_at: deadline_ms
                .and_then(|value| i64::try_from(value).ok())
                .map(|value| Utc::now() + chrono::Duration::milliseconds(value)),
            mode: mode.clone(),
            return_mode: return_mode.clone(),
            message_type: optional_string(tool_args, "message_type")
                .unwrap_or_else(|| "monitor_event_sequence".to_string()),
            priority_mark: optional_string(tool_args, "priority_mark"),
            queue_target,
            guide: extract_tool_guide(tool_args),
        };

        let outcome_rx = self.sequence_engine.register(registration.clone()).await;

        if return_mode == "queue" || mode == "background_monitor" {
            let queue = self.global_message_queue.clone();
            tokio::spawn(async move {
                if let Ok(outcome) = outcome_rx.await {
                    if let Some(queue_target) = registration.queue_target.as_ref() {
                        let envelope = QueueEnvelope {
                            envelope_id: Uuid::new_v4(),
                            message_id: Uuid::new_v4(),
                            message_kind: MessageKind::NormalMessage,
                            source_kind: MessageSourceKind::Monitor,
                            sender_process_id: registration.host_process.id,
                            sender_external_name: registration
                                .host_process
                                .external_slot_name
                                .clone(),
                            sender_program_run_id: registration
                                .host_binding
                                .as_ref()
                                .map(|binding| binding.program_run_id.clone()),
                            sender_program_slot: registration
                                .host_binding
                                .as_ref()
                                .map(|binding| binding.program_slot_name.clone()),
                            target_process_id: queue_target.process.id,
                            target_external_name: queue_target.process.external_slot_name.clone(),
                            target_program_run_id: queue_target
                                .binding
                                .as_ref()
                                .map(|binding| binding.program_run_id.clone()),
                            target_program_slot: queue_target
                                .binding
                                .as_ref()
                                .map(|binding| binding.program_slot_name.clone()),
                            message_type: registration.message_type.clone(),
                            base_priority: registration
                                .priority_mark
                                .as_deref()
                                .and_then(priority_value_from_legacy_mark)
                                .unwrap_or(4),
                            final_priority: Some(
                                registration
                                    .priority_mark
                                    .as_deref()
                                    .and_then(priority_value_from_legacy_mark)
                                    .unwrap_or(4),
                            ),
                            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
                            final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
                            priority_mark: registration
                                .priority_mark
                                .clone()
                                .or(Some("p4".to_string())),
                            delay_ms: None,
                            result_target: ResultTarget::Sender,
                            explicit_result_process_id: None,
                            explicit_result_program_run_id: None,
                            explicit_result_program_slot: None,
                            content: Some(outcome.output_text.clone()),
                            content_ref: None,
                            task_sequence: Vec::new(),
                            hardware_sequence: Vec::new(),
                            metadata: Some(json!({
                                "registration_id": registration.registration_id.to_string(),
                                "status": outcome.status,
                                "matched_event_kinds": outcome.matched_event_kinds,
                                "mode": registration.mode,
                            })),
                            reply_to: None,
                            created_at: Utc::now(),
                        };
                        let _ = queue.enqueue(envelope);
                    }
                }
            });

            Ok((
                serde_json::to_string(&json!({
                    "registration_id": registration.registration_id,
                    "mode": mode,
                    "return_mode": return_mode,
                    "status": "registered",
                }))?,
                None,
                None,
            ))
        } else {
            let outcome = outcome_rx
                .await
                .map_err(|_| anyhow!("monitor_event_sequence registration dropped unexpectedly"))?;
            Ok((serde_json::to_string(&outcome)?, outcome.metadata, None))
        }
    }

    async fn tool_history_query(
        &self,
        _host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let process_id = if let Some(selector) = target_selector_from_value(tool_args) {
            self.resolve_target_process(&selector).await?.process.id
        } else {
            runtime_head.process_id
        };
        let sealed_segments =
            load_history_segments(&self.db, &self.live_bus_registry, process_id).await?;
        let open_live_segment = self
            .live_bus_registry
            .get(process_id)
            .await
            .map(|segment| segment.to_segment_row());

        Ok((
            serde_json::to_string(&history_query_value(
                process_id,
                &sealed_segments,
                open_live_segment.as_ref(),
                tool_args,
            ))?,
            None,
            None,
        ))
    }

    async fn tool_history_annotate(
        &self,
        _host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let segment_id = required_uuid(tool_args, "segment_id")?;
        let annotation = required_string(tool_args, "annotation")?;
        let updated = self
            .db
            .update_segment_patch(segment_id, Some(json!({ "annotation": annotation })))
            .await?;
        Ok((serde_json::to_string(&updated)?, None, None))
    }

    async fn tool_inspect_process_table(
        &self,
        host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let program_run_id = optional_string(tool_args, "program_run_id").or_else(|| {
            host.binding
                .as_ref()
                .map(|binding| binding.program_run_id.clone())
        });
        let value = if let Some(program_run_id) = program_run_id {
            let program = self.db.get_program(&program_run_id).await?;
            let bindings = self
                .db
                .list_process_instances_for_program_run(&program_run_id)
                .await?;
            json!({
                "program": program,
                "bound_processes": bindings,
            })
        } else {
            json!({
                "processes": self.db.list_process_instances().await?,
                "programs": self.db.list_programs().await?,
            })
        };
        Ok((serde_json::to_string(&value)?, None, None))
    }

    async fn tool_make_plan(
        &self,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        if host.binding.is_none() {
            bail!("make_plan requires host process bound to a program slot");
        }
        let guide = extract_tool_guide(tool_args);
        let objective = optional_string(tool_args, "objective")
            .unwrap_or_else(|| "Propose the next slot-local plan step.".to_string());
        let generated = self
            .generate_text_with_provider(
                TOOL_MAKE_PLAN,
                host,
                runtime_head,
                provider_stream_target,
                guide.as_deref(),
                &objective,
                tool_run_id,
            )
            .await?;
        let binding =
            generated.host.binding.as_ref().ok_or_else(|| {
                anyhow!("make_plan requires host process bound to a program slot")
            })?;
        let output = generated.output.clone();

        let mut program = self.db.get_program(&binding.program_run_id).await?;
        let slot_name = optional_string(tool_args, "program_slot_name")
            .unwrap_or_else(|| binding.program_slot_name.clone());
        let inherited_prefix_suffix_definite_id = program
            .plan_state_json
            .slots
            .get(&binding.program_slot_name)
            .and_then(|slot| slot.prefix_suffix_definite_id)
            .or(Some(host.process.prefix_suffix_definite_id));
        let slot_state = program
            .plan_state_json
            .slots
            .entry(slot_name.clone())
            .or_insert_with(|| {
                default_program_slot_state(
                    &slot_name,
                    inherited_prefix_suffix_definite_id,
                    (slot_name != binding.program_slot_name)
                        .then(|| binding.program_slot_name.clone()),
                    slot_name.clone(),
                    slot_name.clone(),
                    Vec::new(),
                )
            });
        slot_state.monitor_triggers = vec![MonitorTriggerEntry {
            trigger_id: format!("{slot_name}.trigger.{}", Uuid::new_v4()),
            trigger_event: MonitorTriggerEvent::SendMessageCompleted,
            emitted_message_kind: MessageKind::NormalMessage,
            text: output.clone(),
            target_slot: slot_name.clone(),
            base_priority: 4,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            content_template: Some(output.clone()),
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: Some(json!({
                "message_type": "plan",
                "trigger_label": "plan",
            })),
            enabled: true,
        }];
        slot_state.updated_at = Utc::now();
        self.db
            .update_program_plan_state(&binding.program_run_id, &program.plan_state_json)
            .await?;

        Ok((output, None, None))
    }

    async fn tool_repair_plan(
        &self,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let guide = extract_tool_guide(tool_args);
        let issue = optional_string(tool_args, "issue")
            .unwrap_or_else(|| "Repair the current slot-local plan.".to_string());
        let generated = self
            .generate_text_with_provider(
                TOOL_REPAIR_PLAN,
                host,
                runtime_head,
                provider_stream_target,
                guide.as_deref(),
                &issue,
                tool_run_id,
            )
            .await?;
        let output = generated.output.clone();

        self.tool_make_plan(
            &generated.host,
            &generated.runtime_head,
            &generated.provider_stream_target,
            tool_run_id,
            &json!({
                "objective": output.clone(),
                "program_slot_name": optional_string(tool_args, "program_slot_name"),
            }),
        )
        .await?;

        Ok((output, None, None))
    }

    async fn tool_append_output_ref(
        &self,
        host: &ProcessRuntimeBinding,
        tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let target = match target_selector_from_value(tool_args) {
            Some(selector) => self.resolve_target_process(&selector).await?,
            None => host.clone(),
        };
        let output_ref = required_string(tool_args, "output_ref")?;
        let segment = append_process_segment_to_live_bus(
            &self.db,
            &self.live_bus_registry,
            &self.runtime_head_registry,
            &self.sequence_engine,
            &target.process,
            target.binding.as_ref(),
            if target.process.id == host.process.id {
                SEND_SEGMENT_KIND
            } else {
                RECEIVE_SEGMENT_KIND
            },
            &format!("[output_ref:{output_ref}]"),
            Some("whitespace"),
            tool_result_patch(
                TOOL_APPEND_OUTPUT_REF,
                tool_run_id,
                host.process.id,
                Some(json!({ "output_ref": output_ref })),
            ),
            Some(host.process.id),
            Some(TOOL_APPEND_OUTPUT_REF),
            Some(tool_run_id),
        )
        .await?;
        Ok((serde_json::to_string(&segment)?, None, Some(segment)))
    }

    async fn tool_delete_result_artifact(
        &self,
        _host: &ProcessRuntimeBinding,
        _tool_run_id: Uuid,
        tool_args: &Value,
    ) -> Result<(String, Option<Value>, Option<SegmentRow>)> {
        let segment_id = required_uuid(tool_args, "segment_id")?;
        let updated = self
            .db
            .update_segment_patch(
                segment_id,
                Some(json!({
                    "tombstone": true,
                    "hidden": true,
                    "deleted": true,
                })),
            )
            .await?;
        Ok((serde_json::to_string(&updated)?, None, None))
    }

    async fn generate_text_with_provider(
        &self,
        tool_name: &str,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        guide: Option<&str>,
        task: &str,
        tool_run_id: Uuid,
    ) -> Result<ProviderGeneration> {
        let mut effective_host = host.clone();
        let mut effective_runtime_head = runtime_head.clone();
        let mut effective_provider_stream_target = provider_stream_target.clone();

        if matches!(
            provider_stream_target.live_owner,
            LiveOwnerRef::Process(process_id) if process_id == host.process.id
        ) {
            if let Some(rotated_host) = self
                .maybe_run_global_reprompt(host, runtime_head, guide, task)
                .await?
            {
                effective_runtime_head = resident_runtime_head(
                    &self.db,
                    &self.live_bus_registry,
                    &self.runtime_head_registry,
                    rotated_host.process.id,
                )
                .await?;
                effective_provider_stream_target =
                    self.retarget_provider_stream_target(&rotated_host, provider_stream_target);
                effective_host = rotated_host;
            }
        }

        let output = self
            .generate_text_with_provider_core(
                tool_name,
                &effective_host,
                &effective_runtime_head,
                &effective_provider_stream_target,
                guide,
                task,
                tool_run_id,
            )
            .await?;
        Ok(ProviderGeneration {
            host: effective_host,
            runtime_head: effective_runtime_head,
            provider_stream_target: effective_provider_stream_target,
            output,
        })
    }

    async fn generate_text_with_provider_core(
        &self,
        tool_name: &str,
        host: &ProcessRuntimeBinding,
        runtime_head: &RuntimeHead,
        provider_stream_target: &ProviderStreamTarget,
        guide: Option<&str>,
        task: &str,
        tool_run_id: Uuid,
    ) -> Result<String> {
        let messages = vec![
            ProviderMessage::system("You are a virtual sub-process attached to a host process."),
            ProviderMessage::system(
                "Use the host runtime head as factual context. Be concise and task-complete.",
            ),
            ProviderMessage::user(render_provider_user_prompt(guide, runtime_head, task)),
        ];

        let _process_stream_permit = match provider_stream_target.live_owner {
            LiveOwnerRef::Process(process_id) => {
                Some(self.acquire_process_stream_slot(process_id).await?)
            }
            LiveOwnerRef::Ephemeral(_) => None,
        };

        let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel::<ProviderChunk>();
        let provider = self.provider.clone();
        let messages_for_task = messages.clone();
        let provider_handle =
            tokio::spawn(async move { provider.stream_text(messages_for_task, chunk_tx).await });

        let mut output = String::new();
        let mut pending = String::new();
        let mut streamed_any_chunk = false;
        while let Some(chunk) = chunk_rx.recv().await {
            if chunk.is_final {
                if !pending.is_empty() {
                    self.tool_run_registry
                        .append_delta(tool_run_id, &pending)
                        .await;
                    pending.clear();
                }
                break;
            }
            if !chunk.delta.is_empty() {
                self.append_provider_chunk(
                    provider_stream_target,
                    tool_name,
                    tool_run_id,
                    &chunk.delta,
                )
                .await?;
                streamed_any_chunk = true;
                output.push_str(&chunk.delta);
                pending.push_str(&chunk.delta);
                if pending.len() >= 64 {
                    self.tool_run_registry
                        .append_delta(tool_run_id, &pending)
                        .await;
                    pending.clear();
                }
            }
        }

        let mut provider_error = None;
        match provider_handle.await {
            Ok(Ok(full_output)) => {
                if output.is_empty() {
                    output = full_output;
                }
            }
            Ok(Err(error)) => {
                self.log_raw_envelope("", &format!("provider error for {tool_name}: {error}"))
                    .await;
                provider_error = Some(error);
            }
            Err(error) => {
                self.log_raw_envelope("", &format!("provider join error for {tool_name}: {error}"))
                    .await;
                provider_error = Some(anyhow!("provider join error for {tool_name}: {error}"));
            }
        }

        if !streamed_any_chunk && !output.is_empty() {
            self.append_provider_chunk(provider_stream_target, tool_name, tool_run_id, &output)
                .await?;
            self.tool_run_registry
                .append_delta(tool_run_id, &output)
                .await;
            streamed_any_chunk = true;
        }

        if streamed_any_chunk {
            match provider_stream_target.live_owner {
                LiveOwnerRef::Process(_) => {
                    self.complete_provider_stream(
                        host,
                        provider_stream_target,
                        tool_name,
                        tool_run_id,
                    )
                    .await?;
                }
                LiveOwnerRef::Ephemeral(_) => {
                    self.complete_provider_stream(
                        host,
                        provider_stream_target,
                        tool_name,
                        tool_run_id,
                    )
                    .await?;
                }
            }
        }

        if let Some(error) = provider_error {
            return Err(error);
        }

        Ok(output)
    }

    async fn resolve_target_process(
        &self,
        target: &TargetSelector,
    ) -> Result<ProcessRuntimeBinding> {
        match target {
            TargetSelector::ProcessId(process_id) => {
                self.db.get_process_runtime_binding(*process_id).await
            }
            TargetSelector::ProgramSlot {
                program_run_id,
                program_slot_name,
            } => {
                if let Some(process) = self
                    .slot_mapping_cache
                    .get(program_run_id, program_slot_name)
                    .await
                {
                    let binding = self.db.get_process_binding(process.id).await?;
                    return Ok(ProcessRuntimeBinding { process, binding });
                }

                let process = self
                    .db
                    .resolve_process_by_slot(program_run_id, program_slot_name)
                    .await?
                    .ok_or_else(|| {
                        anyhow!("slot mapping miss for ({program_run_id}, {program_slot_name})")
                    })?;
                self.slot_mapping_cache
                    .insert(program_run_id, program_slot_name, process.clone())
                    .await;
                let binding = self.db.get_process_binding(process.id).await?;
                Ok(ProcessRuntimeBinding { process, binding })
            }
        }
    }

    async fn log_raw_envelope(&self, raw_envelope: &str, reason: &str) {
        let _ = (&self.error_log_path, raw_envelope, reason);
    }
}

fn required_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{key} is required"))
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn required_uuid(value: &Value, key: &str) -> Result<Uuid> {
    optional_uuid(value, key).ok_or_else(|| anyhow!("{key} must be a UUID string"))
}

fn optional_uuid(value: &Value, key: &str) -> Option<Uuid> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

fn monitor_triggers_from_args(tool_args: &Value, slot_name: &str) -> Vec<MonitorTriggerEntry> {
    if tool_args.get("monitor_triggers").is_some() {
        return monitor_trigger_entries_from_value(tool_args.get("monitor_triggers"), slot_name);
    }

    let mut triggers = Vec::new();
    triggers.extend(
        crate::models::step_entries_from_value(
            tool_args.get("plan_loop"),
            slot_name,
            StepKind::Plan,
        )
        .into_iter()
        .map(|entry| MonitorTriggerEntry {
            trigger_id: format!("{}.send_message_completed", entry.step_id),
            trigger_event: MonitorTriggerEvent::SendMessageCompleted,
            emitted_message_kind: entry.message_kind,
            text: entry.text,
            target_slot: entry.target_slot,
            base_priority: entry.base_priority,
            requested_delivery_action: entry.requested_delivery_action,
            delay_ms: entry.delay_ms,
            result_target: entry.result_target,
            content_template: entry.content_template,
            task_sequence: entry.task_sequence,
            hardware_sequence: entry.hardware_sequence,
            metadata: entry.metadata,
            enabled: true,
        }),
    );
    triggers.extend(
        crate::models::step_entries_from_value(
            tool_args.get("forced_event_loop"),
            slot_name,
            StepKind::ForcedEvent,
        )
        .into_iter()
        .map(|entry| MonitorTriggerEntry {
            trigger_id: format!("{}.send_message_completed", entry.step_id),
            trigger_event: MonitorTriggerEvent::SendMessageCompleted,
            emitted_message_kind: entry.message_kind,
            text: entry.text,
            target_slot: entry.target_slot,
            base_priority: entry.base_priority,
            requested_delivery_action: entry.requested_delivery_action,
            delay_ms: entry.delay_ms,
            result_target: entry.result_target,
            content_template: entry.content_template,
            task_sequence: entry.task_sequence,
            hardware_sequence: entry.hardware_sequence,
            metadata: entry.metadata,
            enabled: true,
        }),
    );
    triggers
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hardware_message_payload_validation_rejects_task_sequence() {
        let task_sequence = vec![TaskStep {
            action_name: "make_plan".to_string(),
            action_args: Value::Null,
        }];
        let hardware_sequence = vec![HardwareStep {
            operation_name: HARDWARE_OP_BOARD_INFO.to_string(),
            device_selector: "board:nucleo".to_string(),
            transport: None,
            operation_args: Value::Null,
            timeout_ms: None,
            exclusive: true,
        }];

        let result = validate_message_payload(
            MessageKind::HardwareMessage,
            None,
            None,
            &task_sequence,
            &hardware_sequence,
        );
        assert!(result.is_err());
    }

    #[test]
    fn hardware_message_mailbox_content_preserves_ephemeral_clone_action() {
        let envelope = QueueEnvelope {
            envelope_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            message_kind: MessageKind::HardwareMessage,
            source_kind: MessageSourceKind::AdHoc,
            sender_process_id: Uuid::new_v4(),
            sender_external_name: "sender".to_string(),
            sender_program_run_id: None,
            sender_program_slot: Some("sender_slot".to_string()),
            target_process_id: Uuid::new_v4(),
            target_external_name: "target".to_string(),
            target_program_run_id: None,
            target_program_slot: Some("target_slot".to_string()),
            message_type: "hardware_message".to_string(),
            base_priority: 4,
            final_priority: Some(2),
            requested_delivery_action: DeliveryAction::EphemeralAsyncClone,
            final_delivery_action: Some(DeliveryAction::EphemeralAsyncClone),
            priority_mark: Some("p2".to_string()),
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_process_id: None,
            explicit_result_program_run_id: None,
            explicit_result_program_slot: None,
            content: Some("probe target".to_string()),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: vec![HardwareStep {
                operation_name: HARDWARE_OP_BOARD_INFO.to_string(),
                device_selector: "board:nucleo".to_string(),
                transport: Some("probe".to_string()),
                operation_args: json!({ "detail": true }),
                timeout_ms: None,
                exclusive: true,
            }],
            metadata: None,
            reply_to: None,
            created_at: Utc::now(),
        };
        let info = QueueBatchInfo {
            batch_id: Uuid::new_v4(),
            target_process_id: envelope.target_process_id,
            target_slot: "target_slot".to_string(),
            target_program_run_id: None,
            priority_mark: "p2".to_string(),
            delivery_policy: "ephemeral_async_clone".to_string(),
            envelope_ids: vec![envelope.envelope_id],
            final_priority: 2,
            final_delivery_action: DeliveryAction::EphemeralAsyncClone,
            message_ids: vec![envelope.message_id],
            first_enqueued_at: Utc::now(),
            last_enqueued_at: Utc::now(),
            frozen: true,
        };

        let rendered = aggregate_batch_content(&info, &[envelope]);

        assert!(rendered.contains("final_delivery_action: ephemeral_async_clone"));
    }

    #[test]
    fn hardware_sequence_summary_lists_device_and_operation() {
        let summary = hardware_sequence_summary(&[HardwareStep {
            operation_name: HARDWARE_OP_GPIO_WRITE.to_string(),
            device_selector: "board:rpi/main".to_string(),
            transport: Some("native".to_string()),
            operation_args: json!({ "pin": 17, "value": 1 }),
            timeout_ms: None,
            exclusive: true,
        }]);

        assert!(summary.contains("gpio_write"));
        assert!(summary.contains("board:rpi/main"));
        assert!(summary.contains("native"));
        assert!(summary.contains("keys:pin,value"));
    }

    #[test]
    fn hardware_operation_aliases_map_to_external_tools() {
        assert_eq!(
            RuntimeEngine::external_tool_name_for_hardware_operation(HARDWARE_OP_BOARD_INFO),
            Some("hardware_board_info")
        );
        assert_eq!(
            RuntimeEngine::external_tool_name_for_hardware_operation(
                HARDWARE_OP_DEVICE_CAPABILITIES
            ),
            Some("hardware_capabilities")
        );
        assert_eq!(
            RuntimeEngine::external_tool_name_for_hardware_operation(HARDWARE_OP_GPIO_READ),
            Some("gpio_read")
        );
        assert_eq!(
            RuntimeEngine::external_tool_name_for_hardware_operation(HARDWARE_OP_PROBE_READ_MEMORY),
            Some("hardware_memory_read")
        );
    }

    #[test]
    fn hardware_step_external_args_include_board_and_transport() {
        let args = RuntimeEngine::external_tool_args_for_hardware_step(&HardwareStep {
            operation_name: HARDWARE_OP_GPIO_WRITE.to_string(),
            device_selector: "board:rpi/main".to_string(),
            transport: Some("native".to_string()),
            operation_args: json!({ "pin": 17, "value": 1 }),
            timeout_ms: None,
            exclusive: true,
        });

        assert_eq!(args.get("board").and_then(Value::as_str), Some("rpi"));
        assert_eq!(
            args.get("transport").and_then(Value::as_str),
            Some("native")
        );
        assert_eq!(args.get("pin").and_then(Value::as_i64), Some(17));
        assert_eq!(args.get("value").and_then(Value::as_i64), Some(1));
    }

    #[tokio::test]
    async fn seed_ephemeral_runtime_head_if_missing_preserves_existing_state() {
        let registry = EphemeralRuntimeHeadRegistry::default();
        let stream_id = Uuid::new_v4();
        registry
            .upsert(ResidentPromptHeadState {
                owner_id: stream_id,
                full_text: "existing".to_string(),
                built_all_len: "existing".len(),
                stream_started: false,
                dirty: false,
                last_bootstrap_segment_seq: 3,
            })
            .await;

        seed_ephemeral_runtime_head_if_missing(
            &registry,
            stream_id,
            &RuntimeHead {
                process_id: Uuid::new_v4(),
                full_text: "snapshot".to_string(),
            },
        )
        .await;

        let state = registry.get(stream_id).await.expect("state should exist");
        assert_eq!(state.full_text, "existing");
        assert_eq!(state.last_bootstrap_segment_seq, 3);
    }

    #[tokio::test]
    async fn append_ephemeral_mailbox_batch_to_live_updates_ephemeral_context() {
        let live_bus_registry = EphemeralLiveBusRegistry::default();
        let runtime_head_registry = EphemeralRuntimeHeadRegistry::default();
        let stream_id = Uuid::new_v4();
        runtime_head_registry
            .upsert(prompt_head_from_runtime_head(
                stream_id,
                &RuntimeHead {
                    process_id: Uuid::new_v4(),
                    full_text: "[seed]".to_string(),
                },
                0,
            ))
            .await;

        let segment = append_ephemeral_mailbox_batch_to_live(
            &live_bus_registry,
            &runtime_head_registry,
            stream_id,
            "[mailbox]\nhello",
            Some(json!({ "batch": true })),
        )
        .await
        .expect("ephemeral mailbox append should succeed");

        assert_eq!(segment.segment_kind, RECEIVE_SEGMENT_KIND);
        assert_eq!(segment.content, "[mailbox]\nhello");

        let state = runtime_head_registry
            .get(stream_id)
            .await
            .expect("runtime head should exist");
        assert!(state.full_text.contains("[mailbox]\nhello"));

        let live_segment = live_bus_registry
            .get(stream_id)
            .await
            .expect("open live segment should exist");
        assert_eq!(live_segment.segment_kind, RECEIVE_SEGMENT_KIND);
        assert_eq!(live_segment.content, "[mailbox]\nhello");
    }

    #[test]
    fn provider_completed_uses_same_seal_decision_as_send_completion() {
        let signal = RuntimeSignal {
            kind: RuntimeSignalKind::ProviderCompleted,
            observed_process_id: Uuid::new_v4(),
            observed_stream_id: None,
            observed_slot: None,
            executor_process_id: None,
            segment_kind: None,
            content: None,
            tool_name: None,
            tool_run_id: None,
            metadata: None,
            observed_at: Utc::now(),
        };

        assert!(matches!(
            boundary_decision_from_signal(&signal),
            BoundaryDecision::SealNow
        ));
    }

    #[test]
    fn provider_prompt_uses_runtime_head_without_duplicate_process_prompt_section() {
        let rendered = render_provider_user_prompt(
            Some("guide"),
            &RuntimeHead {
                process_id: Uuid::new_v4(),
                full_text: "PREFIX\n\n[process_prompt]\nREMINDER\n\n[process_stream]\nhello"
                    .to_string(),
            },
            "task",
        );

        assert_eq!(rendered.matches("[process_prompt]").count(), 1);
    }

    #[test]
    fn parse_mean_score_reads_numeric_line() {
        let parsed = parse_mean_score("mean_score: 8.5\nnote: stable").expect("score should parse");
        assert_eq!(parsed, 8.5);
    }

    #[test]
    fn prompt_adoption_promotes_only_on_strict_improvement() {
        let current = ProcessPromptRow {
            process_id: Uuid::new_v4(),
            prompt_text: "current".to_string(),
            prompt_version: 2,
            prompt_parent_process_id: Some(Uuid::new_v4()),
            adopted_mean_score: Some(9.0),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        assert!(matches!(
            decide_prompt_adoption(&current, 9.1),
            PromptAdoptionDecision::Promote
        ));
        assert!(matches!(
            decide_prompt_adoption(&current, 9.0),
            PromptAdoptionDecision::Rollback
        ));
        assert!(matches!(
            decide_prompt_adoption(&current, 8.9),
            PromptAdoptionDecision::Rollback
        ));
    }

    #[test]
    fn prompt_adoption_promotes_when_no_adopted_score_exists() {
        let current = ProcessPromptRow {
            process_id: Uuid::new_v4(),
            prompt_text: "root".to_string(),
            prompt_version: 1,
            prompt_parent_process_id: None,
            adopted_mean_score: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        assert!(matches!(
            decide_prompt_adoption(&current, 3.0),
            PromptAdoptionDecision::Promote
        ));
    }
}
