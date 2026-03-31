mod provider;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use provider::{LiveChatProvider, ProviderMessage};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tiktoken_rs::cl100k_base;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};

const DEFAULT_BASE_PRIORITY: u8 = 4;
const SYSTEM_PRIORITY: u8 = 1;
const SOFT_TOKEN_LIMIT: usize = 5_600;
const HARD_TOKEN_LIMIT: usize = 7_200;
const RESERVED_OUTPUT_TOKENS: usize = 800;
const SCORE_JUDGE_INTERVAL_SECS: u64 = 10;
const TASK_IDLE_LIMIT_SECS: u64 = 20;
const TASK_WALL_CLOCK_LIMIT_SECS: u64 = 180;
const MAIN_LOOP_POLL_MS: u64 = 100;
const DELIVERY_ACTION_ORDER: [DeliveryAction; 5] = [
    DeliveryAction::InterruptDeliver,
    DeliveryAction::PersistentAsyncClone,
    DeliveryAction::EphemeralAsyncClone,
    DeliveryAction::SegmentBoundaryDeliver,
    DeliveryAction::OutputEndDeliver,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentConfig {
    agent_id: String,
    prompt_file: String,
    tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskConfig {
    task_id: String,
    task_prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepositoryEntry {
    term: String,
    keywords: Vec<String>,
    message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum DeliveryAction {
    InterruptDeliver,
    PersistentAsyncClone,
    SegmentBoundaryDeliver,
    OutputEndDeliver,
    EphemeralAsyncClone,
    BounceWithHint,
    Ignore,
    BlacklistSender,
}

impl DeliveryAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::InterruptDeliver => "interrupt_deliver",
            Self::PersistentAsyncClone => "persistent_async_clone",
            Self::SegmentBoundaryDeliver => "segment_boundary_deliver",
            Self::OutputEndDeliver => "output_end_deliver",
            Self::EphemeralAsyncClone => "ephemeral_async_clone",
            Self::BounceWithHint => "bounce_with_hint",
            Self::Ignore => "ignore",
            Self::BlacklistSender => "blacklist_sender",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MessageSourceKind {
    Agent,
    Optimizer,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueEnvelope {
    message_id: usize,
    created_at_ms: u128,
    from: String,
    to: String,
    content: String,
    source_kind: MessageSourceKind,
    tool_name: Option<String>,
    base_priority: u8,
    requested_delivery_action: DeliveryAction,
    final_priority: Option<u8>,
    final_delivery_action: Option<DeliveryAction>,
    bounce_hint: Option<String>,
    skip_delivery_judge: bool,
    triggers_optimizer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadMessage {
    message_id: usize,
    turn_id: Option<usize>,
    from: String,
    to: String,
    content: String,
    source_kind: MessageSourceKind,
    tool_name: Option<String>,
    base_priority: u8,
    final_priority: u8,
    requested_delivery_action: DeliveryAction,
    final_delivery_action: DeliveryAction,
    created_at_ms: u128,
    delivered_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct AgentDecision {
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    to_agent_id: String,
    #[serde(default)]
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentTurnArtifact {
    turn_id: usize,
    actor: String,
    source_message_ids: Vec<usize>,
    prompt_messages: Vec<ProviderMessage>,
    raw_output: String,
    tool_name: String,
    to_agent_id: String,
    sent_message: String,
    finished_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JudgeViewArtifact {
    at_ms: u128,
    prompt_messages: Vec<ProviderMessage>,
    raw_output: String,
    coordination_score: f32,
    note: String,
}

#[derive(Debug, Clone, Deserialize)]
struct JudgeViewDraft {
    #[serde(default)]
    coordination_score: f32,
    #[serde(default)]
    note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OptimizerEvent {
    event_id: usize,
    trigger_message_id: usize,
    from: String,
    to: String,
    delivery_action: DeliveryAction,
    final_priority: u8,
    content: String,
    bounce_hint: Option<String>,
    created_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReflectionArtifact {
    trigger_event_id: usize,
    target_agent_id: String,
    prompt_messages: Vec<ProviderMessage>,
    raw_output: String,
    message: String,
    created_at_ms: u128,
}

#[derive(Debug, Clone, Deserialize)]
struct LocalOptimizerDraft {
    #[serde(default)]
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AgentPrompt {
    agent_id: String,
    prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepromptArtifact {
    at_ms: u128,
    reason: String,
    replay_anchor_message_id: usize,
    prompt_messages: Vec<ProviderMessage>,
    raw_output: String,
    agent_prompts: Vec<AgentPrompt>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepromptDraft {
    agent_prompts: Vec<AgentPrompt>,
}

#[derive(Debug, Clone, Deserialize)]
struct DeliveryJudgeDraft {
    final_priority: Option<u8>,
    final_delivery_action: Option<DeliveryAction>,
    bounce_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MailboxBatchSnapshot {
    final_priority: u8,
    final_delivery_action: DeliveryAction,
    message_ids: Vec<usize>,
    from_agents: Vec<String>,
    source_kinds: Vec<MessageSourceKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MailboxSnapshot {
    agent_id: String,
    batches: Vec<MailboxBatchSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRunState {
    thread: Vec<ThreadMessage>,
    agent_turns: Vec<AgentTurnArtifact>,
    judge_views: Vec<JudgeViewArtifact>,
    reflections: Vec<ReflectionArtifact>,
    reprompts: Vec<RepromptArtifact>,
    optimizer_events: Vec<OptimizerEvent>,
    mailboxes: Vec<MailboxSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskOutcome {
    final_coordination_score: f32,
    final_prompts: Vec<AgentPrompt>,
    state: TaskRunState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskSummary {
    task_id: String,
    active_prompt_source: String,
    task_coordination_score: f32,
    baseline_coordination_score: Option<f32>,
    decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSummary {
    provider_model: String,
    task_summaries: Vec<TaskSummary>,
    final_accepted_prompts: Vec<AgentPrompt>,
}

#[derive(Debug, Clone)]
struct MailboxBatch {
    final_priority: u8,
    final_delivery_action: DeliveryAction,
    envelopes: Vec<QueueEnvelope>,
}

impl MailboxBatch {
    fn from_envelope(envelope: QueueEnvelope) -> Self {
        let final_priority = envelope.final_priority.unwrap_or(envelope.base_priority);
        let final_delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);
        Self {
            final_priority,
            final_delivery_action,
            envelopes: vec![envelope],
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ProcessMailbox {
    lanes: BTreeMap<(u8, DeliveryAction), VecDeque<MailboxBatch>>,
}

impl ProcessMailbox {
    fn enqueue(&mut self, envelope: QueueEnvelope) {
        let final_priority = envelope.final_priority.unwrap_or(envelope.base_priority);
        let final_delivery_action = envelope
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action);
        let lane = self
            .lanes
            .entry((final_priority, final_delivery_action))
            .or_default();
        if let Some(batch) = lane.back_mut() {
            batch.envelopes.push(envelope);
            return;
        }
        lane.push_back(MailboxBatch::from_envelope(envelope));
    }

    fn pop_next_batch(&mut self) -> Option<MailboxBatch> {
        for priority in 1..=8 {
            for action in DELIVERY_ACTION_ORDER {
                let Some(lane) = self.lanes.get_mut(&(priority, action)) else {
                    continue;
                };
                if let Some(batch) = lane.pop_front() {
                    if lane.is_empty() {
                        self.lanes.remove(&(priority, action));
                    }
                    return Some(batch);
                }
            }
        }
        None
    }

    fn has_pending_optimizer_message(&self) -> bool {
        self.lanes.values().any(|lane| {
            lane.iter().any(|batch| {
                batch
                    .envelopes
                    .iter()
                    .any(|envelope| envelope.source_kind == MessageSourceKind::Optimizer)
            })
        })
    }

    fn is_empty(&self) -> bool {
        self.lanes.values().all(VecDeque::is_empty)
    }
}

#[derive(Debug, Clone, Default)]
struct MailboxRegistry {
    inner: BTreeMap<String, ProcessMailbox>,
}

impl MailboxRegistry {
    fn enqueue(&mut self, envelope: QueueEnvelope) {
        self.inner
            .entry(envelope.to.clone())
            .or_default()
            .enqueue(envelope);
    }

    fn pop_next_batch(&mut self, agent_id: &str) -> Option<MailboxBatch> {
        let mut should_remove = false;
        let batch = self
            .inner
            .get_mut(agent_id)
            .and_then(ProcessMailbox::pop_next_batch);
        if let Some(mailbox) = self.inner.get(agent_id) {
            should_remove = mailbox.is_empty();
        }
        if should_remove {
            self.inner.remove(agent_id);
        }
        batch
    }

    fn has_pending_optimizer_message(&self, agent_id: &str) -> bool {
        self.inner
            .get(agent_id)
            .map(ProcessMailbox::has_pending_optimizer_message)
            .unwrap_or(false)
    }

    fn all_pending_envelopes(&self, agent_id: &str) -> Vec<QueueEnvelope> {
        self.inner
            .get(agent_id)
            .map(|mailbox| {
                let mut envelopes = Vec::new();
                for lane in mailbox.lanes.values() {
                    for batch in lane {
                        envelopes.extend(batch.envelopes.clone());
                    }
                }
                envelopes
            })
            .unwrap_or_default()
    }

    fn is_completely_empty(&self) -> bool {
        self.inner.values().all(ProcessMailbox::is_empty)
    }

    fn snapshots(&self) -> Vec<MailboxSnapshot> {
        let mut snapshots = Vec::new();
        for (agent_id, mailbox) in &self.inner {
            let mut batches = Vec::new();
            for lane in mailbox.lanes.values() {
                for batch in lane {
                    batches.push(MailboxBatchSnapshot {
                        final_priority: batch.final_priority,
                        final_delivery_action: batch.final_delivery_action,
                        message_ids: batch
                            .envelopes
                            .iter()
                            .map(|envelope| envelope.message_id)
                            .collect(),
                        from_agents: batch
                            .envelopes
                            .iter()
                            .map(|envelope| envelope.from.clone())
                            .collect(),
                        source_kinds: batch
                            .envelopes
                            .iter()
                            .map(|envelope| envelope.source_kind)
                            .collect(),
                    });
                }
            }
            snapshots.push(MailboxSnapshot {
                agent_id: agent_id.clone(),
                batches,
            });
        }
        snapshots
    }
}

#[derive(Clone)]
struct GlobalMessageQueue {
    tx: mpsc::UnboundedSender<QueueEnvelope>,
}

impl GlobalMessageQueue {
    fn new() -> (Self, mpsc::UnboundedReceiver<QueueEnvelope>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    fn enqueue(&self, envelope: QueueEnvelope) -> Result<()> {
        self.tx
            .send(envelope)
            .map_err(|error| anyhow!("failed to enqueue queue envelope: {error}"))
    }
}

#[derive(Debug, Clone, Default)]
struct QueueDispatcher {
    mailboxes: MailboxRegistry,
}

#[derive(Debug, Clone)]
struct CompletedAgentTurn {
    agent_id: String,
    source_message_ids: Vec<usize>,
    prompt_messages: Vec<ProviderMessage>,
    raw_output: String,
    decision: AgentDecision,
    finished_at_ms: u128,
}

#[derive(Debug, Clone)]
struct LocalOptimizerResult {
    artifact: ReflectionArtifact,
}

#[derive(Debug, Clone)]
enum AsyncEvent {
    AgentCompleted(CompletedAgentTurn),
    ScoreJudged(JudgeViewArtifact),
    LocalOptimizerGenerated(LocalOptimizerResult),
    GlobalRepromptGenerated(RepromptArtifact),
}

struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create {}", root.display()))?;
        Ok(Self { root })
    }

    fn write_text(&self, name: impl AsRef<Path>, content: &str) -> Result<()> {
        let path = self.root.join(name.as_ref());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&path, content).with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    fn write_json<T: Serialize>(&self, name: impl AsRef<Path>, value: &T) -> Result<()> {
        let text = serde_json::to_string_pretty(value).context("failed to serialize json")?;
        self.write_text(name, &text)
    }
}

fn default_run_dir() -> PathBuf {
    std::env::var("GEPA_ZERO_RUN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./runs/latest"))
}

fn root_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn prompt_path(name: &str) -> PathBuf {
    root_path().join(name)
}

fn load_prompt_text(name: &str) -> Result<String> {
    let path = prompt_path(name);
    fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))
}

fn load_json_file<T: DeserializeOwned>(name: &str) -> Result<T> {
    let path = root_path().join(name);
    let text =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("failed to decode {}", path.display()))
}

fn load_agents() -> Result<Vec<AgentConfig>> {
    let agents: Vec<AgentConfig> = load_json_file("agents.json")?;
    if agents.is_empty() {
        anyhow::bail!("agents.json must contain at least one agent");
    }

    let mut seen = BTreeSet::new();
    for agent in &agents {
        if agent.agent_id.trim().is_empty() {
            anyhow::bail!("agent_id must not be empty");
        }
        if !seen.insert(agent.agent_id.clone()) {
            anyhow::bail!("duplicate agent_id: {}", agent.agent_id);
        }
        if agent.tools.is_empty() {
            anyhow::bail!("agent {} must declare at least one tool", agent.agent_id);
        }
        for tool in &agent.tools {
            match tool.as_str() {
                "send_message" | "search_menu_repository" | "search_ingredient_repository" => {}
                other => anyhow::bail!("unsupported tool {} for agent {}", other, agent.agent_id),
            }
        }
        let prompt = prompt_path(&agent.prompt_file);
        if !prompt.exists() {
            anyhow::bail!(
                "prompt_file {} for agent {} does not exist",
                agent.prompt_file,
                agent.agent_id
            );
        }
    }
    Ok(agents)
}

fn load_tasks() -> Result<Vec<TaskConfig>> {
    let tasks: Vec<TaskConfig> = load_json_file("tasks.json")?;
    if tasks.is_empty() {
        anyhow::bail!("tasks.json must contain at least one task");
    }
    Ok(tasks)
}

fn load_repository(name: &str) -> Result<Vec<RepositoryEntry>> {
    load_json_file(name)
}

fn load_seed_prompts(agents: &[AgentConfig]) -> Result<BTreeMap<String, String>> {
    let mut prompts = BTreeMap::new();
    for agent in agents {
        prompts.insert(
            agent.agent_id.clone(),
            load_prompt_text(&agent.prompt_file)
                .with_context(|| format!("failed to load prompt for {}", agent.agent_id))?,
        );
    }
    Ok(prompts)
}

fn prompt_entries_from_map(prompts: &BTreeMap<String, String>) -> Vec<AgentPrompt> {
    prompts
        .iter()
        .map(|(agent_id, prompt)| AgentPrompt {
            agent_id: agent_id.clone(),
            prompt: prompt.clone(),
        })
        .collect()
}

fn extract_json_blob(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Some(trimmed.to_string());
    }

    let mut start = None;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in trimmed.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            '}' => {
                if depth == 0 {
                    continue;
                }
                depth -= 1;
                if depth == 0 {
                    if let Some(start_index) = start {
                        return Some(trimmed[start_index..=index].to_string());
                    }
                }
            }
            _ => {}
        }
    }

    None
}

fn parse_structured_output<T: DeserializeOwned>(raw_output: &str) -> Result<T> {
    let blob = extract_json_blob(raw_output).context("output missing json object")?;
    serde_json::from_str(&blob).with_context(|| format!("failed to parse json: {}", blob))
}

fn parse_agent_decision(raw_output: &str) -> Result<AgentDecision> {
    parse_structured_output(raw_output)
}

fn now_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis())
}

fn estimate_text_tokens(text: &str) -> usize {
    cl100k_base()
        .ok()
        .map(|bpe| bpe.encode_ordinary(text).len())
        .unwrap_or_else(|| text.chars().count().saturating_div(4).max(1))
}

fn estimate_messages_tokens(messages: &[ProviderMessage]) -> usize {
    messages
        .iter()
        .map(|message| {
            4 + estimate_text_tokens(&message.role) + estimate_text_tokens(&message.content)
        })
        .sum::<usize>()
        + 2
}

fn clamp_score(score: f32) -> f32 {
    score.clamp(0.0, 10.0)
}

fn clamp_priority(priority: u8) -> u8 {
    priority.clamp(1, 8)
}

fn keyword_score(query_text: &str, keyword: &str) -> usize {
    if query_text.is_empty() || keyword.is_empty() {
        return 0;
    }
    if query_text.contains(keyword) {
        return keyword.chars().count() + 100;
    }
    keyword
        .chars()
        .filter(|ch| query_text.contains(*ch))
        .count()
}

fn best_repository_message(entries: &[RepositoryEntry], query_text: &str) -> String {
    let mut best_message = entries
        .first()
        .map(|entry| entry.message.clone())
        .unwrap_or_default();
    let mut best_score = 0usize;

    for entry in entries {
        let mut entry_best = 0usize;
        for keyword in &entry.keywords {
            let score = keyword_score(query_text, keyword);
            if score > entry_best {
                entry_best = score;
            }
        }
        if entry_best > best_score {
            best_score = entry_best;
            best_message = entry.message.clone();
        }
    }

    best_message
}

fn resolve_sent_message(agent: &AgentConfig, decision: &AgentDecision) -> Result<String> {
    if !agent
        .tools
        .iter()
        .any(|tool| tool == decision.tool_name.trim())
    {
        anyhow::bail!(
            "tool {} is not allowed for agent {}",
            decision.tool_name,
            agent.agent_id
        );
    }

    let content = decision.content.trim();
    match decision.tool_name.trim() {
        "send_message" => {
            if content.is_empty() {
                anyhow::bail!("send_message requires non-empty content");
            }
            Ok(content.to_string())
        }
        "search_menu_repository" => {
            let entries = load_repository("menu_repository.json")?;
            Ok(best_repository_message(&entries, content))
        }
        "search_ingredient_repository" => {
            let entries = load_repository("ingredient_repository.json")?;
            Ok(best_repository_message(&entries, content))
        }
        other => anyhow::bail!("unsupported tool: {}", other),
    }
}

fn recent_relation_messages(
    thread: &[ThreadMessage],
    sender: &str,
    target: &str,
) -> Vec<ThreadMessage> {
    let mut related = thread
        .iter()
        .filter(|message| {
            message.from == sender
                || message.to == sender
                || message.from == target
                || message.to == target
        })
        .cloned()
        .collect::<Vec<_>>();
    if related.len() > 24 {
        related.drain(0..related.len() - 24);
    }
    related
}

fn score_history_json(judge_views: &[JudgeViewArtifact]) -> Result<String> {
    serde_json::to_string_pretty(judge_views).context("failed to serialize judge history")
}

fn optimizer_events_json(events: &[OptimizerEvent]) -> Result<String> {
    serde_json::to_string_pretty(events).context("failed to serialize optimizer events")
}

fn batch_messages_json(batch: &MailboxBatch) -> Result<String> {
    serde_json::to_string_pretty(&batch.envelopes).context("failed to serialize mailbox batch")
}

fn agent_prompt_entries_json(
    prompts: &BTreeMap<String, String>,
    agent_ids: &[String],
) -> Result<String> {
    let entries = agent_ids
        .iter()
        .filter_map(|agent_id| {
            prompts.get(agent_id).map(|prompt| AgentPrompt {
                agent_id: agent_id.clone(),
                prompt: prompt.clone(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&entries).context("failed to serialize prompt entries")
}

fn build_agent_prompt_body(
    task: &TaskConfig,
    agent: &AgentConfig,
    agent_ids_json: &str,
    batch_json: &str,
    judge_history_json: &str,
    thread_tail_json: &str,
) -> String {
    let tool_list = agent.tools.join(" | ");
    format!(
        "[task_prompt]\n{}\n\n\
[released_mailbox_batch_json]\n{}\n\n\
[thread_tail_json]\n{}\n\n\
[score_judge_history_json]\n{}\n\n\
[all_agent_ids_json]\n{}\n\n\
[available_tools]\n{}\n\n\
请输出一个 JSON object，字段固定为：\
- tool_name: string\
- to_agent_id: string\
- content: string\
\n\
如果你选 send_message，content 就是要发给目标 agent 的原始消息。\
如果你选 search_menu_repository 或 search_ingredient_repository，content 就是查库关键词；运行时会自动把查到的结果作为发给 to_agent_id 的消息。\
\nJSON only。",
        task.task_prompt,
        batch_json,
        thread_tail_json,
        judge_history_json,
        agent_ids_json,
        tool_list,
    )
}

fn build_agent_prompt(
    task: &TaskConfig,
    agent: &AgentConfig,
    base_prompt: &str,
    batch: &MailboxBatch,
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    agent_ids: &[String],
    replay_anchor_message_id: usize,
) -> Result<Vec<ProviderMessage>> {
    let batch_json = batch_messages_json(batch)?;
    let judge_history_json = score_history_json(judge_views)?;
    let agent_ids_json =
        serde_json::to_string_pretty(agent_ids).context("failed to serialize agent ids")?;
    let excluded_ids = batch
        .envelopes
        .iter()
        .map(|envelope| envelope.message_id)
        .collect::<BTreeSet<_>>();
    let candidates = thread
        .iter()
        .filter(|message| message.message_id > replay_anchor_message_id)
        .filter(|message| !excluded_ids.contains(&message.message_id))
        .cloned()
        .collect::<Vec<_>>();

    let mut start = 0usize;
    loop {
        let tail = &candidates[start..];
        let thread_tail_json =
            serde_json::to_string_pretty(tail).context("failed to serialize thread tail")?;
        let messages = vec![
            ProviderMessage::system(base_prompt.to_string()),
            ProviderMessage::user(build_agent_prompt_body(
                task,
                agent,
                &agent_ids_json,
                &batch_json,
                &judge_history_json,
                &thread_tail_json,
            )),
        ];
        if estimate_messages_tokens(&messages) + RESERVED_OUTPUT_TOKENS <= SOFT_TOKEN_LIMIT
            || start == candidates.len()
        {
            return Ok(messages);
        }
        start += 1;
    }
}

fn build_delivery_judge_body(
    task: &TaskConfig,
    envelope: &QueueEnvelope,
    relation_json: &str,
    relation_history_json: &str,
    score_history_json: &str,
) -> String {
    format!(
        "[task_prompt]\n{}\n\n\
[relation]\n{}\n\n\
[relation_history_json]\n{}\n\n\
[score_judge_history_json]\n{}\n\n\
[message]\nsource_kind: {}\nbase_priority: {}\nrequested_delivery_action: {}\ncontent: {}\n\n\
请只输出一个 JSON object，字段固定为：\
- final_priority: 1-8 的整数\
- final_delivery_action: string\
- bounce_hint: string，可选\
\nJSON only。",
        task.task_prompt,
        relation_json,
        relation_history_json,
        score_history_json,
        match envelope.source_kind {
            MessageSourceKind::Agent => "agent",
            MessageSourceKind::Optimizer => "optimizer",
            MessageSourceKind::System => "system",
        },
        envelope.base_priority,
        envelope.requested_delivery_action.as_str(),
        envelope.content,
    )
}

fn build_delivery_judge_prompt(
    task: &TaskConfig,
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    envelope: &QueueEnvelope,
) -> Result<Vec<ProviderMessage>> {
    let relation_summary = serde_json::json!({
        "from": envelope.from,
        "to": envelope.to,
        "tool_name": envelope.tool_name,
    });
    let relation_json =
        serde_json::to_string_pretty(&relation_summary).context("failed to serialize relation")?;
    let relation_history = recent_relation_messages(thread, &envelope.from, &envelope.to);
    let relation_history_json = serde_json::to_string_pretty(&relation_history)
        .context("failed to serialize relation history")?;
    let score_history = score_history_json(judge_views)?;
    Ok(vec![
        ProviderMessage::system(load_prompt_text(
            "prompts/delivery_judge_system_prompt.txt",
        )?),
        ProviderMessage::user(build_delivery_judge_body(
            task,
            envelope,
            &relation_json,
            &relation_history_json,
            &score_history,
        )),
    ])
}

async fn judge_delivery_decision(
    provider: &LiveChatProvider,
    task: &TaskConfig,
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    envelope: &QueueEnvelope,
) -> Result<(u8, DeliveryAction, Option<String>)> {
    let raw_output = provider
        .complete_text(&build_delivery_judge_prompt(
            task,
            thread,
            judge_views,
            envelope,
        )?)
        .await?;
    let draft: DeliveryJudgeDraft = parse_structured_output(&raw_output)?;
    Ok((
        clamp_priority(draft.final_priority.unwrap_or(envelope.base_priority)),
        draft
            .final_delivery_action
            .unwrap_or(envelope.requested_delivery_action),
        draft.bounce_hint,
    ))
}

fn build_score_judge_prompt(
    task: &TaskConfig,
    thread: &[ThreadMessage],
) -> Result<Vec<ProviderMessage>> {
    let mut start = 0usize;
    loop {
        let tail = &thread[start..];
        let thread_json =
            serde_json::to_string_pretty(tail).context("failed to serialize thread for judge")?;
        let messages = vec![
            ProviderMessage::system(load_prompt_text("prompts/judge_system_prompt.txt")?),
            ProviderMessage::user(format!(
                "[task_prompt]\n{}\n\n\
[full_thread_json]\n{}\n\n\
请输出一个 JSON object，字段固定为：\
- coordination_score: number\
- note: string，可选\
\nJSON only。",
                task.task_prompt, thread_json
            )),
        ];
        if estimate_messages_tokens(&messages) + RESERVED_OUTPUT_TOKENS <= HARD_TOKEN_LIMIT
            || start == thread.len()
        {
            return Ok(messages);
        }
        start += 1;
    }
}

fn build_local_optimizer_prompt_body(
    task: &TaskConfig,
    target_agent_id: &str,
    current_prompt: &str,
    optimizer_event_json: &str,
    score_history_json: &str,
    thread_json: &str,
    suffix: &str,
) -> String {
    format!(
        "[task_prompt]\n{}\n\n\
[target_agent_id]\n{}\n\n\
[current_agent_prompt]\n{}\n\n\
[trigger_optimizer_event_json]\n{}\n\n\
[score_judge_history_json]\n{}\n\n\
[full_thread_json]\n{}\n\n\
{}",
        task.task_prompt,
        target_agent_id,
        current_prompt,
        optimizer_event_json,
        score_history_json,
        thread_json,
        suffix,
    )
}

fn build_local_optimizer_prompt(
    task: &TaskConfig,
    target_agent_id: &str,
    current_prompt: &str,
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    event: &OptimizerEvent,
) -> Result<Vec<ProviderMessage>> {
    let event_json =
        serde_json::to_string_pretty(event).context("failed to serialize optimizer event")?;
    let score_history = score_history_json(judge_views)?;
    let suffix = load_prompt_text("prompts/reflection_suffix.txt")?;
    let mut start = 0usize;
    loop {
        let tail = &thread[start..];
        let thread_json =
            serde_json::to_string_pretty(tail).context("failed to serialize optimizer thread")?;
        let messages = vec![
            ProviderMessage::system(load_prompt_text(
                "prompts/optimizer_meta_system_prompt.txt",
            )?),
            ProviderMessage::user(build_local_optimizer_prompt_body(
                task,
                target_agent_id,
                current_prompt,
                &event_json,
                &score_history,
                &thread_json,
                &suffix,
            )),
        ];
        if estimate_messages_tokens(&messages) + RESERVED_OUTPUT_TOKENS <= HARD_TOKEN_LIMIT
            || start == thread.len()
        {
            return Ok(messages);
        }
        start += 1;
    }
}

fn build_global_reprompt_prompt_body(
    task: &TaskConfig,
    prompts_json: &str,
    agent_ids_json: &str,
    optimizer_events_json: &str,
    score_history_json: &str,
    thread_json: &str,
    suffix: &str,
) -> String {
    format!(
        "[task_prompt]\n{}\n\n\
[current_agent_prompts_json]\n{}\n\n\
[all_agent_ids_json]\n{}\n\n\
[optimizer_events_json]\n{}\n\n\
[score_judge_history_json]\n{}\n\n\
[full_thread_json]\n{}\n\n\
{}",
        task.task_prompt,
        prompts_json,
        agent_ids_json,
        optimizer_events_json,
        score_history_json,
        thread_json,
        suffix,
    )
}

fn build_global_reprompt_prompt(
    task: &TaskConfig,
    prompts: &BTreeMap<String, String>,
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    optimizer_events: &[OptimizerEvent],
    agent_ids: &[String],
) -> Result<Vec<ProviderMessage>> {
    let prompts_json = agent_prompt_entries_json(prompts, agent_ids)?;
    let agent_ids_json =
        serde_json::to_string_pretty(agent_ids).context("failed to serialize agent ids")?;
    let optimizer_events_json = optimizer_events_json(optimizer_events)?;
    let score_history = score_history_json(judge_views)?;
    let suffix = load_prompt_text("prompts/reprompt_suffix.txt")?;
    let mut start = 0usize;
    loop {
        let tail = &thread[start..];
        let thread_json =
            serde_json::to_string_pretty(tail).context("failed to serialize reprompt thread")?;
        let messages = vec![
            ProviderMessage::system(load_prompt_text(
                "prompts/optimizer_meta_system_prompt.txt",
            )?),
            ProviderMessage::user(build_global_reprompt_prompt_body(
                task,
                &prompts_json,
                &agent_ids_json,
                &optimizer_events_json,
                &score_history,
                &thread_json,
                &suffix,
            )),
        ];
        if estimate_messages_tokens(&messages) + RESERVED_OUTPUT_TOKENS <= HARD_TOKEN_LIMIT
            || start == thread.len()
        {
            return Ok(messages);
        }
        start += 1;
    }
}

fn validate_agent_prompts(
    prompts: &[AgentPrompt],
    agent_ids: &[String],
) -> Result<Vec<AgentPrompt>> {
    let mut by_agent = BTreeMap::new();
    for prompt in prompts {
        let trimmed = prompt.prompt.trim();
        if trimmed.is_empty() {
            anyhow::bail!("empty prompt for {}", prompt.agent_id);
        }
        by_agent.insert(
            prompt.agent_id.clone(),
            AgentPrompt {
                agent_id: prompt.agent_id.clone(),
                prompt: trimmed.to_string(),
            },
        );
    }

    let mut validated = Vec::with_capacity(agent_ids.len());
    for agent_id in agent_ids {
        let prompt = by_agent
            .remove(agent_id)
            .with_context(|| format!("missing reprompt output for {}", agent_id))?;
        validated.push(prompt);
    }
    if let Some(extra) = by_agent.keys().next() {
        anyhow::bail!("unexpected reprompt output for {}", extra);
    }
    Ok(validated)
}

fn decision_from_scores(baseline_score: Option<f32>, current_score: f32) -> String {
    match baseline_score {
        None => "seed".to_string(),
        Some(score) if current_score > score => "accept".to_string(),
        Some(_) => "reject".to_string(),
    }
}

fn should_trigger_local_optimizer(action: DeliveryAction) -> bool {
    matches!(
        action,
        DeliveryAction::PersistentAsyncClone
            | DeliveryAction::EphemeralAsyncClone
            | DeliveryAction::BounceWithHint
            | DeliveryAction::Ignore
            | DeliveryAction::BlacklistSender
    )
}

fn sender_is_blacklisted(
    blacklists: &BTreeMap<String, BTreeSet<String>>,
    sender: &str,
    target: &str,
) -> bool {
    blacklists
        .get(target)
        .map(|senders| senders.contains(sender))
        .unwrap_or(false)
}

fn completion_keywords() -> [&'static str; 7] {
    [
        "任务完成",
        "全部完成",
        "已经完成",
        "结束",
        "收工",
        "done",
        "complete",
    ]
}

fn message_signals_completion(message: &ThreadMessage) -> bool {
    message.source_kind == MessageSourceKind::Agent
        && completion_keywords().iter().any(|keyword| {
            message
                .content
                .to_lowercase()
                .contains(&keyword.to_lowercase())
        })
}

fn task_completion_signaled(thread: &[ThreadMessage]) -> bool {
    thread.iter().rev().take(6).any(message_signals_completion)
}

fn reprompt_soft_threshold(agent_count: usize) -> usize {
    ((4 * agent_count) + 4) / 5
}

fn estimate_agent_context_pressure(
    task: &TaskConfig,
    agent: &AgentConfig,
    base_prompt: &str,
    pending_envelopes: &[QueueEnvelope],
    thread: &[ThreadMessage],
    judge_views: &[JudgeViewArtifact],
    agent_ids: &[String],
    replay_anchor_message_id: usize,
) -> Result<usize> {
    let pending_batch = MailboxBatch {
        final_priority: DEFAULT_BASE_PRIORITY,
        final_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
        envelopes: pending_envelopes.to_vec(),
    };
    let batch_json = batch_messages_json(&pending_batch)?;
    let agent_ids_json =
        serde_json::to_string_pretty(agent_ids).context("failed to serialize agent ids")?;
    let judge_history_json = score_history_json(judge_views)?;
    let thread_tail = thread
        .iter()
        .filter(|message| message.message_id > replay_anchor_message_id)
        .cloned()
        .collect::<Vec<_>>();
    let thread_tail_json =
        serde_json::to_string_pretty(&thread_tail).context("failed to serialize thread tail")?;
    let messages = vec![
        ProviderMessage::system(base_prompt.to_string()),
        ProviderMessage::user(build_agent_prompt_body(
            task,
            agent,
            &agent_ids_json,
            &batch_json,
            &judge_history_json,
            &thread_tail_json,
        )),
    ];
    Ok(estimate_messages_tokens(&messages) + RESERVED_OUTPUT_TOKENS)
}

fn should_trigger_global_reprompt(
    task: &TaskConfig,
    agents: &[AgentConfig],
    prompts: &BTreeMap<String, String>,
    dispatcher: &QueueDispatcher,
    state: &TaskRunState,
    agent_ids: &[String],
    replay_anchor_message_id: usize,
) -> Result<bool> {
    if agents.is_empty() {
        return Ok(false);
    }

    let mut soft_hits = 0usize;
    for agent in agents {
        let prompt = prompts
            .get(&agent.agent_id)
            .with_context(|| format!("missing current prompt for {}", agent.agent_id))?;
        let pending = dispatcher.mailboxes.all_pending_envelopes(&agent.agent_id);
        let pressure = estimate_agent_context_pressure(
            task,
            agent,
            prompt,
            &pending,
            &state.thread,
            &state.judge_views,
            agent_ids,
            replay_anchor_message_id,
        )?;
        if pressure >= HARD_TOKEN_LIMIT {
            return Ok(true);
        }
        if pressure >= SOFT_TOKEN_LIMIT {
            soft_hits += 1;
        }
    }

    Ok(soft_hits >= reprompt_soft_threshold(agents.len()))
}

fn next_message_id(counter: &mut usize) -> usize {
    let current = *counter;
    *counter += 1;
    current
}

fn next_turn_id(counter: &mut usize) -> usize {
    let current = *counter;
    *counter += 1;
    current
}

fn make_optimizer_envelope(
    message_id: usize,
    target_agent_id: &str,
    message: &str,
) -> Result<QueueEnvelope> {
    Ok(QueueEnvelope {
        message_id,
        created_at_ms: now_ms()?,
        from: "__optimizer__".to_string(),
        to: target_agent_id.to_string(),
        content: message.to_string(),
        source_kind: MessageSourceKind::Optimizer,
        tool_name: None,
        base_priority: SYSTEM_PRIORITY,
        requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
        final_priority: Some(SYSTEM_PRIORITY),
        final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
        bounce_hint: None,
        skip_delivery_judge: true,
        triggers_optimizer: false,
    })
}

fn make_bounce_envelope(
    message_id: usize,
    envelope: &QueueEnvelope,
    bounce_hint: Option<String>,
) -> Result<QueueEnvelope> {
    Ok(QueueEnvelope {
        message_id,
        created_at_ms: now_ms()?,
        from: "__delivery_judge__".to_string(),
        to: envelope.from.clone(),
        content: bounce_hint
            .unwrap_or_else(|| "消息被优先级判断器回退，请换一种协作方式。".to_string()),
        source_kind: MessageSourceKind::System,
        tool_name: None,
        base_priority: SYSTEM_PRIORITY,
        requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
        final_priority: Some(SYSTEM_PRIORITY),
        final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
        bounce_hint: None,
        skip_delivery_judge: true,
        triggers_optimizer: false,
    })
}

fn persist_task_state(
    task_artifacts: &ArtifactStore,
    task: &TaskConfig,
    state: &TaskRunState,
) -> Result<()> {
    task_artifacts.write_json("task.json", task)?;
    task_artifacts.write_json("thread.json", &state.thread)?;
    task_artifacts.write_json("agent_turns.json", &state.agent_turns)?;
    task_artifacts.write_json("judge_views.json", &state.judge_views)?;
    task_artifacts.write_json("reflections.json", &state.reflections)?;
    task_artifacts.write_json("reprompts.json", &state.reprompts)?;
    task_artifacts.write_json("run_state.json", state)?;
    Ok(())
}

fn spawn_agent_generation(
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    provider: LiveChatProvider,
    task: TaskConfig,
    agent: AgentConfig,
    base_prompt: String,
    batch: MailboxBatch,
    thread: Vec<ThreadMessage>,
    judge_views: Vec<JudgeViewArtifact>,
    agent_ids: Vec<String>,
    replay_anchor_message_id: usize,
) -> Result<()> {
    let prompt_messages = build_agent_prompt(
        &task,
        &agent,
        &base_prompt,
        &batch,
        &thread,
        &judge_views,
        &agent_ids,
        replay_anchor_message_id,
    )?;
    let source_message_ids = batch
        .envelopes
        .iter()
        .map(|envelope| envelope.message_id)
        .collect::<Vec<_>>();
    joinset.spawn(async move {
        let raw_output = provider.complete_text(&prompt_messages).await?;
        let decision = parse_agent_decision(&raw_output)?;
        if decision.to_agent_id.trim().is_empty() {
            anyhow::bail!("agent {} returned empty to_agent_id", agent.agent_id);
        }
        Ok(AsyncEvent::AgentCompleted(CompletedAgentTurn {
            agent_id: agent.agent_id,
            source_message_ids,
            prompt_messages,
            raw_output,
            decision,
            finished_at_ms: now_ms()?,
        }))
    });
    Ok(())
}

fn spawn_score_judge(
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    provider: LiveChatProvider,
    task: TaskConfig,
    thread: Vec<ThreadMessage>,
) -> Result<()> {
    let prompt_messages = build_score_judge_prompt(&task, &thread)?;
    joinset.spawn(async move {
        let raw_output = provider.complete_text(&prompt_messages).await?;
        let draft: JudgeViewDraft = parse_structured_output(&raw_output)?;
        Ok(AsyncEvent::ScoreJudged(JudgeViewArtifact {
            at_ms: now_ms()?,
            prompt_messages,
            raw_output,
            coordination_score: clamp_score(draft.coordination_score),
            note: draft.note.trim().to_string(),
        }))
    });
    Ok(())
}

fn spawn_local_optimizer(
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    provider: LiveChatProvider,
    task: TaskConfig,
    target_agent_id: String,
    current_prompt: String,
    thread: Vec<ThreadMessage>,
    judge_views: Vec<JudgeViewArtifact>,
    event: OptimizerEvent,
) -> Result<()> {
    let prompt_messages = build_local_optimizer_prompt(
        &task,
        &target_agent_id,
        &current_prompt,
        &thread,
        &judge_views,
        &event,
    )?;
    joinset.spawn(async move {
        let raw_output = provider.complete_text(&prompt_messages).await?;
        let draft: LocalOptimizerDraft = parse_structured_output(&raw_output)?;
        let message = draft.message.trim().to_string();
        if message.is_empty() {
            anyhow::bail!(
                "optimizer returned empty reflection message for {}",
                target_agent_id
            );
        }
        Ok(AsyncEvent::LocalOptimizerGenerated(LocalOptimizerResult {
            artifact: ReflectionArtifact {
                trigger_event_id: event.event_id,
                target_agent_id,
                prompt_messages,
                raw_output,
                message,
                created_at_ms: now_ms()?,
            },
        }))
    });
    Ok(())
}

fn spawn_global_reprompt(
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    provider: LiveChatProvider,
    task: TaskConfig,
    prompts: BTreeMap<String, String>,
    thread: Vec<ThreadMessage>,
    judge_views: Vec<JudgeViewArtifact>,
    optimizer_events: Vec<OptimizerEvent>,
    agent_ids: Vec<String>,
    replay_anchor_message_id: usize,
) -> Result<()> {
    let prompt_messages = build_global_reprompt_prompt(
        &task,
        &prompts,
        &thread,
        &judge_views,
        &optimizer_events,
        &agent_ids,
    )?;
    joinset.spawn(async move {
        let raw_output = provider.complete_text(&prompt_messages).await?;
        let draft: RepromptDraft = parse_structured_output(&raw_output)?;
        let agent_prompts = validate_agent_prompts(&draft.agent_prompts, &agent_ids)?;
        Ok(AsyncEvent::GlobalRepromptGenerated(RepromptArtifact {
            at_ms: now_ms()?,
            reason: "token_pressure".to_string(),
            replay_anchor_message_id,
            prompt_messages,
            raw_output,
            agent_prompts,
        }))
    });
    Ok(())
}

fn materialize_batch_into_thread(
    batch: &MailboxBatch,
    thread: &mut Vec<ThreadMessage>,
) -> Result<()> {
    let delivered_at_ms = now_ms()?;
    for envelope in &batch.envelopes {
        thread.push(ThreadMessage {
            message_id: envelope.message_id,
            turn_id: None,
            from: envelope.from.clone(),
            to: envelope.to.clone(),
            content: envelope.content.clone(),
            source_kind: envelope.source_kind,
            tool_name: envelope.tool_name.clone(),
            base_priority: envelope.base_priority,
            final_priority: envelope.final_priority.unwrap_or(envelope.base_priority),
            requested_delivery_action: envelope.requested_delivery_action,
            final_delivery_action: envelope
                .final_delivery_action
                .unwrap_or(envelope.requested_delivery_action),
            created_at_ms: envelope.created_at_ms,
            delivered_at_ms,
        });
    }
    Ok(())
}

fn maybe_start_ready_agents(
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    provider: &LiveChatProvider,
    task: &TaskConfig,
    agents: &[AgentConfig],
    prompts: &BTreeMap<String, String>,
    dispatcher: &mut QueueDispatcher,
    state: &mut TaskRunState,
    busy_agents: &mut BTreeSet<String>,
    bootstrapped_agents: &mut BTreeSet<String>,
    agent_ids: &[String],
    replay_anchor_message_id: usize,
    activation_paused: bool,
) -> Result<bool> {
    if activation_paused {
        state.mailboxes = dispatcher.mailboxes.snapshots();
        return Ok(false);
    }

    let mut started_any = false;
    for agent in agents {
        if busy_agents.contains(&agent.agent_id) {
            continue;
        }

        let batch = if let Some(batch) = dispatcher.mailboxes.pop_next_batch(&agent.agent_id) {
            batch
        } else if !bootstrapped_agents.contains(&agent.agent_id) {
            MailboxBatch {
                final_priority: DEFAULT_BASE_PRIORITY,
                final_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
                envelopes: Vec::new(),
            }
        } else {
            continue;
        };

        bootstrapped_agents.insert(agent.agent_id.clone());
        materialize_batch_into_thread(&batch, &mut state.thread)?;
        let prompt = prompts
            .get(&agent.agent_id)
            .cloned()
            .with_context(|| format!("missing current prompt for {}", agent.agent_id))?;
        spawn_agent_generation(
            joinset,
            provider.clone(),
            task.clone(),
            agent.clone(),
            prompt,
            batch,
            state.thread.clone(),
            state.judge_views.clone(),
            agent_ids.to_vec(),
            replay_anchor_message_id,
        )?;
        busy_agents.insert(agent.agent_id.clone());
        started_any = true;
    }

    state.mailboxes = dispatcher.mailboxes.snapshots();
    Ok(started_any)
}

async fn handle_agent_completion(
    completed: CompletedAgentTurn,
    task: &TaskConfig,
    agents_by_id: &BTreeMap<String, AgentConfig>,
    queue: &GlobalMessageQueue,
    queued_count: &mut usize,
    state: &mut TaskRunState,
    turn_counter: &mut usize,
    message_counter: &mut usize,
) -> Result<()> {
    let agent = agents_by_id
        .get(&completed.agent_id)
        .with_context(|| format!("missing agent config for {}", completed.agent_id))?;
    let sent_message = resolve_sent_message(agent, &completed.decision)?;
    let to_agent_id = completed.decision.to_agent_id.trim().to_string();
    let turn_id = next_turn_id(turn_counter);

    state.agent_turns.push(AgentTurnArtifact {
        turn_id,
        actor: completed.agent_id.clone(),
        source_message_ids: completed.source_message_ids,
        prompt_messages: completed.prompt_messages,
        raw_output: completed.raw_output,
        tool_name: completed.decision.tool_name.clone(),
        to_agent_id: to_agent_id.clone(),
        sent_message: sent_message.clone(),
        finished_at_ms: completed.finished_at_ms,
    });

    let envelope = QueueEnvelope {
        message_id: next_message_id(message_counter),
        created_at_ms: now_ms()?,
        from: completed.agent_id,
        to: to_agent_id,
        content: sent_message,
        source_kind: MessageSourceKind::Agent,
        tool_name: Some(completed.decision.tool_name),
        base_priority: DEFAULT_BASE_PRIORITY,
        requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
        final_priority: None,
        final_delivery_action: None,
        bounce_hint: None,
        skip_delivery_judge: false,
        triggers_optimizer: true,
    };
    queue.enqueue(envelope)?;
    *queued_count += 1;
    let _ = task;
    Ok(())
}

async fn accept_envelope(
    provider: &LiveChatProvider,
    task: &TaskConfig,
    envelope: QueueEnvelope,
    state: &mut TaskRunState,
    dispatcher: &mut QueueDispatcher,
    queue: &GlobalMessageQueue,
    queued_count: &mut usize,
    blacklists: &mut BTreeMap<String, BTreeSet<String>>,
    prompts: &BTreeMap<String, String>,
    pending_local_optimizer: &mut BTreeSet<String>,
    joinset: &mut JoinSet<Result<AsyncEvent>>,
    message_counter: &mut usize,
    agent_ids: &[String],
) -> Result<()> {
    let (final_priority, final_delivery_action, bounce_hint) = if envelope.skip_delivery_judge {
        (
            clamp_priority(envelope.final_priority.unwrap_or(envelope.base_priority)),
            envelope
                .final_delivery_action
                .unwrap_or(envelope.requested_delivery_action),
            envelope.bounce_hint.clone(),
        )
    } else if sender_is_blacklisted(blacklists, &envelope.from, &envelope.to) {
        (SYSTEM_PRIORITY, DeliveryAction::BlacklistSender, None)
    } else {
        judge_delivery_decision(provider, task, &state.thread, &state.judge_views, &envelope)
            .await?
    };

    let prepared_envelope = QueueEnvelope {
        final_priority: Some(final_priority),
        final_delivery_action: Some(final_delivery_action),
        bounce_hint: bounce_hint.clone(),
        ..envelope
    };

    if should_trigger_local_optimizer(final_delivery_action) && prepared_envelope.triggers_optimizer
    {
        let event = OptimizerEvent {
            event_id: state.optimizer_events.len() + 1,
            trigger_message_id: prepared_envelope.message_id,
            from: prepared_envelope.from.clone(),
            to: prepared_envelope.to.clone(),
            delivery_action: final_delivery_action,
            final_priority,
            content: prepared_envelope.content.clone(),
            bounce_hint: bounce_hint.clone(),
            created_at_ms: now_ms()?,
        };
        state.optimizer_events.push(event.clone());
        if prompts.contains_key(&prepared_envelope.from)
            && !dispatcher
                .mailboxes
                .has_pending_optimizer_message(&prepared_envelope.from)
            && !pending_local_optimizer.contains(&prepared_envelope.from)
        {
            if let Some(current_prompt) = prompts.get(&prepared_envelope.from) {
                spawn_local_optimizer(
                    joinset,
                    provider.clone(),
                    task.clone(),
                    prepared_envelope.from.clone(),
                    current_prompt.clone(),
                    state.thread.clone(),
                    state.judge_views.clone(),
                    event,
                )?;
                pending_local_optimizer.insert(prepared_envelope.from.clone());
            }
        }
    }

    match final_delivery_action {
        DeliveryAction::BounceWithHint => {
            let bounce_envelope = make_bounce_envelope(
                next_message_id(message_counter),
                &prepared_envelope,
                bounce_hint,
            )?;
            queue.enqueue(bounce_envelope)?;
            *queued_count += 1;
        }
        DeliveryAction::Ignore => {}
        DeliveryAction::BlacklistSender => {
            blacklists
                .entry(prepared_envelope.to.clone())
                .or_default()
                .insert(prepared_envelope.from.clone());
        }
        DeliveryAction::InterruptDeliver
        | DeliveryAction::PersistentAsyncClone
        | DeliveryAction::EphemeralAsyncClone
        | DeliveryAction::SegmentBoundaryDeliver
        | DeliveryAction::OutputEndDeliver => {
            dispatcher.mailboxes.enqueue(prepared_envelope);
        }
    }

    state.mailboxes = dispatcher.mailboxes.snapshots();
    let _ = agent_ids;
    Ok(())
}

async fn run_final_score_judge(
    provider: &LiveChatProvider,
    task: &TaskConfig,
    state: &mut TaskRunState,
) -> Result<f32> {
    if state.thread.is_empty() {
        return Ok(0.0);
    }
    let prompt_messages = build_score_judge_prompt(task, &state.thread)?;
    let raw_output = provider.complete_text(&prompt_messages).await?;
    let draft: JudgeViewDraft = parse_structured_output(&raw_output)?;
    let score = clamp_score(draft.coordination_score);
    state.judge_views.push(JudgeViewArtifact {
        at_ms: now_ms()?,
        prompt_messages,
        raw_output,
        coordination_score: score,
        note: draft.note.trim().to_string(),
    });
    Ok(score)
}

async fn run_task(
    provider: &LiveChatProvider,
    task: &TaskConfig,
    agents: &[AgentConfig],
    base_prompts: &BTreeMap<String, String>,
    task_artifacts: &ArtifactStore,
) -> Result<TaskOutcome> {
    let agent_ids = agents
        .iter()
        .map(|agent| agent.agent_id.clone())
        .collect::<Vec<_>>();
    let agents_by_id = agents
        .iter()
        .cloned()
        .map(|agent| (agent.agent_id.clone(), agent))
        .collect::<BTreeMap<_, _>>();
    let mut current_prompts = base_prompts.clone();
    let mut dispatcher = QueueDispatcher::default();
    let mut state = TaskRunState {
        thread: Vec::new(),
        agent_turns: Vec::new(),
        judge_views: Vec::new(),
        reflections: Vec::new(),
        reprompts: Vec::new(),
        optimizer_events: Vec::new(),
        mailboxes: Vec::new(),
    };
    let (queue, mut queue_rx) = GlobalMessageQueue::new();
    let mut queued_count = 0usize;
    let mut busy_agents = BTreeSet::new();
    let mut bootstrapped_agents = BTreeSet::new();
    let mut pending_local_optimizer = BTreeSet::new();
    let mut blacklists = BTreeMap::<String, BTreeSet<String>>::new();
    let mut running_score_judge = false;
    let mut running_global_reprompt = false;
    let mut reprompt_pending = false;
    let mut replay_anchor_message_id = 0usize;
    let mut turn_counter = 1usize;
    let mut message_counter = 1usize;
    let mut joinset = JoinSet::<Result<AsyncEvent>>::new();
    let mut judge_interval = interval(Duration::from_secs(SCORE_JUDGE_INTERVAL_SECS));
    judge_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    judge_interval.tick().await;
    let task_started_at = Instant::now();
    let mut last_progress_at = Instant::now();

    state.mailboxes = dispatcher.mailboxes.snapshots();
    persist_task_state(task_artifacts, task, &state)?;

    maybe_start_ready_agents(
        &mut joinset,
        provider,
        task,
        agents,
        &current_prompts,
        &mut dispatcher,
        &mut state,
        &mut busy_agents,
        &mut bootstrapped_agents,
        &agent_ids,
        replay_anchor_message_id,
        false,
    )?;
    persist_task_state(task_artifacts, task, &state)?;

    loop {
        if !reprompt_pending
            && !running_global_reprompt
            && should_trigger_global_reprompt(
                task,
                agents,
                &current_prompts,
                &dispatcher,
                &state,
                &agent_ids,
                replay_anchor_message_id,
            )?
        {
            reprompt_pending = true;
        }

        if reprompt_pending
            && !running_global_reprompt
            && busy_agents.is_empty()
            && pending_local_optimizer.is_empty()
        {
            spawn_global_reprompt(
                &mut joinset,
                provider.clone(),
                task.clone(),
                current_prompts.clone(),
                state.thread.clone(),
                state.judge_views.clone(),
                state.optimizer_events.clone(),
                agent_ids.clone(),
                state
                    .thread
                    .last()
                    .map(|message| message.message_id)
                    .unwrap_or(0),
            )?;
            running_global_reprompt = true;
        }

        maybe_start_ready_agents(
            &mut joinset,
            provider,
            task,
            agents,
            &current_prompts,
            &mut dispatcher,
            &mut state,
            &mut busy_agents,
            &mut bootstrapped_agents,
            &agent_ids,
            replay_anchor_message_id,
            reprompt_pending || running_global_reprompt,
        )?;

        persist_task_state(task_artifacts, task, &state)?;

        let is_idle = queued_count == 0
            && busy_agents.is_empty()
            && pending_local_optimizer.is_empty()
            && !running_global_reprompt
            && !running_score_judge
            && dispatcher.mailboxes.is_completely_empty()
            && joinset.is_empty();
        if task_completion_signaled(&state.thread) && is_idle {
            break;
        }
        if task_started_at.elapsed() >= Duration::from_secs(TASK_WALL_CLOCK_LIMIT_SECS) {
            break;
        }
        if is_idle && last_progress_at.elapsed() >= Duration::from_secs(TASK_IDLE_LIMIT_SECS) {
            break;
        }

        tokio::select! {
            joined = joinset.join_next(), if !joinset.is_empty() => {
                let event = joined.context("background task dropped")??;
                match event {
                    Ok(AsyncEvent::AgentCompleted(completed)) => {
                        busy_agents.remove(&completed.agent_id);
                        handle_agent_completion(
                            completed,
                            task,
                            &agents_by_id,
                            &queue,
                            &mut queued_count,
                            &mut state,
                            &mut turn_counter,
                            &mut message_counter,
                        ).await?;
                    }
                    Ok(AsyncEvent::ScoreJudged(view)) => {
                        running_score_judge = false;
                        state.judge_views.push(view);
                    }
                    Ok(AsyncEvent::LocalOptimizerGenerated(result)) => {
                        pending_local_optimizer.remove(&result.artifact.target_agent_id);
                        let envelope = make_optimizer_envelope(
                            next_message_id(&mut message_counter),
                            &result.artifact.target_agent_id,
                            &result.artifact.message,
                        )?;
                        state.reflections.push(result.artifact);
                        queue.enqueue(envelope)?;
                        queued_count += 1;
                    }
                    Ok(AsyncEvent::GlobalRepromptGenerated(reprompt)) => {
                        running_global_reprompt = false;
                        reprompt_pending = false;
                        replay_anchor_message_id = reprompt.replay_anchor_message_id;
                        current_prompts = reprompt
                            .agent_prompts
                            .iter()
                            .map(|entry| (entry.agent_id.clone(), entry.prompt.clone()))
                            .collect();
                        state.reprompts.push(reprompt);
                    }
                    Err(error) => return Err(error),
                }
                state.mailboxes = dispatcher.mailboxes.snapshots();
                last_progress_at = Instant::now();
            }
            maybe_envelope = queue_rx.recv(), if queued_count > 0 => {
                if let Some(envelope) = maybe_envelope {
                    queued_count = queued_count.saturating_sub(1);
                    accept_envelope(
                        provider,
                        task,
                        envelope,
                        &mut state,
                        &mut dispatcher,
                        &queue,
                        &mut queued_count,
                        &mut blacklists,
                        &current_prompts,
                        &mut pending_local_optimizer,
                        &mut joinset,
                        &mut message_counter,
                        &agent_ids,
                    ).await?;
                    last_progress_at = Instant::now();
                }
            }
            _ = judge_interval.tick() => {
                if !running_score_judge && !state.thread.is_empty() {
                    spawn_score_judge(&mut joinset, provider.clone(), task.clone(), state.thread.clone())?;
                    running_score_judge = true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(MAIN_LOOP_POLL_MS)) => {}
        }
    }

    let final_coordination_score = run_final_score_judge(provider, task, &mut state).await?;
    state.mailboxes = dispatcher.mailboxes.snapshots();
    persist_task_state(task_artifacts, task, &state)?;

    Ok(TaskOutcome {
        final_coordination_score,
        final_prompts: prompt_entries_from_map(&current_prompts),
        state,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let run_dir = default_run_dir();
    let artifacts = ArtifactStore::new(&run_dir)?;
    let provider = LiveChatProvider::from_env()?;

    let agents = load_agents()?;
    let tasks = load_tasks()?;
    let menu_repository: Vec<RepositoryEntry> = load_repository("menu_repository.json")?;
    let ingredient_repository: Vec<RepositoryEntry> =
        load_repository("ingredient_repository.json")?;

    artifacts.write_json("agents.json", &agents)?;
    artifacts.write_json("tasks.json", &tasks)?;
    artifacts.write_json("menu_repository.json", &menu_repository)?;
    artifacts.write_json("ingredient_repository.json", &ingredient_repository)?;

    let mut accepted_prompts = load_seed_prompts(&agents)?;
    let mut last_accepted_score: Option<f32> = None;
    let mut task_summaries = Vec::with_capacity(tasks.len());

    println!("== GEPA Zero Async Multi-Agent ==");
    println!("run_dir: {}", run_dir.display());
    println!("model: {}", provider.model_name());
    println!("agents={}", agents.len());
    println!("tasks={}", tasks.len());

    for task in &tasks {
        let task_artifacts = ArtifactStore::new(run_dir.join(&task.task_id))?;
        let baseline_score = last_accepted_score;
        let accepted_before = accepted_prompts.clone();

        println!("\n================ {} ================", task.task_id);
        println!("task:\n{}\n", task.task_prompt);

        let outcome =
            run_task(&provider, task, &agents, &accepted_prompts, &task_artifacts).await?;
        let task_final_prompts = outcome
            .final_prompts
            .iter()
            .map(|entry| (entry.agent_id.clone(), entry.prompt.clone()))
            .collect::<BTreeMap<_, _>>();
        let decision = decision_from_scores(baseline_score, outcome.final_coordination_score);
        match baseline_score {
            None => {
                accepted_prompts = task_final_prompts.clone();
                last_accepted_score = Some(outcome.final_coordination_score);
            }
            Some(score) if outcome.final_coordination_score > score => {
                accepted_prompts = task_final_prompts.clone();
                last_accepted_score = Some(outcome.final_coordination_score);
            }
            Some(_) => {
                accepted_prompts = accepted_before;
            }
        }

        task_artifacts.write_json("final_prompts_end_of_task.json", &outcome.final_prompts)?;
        task_artifacts.write_json(
            "accepted_prompts_after_task.json",
            &prompt_entries_from_map(&accepted_prompts),
        )?;

        println!(
            "active_prompt_source=accepted task_coordination_score={:.2} baseline={:?} decision={}",
            outcome.final_coordination_score, baseline_score, decision
        );

        task_summaries.push(TaskSummary {
            task_id: task.task_id.clone(),
            active_prompt_source: "accepted".to_string(),
            task_coordination_score: outcome.final_coordination_score,
            baseline_coordination_score: baseline_score,
            decision,
        });
    }

    let summary = RunSummary {
        provider_model: provider.model_name().to_string(),
        task_summaries,
        final_accepted_prompts: prompt_entries_from_map(&accepted_prompts),
    };
    artifacts.write_json("summary.json", &summary)?;
    artifacts.write_json(
        "accepted_prompts_latest.json",
        &prompt_entries_from_map(&accepted_prompts),
    )?;

    println!("\n================ summary ================");
    for task in &summary.task_summaries {
        println!(
            "{} score={:.2} baseline={:?} decision={}",
            task.task_id,
            task.task_coordination_score,
            task.baseline_coordination_score,
            task.decision
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        decision_from_scores, extract_json_blob, parse_agent_decision, parse_structured_output,
        reprompt_soft_threshold, should_trigger_local_optimizer, validate_agent_prompts,
        AgentDecision, AgentPrompt, DeliveryAction, LocalOptimizerDraft, MessageSourceKind,
        ProcessMailbox, QueueEnvelope, RepromptDraft,
    };

    #[test]
    fn json_blob_extraction_handles_markdown_wrapper() {
        let text = "```json\n{\"coordination_score\":7.5}\n```";
        assert_eq!(
            extract_json_blob(text).as_deref(),
            Some("{\"coordination_score\":7.5}")
        );
    }

    #[test]
    fn agent_decision_parses_minimal_fields() {
        let raw = r#"{"tool_name":"send_message","to_agent_id":"apprentice","content":"test"}"#;
        let parsed: AgentDecision = parse_agent_decision(raw).unwrap();

        assert_eq!(
            parsed,
            AgentDecision {
                tool_name: "send_message".to_string(),
                to_agent_id: "apprentice".to_string(),
                content: "test".to_string(),
            }
        );
    }

    #[test]
    fn reprompt_output_requires_all_agents() {
        let draft = RepromptDraft {
            agent_prompts: vec![
                AgentPrompt {
                    agent_id: "master".to_string(),
                    prompt: "a".to_string(),
                },
                AgentPrompt {
                    agent_id: "apprentice".to_string(),
                    prompt: "b".to_string(),
                },
            ],
        };
        let validated = validate_agent_prompts(
            &draft.agent_prompts,
            &["master".to_string(), "apprentice".to_string()],
        )
        .unwrap();
        assert_eq!(validated.len(), 2);
    }

    #[test]
    fn score_decision_matches_sequential_accept_rule() {
        assert_eq!(decision_from_scores(None, 6.0), "seed");
        assert_eq!(decision_from_scores(Some(6.0), 7.0), "accept");
        assert_eq!(decision_from_scores(Some(6.0), 5.0), "reject");
    }

    #[test]
    fn structured_output_ignores_wrapper_text() {
        let raw = r#"前言
{"message":"short reminder"}
后言"#;
        let parsed: LocalOptimizerDraft = parse_structured_output(raw).unwrap();
        assert_eq!(parsed.message, "short reminder");
    }

    #[test]
    fn non_default_actions_trigger_optimizer() {
        assert!(should_trigger_local_optimizer(
            DeliveryAction::PersistentAsyncClone
        ));
        assert!(should_trigger_local_optimizer(
            DeliveryAction::BounceWithHint
        ));
        assert!(!should_trigger_local_optimizer(
            DeliveryAction::SegmentBoundaryDeliver
        ));
    }

    #[test]
    fn mailbox_detects_pending_optimizer_message() {
        let mut mailbox = ProcessMailbox::default();
        mailbox.enqueue(QueueEnvelope {
            message_id: 1,
            created_at_ms: 0,
            from: "__optimizer__".to_string(),
            to: "master".to_string(),
            content: "keep requests tighter".to_string(),
            source_kind: MessageSourceKind::Optimizer,
            tool_name: None,
            base_priority: 1,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            final_priority: Some(1),
            final_delivery_action: Some(DeliveryAction::SegmentBoundaryDeliver),
            bounce_hint: None,
            skip_delivery_judge: true,
            triggers_optimizer: false,
        });
        assert!(mailbox.has_pending_optimizer_message());
    }

    #[test]
    fn eighty_percent_threshold_rounds_up() {
        assert_eq!(reprompt_soft_threshold(1), 1);
        assert_eq!(reprompt_soft_threshold(2), 2);
        assert_eq!(reprompt_soft_threshold(5), 4);
    }
}
