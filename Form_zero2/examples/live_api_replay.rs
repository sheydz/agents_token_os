use std::{collections::{BTreeMap, HashMap}, env};

use anyhow::{anyhow, bail, Context, Result};
use form_zero::{
    Database, DefinitionPart, DeliveryAction, MessageKind, ResultTarget, RuntimeConfig,
    RuntimeEngine, SendMessageRequest, TargetSelector, TaskStep,
};
use serde_json::{json, Value};
use tokio::time::{sleep, Duration, Instant};

const PHASE_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy)]
struct StageSpec {
    name: &'static str,
    provider_input_budget: usize,
    compaction_trigger_ratio: f32,
    max_probe_messages: usize,
    long_message_repeat: usize,
}

struct StageSetup {
    program_run_id: String,
    slot_processes: BTreeMap<String, uuid::Uuid>,
}

struct PhaseObservation {
    saw_reprompt_running: bool,
    saw_empty_lengths_while_reprompt_running: bool,
    final_phase: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    mirror_provider2_env();

    let stage = match env::args().nth(1).as_deref() {
        Some("stage_b") => StageSpec {
            name: "stage_b",
            provider_input_budget: 8_000,
            compaction_trigger_ratio: 0.7,
            max_probe_messages: 20,
            long_message_repeat: 140,
        },
        Some("stage_a") | None => StageSpec {
            name: "stage_a",
            provider_input_budget: 512,
            compaction_trigger_ratio: 0.2,
            max_probe_messages: 10,
            long_message_repeat: 48,
        },
        Some(other) => bail!("unknown stage `{other}`; use `stage_a` or `stage_b`"),
    };

    let database_url = env::var("FORM_ZERO_DATABASE_URL")
        .context("FORM_ZERO_DATABASE_URL is required for live replay example")?;
    let db = Database::connect(&database_url).await?;
    db.init_schema().await?;

    let setup = setup_program(&db, stage.name).await?;
    let initial_bindings = current_bindings(&db, &setup.program_run_id).await?;
    let initial_prompt_versions = current_prompt_versions(&db, &setup.program_run_id).await?;

    let runtime = RuntimeEngine::new(
        db.clone(),
        RuntimeConfig {
            provider_input_budget: stage.provider_input_budget,
            compaction_trigger_ratio: stage.compaction_trigger_ratio,
            ..RuntimeConfig::default()
        },
    )
    .await?;

    println!(
        "{} runtime started for program {}",
        stage.name, setup.program_run_id
    );

    let provider1_result_seen = run_persistent_branch_task(&runtime, &setup).await?;

    let ephemeral_clone_seen = if stage.name == "stage_b" {
        run_ephemeral_clone_task(&runtime, &setup).await?
    } else {
        false
    };

    let mut delivery_probes = Vec::new();
    let mut saw_reprompt_running = false;
    for index in 0..stage.max_probe_messages {
        let envelope = send_non_direct_probe(
            &runtime,
            &setup.program_run_id,
            stage.name,
            index,
            stage.long_message_repeat,
        )
        .await?;
        delivery_probes.push(json!({
            "index": index,
            "final_priority": envelope.final_priority,
            "final_delivery_action": envelope.final_delivery_action.map(|value| value.as_str().to_string()),
        }));
        let phase = db
            .get_program(&setup.program_run_id)
            .await?
            .plan_state_json
            .global_phase;
        println!(
            "{} non-direct probe {} -> phase {}",
            stage.name, index, phase
        );
        if phase == "reprompt_running" {
            saw_reprompt_running = true;
            break;
        }
        sleep(Duration::from_millis(350)).await;
    }

    let phase_observation = wait_for_phase_cycle(&db, &setup.program_run_id, saw_reprompt_running).await?;

    let post_reprompt_refill_seen = if phase_observation.final_phase == "running" {
        refill_lengths_after_reprompt(&db, &runtime, &setup).await?
    } else {
        false
    };
    let final_bindings = current_bindings(&db, &setup.program_run_id).await?;
    let final_prompt_versions = current_prompt_versions(&db, &setup.program_run_id).await?;
    let rotated_slots = rotated_normal_slots(&initial_bindings, &final_bindings);

    let score_judge_process_id = *final_bindings
        .get("__score_judge__")
        .ok_or_else(|| anyhow!("missing __score_judge__ binding"))?;
    let planner_process_id = *final_bindings
        .get("planner")
        .ok_or_else(|| anyhow!("missing planner binding"))?;
    let writer_process_id = *final_bindings
        .get("writer")
        .ok_or_else(|| anyhow!("missing writer binding"))?;
    let reviewer_process_id = *final_bindings
        .get("reviewer")
        .ok_or_else(|| anyhow!("missing reviewer binding"))?;

    let score_segments = db.list_process_segments(score_judge_process_id).await?;
    let planner_segments = db.list_process_segments(planner_process_id).await?;
    let writer_segments = db.list_process_segments(writer_process_id).await?;
    let reviewer_segments = db.list_process_segments(reviewer_process_id).await?;

    let score_history_has_mean_score = segments_contain(&score_segments, "mean_score:");
    let score_history_has_runtime_head_refusal =
        segments_contain(&score_segments, "runtime head") || segments_contain(&score_segments, "I cannot do this.");
    let normal_history_has_reprompt_blocks =
        segments_contain(&planner_segments, "@@process ")
            || segments_contain(&writer_segments, "@@process ")
            || segments_contain(&reviewer_segments, "@@process ");
    let planner_has_tool_result = segments_contain(&planner_segments, "[tool_executor_result]")
        && segments_contain(&planner_segments, "executed_action_name: spawn_branch_process");
    let writer_has_tool_result = segments_contain(&writer_segments, "[tool_executor_result]");

    let final_program = db.get_program(&setup.program_run_id).await?;
    let summary = json!({
        "stage": stage.name,
        "program_run_id": setup.program_run_id,
        "provider_input_budget": stage.provider_input_budget,
        "compaction_trigger_ratio": stage.compaction_trigger_ratio,
        "provider1_result_seen": provider1_result_seen,
        "ephemeral_clone_result_seen": ephemeral_clone_seen,
        "delivery_probes": delivery_probes,
        "phase": {
            "saw_reprompt_running": phase_observation.saw_reprompt_running,
            "saw_empty_lengths_while_reprompt_running": phase_observation.saw_empty_lengths_while_reprompt_running,
            "final_phase": phase_observation.final_phase,
        },
        "post_reprompt_refill_seen": post_reprompt_refill_seen,
        "rotated_normal_slots": rotated_slots,
        "initial_bindings": stringify_bindings(&initial_bindings),
        "final_bindings": stringify_bindings(&final_bindings),
        "initial_prompt_versions": initial_prompt_versions,
        "final_prompt_versions": final_prompt_versions,
        "final_process_context_lengths": final_program.plan_state_json.process_context_lengths,
        "history_checks": {
            "score_judge_history_has_mean_score": score_history_has_mean_score,
            "score_judge_history_has_runtime_head_refusal": score_history_has_runtime_head_refusal,
            "normal_history_has_reprompt_blocks": normal_history_has_reprompt_blocks,
            "planner_has_tool_result": planner_has_tool_result,
            "writer_has_tool_result": writer_has_tool_result,
        }
    });

    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn mirror_provider2_env() {
    mirror_env("OPENAI_API_KEY", "FORM_ZERO_PROVIDER_API_KEY");
    mirror_env("FORM_ZERO_PROVIDER_API_KEY", "FORM_ZERO_PROVIDER2_API_KEY");
    mirror_env("FORM_ZERO_PROVIDER_BASE_URL", "FORM_ZERO_PROVIDER2_BASE_URL");
    mirror_env("FORM_ZERO_PROVIDER_MODEL", "FORM_ZERO_PROVIDER2_MODEL");
    mirror_env(
        "FORM_ZERO_PROVIDER_TEMPERATURE",
        "FORM_ZERO_PROVIDER2_TEMPERATURE",
    );
}

fn mirror_env(src: &str, dst: &str) {
    if env::var_os(dst).is_none() {
        if let Some(value) = env::var_os(src) {
            env::set_var(dst, value);
        }
    }
}

async fn setup_program(db: &Database, stage_name: &str) -> Result<StageSetup> {
    let slot_defs = vec![
        ("planner", "planner runtime", "planner suffix"),
        ("writer", "writer runtime", "writer suffix"),
        ("reviewer", "reviewer runtime", "reviewer suffix"),
        ("__optimizer__", "optimizer runtime", "optimizer suffix"),
        ("__score_judge__", "score runtime", "score suffix"),
    ];

    let mut prefix_ids = BTreeMap::new();
    for (slot_name, prefix_text, suffix_text) in slot_defs {
        let definite = db
            .create_prefix_suffix_definite(
                &format!("{stage_name}_{slot_name}_prefix"),
                &format!("{stage_name}_{slot_name}_suffix"),
                None,
            )
            .await?;
        db.insert_prefix_suffix_segment(
            definite.id,
            DefinitionPart::Prefix,
            "system",
            prefix_text,
            None,
            Some("whitespace"),
            None,
        )
        .await?;
        db.insert_prefix_suffix_segment(
            definite.id,
            DefinitionPart::Suffix,
            "assistant",
            suffix_text,
            None,
            Some("whitespace"),
            None,
        )
        .await?;
        db.compile_prefix_suffix_definite(definite.id).await?;
        prefix_ids.insert(slot_name.to_string(), definite.id);
    }

    let workflow = db
        .create_workflow_definite(
            &format!("{}_workflow", stage_name),
            json!({
                "template_kind": "live_api_replay",
                "slots": [
                    {
                        "slot_name": "planner",
                        "prefix_suffix_definite_id": prefix_ids["planner"].to_string(),
                        "upstream_group": "planning",
                        "downstream_group": "writing"
                    },
                    {
                        "slot_name": "writer",
                        "prefix_suffix_definite_id": prefix_ids["writer"].to_string(),
                        "upstream_group": "writing",
                        "downstream_group": "review"
                    },
                    {
                        "slot_name": "reviewer",
                        "prefix_suffix_definite_id": prefix_ids["reviewer"].to_string(),
                        "upstream_group": "review",
                        "downstream_group": "planning"
                    },
                    {
                        "slot_name": "__optimizer__",
                        "prefix_suffix_definite_id": prefix_ids["__optimizer__"].to_string(),
                        "upstream_group": "__optimizer__",
                        "downstream_group": "__optimizer__"
                    },
                    {
                        "slot_name": "__score_judge__",
                        "prefix_suffix_definite_id": prefix_ids["__score_judge__"].to_string(),
                        "upstream_group": "__score_judge__",
                        "downstream_group": "__score_judge__"
                    }
                ]
            }),
            None,
        )
        .await?;

    let program_run_id = format!("{}_{}", stage_name, &uuid::Uuid::new_v4().simple().to_string()[..8]);
    db.create_program(
        &program_run_id,
        workflow.id,
        &HashMap::new(),
        "running",
        None,
    )
    .await?;

    let bound = db.list_process_instances_for_program_run(&program_run_id).await?;
    let mut slot_processes = BTreeMap::new();
    for row in &bound {
        slot_processes.insert(row.binding.program_slot_name.clone(), row.process.id);
    }

    for (slot, prompt) in [
        (
            "planner",
            "planner prompt: keep coordination messages short and concrete.",
        ),
        (
            "writer",
            "writer prompt: respond with compact execution-oriented text.",
        ),
        (
            "reviewer",
            "reviewer prompt: be strict, concise, and cooperative with temporary tasks.",
        ),
    ] {
        let process_id = *slot_processes
            .get(slot)
            .ok_or_else(|| anyhow!("missing bound process for slot `{slot}`"))?;
        db.rewrite_process_prompt(process_id, prompt).await?;
    }

    db.rebuild_program_process_context_lengths(&program_run_id).await?;

    Ok(StageSetup {
        program_run_id,
        slot_processes,
    })
}

async fn run_persistent_branch_task(
    runtime: &RuntimeEngine,
    setup: &StageSetup,
) -> Result<bool> {
    runtime
        .send_message(SendMessageRequest {
            sender: slot_target(&setup.program_run_id, "planner"),
            target: slot_target(&setup.program_run_id, "writer"),
            message_kind: MessageKind::TaskMessage,
            base_priority: 4,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_target: None,
            content: None,
            content_ref: None,
            task_sequence: vec![TaskStep {
                action_name: "spawn_branch_process".to_string(),
                action_args: json!({
                    "task": "Generate one short branch note for the current runtime context.",
                    "materialize_to_host": true
                }),
            }],
            hardware_sequence: Vec::new(),
            metadata: None,
        })
        .await?;

    flush_planner_mailbox(runtime, &setup.program_run_id, "persistent branch flush").await?;

    wait_for_runtime_head_text(
        runtime,
        *setup
            .slot_processes
            .get("planner")
            .ok_or_else(|| anyhow!("missing planner process"))?,
        "executed_action_name: spawn_branch_process",
        Duration::from_secs(8),
    )
    .await
}

async fn run_ephemeral_clone_task(
    runtime: &RuntimeEngine,
    setup: &StageSetup,
) -> Result<bool> {
    runtime
        .send_message(SendMessageRequest {
            sender: slot_target(&setup.program_run_id, "planner"),
            target: slot_target(&setup.program_run_id, "writer"),
            message_kind: MessageKind::TaskMessage,
            base_priority: 4,
            requested_delivery_action: DeliveryAction::EphemeralAsyncClone,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_target: None,
            content: None,
            content_ref: None,
            task_sequence: vec![TaskStep {
                action_name: "spawn_branch_process".to_string(),
                action_args: json!({
                    "task": "Generate one short branch note for the current runtime context.",
                    "materialize_to_host": false
                }),
            }],
            hardware_sequence: Vec::new(),
            metadata: None,
        })
        .await?;

    flush_planner_mailbox(runtime, &setup.program_run_id, "ephemeral clone flush").await?;

    let planner_process_id = *setup
        .slot_processes
        .get("planner")
        .ok_or_else(|| anyhow!("missing planner process"))?;
    let writer_process_id = *setup
        .slot_processes
        .get("writer")
        .ok_or_else(|| anyhow!("missing writer process"))?;

    let seen = wait_for_runtime_head_text(
        runtime,
        planner_process_id,
        "executed_action_name: spawn_branch_process",
        Duration::from_secs(8),
    )
    .await?;
    if !seen {
        return Ok(false);
    }

    let writer_head = runtime.load_runtime_head(writer_process_id).await?;
    Ok(!runtime_head_contains(&writer_head, "[tool_executor_result]"))
}

async fn flush_planner_mailbox(runtime: &RuntimeEngine, program_run_id: &str, content: &str) -> Result<()> {
    runtime
        .send_message(SendMessageRequest {
            sender: slot_target(program_run_id, "reviewer"),
            target: slot_target(program_run_id, "planner"),
            message_kind: MessageKind::NormalMessage,
            base_priority: 4,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_target: None,
            content: Some(content.to_string()),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: None,
        })
        .await?;
    Ok(())
}

async fn send_non_direct_probe(
    runtime: &RuntimeEngine,
    program_run_id: &str,
    stage_name: &str,
    index: usize,
    repeat: usize,
) -> Result<form_zero::QueueEnvelope> {
    runtime
        .send_message(SendMessageRequest {
            sender: slot_target(program_run_id, "planner"),
            target: slot_target(program_run_id, "reviewer"),
            message_kind: MessageKind::NormalMessage,
            base_priority: 4,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_target: None,
            content: Some(long_probe_message(stage_name, index, repeat)),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: None,
        })
        .await
}

fn long_probe_message(stage_name: &str, index: usize, repeat: usize) -> String {
    let mut parts = Vec::with_capacity(repeat + 1);
    parts.push(format!(
        "{stage_name} delivery judge probe {index}: review a long coordination transcript."
    ));
    for chunk in 0..repeat {
        parts.push(format!(
            "chunk {chunk}: planner hands off a dense status packet with requirements, risks, pending blockers, resolution notes, and follow-up checkpoints."
        ));
    }
    parts.join(" ")
}

async fn wait_for_phase_cycle(
    db: &Database,
    program_run_id: &str,
    mut saw_reprompt_running: bool,
) -> Result<PhaseObservation> {
    let start = Instant::now();
    let mut saw_empty_lengths_while_reprompt_running = false;
    let mut last_phase = String::new();

    loop {
        let program = db.get_program(program_run_id).await?;
        let phase = program.plan_state_json.global_phase.clone();
        if phase != last_phase {
            println!("{program_run_id} phase -> {phase}");
            last_phase = phase.clone();
        }

        if phase == "reprompt_running" {
            saw_reprompt_running = true;
            if program.plan_state_json.process_context_lengths.is_empty() {
                saw_empty_lengths_while_reprompt_running = true;
            }
        }

        if saw_reprompt_running && phase == "running" {
            return Ok(PhaseObservation {
                saw_reprompt_running,
                saw_empty_lengths_while_reprompt_running,
                final_phase: phase,
            });
        }

        if start.elapsed() > PHASE_TIMEOUT {
            return Ok(PhaseObservation {
                saw_reprompt_running,
                saw_empty_lengths_while_reprompt_running,
                final_phase: phase,
            });
        }

        sleep(POLL_INTERVAL).await;
    }
}

async fn refill_lengths_after_reprompt(
    db: &Database,
    runtime: &RuntimeEngine,
    setup: &StageSetup,
) -> Result<bool> {
    runtime
        .send_message(SendMessageRequest {
            sender: slot_target(&setup.program_run_id, "planner"),
            target: slot_target(&setup.program_run_id, "writer"),
            message_kind: MessageKind::NormalMessage,
            base_priority: 4,
            requested_delivery_action: DeliveryAction::SegmentBoundaryDeliver,
            delay_ms: None,
            result_target: ResultTarget::Sender,
            explicit_result_target: None,
            content: Some("post reprompt refill ping".to_string()),
            content_ref: None,
            task_sequence: Vec::new(),
            hardware_sequence: Vec::new(),
            metadata: None,
        })
        .await?;

    let start = Instant::now();
    loop {
        let program = db.get_program(&setup.program_run_id).await?;
        if !program.plan_state_json.process_context_lengths.is_empty() {
            return Ok(true);
        }
        if start.elapsed() > Duration::from_secs(30) {
            return Ok(false);
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn current_bindings(
    db: &Database,
    program_run_id: &str,
) -> Result<BTreeMap<String, uuid::Uuid>> {
    let mut map = BTreeMap::new();
    for row in db.list_process_instances_for_program_run(program_run_id).await? {
        map.insert(row.binding.program_slot_name.clone(), row.process.id);
    }
    Ok(map)
}

async fn current_prompt_versions(
    db: &Database,
    program_run_id: &str,
) -> Result<BTreeMap<String, Value>> {
    let mut map = BTreeMap::new();
    for (binding, prompt) in db.list_process_prompts_for_program_run(program_run_id).await? {
        map.insert(
            binding.program_slot_name.clone(),
            json!({
                "process_id": prompt.process_id.to_string(),
                "prompt_version": prompt.prompt_version,
                "prompt_text": prompt.prompt_text,
            }),
        );
    }
    Ok(map)
}

fn rotated_normal_slots(
    initial_bindings: &BTreeMap<String, uuid::Uuid>,
    final_bindings: &BTreeMap<String, uuid::Uuid>,
) -> Vec<String> {
    ["planner", "writer", "reviewer"]
        .into_iter()
        .filter(|slot| initial_bindings.get(*slot) != final_bindings.get(*slot))
        .map(ToOwned::to_owned)
        .collect()
}

fn stringify_bindings(bindings: &BTreeMap<String, uuid::Uuid>) -> BTreeMap<String, String> {
    bindings
        .iter()
        .map(|(slot, process_id)| (slot.clone(), process_id.to_string()))
        .collect()
}

async fn wait_for_runtime_head_text(
    runtime: &RuntimeEngine,
    process_id: uuid::Uuid,
    needle: &str,
    timeout: Duration,
) -> Result<bool> {
    let start = Instant::now();
    loop {
        let head = runtime.load_runtime_head(process_id).await?;
        if runtime_head_contains(&head, needle) {
            return Ok(true);
        }
        if start.elapsed() > timeout {
            return Ok(false);
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn segments_contain(segments: &[form_zero::SegmentRow], needle: &str) -> bool {
    segments.iter().any(|segment| segment.content.contains(needle))
}

fn runtime_head_contains(head: &form_zero::RuntimeHead, needle: &str) -> bool {
    head.full_context
        .iter()
        .any(|message| message.content.contains(needle))
}

fn slot_target(program_run_id: &str, slot_name: &str) -> TargetSelector {
    TargetSelector::ProgramSlot {
        program_run_id: program_run_id.to_string(),
        program_slot_name: slot_name.to_string(),
    }
}
