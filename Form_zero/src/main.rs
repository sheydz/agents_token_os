use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use form_zero::{Database, DefinitionPart};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "form_zero",
    about = "Form_zero rewrite: send_message + remote act over workflow_definite/prefix_suffix_definite"
)]
struct Cli {
    #[arg(long, env = "FORM_ZERO_DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    InitSchema,
    CreateWorkflowDefinite {
        #[arg(long)]
        workflow_name: String,
        #[arg(long)]
        workflow_json: String,
        #[arg(long)]
        metadata_json: Option<String>,
    },
    ShowWorkflowDefinite {
        #[arg(long)]
        workflow_definite_id: Uuid,
    },
    CreatePrefixSuffixDefinite {
        #[arg(long)]
        prefix_name: String,
        #[arg(long)]
        suffix_name: String,
        #[arg(long)]
        metadata_json: Option<String>,
    },
    ShowPrefixSuffixDefinite {
        #[arg(long)]
        prefix_suffix_definite_id: Uuid,
    },
    AddPrefixSuffixSegment {
        #[arg(long)]
        prefix_suffix_definite_id: Uuid,
        #[arg(long, value_enum)]
        part: DefinitionPartArg,
        #[arg(long)]
        segment_kind: String,
        #[arg(long)]
        content: String,
        #[arg(long)]
        token_count: Option<i32>,
        #[arg(long)]
        tokenizer: Option<String>,
        #[arg(long)]
        patch_json: Option<String>,
    },
    CompilePrefixSuffixDefinite {
        #[arg(long)]
        prefix_suffix_definite_id: Uuid,
    },
    SpawnFreeProcess {
        #[arg(long)]
        prefix_suffix_definite_id: Uuid,
        #[arg(long, default_value = "idle")]
        status: String,
    },
    CreateProgram {
        #[arg(long)]
        workflow_definite_id: Uuid,
        #[arg(long)]
        program_run_id: String,
        #[arg(long)]
        bindings_json: String,
        #[arg(long, default_value = "running")]
        status: String,
        #[arg(long)]
        metadata_json: Option<String>,
    },
    ShowProgram {
        #[arg(long)]
        program_run_id: String,
    },
    ShowProcess {
        #[arg(long)]
        process_id: Uuid,
    },
    AppendProcessSegment {
        #[arg(long)]
        process_id: Uuid,
        #[arg(long)]
        segment_kind: String,
        #[arg(long)]
        content: String,
        #[arg(long)]
        token_count: Option<i32>,
        #[arg(long)]
        tokenizer: Option<String>,
        #[arg(long)]
        patch_json: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DefinitionPartArg {
    Prefix,
    Suffix,
}

impl From<DefinitionPartArg> for DefinitionPart {
    fn from(value: DefinitionPartArg) -> Self {
        match value {
            DefinitionPartArg::Prefix => DefinitionPart::Prefix,
            DefinitionPartArg::Suffix => DefinitionPart::Suffix,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let db = Database::connect(&cli.database_url)
        .await
        .context("unable to connect Form_zero CLI to Postgres")?;

    match cli.command {
        Command::InitSchema => {
            db.init_schema().await?;
            print_json(&json!({ "ok": true, "schema": "rewritten" }))?;
        }
        Command::CreateWorkflowDefinite {
            workflow_name,
            workflow_json,
            metadata_json,
        } => {
            let workflow_json = parse_json(Some(workflow_json), Value::Object(Default::default()))?;
            let metadata = parse_json_optional(metadata_json)?;
            let row = db
                .create_workflow_definite(&workflow_name, workflow_json, metadata)
                .await?;
            print_json(&row)?;
        }
        Command::ShowWorkflowDefinite {
            workflow_definite_id,
        } => {
            print_json(&db.get_workflow_definite(workflow_definite_id).await?)?;
        }
        Command::CreatePrefixSuffixDefinite {
            prefix_name,
            suffix_name,
            metadata_json,
        } => {
            let metadata = parse_json_optional(metadata_json)?;
            let row = db
                .create_prefix_suffix_definite(&prefix_name, &suffix_name, metadata)
                .await?;
            print_json(&row)?;
        }
        Command::ShowPrefixSuffixDefinite {
            prefix_suffix_definite_id,
        } => {
            let definite = db
                .get_prefix_suffix_definite(prefix_suffix_definite_id)
                .await?;
            let (prefix_segments, suffix_segments, built_all) = db
                .list_prefix_suffix_segments(prefix_suffix_definite_id)
                .await?;
            print_json(&json!({
                "prefix_suffix_definite": definite,
                "prefix_segments": prefix_segments,
                "suffix_segments": suffix_segments,
                "built_all": built_all,
            }))?;
        }
        Command::AddPrefixSuffixSegment {
            prefix_suffix_definite_id,
            part,
            segment_kind,
            content,
            token_count,
            tokenizer,
            patch_json,
        } => {
            let patch = parse_json_optional(patch_json)?;
            let row = db
                .insert_prefix_suffix_segment(
                    prefix_suffix_definite_id,
                    part.into(),
                    &segment_kind,
                    &content,
                    token_count,
                    tokenizer.as_deref(),
                    patch,
                )
                .await?;
            print_json(&row)?;
        }
        Command::CompilePrefixSuffixDefinite {
            prefix_suffix_definite_id,
        } => {
            print_json(
                &db.compile_prefix_suffix_definite(prefix_suffix_definite_id)
                    .await?,
            )?;
        }
        Command::SpawnFreeProcess {
            prefix_suffix_definite_id,
            status,
        } => {
            print_json(
                &db.spawn_free_process(prefix_suffix_definite_id, &status)
                    .await?,
            )?;
        }
        Command::CreateProgram {
            workflow_definite_id,
            program_run_id,
            bindings_json,
            status,
            metadata_json,
        } => {
            let bindings_value =
                parse_json(Some(bindings_json), Value::Object(Default::default()))?;
            let bindings_object = bindings_value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("bindings_json must be a JSON object"))?;
            let mut bindings = HashMap::new();
            for (slot, raw_process_id) in bindings_object {
                let process_id = raw_process_id
                    .as_str()
                    .and_then(|raw| Uuid::parse_str(raw).ok())
                    .ok_or_else(|| {
                        anyhow::anyhow!("binding for slot `{slot}` must be a UUID string")
                    })?;
                bindings.insert(slot.clone(), process_id);
            }
            let metadata = parse_json_optional(metadata_json)?;
            let program = db
                .create_program(
                    &program_run_id,
                    workflow_definite_id,
                    &bindings,
                    &status,
                    metadata,
                )
                .await?;
            let bound_processes = db
                .list_process_instances_for_program_run(&program_run_id)
                .await?;
            print_json(&json!({
                "program": program,
                "bound_processes": bound_processes,
            }))?;
        }
        Command::ShowProgram { program_run_id } => {
            let program = db.get_program(&program_run_id).await?;
            let bound_processes = db
                .list_process_instances_for_program_run(&program_run_id)
                .await?;
            print_json(&json!({
                "program": program,
                "bound_processes": bound_processes,
            }))?;
        }
        Command::ShowProcess { process_id } => {
            let process = db.get_process_instance(process_id).await?;
            let binding = db.get_process_binding(process_id).await?;
            print_json(&json!({
                "process": process,
                "binding": binding,
            }))?;
        }
        Command::AppendProcessSegment {
            process_id,
            segment_kind,
            content,
            token_count,
            tokenizer,
            patch_json,
        } => {
            let patch = parse_json_optional(patch_json)?;
            let row = db
                .append_process_segment(
                    process_id,
                    &segment_kind,
                    &content,
                    token_count,
                    tokenizer.as_deref(),
                    patch,
                )
                .await?;
            print_json(&row)?;
        }
    }

    Ok(())
}

fn parse_json(raw: Option<String>, fallback: Value) -> Result<Value> {
    match raw {
        Some(raw) => Ok(serde_json::from_str::<Value>(&raw)
            .with_context(|| format!("invalid JSON value: {raw}"))?),
        None => Ok(fallback),
    }
}

fn parse_json_optional(raw: Option<String>) -> Result<Option<Value>> {
    match raw {
        Some(raw) => Ok(Some(
            serde_json::from_str::<Value>(&raw)
                .with_context(|| format!("invalid JSON value: {raw}"))?,
        )),
        None => Ok(None),
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
