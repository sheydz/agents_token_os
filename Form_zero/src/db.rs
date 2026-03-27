use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

use crate::models::{
    approximate_token_count, assemble_runtime_text, compensation_records_from_metadata,
    compile_version_from_patch, concatenate_segment_contents, default_program_plan_state,
    merge_patch, metadata_with_compensation_records, runtime_state_from_patch, with_runtime_state,
    workflow_seed_slots, BoundProcessRow, CompiledPrefixSuffixDefinite, DefinitionPart, OwnerKind,
    PersistenceCompensationRecord, PrefixSuffixDefiniteRow, ProcessInstanceRow,
    ProcessRuntimeBinding, ProgramPlanState, ProgramProcessBindingRow, ProgramRow, RuntimeHead,
    SegmentRow, WorkflowDefiniteRow,
};

#[derive(Clone)]
pub struct Database {
    client: Arc<Mutex<Client>>,
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .context("failed to connect to Postgres")?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("form_zero postgres connection error: {error}");
            }
        });

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    pub async fn init_schema(&self) -> Result<()> {
        let sql = r#"
        DROP TABLE IF EXISTS program_process_bindings CASCADE;
        DROP TABLE IF EXISTS segment CASCADE;
        DROP TABLE IF EXISTS process_instances CASCADE;
        DROP TABLE IF EXISTS programs CASCADE;
        DROP TABLE IF EXISTS workflow_definite CASCADE;
        DROP TABLE IF EXISTS prefix_suffix_definite CASCADE;
        DROP TABLE IF EXISTS workflow_prefix_prefix CASCADE;

        CREATE TABLE workflow_definite (
            id UUID PRIMARY KEY,
            workflow_name TEXT NOT NULL,
            workflow_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            version BIGINT NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'active',
            metadata_json JSONB NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE prefix_suffix_definite (
            id UUID PRIMARY KEY,
            prefix_name TEXT NOT NULL,
            suffix_name TEXT NOT NULL,
            compile_version BIGINT NOT NULL DEFAULT 0,
            compile_status TEXT NOT NULL DEFAULT 'pending',
            metadata_json JSONB NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE programs (
            program_run_id TEXT PRIMARY KEY,
            workflow_definite_id UUID NOT NULL REFERENCES workflow_definite(id) ON DELETE RESTRICT,
            status TEXT NOT NULL DEFAULT 'running',
            plan_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata_json JSONB NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE process_instances (
            id UUID PRIMARY KEY,
            external_slot_name TEXT NOT NULL UNIQUE,
            prefix_suffix_definite_id UUID NOT NULL REFERENCES prefix_suffix_definite(id) ON DELETE RESTRICT,
            status TEXT NOT NULL,
            policy_json JSONB NULL,
            last_segment_seq BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE program_process_bindings (
            program_run_id TEXT NOT NULL REFERENCES programs(program_run_id) ON DELETE CASCADE,
            program_slot_name TEXT NOT NULL,
            process_id UUID NOT NULL REFERENCES process_instances(id) ON DELETE CASCADE,
            attached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata_json JSONB NULL,
            PRIMARY KEY (program_run_id, program_slot_name),
            UNIQUE (process_id)
        );

        CREATE TABLE segment (
            id UUID PRIMARY KEY,
            owner_kind TEXT NOT NULL CHECK (owner_kind IN ('prefix_suffix_definite', 'process')),
            owner_id UUID NOT NULL,
            owner_seq BIGINT NOT NULL,
            definition_part TEXT NULL CHECK (
                definition_part IS NULL OR definition_part IN ('prefix', 'suffix', 'built_all')
            ),
            segment_kind TEXT NOT NULL,
            content TEXT NOT NULL,
            token_count INTEGER NOT NULL DEFAULT 0 CHECK (token_count >= 0),
            tokenizer TEXT NULL,
            patch JSONB NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (
                (owner_kind = 'prefix_suffix_definite' AND definition_part IS NOT NULL)
                OR
                (owner_kind = 'process' AND definition_part IS NULL)
            )
        );

        CREATE INDEX idx_segment_owner ON segment(owner_kind, owner_id);
        CREATE UNIQUE INDEX idx_segment_prefix_suffix_part_seq
            ON segment(owner_id, definition_part, owner_seq)
            WHERE owner_kind = 'prefix_suffix_definite' AND definition_part IS NOT NULL;
        CREATE UNIQUE INDEX idx_segment_process_seq
            ON segment(owner_id, owner_seq)
            WHERE owner_kind = 'process';
        CREATE UNIQUE INDEX idx_segment_prefix_suffix_built_all
            ON segment(owner_id)
            WHERE owner_kind = 'prefix_suffix_definite' AND definition_part = 'built_all';
        CREATE INDEX idx_program_process_bindings_program ON program_process_bindings(program_run_id);
        "#;

        let client = self.client.lock().await;
        client
            .batch_execute(sql)
            .await
            .context("failed to initialize Form_zero schema")?;
        Ok(())
    }

    pub async fn create_workflow_definite(
        &self,
        workflow_name: &str,
        workflow_json: Value,
        metadata_json: Option<Value>,
    ) -> Result<WorkflowDefiniteRow> {
        let id = Uuid::new_v4();
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    INSERT INTO workflow_definite (
                        id,
                        workflow_name,
                        workflow_json,
                        version,
                        status,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, 1, 'active', $4)
                    RETURNING
                        id,
                        workflow_name,
                        workflow_json,
                        version,
                        status,
                        metadata_json,
                        created_at,
                        updated_at
                    "#,
                    &[&id, &workflow_name, &workflow_json, &metadata_json],
                )
                .await
                .context("failed to create workflow_definite row")?
        };

        map_workflow_definite_row(&row)
    }

    pub async fn get_workflow_definite(
        &self,
        workflow_definite_id: Uuid,
    ) -> Result<WorkflowDefiniteRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        workflow_name,
                        workflow_json,
                        version,
                        status,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM workflow_definite
                    WHERE id = $1
                    "#,
                    &[&workflow_definite_id],
                )
                .await
                .context("failed to fetch workflow_definite row")?
        }
        .ok_or_else(|| anyhow!("workflow_definite {workflow_definite_id} not found"))?;

        map_workflow_definite_row(&row)
    }

    pub async fn list_workflow_definite(&self) -> Result<Vec<WorkflowDefiniteRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        workflow_name,
                        workflow_json,
                        version,
                        status,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM workflow_definite
                    ORDER BY workflow_name, id
                    "#,
                    &[],
                )
                .await
                .context("failed to list workflow_definite rows")?
        };

        rows.iter().map(map_workflow_definite_row).collect()
    }

    pub async fn create_prefix_suffix_definite(
        &self,
        prefix_name: &str,
        suffix_name: &str,
        metadata_json: Option<Value>,
    ) -> Result<PrefixSuffixDefiniteRow> {
        let id = Uuid::new_v4();
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    INSERT INTO prefix_suffix_definite (
                        id,
                        prefix_name,
                        suffix_name,
                        compile_version,
                        compile_status,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, 0, 'pending', $4)
                    RETURNING
                        id,
                        prefix_name,
                        suffix_name,
                        compile_version,
                        compile_status,
                        metadata_json,
                        created_at,
                        updated_at
                    "#,
                    &[&id, &prefix_name, &suffix_name, &metadata_json],
                )
                .await
                .context("failed to create prefix_suffix_definite row")?
        };

        map_prefix_suffix_definite_row(&row)
    }

    pub async fn get_prefix_suffix_definite(
        &self,
        prefix_suffix_definite_id: Uuid,
    ) -> Result<PrefixSuffixDefiniteRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        prefix_name,
                        suffix_name,
                        compile_version,
                        compile_status,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM prefix_suffix_definite
                    WHERE id = $1
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to fetch prefix_suffix_definite row")?
        }
        .ok_or_else(|| anyhow!("prefix_suffix_definite {prefix_suffix_definite_id} not found"))?;

        map_prefix_suffix_definite_row(&row)
    }

    pub async fn list_prefix_suffix_definite(&self) -> Result<Vec<PrefixSuffixDefiniteRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        prefix_name,
                        suffix_name,
                        compile_version,
                        compile_status,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM prefix_suffix_definite
                    ORDER BY prefix_name, suffix_name, id
                    "#,
                    &[],
                )
                .await
                .context("failed to list prefix_suffix_definite rows")?
        };

        rows.iter().map(map_prefix_suffix_definite_row).collect()
    }

    pub async fn list_prefix_suffix_segments(
        &self,
        prefix_suffix_definite_id: Uuid,
    ) -> Result<(Vec<SegmentRow>, Vec<SegmentRow>, Option<SegmentRow>)> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'prefix_suffix_definite' AND owner_id = $1
                    ORDER BY
                        CASE definition_part
                            WHEN 'prefix' THEN 1
                            WHEN 'suffix' THEN 2
                            WHEN 'built_all' THEN 3
                            ELSE 4
                        END,
                        owner_seq
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to list prefix_suffix_definite-owned segments")?
        };

        let mut prefix_segments = Vec::new();
        let mut suffix_segments = Vec::new();
        let mut built_all = None;

        for row in rows {
            let segment = map_segment_row(&row)?;
            match segment.definition_part {
                Some(DefinitionPart::Prefix) => prefix_segments.push(segment),
                Some(DefinitionPart::Suffix) => suffix_segments.push(segment),
                Some(DefinitionPart::BuiltAll) => built_all = Some(segment),
                None => {}
            }
        }

        Ok((prefix_segments, suffix_segments, built_all))
    }

    pub async fn insert_prefix_suffix_segment(
        &self,
        prefix_suffix_definite_id: Uuid,
        definition_part: DefinitionPart,
        segment_kind: &str,
        content: &str,
        token_count: Option<i32>,
        tokenizer: Option<&str>,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        if definition_part == DefinitionPart::BuiltAll {
            bail!("built_all is reserved for compile_prefix_suffix_definite");
        }

        let row = {
            let mut client = self.client.lock().await;
            let tx = client
                .transaction()
                .await
                .context("failed to start insert_prefix_suffix_segment transaction")?;

            let exists = tx
                .query_opt(
                    "SELECT 1 FROM prefix_suffix_definite WHERE id = $1",
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to verify prefix_suffix_definite before inserting segment")?;
            if exists.is_none() {
                bail!("prefix_suffix_definite {prefix_suffix_definite_id} not found");
            }

            let next_seq: i64 = tx
                .query_one(
                    r#"
                    SELECT COALESCE(MAX(owner_seq), 0) + 1
                    FROM segment
                    WHERE owner_kind = 'prefix_suffix_definite'
                      AND owner_id = $1
                      AND definition_part = $2
                    "#,
                    &[&prefix_suffix_definite_id, &definition_part.as_str()],
                )
                .await
                .context("failed to compute next prefix_suffix_definite segment sequence")?
                .get(0);

            let segment_id = Uuid::new_v4();
            let token_count = token_count.unwrap_or_else(|| approximate_token_count(content));
            let row = tx
                .query_one(
                    r#"
                    INSERT INTO segment (
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch
                    )
                    VALUES ($1, 'prefix_suffix_definite', $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    "#,
                    &[
                        &segment_id,
                        &prefix_suffix_definite_id,
                        &next_seq,
                        &definition_part.as_str(),
                        &segment_kind,
                        &content,
                        &token_count,
                        &tokenizer,
                        &patch,
                    ],
                )
                .await
                .context("failed to insert prefix_suffix_definite-owned segment")?;

            tx.execute(
                r#"
                UPDATE prefix_suffix_definite
                SET compile_status = 'dirty', updated_at = NOW()
                WHERE id = $1
                "#,
                &[&prefix_suffix_definite_id],
            )
            .await
            .context("failed to mark prefix_suffix_definite as dirty")?;

            tx.commit()
                .await
                .context("failed to commit insert_prefix_suffix_segment transaction")?;

            row
        };

        map_segment_row(&row)
    }

    pub async fn compile_prefix_suffix_definite(
        &self,
        prefix_suffix_definite_id: Uuid,
    ) -> Result<CompiledPrefixSuffixDefinite> {
        let compiled = {
            let mut client = self.client.lock().await;
            let tx = client
                .transaction()
                .await
                .context("failed to start compile_prefix_suffix_definite transaction")?;

            let definite_row = tx
                .query_opt(
                    r#"
                    SELECT
                        id,
                        prefix_name,
                        suffix_name,
                        compile_version,
                        compile_status,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM prefix_suffix_definite
                    WHERE id = $1
                    FOR UPDATE
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to fetch prefix_suffix_definite for compile")?
                .ok_or_else(|| {
                    anyhow!("prefix_suffix_definite {prefix_suffix_definite_id} not found")
                })?;

            let definite = map_prefix_suffix_definite_row(&definite_row)?;

            let prefix_rows = tx
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'prefix_suffix_definite'
                      AND owner_id = $1
                      AND definition_part = 'prefix'
                    ORDER BY owner_seq
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to fetch prefix segments for compilation")?;
            let suffix_rows = tx
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'prefix_suffix_definite'
                      AND owner_id = $1
                      AND definition_part = 'suffix'
                    ORDER BY owner_seq
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to fetch suffix segments for compilation")?;

            let prefix_segments: Vec<SegmentRow> = prefix_rows
                .iter()
                .map(map_segment_row)
                .collect::<Result<Vec<_>>>()?;
            let suffix_segments: Vec<SegmentRow> = suffix_rows
                .iter()
                .map(map_segment_row)
                .collect::<Result<Vec<_>>>()?;

            let mut source_segments = prefix_segments.clone();
            source_segments.extend(suffix_segments.clone());
            let built_all_text = concatenate_segment_contents(&source_segments);
            let next_compile_version = definite.compile_version + 1;
            let built_all_patch = json!({
                "compile_version": next_compile_version,
                "source": "compile_prefix_suffix_definite"
            });
            let built_all_token_count = approximate_token_count(&built_all_text);

            tx.execute(
                r#"
                DELETE FROM segment
                WHERE owner_kind = 'prefix_suffix_definite'
                  AND owner_id = $1
                  AND definition_part = 'built_all'
                "#,
                &[&prefix_suffix_definite_id],
            )
            .await
            .context("failed to clear previous built_all segment")?;

            tx.execute(
                r#"
                INSERT INTO segment (
                    id,
                    owner_kind,
                    owner_id,
                    owner_seq,
                    definition_part,
                    segment_kind,
                    content,
                    token_count,
                    tokenizer,
                    patch
                )
                VALUES ($1, 'prefix_suffix_definite', $2, 1, 'built_all', 'compiled_prompt', $3, $4, 'whitespace', $5)
                "#,
                &[
                    &Uuid::new_v4(),
                    &prefix_suffix_definite_id,
                    &built_all_text,
                    &built_all_token_count,
                    &built_all_patch,
                ],
            )
            .await
            .context("failed to insert built_all segment")?;

            tx.execute(
                r#"
                UPDATE prefix_suffix_definite
                SET
                    compile_version = $2,
                    compile_status = 'ready',
                    updated_at = NOW()
                WHERE id = $1
                "#,
                &[&prefix_suffix_definite_id, &next_compile_version],
            )
            .await
            .context("failed to update prefix_suffix_definite compile metadata")?;

            tx.commit()
                .await
                .context("failed to commit compile_prefix_suffix_definite transaction")?;

            CompiledPrefixSuffixDefinite {
                prefix_suffix_definite_id,
                compile_version: next_compile_version,
                built_all_text,
                prefix_segments,
                suffix_segments,
            }
        };

        Ok(compiled)
    }

    pub async fn spawn_free_process(
        &self,
        prefix_suffix_definite_id: Uuid,
        status: &str,
    ) -> Result<ProcessInstanceRow> {
        self.get_prefix_suffix_definite(prefix_suffix_definite_id)
            .await?;
        let process_id = Uuid::new_v4();
        let external_slot_name = self.next_external_slot_name().await?;
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    INSERT INTO process_instances (
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq
                    )
                    VALUES ($1, $2, $3, $4, NULL, 0)
                    RETURNING
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq,
                        created_at,
                        updated_at
                    "#,
                    &[
                        &process_id,
                        &external_slot_name,
                        &prefix_suffix_definite_id,
                        &status,
                    ],
                )
                .await
                .context("failed to insert process_instance")?
        };

        map_process_row(&row)
    }

    pub async fn get_process_instance(&self, process_id: Uuid) -> Result<ProcessInstanceRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq,
                        created_at,
                        updated_at
                    FROM process_instances
                    WHERE id = $1
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to fetch process_instance")?
        }
        .ok_or_else(|| anyhow!("process_instance {process_id} not found"))?;

        map_process_row(&row)
    }

    pub async fn list_process_instances(&self) -> Result<Vec<ProcessInstanceRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq,
                        created_at,
                        updated_at
                    FROM process_instances
                    ORDER BY external_slot_name
                    "#,
                    &[],
                )
                .await
                .context("failed to list process_instances")?
        };

        rows.iter().map(map_process_row).collect()
    }

    pub async fn get_process_runtime_binding(
        &self,
        process_id: Uuid,
    ) -> Result<ProcessRuntimeBinding> {
        Ok(ProcessRuntimeBinding {
            process: self.get_process_instance(process_id).await?,
            binding: self.get_process_binding(process_id).await?,
        })
    }

    pub async fn get_process_binding(
        &self,
        process_id: Uuid,
    ) -> Result<Option<ProgramProcessBindingRow>> {
        let maybe_row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        program_run_id,
                        program_slot_name,
                        process_id,
                        attached_at,
                        metadata_json
                    FROM program_process_bindings
                    WHERE process_id = $1
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to fetch program_process_binding by process_id")?
        };

        maybe_row.as_ref().map(map_binding_row).transpose()
    }

    pub async fn list_program_bindings(
        &self,
        program_run_id: &str,
    ) -> Result<Vec<ProgramProcessBindingRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        program_run_id,
                        program_slot_name,
                        process_id,
                        attached_at,
                        metadata_json
                    FROM program_process_bindings
                    WHERE program_run_id = $1
                    ORDER BY program_slot_name
                    "#,
                    &[&program_run_id],
                )
                .await
                .context("failed to list program_process_bindings")?
        };

        rows.iter().map(map_binding_row).collect()
    }

    pub async fn list_process_instances_for_program_run(
        &self,
        program_run_id: &str,
    ) -> Result<Vec<BoundProcessRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        b.program_run_id,
                        b.program_slot_name,
                        b.process_id,
                        b.attached_at,
                        b.metadata_json,
                        p.id,
                        p.external_slot_name,
                        p.prefix_suffix_definite_id,
                        p.status,
                        p.policy_json,
                        p.last_segment_seq,
                        p.created_at,
                        p.updated_at
                    FROM program_process_bindings b
                    JOIN process_instances p ON p.id = b.process_id
                    WHERE b.program_run_id = $1
                    ORDER BY b.program_slot_name
                    "#,
                    &[&program_run_id],
                )
                .await
                .context("failed to list bound processes for program")?
        };

        rows.iter().map(map_bound_process_row).collect()
    }

    pub async fn resolve_process_by_slot(
        &self,
        program_run_id: &str,
        program_slot_name: &str,
    ) -> Result<Option<ProcessInstanceRow>> {
        let maybe_row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        p.id,
                        p.external_slot_name,
                        p.prefix_suffix_definite_id,
                        p.status,
                        p.policy_json,
                        p.last_segment_seq,
                        p.created_at,
                        p.updated_at
                    FROM program_process_bindings b
                    JOIN process_instances p ON p.id = b.process_id
                    WHERE b.program_run_id = $1 AND b.program_slot_name = $2
                    "#,
                    &[&program_run_id, &program_slot_name],
                )
                .await
                .context("failed to resolve process by slot")?
        };

        maybe_row.as_ref().map(map_process_row).transpose()
    }

    pub async fn create_program(
        &self,
        program_run_id: &str,
        workflow_definite_id: Uuid,
        bindings: &HashMap<String, Uuid>,
        status: &str,
        metadata_json: Option<Value>,
    ) -> Result<ProgramRow> {
        let workflow = self.get_workflow_definite(workflow_definite_id).await?;
        let seed_slots = workflow_seed_slots(&workflow.workflow_json);
        if seed_slots.is_empty() {
            bail!("workflow_definite {workflow_definite_id} has no seed slots");
        }

        let required_slots = seed_slots
            .iter()
            .map(|slot| slot.slot_name.clone())
            .collect::<Vec<_>>();
        for required_slot in &required_slots {
            if !bindings.contains_key(required_slot) {
                bail!("create_program missing required slot binding for {required_slot}");
            }
        }
        for provided_slot in bindings.keys() {
            if !required_slots
                .iter()
                .any(|required| required == provided_slot)
            {
                bail!("create_program received unknown workflow slot {provided_slot}");
            }
        }

        let plan_state = default_program_plan_state(&workflow.workflow_json);
        let encoded_plan_state =
            serde_json::to_value(&plan_state).context("failed to encode plan_state_json")?;

        {
            let mut client = self.client.lock().await;
            let tx = client
                .transaction()
                .await
                .context("failed to start create_program transaction")?;

            let existing = tx
                .query_opt(
                    "SELECT 1 FROM programs WHERE program_run_id = $1",
                    &[&program_run_id],
                )
                .await
                .context("failed to check existing program_run_id")?;
            if existing.is_some() {
                bail!("program {program_run_id} already exists");
            }

            let mut seen_process_ids = std::collections::HashSet::new();
            for process_id in bindings.values() {
                if !seen_process_ids.insert(*process_id) {
                    bail!("create_program cannot bind the same process twice");
                }

                let process_exists = tx
                    .query_opt(
                        "SELECT 1 FROM process_instances WHERE id = $1 FOR UPDATE",
                        &[process_id],
                    )
                    .await
                    .context("failed to verify process before binding")?;
                if process_exists.is_none() {
                    bail!("process_instance {process_id} not found");
                }

                let existing_binding = tx
                    .query_opt(
                        "SELECT 1 FROM program_process_bindings WHERE process_id = $1",
                        &[process_id],
                    )
                    .await
                    .context("failed to check whether process is already bound")?;
                if existing_binding.is_some() {
                    bail!("process_instance {process_id} is already bound to a program");
                }
            }

            tx.execute(
                r#"
                INSERT INTO programs (
                    program_run_id,
                    workflow_definite_id,
                    status,
                    plan_state_json,
                    metadata_json
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
                &[
                    &program_run_id,
                    &workflow_definite_id,
                    &status,
                    &encoded_plan_state,
                    &metadata_json,
                ],
            )
            .await
            .with_context(|| format!("failed to insert program {program_run_id}"))?;

            for (program_slot_name, process_id) in bindings {
                tx.execute(
                    r#"
                    INSERT INTO program_process_bindings (
                        program_run_id,
                        program_slot_name,
                        process_id,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, NULL)
                    "#,
                    &[&program_run_id, &program_slot_name, &process_id],
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to bind process {} into slot {} for program {}",
                        process_id, program_slot_name, program_run_id
                    )
                })?;
            }

            tx.commit()
                .await
                .context("failed to commit create_program transaction")?;
        }

        self.get_program(program_run_id).await
    }

    pub async fn get_program(&self, program_run_id: &str) -> Result<ProgramRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        program_run_id,
                        workflow_definite_id,
                        status,
                        plan_state_json,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM programs
                    WHERE program_run_id = $1
                    "#,
                    &[&program_run_id],
                )
                .await
                .with_context(|| format!("failed to fetch program {program_run_id}"))?
        }
        .ok_or_else(|| anyhow!("program {program_run_id} not found"))?;

        map_program_row(&row)
    }

    pub async fn list_programs(&self) -> Result<Vec<ProgramRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        program_run_id,
                        workflow_definite_id,
                        status,
                        plan_state_json,
                        metadata_json,
                        created_at,
                        updated_at
                    FROM programs
                    ORDER BY program_run_id
                    "#,
                    &[],
                )
                .await
                .context("failed to list programs")?
        };

        rows.iter().map(map_program_row).collect()
    }

    pub async fn update_program_plan_state(
        &self,
        program_run_id: &str,
        plan_state_json: &ProgramPlanState,
    ) -> Result<ProgramRow> {
        let encoded = serde_json::to_value(plan_state_json)
            .context("failed to encode plan_state_json for update")?;
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    UPDATE programs
                    SET
                        plan_state_json = $2,
                        updated_at = NOW()
                    WHERE program_run_id = $1
                    RETURNING
                        program_run_id,
                        workflow_definite_id,
                        status,
                        plan_state_json,
                        metadata_json,
                        created_at,
                        updated_at
                    "#,
                    &[&program_run_id, &encoded],
                )
                .await
                .with_context(|| format!("failed to update plan_state_json for {program_run_id}"))?
        };

        map_program_row(&row)
    }

    pub async fn update_program_metadata(
        &self,
        program_run_id: &str,
        metadata_json: Option<Value>,
    ) -> Result<ProgramRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    UPDATE programs
                    SET
                        metadata_json = $2,
                        updated_at = NOW()
                    WHERE program_run_id = $1
                    RETURNING
                        program_run_id,
                        workflow_definite_id,
                        status,
                        plan_state_json,
                        metadata_json,
                        created_at,
                        updated_at
                    "#,
                    &[&program_run_id, &metadata_json],
                )
                .await
                .with_context(|| {
                    format!("failed to update program metadata for {program_run_id}")
                })?
        };

        map_program_row(&row)
    }

    pub async fn bind_process_to_program(
        &self,
        program_run_id: &str,
        program_slot_name: &str,
        process_id: Uuid,
        metadata_json: Option<Value>,
    ) -> Result<ProgramProcessBindingRow> {
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .context("failed to start bind_process_to_program transaction")?;

        let program_exists = tx
            .query_opt(
                "SELECT 1 FROM programs WHERE program_run_id = $1 FOR UPDATE",
                &[&program_run_id],
            )
            .await
            .context("failed to lock program before binding")?;
        if program_exists.is_none() {
            bail!("program {program_run_id} not found");
        }

        let process_exists = tx
            .query_opt(
                "SELECT 1 FROM process_instances WHERE id = $1 FOR UPDATE",
                &[&process_id],
            )
            .await
            .context("failed to lock process before binding")?;
        if process_exists.is_none() {
            bail!("process_instance {process_id} not found");
        }

        let existing_process_binding = tx
            .query_opt(
                "SELECT 1 FROM program_process_bindings WHERE process_id = $1",
                &[&process_id],
            )
            .await
            .context("failed to check existing binding for process")?;
        if existing_process_binding.is_some() {
            bail!("process_instance {process_id} is already bound to a program");
        }

        let existing_slot_binding = tx
            .query_opt(
                r#"
                SELECT 1
                FROM program_process_bindings
                WHERE program_run_id = $1 AND program_slot_name = $2
                "#,
                &[&program_run_id, &program_slot_name],
            )
            .await
            .context("failed to check existing slot binding")?;
        if existing_slot_binding.is_some() {
            bail!("slot {program_slot_name} in program {program_run_id} is already occupied");
        }

        let row = tx
            .query_one(
                r#"
                INSERT INTO program_process_bindings (
                    program_run_id,
                    program_slot_name,
                    process_id,
                    metadata_json
                )
                VALUES ($1, $2, $3, $4)
                RETURNING
                    program_run_id,
                    program_slot_name,
                    process_id,
                    attached_at,
                    metadata_json
                "#,
                &[
                    &program_run_id,
                    &program_slot_name,
                    &process_id,
                    &metadata_json,
                ],
            )
            .await
            .context("failed to insert program_process_binding")?;

        tx.commit()
            .await
            .context("failed to commit bind_process_to_program transaction")?;

        map_binding_row(&row)
    }

    pub async fn unbind_process_from_program(
        &self,
        process_id: Uuid,
    ) -> Result<Option<ProgramProcessBindingRow>> {
        let maybe_row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    DELETE FROM program_process_bindings
                    WHERE process_id = $1
                    RETURNING
                        program_run_id,
                        program_slot_name,
                        process_id,
                        attached_at,
                        metadata_json
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to unbind process from program")?
        };

        maybe_row.as_ref().map(map_binding_row).transpose()
    }

    pub async fn terminate_process_instance(&self, process_id: Uuid) -> Result<ProcessInstanceRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    UPDATE process_instances
                    SET
                        status = 'terminated',
                        updated_at = NOW()
                    WHERE id = $1
                    RETURNING
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq,
                        created_at,
                        updated_at
                    "#,
                    &[&process_id],
                )
                .await
                .with_context(|| format!("failed to terminate process_instance {process_id}"))?
        };

        map_process_row(&row)
    }

    pub async fn update_process_policy(
        &self,
        process_id: Uuid,
        policy_json: Option<Value>,
    ) -> Result<ProcessInstanceRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    UPDATE process_instances
                    SET
                        policy_json = $2,
                        updated_at = NOW()
                    WHERE id = $1
                    RETURNING
                        id,
                        external_slot_name,
                        prefix_suffix_definite_id,
                        status,
                        policy_json,
                        last_segment_seq,
                        created_at,
                        updated_at
                    "#,
                    &[&process_id, &policy_json],
                )
                .await
                .with_context(|| format!("failed to update process policy for {process_id}"))?
        };

        map_process_row(&row)
    }

    pub async fn append_process_segment(
        &self,
        process_id: Uuid,
        segment_kind: &str,
        content: &str,
        token_count: Option<i32>,
        tokenizer: Option<&str>,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        let row = {
            let mut client = self.client.lock().await;
            let tx = client
                .transaction()
                .await
                .context("failed to start append_process_segment transaction")?;

            let process = tx
                .query_opt(
                    r#"
                    SELECT last_segment_seq
                    FROM process_instances
                    WHERE id = $1
                    FOR UPDATE
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to lock process before append_process_segment")?
                .ok_or_else(|| anyhow!("process_instance {process_id} not found"))?;
            let last_segment_seq: i64 = process.get("last_segment_seq");
            let next_seq = last_segment_seq + 1;
            let token_count = token_count.unwrap_or_else(|| approximate_token_count(content));
            let patch = match runtime_state_from_patch(patch.as_ref()) {
                Some(_) => patch,
                None => with_runtime_state(patch, "sealed"),
            };

            let row = tx
                .query_one(
                    r#"
                    INSERT INTO segment (
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch
                    )
                    VALUES ($1, 'process', $2, $3, NULL, $4, $5, $6, $7, $8)
                    RETURNING
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    "#,
                    &[
                        &Uuid::new_v4(),
                        &process_id,
                        &next_seq,
                        &segment_kind,
                        &content,
                        &token_count,
                        &tokenizer,
                        &patch,
                    ],
                )
                .await
                .context("failed to insert process-owned segment")?;

            tx.execute(
                r#"
                UPDATE process_instances
                SET
                    last_segment_seq = $2,
                    updated_at = NOW()
                WHERE id = $1
                "#,
                &[&process_id, &next_seq],
            )
            .await
            .context("failed to update process last_segment_seq")?;

            tx.commit()
                .await
                .context("failed to commit append_process_segment transaction")?;

            row
        };

        map_segment_row(&row)
    }

    pub async fn list_process_segments(&self, process_id: Uuid) -> Result<Vec<SegmentRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'process' AND owner_id = $1
                    ORDER BY owner_seq
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to list process-owned segments")?
        };

        rows.iter().map(map_segment_row).collect()
    }

    pub async fn latest_process_segment_by_kind(
        &self,
        process_id: Uuid,
        segment_kind: &str,
    ) -> Result<Option<SegmentRow>> {
        let maybe_row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'process'
                      AND owner_id = $1
                      AND segment_kind = $2
                    ORDER BY owner_seq DESC
                    LIMIT 1
                    "#,
                    &[&process_id, &segment_kind],
                )
                .await
                .context("failed to fetch latest segment by kind")?
        };

        maybe_row.as_ref().map(map_segment_row).transpose()
    }

    pub async fn get_segment(&self, segment_id: Uuid) -> Result<SegmentRow> {
        let row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE id = $1
                    "#,
                    &[&segment_id],
                )
                .await
                .context("failed to fetch segment")?
        }
        .ok_or_else(|| anyhow!("segment {segment_id} not found"))?;

        map_segment_row(&row)
    }

    pub async fn update_segment_patch(
        &self,
        segment_id: Uuid,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        let existing = self.get_segment(segment_id).await?;
        let merged_patch = merge_patch(existing.patch.clone(), patch);
        let row = {
            let client = self.client.lock().await;
            client
                .query_one(
                    r#"
                    UPDATE segment
                    SET patch = $2
                    WHERE id = $1
                    RETURNING
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    "#,
                    &[&segment_id, &merged_patch],
                )
                .await
                .with_context(|| format!("failed to update segment patch for {segment_id}"))?
        };

        map_segment_row(&row)
    }

    pub async fn upsert_live_segment(
        &self,
        process_id: Uuid,
        segment_id: Uuid,
        segment_kind: &str,
        content: &str,
        token_count: i32,
        tokenizer: Option<&str>,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        let _ = (
            process_id,
            segment_id,
            segment_kind,
            content,
            token_count,
            tokenizer,
            patch,
        );
        bail!("upsert_live_segment is disabled: open live segments are memory-only")
    }

    pub async fn persist_sealed_live_segment(
        &self,
        process_id: Uuid,
        segment_id: Uuid,
        segment_kind: &str,
        content: &str,
        token_count: i32,
        tokenizer: Option<&str>,
        patch: Option<Value>,
    ) -> Result<SegmentRow> {
        let row = {
            let mut client = self.client.lock().await;
            let tx = client
                .transaction()
                .await
                .context("failed to start persist_sealed_live_segment transaction")?;

            let process = tx
                .query_opt(
                    r#"
                    SELECT last_segment_seq
                    FROM process_instances
                    WHERE id = $1
                    FOR UPDATE
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to lock process before persist_sealed_live_segment")?
                .ok_or_else(|| anyhow!("process_instance {process_id} not found"))?;
            let last_segment_seq: i64 = process.get("last_segment_seq");
            let next_seq = last_segment_seq + 1;
            let patch = with_runtime_state(patch, "sealed");

            let existing = tx
                .query_opt(
                    r#"
                    SELECT id
                    FROM segment
                    WHERE id = $1 AND owner_kind = 'process' AND owner_id = $2
                    FOR UPDATE
                    "#,
                    &[&segment_id, &process_id],
                )
                .await
                .context("failed to lock existing live segment before seal")?;

            let row = if existing.is_some() {
                tx.query_one(
                    r#"
                    UPDATE segment
                    SET
                        owner_seq = $3,
                        segment_kind = $4,
                        content = $5,
                        token_count = $6,
                        tokenizer = $7,
                        patch = $8
                    WHERE id = $1 AND owner_kind = 'process' AND owner_id = $2
                    RETURNING
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    "#,
                    &[
                        &segment_id,
                        &process_id,
                        &next_seq,
                        &segment_kind,
                        &content,
                        &token_count,
                        &tokenizer,
                        &patch,
                    ],
                )
                .await
                .context("failed to resequence and seal live segment")?
            } else {
                tx.query_one(
                    r#"
                    INSERT INTO segment (
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch
                    )
                    VALUES ($1, 'process', $2, $3, NULL, $4, $5, $6, $7, $8)
                    RETURNING
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    "#,
                    &[
                        &segment_id,
                        &process_id,
                        &next_seq,
                        &segment_kind,
                        &content,
                        &token_count,
                        &tokenizer,
                        &patch,
                    ],
                )
                .await
                .context("failed to insert sealed live segment")?
            };

            tx.execute(
                r#"
                UPDATE process_instances
                SET
                    last_segment_seq = $2,
                    updated_at = NOW()
                WHERE id = $1
                "#,
                &[&process_id, &next_seq],
            )
            .await
            .context(
                "failed to bump process last_segment_seq during persist_sealed_live_segment",
            )?;

            tx.commit()
                .await
                .context("failed to commit persist_sealed_live_segment transaction")?;

            row
        };

        map_segment_row(&row)
    }

    pub async fn recover_open_live_segments(&self) -> Result<Vec<SegmentRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'process'
                      AND COALESCE(patch->>'runtime_state', '') = 'open'
                    ORDER BY created_at
                    "#,
                    &[],
                )
                .await
                .context("failed to recover open live segments")?
        };

        rows.iter().map(map_segment_row).collect()
    }

    pub async fn list_sealed_process_segments(&self, process_id: Uuid) -> Result<Vec<SegmentRow>> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'process'
                      AND owner_id = $1
                      AND COALESCE(patch->>'runtime_state', 'sealed') <> 'open'
                    ORDER BY owner_seq
                    "#,
                    &[&process_id],
                )
                .await
                .context("failed to list sealed process-owned segments")?
        };

        let mut sealed_segments = Vec::new();
        for row in rows {
            sealed_segments.push(map_segment_row(&row)?);
        }

        Ok(sealed_segments)
    }

    pub async fn load_runtime_head(&self, process_id: Uuid) -> Result<RuntimeHead> {
        let process = self.get_process_instance(process_id).await?;
        let prefix_suffix = self
            .get_prefix_suffix_definite(process.prefix_suffix_definite_id)
            .await?;
        let mut built_all = self
            .fetch_built_all_segment(process.prefix_suffix_definite_id)
            .await?;

        if built_all.is_none() || prefix_suffix.compile_status != "ready" {
            self.compile_prefix_suffix_definite(process.prefix_suffix_definite_id)
                .await?;
            built_all = self
                .fetch_built_all_segment(process.prefix_suffix_definite_id)
                .await?;
        }

        let built_all = built_all.ok_or_else(|| {
            anyhow!(
                "compiled built_all segment missing for prefix_suffix_definite {}",
                process.prefix_suffix_definite_id
            )
        })?;
        let built_all_compile_version =
            compile_version_from_patch(built_all.patch.as_ref()).unwrap_or(0);
        let sealed_segments = self.list_sealed_process_segments(process_id).await?;

        // Runtime head only reads segment content. Patch remains attached metadata instead of
        // automatically entering the process prompt.
        let full_text = assemble_runtime_text(&built_all.content, &sealed_segments, None);
        let runtime_head = RuntimeHead {
            process_id,
            full_text,
        };

        if built_all_compile_version == 0 {
            bail!("runtime head assembled from unversioned built_all segment");
        }

        Ok(runtime_head)
    }

    pub async fn push_persistence_compensation(
        &self,
        program_run_id: &str,
        record: &PersistenceCompensationRecord,
    ) -> Result<ProgramRow> {
        let program = self.get_program(program_run_id).await?;
        let mut records = compensation_records_from_metadata(program.metadata_json.as_ref());
        records.retain(|existing| existing.operation_id != record.operation_id);
        records.push(record.clone());
        self.update_program_metadata(
            program_run_id,
            metadata_with_compensation_records(program.metadata_json.clone(), &records),
        )
        .await
    }

    pub async fn clear_persistence_compensation(
        &self,
        program_run_id: &str,
        operation_id: Uuid,
    ) -> Result<ProgramRow> {
        let program = self.get_program(program_run_id).await?;
        let mut records = compensation_records_from_metadata(program.metadata_json.as_ref());
        records.retain(|existing| existing.operation_id != operation_id);
        self.update_program_metadata(
            program_run_id,
            metadata_with_compensation_records(program.metadata_json.clone(), &records),
        )
        .await
    }

    async fn fetch_built_all_segment(
        &self,
        prefix_suffix_definite_id: Uuid,
    ) -> Result<Option<SegmentRow>> {
        let maybe_row = {
            let client = self.client.lock().await;
            client
                .query_opt(
                    r#"
                    SELECT
                        id,
                        owner_kind,
                        owner_id,
                        owner_seq,
                        definition_part,
                        segment_kind,
                        content,
                        token_count,
                        tokenizer,
                        patch,
                        created_at
                    FROM segment
                    WHERE owner_kind = 'prefix_suffix_definite'
                      AND owner_id = $1
                      AND definition_part = 'built_all'
                    "#,
                    &[&prefix_suffix_definite_id],
                )
                .await
                .context("failed to fetch built_all segment")?
        };

        maybe_row.as_ref().map(map_segment_row).transpose()
    }

    async fn next_external_slot_name(&self) -> Result<String> {
        let rows = {
            let client = self.client.lock().await;
            client
                .query("SELECT external_slot_name FROM process_instances", &[])
                .await
                .context("failed to list existing external_slot_name values")?
        };

        let next_index = rows
            .iter()
            .filter_map(|row| {
                row.get::<_, String>("external_slot_name")
                    .strip_prefix("free")
                    .map(ToOwned::to_owned)
            })
            .filter_map(|value| value.parse::<u64>().ok())
            .max()
            .unwrap_or(0)
            + 1;
        Ok(format!("free{next_index}"))
    }
}

fn map_workflow_definite_row(row: &Row) -> Result<WorkflowDefiniteRow> {
    Ok(WorkflowDefiniteRow {
        id: row.get("id"),
        workflow_name: row.get("workflow_name"),
        workflow_json: row.get("workflow_json"),
        version: row.get("version"),
        status: row.get("status"),
        metadata_json: row.get("metadata_json"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_prefix_suffix_definite_row(row: &Row) -> Result<PrefixSuffixDefiniteRow> {
    Ok(PrefixSuffixDefiniteRow {
        id: row.get("id"),
        prefix_name: row.get("prefix_name"),
        suffix_name: row.get("suffix_name"),
        compile_version: row.get("compile_version"),
        compile_status: row.get("compile_status"),
        metadata_json: row.get("metadata_json"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_program_row(row: &Row) -> Result<ProgramRow> {
    Ok(ProgramRow {
        program_run_id: row.get("program_run_id"),
        workflow_definite_id: row.get("workflow_definite_id"),
        status: row.get("status"),
        plan_state_json: serde_json::from_value(row.get("plan_state_json"))
            .context("failed to decode programs.plan_state_json")?,
        metadata_json: row.get("metadata_json"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_binding_row(row: &Row) -> Result<ProgramProcessBindingRow> {
    Ok(ProgramProcessBindingRow {
        program_run_id: row.get("program_run_id"),
        program_slot_name: row.get("program_slot_name"),
        process_id: row.get("process_id"),
        attached_at: row.get("attached_at"),
        metadata_json: row.get("metadata_json"),
    })
}

fn map_process_row(row: &Row) -> Result<ProcessInstanceRow> {
    Ok(ProcessInstanceRow {
        id: row.get("id"),
        external_slot_name: row.get("external_slot_name"),
        prefix_suffix_definite_id: row.get("prefix_suffix_definite_id"),
        status: row.get("status"),
        policy_json: row.get("policy_json"),
        last_segment_seq: row.get("last_segment_seq"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_segment_row(row: &Row) -> Result<SegmentRow> {
    let owner_kind = match row.get::<_, String>("owner_kind").as_str() {
        "prefix_suffix_definite" => OwnerKind::PrefixSuffixDefinite,
        "process" => OwnerKind::Process,
        value => bail!("unsupported owner_kind {value}"),
    };
    let definition_part = match row.get::<_, Option<String>>("definition_part") {
        Some(value) => Some(match value.as_str() {
            "prefix" => DefinitionPart::Prefix,
            "suffix" => DefinitionPart::Suffix,
            "built_all" => DefinitionPart::BuiltAll,
            other => bail!("unsupported definition_part {other}"),
        }),
        None => None,
    };

    Ok(SegmentRow {
        id: row.get("id"),
        owner_kind,
        owner_id: row.get("owner_id"),
        owner_seq: row.get("owner_seq"),
        definition_part,
        segment_kind: row.get("segment_kind"),
        content: row.get("content"),
        token_count: row.get("token_count"),
        tokenizer: row.get("tokenizer"),
        patch: row.get("patch"),
        created_at: row.get("created_at"),
    })
}

fn map_bound_process_row(row: &Row) -> Result<BoundProcessRow> {
    Ok(BoundProcessRow {
        binding: ProgramProcessBindingRow {
            program_run_id: row.get("program_run_id"),
            program_slot_name: row.get("program_slot_name"),
            process_id: row.get("process_id"),
            attached_at: row.get("attached_at"),
            metadata_json: row.get("metadata_json"),
        },
        process: ProcessInstanceRow {
            id: row.get("id"),
            external_slot_name: row.get("external_slot_name"),
            prefix_suffix_definite_id: row.get("prefix_suffix_definite_id"),
            status: row.get("status"),
            policy_json: row.get("policy_json"),
            last_segment_seq: row.get("last_segment_seq"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        },
    })
}
