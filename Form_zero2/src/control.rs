use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::models::{
    DeliveryAction, HardwareStep, MessageKind, ProcessRuntimeBinding, QueueEnvelope, ResultTarget,
    TaskStep,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TargetSelector {
    ProcessId(Uuid),
    ProgramSlot {
        program_run_id: String,
        program_slot_name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    pub sender: TargetSelector,
    pub target: TargetSelector,
    pub message_kind: MessageKind,
    pub base_priority: u8,
    pub requested_delivery_action: DeliveryAction,
    pub delay_ms: Option<u64>,
    pub result_target: ResultTarget,
    pub explicit_result_target: Option<TargetSelector>,
    pub content: Option<String>,
    pub content_ref: Option<String>,
    pub task_sequence: Vec<TaskStep>,
    pub hardware_sequence: Vec<HardwareStep>,
    pub metadata: Option<Value>,
}

pub(crate) enum RuntimeControlCommand {
    SendMessage {
        request: SendMessageRequest,
        response_tx: oneshot::Sender<Result<QueueEnvelope>>,
    },
    ResolveSlot {
        target: TargetSelector,
        response_tx: oneshot::Sender<Result<ProcessRuntimeBinding>>,
    },
}

#[derive(Clone)]
pub struct RuntimeControlQueue {
    tx: mpsc::UnboundedSender<RuntimeControlCommand>,
}

impl RuntimeControlQueue {
    pub(crate) fn new() -> (Self, mpsc::UnboundedReceiver<RuntimeControlCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    pub async fn send_message(&self, request: SendMessageRequest) -> Result<QueueEnvelope> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(RuntimeControlCommand::SendMessage {
                request,
                response_tx,
            })
            .map_err(|error| anyhow!("failed to enqueue send_message control command: {error}"))?;
        response_rx
            .await
            .map_err(|_| anyhow!("send_message control command was dropped"))?
    }

    pub async fn resolve_slot(&self, target: TargetSelector) -> Result<ProcessRuntimeBinding> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(RuntimeControlCommand::ResolveSlot {
                target,
                response_tx,
            })
            .map_err(|error| anyhow!("failed to enqueue resolve_slot control command: {error}"))?;
        response_rx
            .await
            .map_err(|_| anyhow!("resolve_slot control command was dropped"))?
    }
}
