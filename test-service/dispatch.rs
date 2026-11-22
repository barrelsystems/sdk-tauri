use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use tauri_plugin_barrel_kit::protocol::pre_action_check::Stage;
use tauri_plugin_barrel_kit::test_service;

use crate::runtime::{DispatchState, ThreadRequest};
use crate::wire_types::{AdapterCommand, ResultWrapper};

pub(crate) async fn dispatch_command(
    State(state): State<DispatchState>,
    Json(command): Json<AdapterCommand>,
) -> Response {
    match command {
        AdapterCommand::GetCadenceState => Json(ResultWrapper { result: test_service::resilience_commands::cadence_state(&state.app) }).into_response(),
        AdapterCommand::RestoreInstallationPersistence => Json(ResultWrapper { result: test_service::resilience_commands::restore_installation_persistence(&state.app) }).into_response(),
        AdapterCommand::GetPersistedInstallationId => Json(ResultWrapper {
            result: test_service::resilience_commands::persisted_installation_id(&state.app).await,
        }).into_response(),
        AdapterCommand::TriggerLifecycle { event } => {
            if !test_service::resilience_commands::trigger_lifecycle(&event) { return StatusCode::BAD_REQUEST.into_response(); }
            Json(ResultWrapper { result: serde_json::Value::Null }).into_response()
        }
        AdapterCommand::SetBufferPersistence { mode } => {
            test_service::buffer_persistence_control::set_buffer_persistence(&state.app, mode).await;
            Json(ResultWrapper {
                result: serde_json::Value::Null,
            })
            .into_response()
        }
        AdapterCommand::GetBufferPersistenceState => Json(ResultWrapper {
            result: serde_json::json!({ "blockedWrite": test_service::buffer_persistence_control::buffer_write_blocked() }),
        })
        .into_response(),
        AdapterCommand::RecordFacts { facts, concurrent } => {
            test_service::buffer_commands::record_facts(&state.app, facts, concurrent).await;
            Json(ResultWrapper { result: serde_json::Value::Null }).into_response()
        }
        AdapterCommand::GetBufferedFacts => Json(ResultWrapper {
            result: test_service::buffer_commands::buffered_facts(&state.app).await,
        }).into_response(),
        AdapterCommand::ClearBuffer => {
            test_service::buffer_commands::clear_buffer(&state.app).await;
            Json(ResultWrapper { result: serde_json::Value::Null }).into_response()
        }
        AdapterCommand::GetPersistedBuffer => Json(ResultWrapper {
            result: test_service::buffer_commands::persisted_buffer(&state.app).await,
        }).into_response(),
        AdapterCommand::ProbeBufferExit { mode, facts } => Json(ResultWrapper {
            result: crate::buffer_exit_probe::run_probe(mode, facts).await,
        })
        .into_response(),
        AdapterCommand::GetPolicy => Json(ResultWrapper {
            result: tauri_plugin_barrel_kit::get_policy(),
        })
        .into_response(),
        AdapterCommand::GetAdapterState => {
            send_adapter_request(&state.request_tx, ThreadRequest::GetState).await
        }
        AdapterCommand::GetLastUpdateError => {
            send_adapter_request(&state.request_tx, ThreadRequest::GetLastError).await
        }
        AdapterCommand::InjectEvent { event_name, .. } if event_name == "triggerDownload" => {
            send_adapter_request(&state.request_tx, ThreadRequest::TriggerDownload).await
        }
        AdapterCommand::InjectEvent { event_name, .. } if event_name == "startDownload" => {
            send_adapter_request(&state.request_tx, ThreadRequest::StartDownload).await
        }
        AdapterCommand::InjectEvent { event_name, .. } => (
            StatusCode::BAD_REQUEST,
            format!("tauri test-service: unknown injectEvent eventName {event_name:?}"),
        )
            .into_response(),
        AdapterCommand::GetUpdaterActivity => Json(ResultWrapper {
            result: test_service::runtime::activity(),
        })
        .into_response(),
        AdapterCommand::CheckNow => {
            test_service::runtime::check_now(&state.app).await;
            Json(ResultWrapper {
                result: serde_json::Value::Null,
            })
            .into_response()
        }
        AdapterCommand::PrepareAction { stage } => {
            let Some(stage) = parse_stage(&stage) else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            let (reply, result) = tokio::sync::oneshot::channel();
            let _ = state
                .request_tx
                .send(ThreadRequest::PrepareAction { stage, reply })
                .await;
            let _ = result.await;
            Json(ResultWrapper {
                result: serde_json::Value::Null,
            })
            .into_response()
        }
        AdapterCommand::RunAction { stage } => {
            let Some(stage) = parse_stage(&stage) else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            test_service::runtime::run_action(&state.app, stage).await;
            Json(ResultWrapper {
                result: test_service::runtime::activity(),
            })
            .into_response()
        }
    }
}

fn parse_stage(stage: &str) -> Option<Stage> {
    match stage {
        "pre-download" => Some(Stage::PreDownload),
        "pre-apply" => Some(Stage::PreApply),
        _ => None,
    }
}

/// Shared shape behind every `AdapterCommand` that round-trips to the
/// adapter thread: build a reply channel, send the request it constructs,
/// await the reply, and wrap it as this protocol's own `{ result }` body.
async fn send_adapter_request<T: serde::Serialize>(
    request_tx: &tokio::sync::mpsc::Sender<ThreadRequest>,
    make_request: impl FnOnce(tokio::sync::oneshot::Sender<Option<T>>) -> ThreadRequest,
) -> Response {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if request_tx.send(make_request(reply_tx)).await.is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "adapter thread gone").into_response();
    }
    let result: Option<T> = reply_rx.await.unwrap_or(None);
    Json(ResultWrapper { result }).into_response()
}
