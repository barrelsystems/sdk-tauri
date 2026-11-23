//! The harness's own normative wire shapes -- this platform's independent
//! copy, mirroring `wireTypes.ts`/`WireTypes.swift`.

use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct Capabilities {
    #[serde(rename = "sdkName")]
    pub sdk_name: &'static str,
    #[serde(rename = "sdkVersion")]
    pub sdk_version: &'static str,
    #[serde(rename = "supportsCheckIn")]
    pub supports_check_in: bool,
}

#[derive(Deserialize)]
pub struct CreateAdapterRequest {
    pub attributes: Option<std::collections::HashMap<String, String>>,
    pub debug: Option<bool>,
    #[serde(rename = "checkInIntervalMs")]
    pub check_in_interval_ms: Option<u64>,
    pub lifecycle: Option<bool>,
    #[serde(rename = "installationPersistence")]
    pub installation_persistence: Option<String>,

    pub channel: Option<String>,
    #[serde(rename = "checkInBaseUrl")]
    pub check_in_base_url: String,
    #[serde(rename = "publicKeyPem")]
    pub public_key_pem: String,
}

#[derive(Serialize)]
pub struct CreateAdapterResponse {
    pub id: String,
}

#[derive(Deserialize)]
pub struct JitterFractionRequest {
    #[serde(rename = "installationId")]
    #[allow(dead_code)]
    pub installation_id: String,
}

/// The wire-level shape a request body decodes into -- distinct from
/// `ThreadRequest` in `dispatch.rs`, which is what this then translates
/// into for the adapter thread.
#[derive(Deserialize)]
#[serde(tag = "command")]
pub enum AdapterCommand {
    #[serde(rename = "getCadenceState")]
    GetCadenceState,
    #[serde(rename = "restoreInstallationPersistence")]
    RestoreInstallationPersistence,
    #[serde(rename = "getPersistedInstallationId")]
    GetPersistedInstallationId,
    #[serde(rename = "triggerLifecycle")]
    TriggerLifecycle { event: String },
    #[serde(rename = "setBufferPersistence")]
    SetBufferPersistence {
        mode: tauri_plugin_barrel_kit::test_service::buffer_persistence_control::BufferPersistenceMode,
    },
    #[serde(rename = "getBufferPersistenceState")]
    GetBufferPersistenceState,
    #[serde(rename = "recordFacts")]
    RecordFacts {
        facts: Vec<tauri_plugin_barrel_kit::test_service::buffer_commands::FactObservation>,
        concurrent: bool,
    },
    #[serde(rename = "getBufferedFacts")]
    GetBufferedFacts,
    #[serde(rename = "getPersistedBuffer")]
    GetPersistedBuffer,
    #[serde(rename = "clearBuffer")]
    ClearBuffer,
    #[serde(rename = "probeBufferExit")]
    ProbeBufferExit {
        mode: String,
        facts: Vec<tauri_plugin_barrel_kit::test_service::buffer_commands::FactObservation>,
    },
    #[serde(rename = "checkNow")]
    CheckNow,
    #[serde(rename = "getUpdateState")]
    GetUpdateState,
    #[serde(rename = "getUpdateEligibility")]
    GetUpdateEligibility,
    #[serde(rename = "prepareUpdaterFailure")]
    PrepareUpdaterFailure,
    #[serde(rename = "getLastUpdateError")]
    GetLastUpdateError,
    #[serde(rename = "getUpdaterActivity")]
    GetUpdaterActivity,
    #[serde(rename = "prepareAction")]
    PrepareAction { stage: String },
    #[serde(rename = "runAction")]
    RunAction { stage: String },
}

/// Every command's own result shape differs, so this is generic over
/// whatever each call site actually returns.
#[derive(Serialize)]
pub struct ResultWrapper<T: Serialize> {
    pub result: T,
}
