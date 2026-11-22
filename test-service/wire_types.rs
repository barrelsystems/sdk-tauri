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
    pub channel: Option<String>,
    #[serde(rename = "checkInBaseUrl")]
    pub check_in_base_url: String,
    #[serde(rename = "publicKeyPem")]
    pub public_key_pem: String,
    #[serde(default)]
    pub variant: Option<String>,
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
    #[serde(rename = "checkNow")]
    CheckNow,
    #[serde(rename = "getAdapterState")]
    GetAdapterState,
    #[serde(rename = "getLastUpdateError")]
    GetLastUpdateError,
    #[serde(rename = "injectEvent")]
    InjectEvent {
        #[serde(rename = "eventName")]
        event_name: String,
        #[allow(dead_code)]
        payload: Option<serde_json::Value>,
    },
    #[serde(rename = "triggerPreActionCheck")]
    TriggerPreActionCheck { stage: String },
}

/// Every command's own result shape differs, so this is generic over
/// whatever each call site actually returns.
#[derive(Serialize)]
pub struct ResultWrapper<T: Serialize> {
    pub result: T,
}
