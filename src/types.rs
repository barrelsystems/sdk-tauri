use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::persistence::buffered_facts::BufferedFact;

pub const PROTOCOL_VERSION: u32 = 1;

/// Config passed to `init()`. `credential` is the only required field.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    pub credential: String,
    pub base_url: Option<String>,
    /// Developer-configurable; the server-enforced floor is 15 minutes regardless of what's passed here.
    pub check_in_interval_ms: Option<u64>,
    pub attributes: HashMap<String, String>,
    /// On by default. When off, a found update is never downloaded.
    pub auto_download: Option<bool>,
    /// Where your own update feed lives. Omit to use `tauri.conf.json`'s
    /// own static `endpoints` instead.
    pub update_feed_url: Option<String>,
    /// Off by default. Logs what the SDK is doing via `eprintln!` -- never noisy unless opted in.
    pub debug: bool,
    /// How long a cached `held` Policy stays in effect with Barrel
    /// unreachable, before failing open. Default 24h, hard ceiling 7 days.
    pub hold_cache_max_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum AdapterState {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "checking")]
    Checking,
    #[serde(rename = "available")]
    Available {
        #[serde(skip_serializing_if = "Option::is_none")]
        release_notes: Option<String>,
    },
    #[serde(rename = "downloading")]
    Downloading {
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<u32>,
    },
    #[serde(rename = "downloaded")]
    Downloaded,
    #[serde(rename = "update-cancelled")]
    UpdateCancelled,
    #[serde(rename = "failed")]
    Failed {
        category: FailureCategory,
        code: UpdateFailureMode,
        platform_code: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FailureCategory {
    Check,
    Download,
    Updater,
}

/// The stable, cross-platform fingerprint for a classified update failure.
/// `Unknown` is load-bearing, not a fallback that "shouldn't happen" -- a
/// library update can always add a variant this SDK hasn't mapped yet.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateFailureMode {
    ManifestUnreachable,
    ManifestParseFailed,
    VersionInvalid,
    SignatureMissing,
    SignatureInvalid,
    SignatureVersionMismatch,
    ChecksumMismatch,
    DownloadTransportFailed,
    DownloadFileInvalid,
    DownloadSizeExceeded,
    DownloadStreamIncomplete,
    DiffPatchIncompatible,
    ExtractionFailed,
    TargetNotFoundInManifest,
    TempStorageConstraint,
    InstallWriteFailed,
    InstallAuthRequiredOrFailed,
    InstallDowngradeBlocked,
    InstallRelaunchFailed,
    InstallMissingUpdate,
    ConfigInvalid,
    CorsMisconfigured,
    PlatformUnsupported,
    TranslocationBlocked,
    WebkitTerminated,
    ReleaseNotesFailed,
    ResumeAppcastFailed,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Check,
    Verify,
    Download,
    Extract,
    Install,
    Config,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateFailure {
    pub phase: Phase,
    pub failure_mode: UpdateFailureMode,
    /// Resolved from a static lookup table at return time -- never persisted or transmitted.
    pub title: String,
    pub platform: String,
    pub platform_code: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInRequest {
    pub protocol_version: u32,
    pub installation_id: String,
    pub event_id: String,
    pub checked_in_at: String,
    pub sdk_version: String,
    pub observed_version: String,
    pub platform: String,
    pub arch: String,
    pub os_version: String,
    pub attributes: HashMap<String, String>,
    pub adapter_state: AdapterState,
    pub buffered_facts: Vec<WireBufferedFact>,
}

/// The wire shape a check-in's `bufferedFacts` field actually expects --
/// flatter than the internal, disk-persisted `BufferedFact`, which nests
/// an `updateError`'s fields under `failure` plus a client-only `title`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum WireBufferedFact {
    #[serde(rename = "adapterState")]
    AdapterState {
        state: AdapterState,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
    #[serde(rename = "updateError")]
    UpdateError {
        phase: Phase,
        failure_mode: UpdateFailureMode,
        platform_code: String,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
}

impl From<&BufferedFact> for WireBufferedFact {
    fn from(fact: &BufferedFact) -> Self {
        match fact {
            BufferedFact::AdapterState {
                state,
                first_observed_at,
                last_observed_at,
                occurrence_count,
            } => WireBufferedFact::AdapterState {
                state: state.clone(),
                first_observed_at: first_observed_at.clone(),
                last_observed_at: last_observed_at.clone(),
                occurrence_count: *occurrence_count,
            },
            BufferedFact::UpdateError {
                failure,
                first_observed_at,
                last_observed_at,
                occurrence_count,
            } => WireBufferedFact::UpdateError {
                phase: failure.phase,
                failure_mode: failure.failure_mode,
                platform_code: failure.platform_code.clone(),
                first_observed_at: first_observed_at.clone(),
                last_observed_at: last_observed_at.clone(),
                occurrence_count: *occurrence_count,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum Policy {
    #[serde(rename = "current")]
    Current {
        issued_at: String,
        environment: String,
    },
    #[serde(rename = "held")]
    Held {
        issued_at: String,
        environment: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        until: Option<String>,
    },
    #[serde(rename = "updateEligible")]
    UpdateEligible {
        issued_at: String,
        environment: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        priority: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInResponse {
    pub protocol_version: u32,
    pub policy: String,
    pub signature: String,
    /// A key-rotation announcement: a new public key, signed by the key
    /// this SDK currently trusts. Both absent when no rotation is in
    /// flight -- additive, never required by a server with nothing to rotate.
    pub next_public_key_pem: Option<String>,
    pub next_public_key_signature: Option<String>,
}
