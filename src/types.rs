use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::persistence::buffered_facts::BufferedFact;

/// The check-in wire protocol's own version, sent on every request and
/// bumped only on a breaking change to its shape.
pub const PROTOCOL_VERSION: u32 = 1;

/// The three fields every backend round-trip needs, bundled once instead
/// of traveling as three separate params on every protocol-adjacent type.
#[derive(Debug, Clone, Copy)]
pub struct BackendConnection<'a> {
    /// The check-in endpoint's base URL.
    pub base_url: &'a str,
    /// Sent as a bearer token.
    pub credential: &'a str,
    /// Stable per-install identifier.
    pub installation_id: &'a str,
}

/// Config passed to `init()`. `credential` is the only required field.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    /// The SDK credential issued by Barrel for this app.
    pub credential: String,
    /// Where check-ins are sent. Omit to use Barrel's own production endpoint.
    pub base_url: Option<String>,
    /// Developer-configurable; the server-enforced floor is 15 minutes regardless of what's passed here.
    pub check_in_interval_ms: Option<u64>,
    /// Arbitrary key-value pairs sent with every check-in, for segmenting
    /// policy decisions (e.g. a cohort).
    pub attributes: HashMap<String, String>,
    /// The host app's release channel, reported verbatim. Defaults to "stable".
    pub channel: Option<String>,
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

/// The updater's current lifecycle state, as `get_adapter_state()` returns
/// it and as every check-in reports it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum AdapterState {
    /// No check has run yet, or the last one found nothing.
    #[serde(rename = "idle")]
    Idle,
    /// A check is in flight.
    #[serde(rename = "checking")]
    Checking,
    /// A check found an update not yet downloaded.
    #[serde(rename = "available")]
    Available {
        /// Carried through from the manifest, when it has one.
        #[serde(skip_serializing_if = "Option::is_none")]
        release_notes: Option<String>,
    },
    /// The update is downloading.
    #[serde(rename = "downloading")]
    Downloading {
        /// 0-100, when the adapter library reports one; absent otherwise.
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<u32>,
    },
    /// The update finished downloading and is ready to install.
    #[serde(rename = "downloaded")]
    Downloaded,
    /// The user or the OS cancelled an in-progress download or install.
    #[serde(rename = "update-cancelled")]
    UpdateCancelled,
    /// Something in the check/download/install pipeline failed.
    #[serde(rename = "failed")]
    Failed {
        /// The coarse grouping `code` belongs to.
        category: FailureCategory,
        /// The stable, cross-platform fingerprint for what went wrong.
        code: UpdateFailureMode,
        /// The adapter library's own error identifier, unmapped.
        platform_code: String,
    },
}

/// Which stage of the pipeline a `Failed` state's error belongs to, at the
/// coarser granularity a dashboard groups by (`Phase` is the finer one).
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
    /// Could not reach the update manifest.
    ManifestUnreachable,
    /// The update manifest could not be parsed.
    ManifestParseFailed,
    /// The update manifest's version is invalid.
    VersionInvalid,
    /// The update manifest's signature is invalid.
    SignatureInvalid,
    /// The update's signed version doesn't match the manifest.
    SignatureVersionMismatch,
    /// The update download failed at the network layer.
    DownloadTransportFailed,
    /// The downloaded update could not be extracted.
    ExtractionFailed,
    /// The expected update target is missing from the manifest.
    TargetNotFoundInManifest,
    /// A temporary-storage constraint blocked the update.
    TempStorageConstraint,
    /// The update could not be written to disk.
    InstallWriteFailed,
    /// The update install needs or failed authorization.
    InstallAuthRequiredOrFailed,
    /// The updater's configuration is invalid.
    ConfigInvalid,
    /// This update isn't supported on the current platform.
    PlatformUnsupported,
    /// The app quit to apply an update, but never came back up on the new version.
    ApplyNotConfirmed,
    /// An unclassified update failure occurred.
    Unknown,
}

/// Which stage of the pipeline a `Failed` state's error occurred in, at the
/// granularity `UpdateFailure::phase` reports.
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

/// `get_last_update_error()`'s own return shape -- the fields a bare
/// `AdapterState::Failed` doesn't carry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateFailure {
    /// Which stage of the pipeline this failure occurred in.
    pub phase: Phase,
    /// The stable, cross-platform fingerprint for what went wrong.
    pub failure_mode: UpdateFailureMode,
    /// Resolved from a static lookup table at return time -- never persisted or transmitted.
    pub title: String,
    /// The canonical name of the adapter library this failure came from, e.g. `"tauri-updater"`.
    pub platform: String,
    /// The adapter library's own error identifier, unmapped.
    pub platform_code: String,
}

/// The body of every check-in request this SDK sends.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInRequest {
    pub protocol_version: u32,
    /// Stable per-install identifier, generated once and persisted to disk.
    pub installation_id: String,
    /// A fresh UUID per check-in, letting the server de-duplicate retries.
    pub event_id: String,
    /// ISO 8601, set at the moment this request is built.
    pub checked_in_at: String,
    pub sdk_version: String,
    pub channel: String,
    /// The app version this installation is currently running.
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

/// What a check-in response resolves to once its signature is verified.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum Policy {
    /// The installation is current; nothing to do.
    #[serde(rename = "current")]
    Current {
        /// ISO 8601, set by the server when it issued this policy.
        issued_at: String,
        /// The server's own environment name (e.g. `"production"`).
        environment: String,
    },
    /// Barrel is unreachable or undecided -- keep the last-known policy in
    /// effect, no longer than `hold_cache_max_age_ms` allows.
    #[serde(rename = "held")]
    Held {
        /// ISO 8601, set by the server when it issued this policy.
        issued_at: String,
        /// The server's own environment name (e.g. `"production"`).
        environment: String,
        /// Why the decision is held, when the server has one to share.
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// ISO 8601; past this point, fail open rather than keep holding.
        #[serde(skip_serializing_if = "Option::is_none")]
        until: Option<String>,
    },
    /// An update is available and this installation is eligible for it.
    #[serde(rename = "updateEligible")]
    UpdateEligible {
        /// ISO 8601, set by the server when it issued this policy.
        issued_at: String,
        /// The server's own environment name (e.g. `"production"`).
        environment: String,
        /// The server's own urgency hint for this update, when it has one.
        #[serde(skip_serializing_if = "Option::is_none")]
        priority: Option<String>,
    },
}

/// The raw response to a check-in, before its signature is verified and
/// `policy` is parsed into a `Policy`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInResponse {
    pub protocol_version: u32,
    /// JSON-encoded `Policy`, verified against `signature` before parsing.
    pub policy: String,
    /// Base64-encoded signature over `policy`'s raw bytes.
    pub signature: String,
    /// A key-rotation announcement: a new public key, signed by the key
    /// this SDK currently trusts. Both absent when no rotation is in
    /// flight -- additive, never required by a server with nothing to rotate.
    pub next_public_key_pem: Option<String>,
    pub next_public_key_signature: Option<String>,
}
