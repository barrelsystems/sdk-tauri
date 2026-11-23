/// Current wire-protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

use crate::updates::models::failure::{Phase, UpdateFailureMode};
use crate::updates::models::state::UpdateState;
use crate::updates::persistence::buffered_facts::BufferedFact;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub lifecycle_state: UpdateState,
    pub buffered_facts: Vec<WireBufferedFact>,
    /// This SDK's own configured cadence, after its local floor/ceiling
    /// clamp -- lets the backend scale stuck-tier detection per installation.
    pub check_in_interval_ms: u64,
}

/// The wire shape a check-in's `bufferedFacts` field actually expects --
/// flatter than the internal, disk-persisted `BufferedFact`, which nests
/// an `updateError`'s fields under `failure` plus a client-only `title`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum WireBufferedFact {
    #[serde(rename = "lifecycleState")]
    LifecycleState {
        state: UpdateState,
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
            BufferedFact::LifecycleState {
                state,
                first_observed_at,
                last_observed_at,
                occurrence_count,
                ..
            } => WireBufferedFact::LifecycleState {
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
                ..
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

/// The raw response to a check-in, before its signature is verified and
/// `update_eligibility` is parsed into an `UpdateEligibility`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInResponse {
    pub protocol_version: u32,
    /// JSON-encoded `UpdateEligibility`, verified against `signature` before parsing.
    pub update_eligibility: String,
    /// Base64-encoded signature over `update_eligibility`'s raw bytes.
    pub signature: String,
    /// A key-rotation announcement: a new public key, signed by the key
    /// this SDK currently trusts. Both absent when no rotation is in
    /// flight -- additive, never required by a server with nothing to rotate.
    pub next_public_key_pem: Option<String>,
    pub next_public_key_signature: Option<String>,
}
