use serde::{Deserialize, Serialize};

/// What a check-in response resolves to once its signature is verified.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum UpdateEligibility {
    /// No hold or eligibility decision in effect; proceed normally.
    #[serde(rename = "unrestricted")]
    Unrestricted {
        /// ISO 8601, set by the server when it issued this decision.
        issued_at: String,
        /// The server's own environment name (e.g. `"production"`).
        environment: String,
    },
    /// Barrel is unreachable or undecided -- keep the last-known decision in
    /// effect, no longer than `hold_cache_max_age_ms` allows.
    #[serde(rename = "held")]
    Held {
        /// ISO 8601, set by the server when it issued this decision.
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
    #[serde(rename = "eligible")]
    Eligible {
        /// ISO 8601, set by the server when it issued this decision.
        issued_at: String,
        /// The server's own environment name (e.g. `"production"`).
        environment: String,
        /// The server's own urgency hint for this update, when it has one.
        #[serde(skip_serializing_if = "Option::is_none")]
        priority: Option<String>,
    },
}
