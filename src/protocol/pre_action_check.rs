use serde::Serialize;
use std::time::Duration;

use super::policy::verify_and_parse_policy;
use super::request::{post_and_validate_envelope, RequestEnvelopeParams};
use crate::types::{Policy, PROTOCOL_VERSION};

const PRE_ACTION_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    PreDownload,
    PreApply,
}

pub struct PreActionCheckParams<'a> {
    pub base_url: &'a str,
    pub credential: &'a str,
    pub installation_id: &'a str,
    pub stage: Stage,
    pub trusted_key_pem: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreActionCheckBody<'a> {
    protocol_version: u32,
    installation_id: &'a str,
    stage: Stage,
}

/// Asks whether it's still safe to act right now -- narrower than a full
/// check-in, no state sync. Any failure fails open: the caller proceeds
/// exactly as it would under a `current` Policy.
pub async fn check_pre_action(
    client: &reqwest::Client,
    params: PreActionCheckParams<'_>,
    log: &(impl Fn(&str) + ?Sized),
) -> Option<Policy> {
    let body = PreActionCheckBody {
        protocol_version: PROTOCOL_VERSION,
        installation_id: params.installation_id,
        stage: params.stage,
    };

    let parsed = post_and_validate_envelope(
        client,
        RequestEnvelopeParams {
            base_url: params.base_url,
            path: "/pre-action-check",
            credential: params.credential,
            timeout: PRE_ACTION_CHECK_TIMEOUT,
            operation: "pre-action-check",
        },
        &body,
        log,
    )
    .await?;

    verify_and_parse_policy(&parsed, params.trusted_key_pem)
}
