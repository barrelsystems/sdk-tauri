use serde::Serialize;
use std::time::Duration;

use super::protocol_request::{post_protocol_request, PostProtocolRequestParams};
use super::update_eligibility_verification::verify_and_parse_update_eligibility;
use crate::updates::models::eligibility::UpdateEligibility;
use crate::updates::protocol::wire_types::BackendConnection;
use crate::updates::protocol::wire_types::PROTOCOL_VERSION;

const PRE_ACTION_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// Which point in the update lifecycle a pre-action check guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    /// Before downloading an available update.
    PreDownload,
    /// Before applying a downloaded update.
    PreApply,
}

/// Parameters for one `check_pre_action` call.
#[derive(Debug)]
pub struct PreActionCheckParams<'a> {
    /// Where this check is sent, and how it authenticates.
    pub connection: BackendConnection<'a>,
    /// Which lifecycle point this check guards.
    pub stage: Stage,
    /// The key this response's signature is verified against.
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
/// exactly as it would under an `unrestricted` eligibility decision.
pub async fn check_pre_action(
    client: &reqwest::Client,
    params: PreActionCheckParams<'_>,
    log: &(impl Fn(&str) + ?Sized),
) -> Option<UpdateEligibility> {
    let body = PreActionCheckBody {
        protocol_version: PROTOCOL_VERSION,
        installation_id: params.connection.installation_id,
        stage: params.stage,
    };

    let parsed = post_protocol_request(
        client,
        PostProtocolRequestParams {
            base_url: params.connection.base_url,
            path: "/pre-action-check",
            credential: params.connection.credential,
            timeout: PRE_ACTION_CHECK_TIMEOUT,
            operation: "pre-action-check",
        },
        &body,
        log,
    )
    .await?;

    verify_and_parse_update_eligibility(&parsed, params.trusted_key_pem)
}
