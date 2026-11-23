use serde::Serialize;

use super::eligibility::UpdateEligibility;
use super::failure::UpdateFailure;
use super::state::UpdateState;

/// Complete local update status delivered by subscriptions.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// Includes `Checking`, for a host app's own UI -- never what a
    /// check-in reports.
    pub state: UpdateState,
    /// `None` before the first successful check-in -- never a synthesized
    /// default.
    pub eligibility: Option<UpdateEligibility>,
    /// `None` only if nothing's failed this session, and no buffered
    /// `updateError` fact survives from a prior one.
    pub last_error: Option<UpdateFailure>,
}
