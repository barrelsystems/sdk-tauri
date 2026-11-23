use super::failure::{FailureCategory, UpdateFailureMode};
use serde::{Deserialize, Serialize};

/// Native lifecycle state carried by each subscription snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum UpdateState {
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
    /// The update is being applied -- set immediately before the native
    /// install/relaunch call, since that call can tear this process down
    /// with no further chance to report anything.
    #[serde(rename = "applying")]
    Applying,
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

impl UpdateState {
    /// `Checking` is local SDK activity, never a fact the backend learns
    /// about -- live, buffered-facts history, or otherwise.
    pub(crate) fn is_reportable(&self) -> bool {
        !matches!(self, UpdateState::Checking)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_checking_is_not_reportable() {
        assert!(!UpdateState::Checking.is_reportable());
        assert!(UpdateState::Idle.is_reportable());
        assert!(UpdateState::Downloaded.is_reportable());
        assert!(UpdateState::Applying.is_reportable());
        assert!(UpdateState::UpdateCancelled.is_reportable());
    }
}
