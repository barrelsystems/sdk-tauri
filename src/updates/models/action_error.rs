/// A manual action could not complete; native failures retain their classification.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "code", content = "failure", rename_all = "camelCase")]
pub enum ActionError {
    /// The SDK has not finished initializing.
    NotReady,
    /// No available update can be downloaded.
    NoUpdateAvailable,
    /// No verified download is ready to apply.
    NothingDownloadedYet,
    /// Another action is already running.
    AlreadyInProgress,
    /// A fresh verified Hold denied this action.
    Held,
    /// The download was cancelled.
    Cancelled,
    /// A native action failed, with its classification when available.
    Failed(Option<super::failure::UpdateFailure>),
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Update action: {self:?}")
    }
}

impl std::error::Error for ActionError {}
