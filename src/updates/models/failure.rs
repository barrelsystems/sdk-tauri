use serde::{Deserialize, Serialize};

/// Which stage of the pipeline a `Failed` state's error belongs to, at the
/// coarser granularity a dashboard groups by (`Phase` is the finer one).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FailureCategory {
    /// Manifest discovery or update checking.
    Check,
    /// Artifact transfer.
    Download,
    /// Updater configuration or installation.
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
    /// Manifest discovery or update checking.
    Check,
    /// Artifact integrity or signature verification.
    Verify,
    /// Artifact transfer.
    Download,
    /// Archive extraction.
    Extract,
    /// Native installation and relaunch.
    Install,
    /// Updater configuration.
    Config,
    /// The native failure has no classified phase.
    Unknown,
}

/// Classified native failure carried in the subscription’s `last_error` field.
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
