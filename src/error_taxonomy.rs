use crate::types::{AdapterState, FailureCategory, Phase, UpdateFailure, UpdateFailureMode};

fn failure_mode_title(failure_mode: UpdateFailureMode) -> &'static str {
    use UpdateFailureMode::*;
    match failure_mode {
        ManifestUnreachable => "Could not reach the update manifest.",
        ManifestParseFailed => "The update manifest could not be parsed.",
        VersionInvalid => "The update manifest's version is invalid.",
        SignatureMissing => "The update manifest has no signature.",
        SignatureInvalid => "The update manifest's signature is invalid.",
        SignatureVersionMismatch => "The update's signed version doesn't match the manifest.",
        ChecksumMismatch => "The downloaded update failed its checksum check.",
        DownloadTransportFailed => "The update download failed at the network layer.",
        DownloadFileInvalid => "The downloaded update file is invalid.",
        DownloadSizeExceeded => "The update download exceeded its expected size.",
        DownloadStreamIncomplete => "The update download stream ended early.",
        DiffPatchIncompatible => "The differential update patch is incompatible.",
        ExtractionFailed => "The downloaded update could not be extracted.",
        TargetNotFoundInManifest => "The expected update target is missing from the manifest.",
        TempStorageConstraint => "A temporary-storage constraint blocked the update.",
        InstallWriteFailed => "The update could not be written to disk.",
        InstallAuthRequiredOrFailed => "The update install needs or failed authorization.",
        InstallDowngradeBlocked => "The update was blocked as a version downgrade.",
        InstallRelaunchFailed => "The app could not relaunch after installing the update.",
        InstallMissingUpdate => "The downloaded update file is missing at install time.",
        ConfigInvalid => "The updater's configuration is invalid.",
        CorsMisconfigured => "The update server's CORS headers are misconfigured.",
        PlatformUnsupported => "This update isn't supported on the current platform.",
        TranslocationBlocked => "macOS App Translocation blocked the update.",
        WebkitTerminated => "The updater's WebKit process terminated unexpectedly.",
        ReleaseNotesFailed => "The update's release notes could not be loaded.",
        ResumeAppcastFailed => "Resuming the update feed after a prior failure didn't work.",
        Unknown => "An unclassified update failure occurred.",
    }
}

/// Mechanical, fixed derivation -- never a second independent classification.
fn category_for_phase(phase: Phase) -> FailureCategory {
    match phase {
        Phase::Check => FailureCategory::Check,
        Phase::Verify | Phase::Download | Phase::Extract => FailureCategory::Download,
        _ => FailureCategory::Updater,
    }
}

pub fn build_failure_state(
    platform: &str,
    failure_mode: UpdateFailureMode,
    phase: Phase,
    platform_code: &str,
) -> (UpdateFailure, AdapterState) {
    let failure = UpdateFailure {
        phase,
        failure_mode,
        title: failure_mode_title(failure_mode).to_string(),
        platform: platform.to_string(),
        platform_code: platform_code.to_string(),
    };
    let state = AdapterState::Failed {
        category: category_for_phase(phase),
        code: failure_mode,
        platform_code: platform_code.to_string(),
    };
    (failure, state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_failure_mode_resolves_a_non_empty_title() {
        use UpdateFailureMode::*;
        for mode in [
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
        ] {
            assert!(!failure_mode_title(mode).is_empty());
        }
    }

    #[test]
    fn check_phase_categorizes_as_check() {
        assert_eq!(category_for_phase(Phase::Check), FailureCategory::Check);
    }

    #[test]
    fn verify_download_and_extract_phases_all_categorize_as_download() {
        for phase in [Phase::Verify, Phase::Download, Phase::Extract] {
            assert_eq!(category_for_phase(phase), FailureCategory::Download);
        }
    }

    #[test]
    fn install_config_and_unknown_phases_categorize_as_updater() {
        for phase in [Phase::Install, Phase::Config, Phase::Unknown] {
            assert_eq!(category_for_phase(phase), FailureCategory::Updater);
        }
    }

    #[test]
    fn builds_a_failure_and_a_matching_failed_adapter_state_from_one_classification() {
        let (failure, state) = build_failure_state(
            "tauri-updater",
            UpdateFailureMode::ChecksumMismatch,
            Phase::Download,
            "checksum-mismatch",
        );

        assert_eq!(failure.phase, Phase::Download);
        assert_eq!(failure.failure_mode, UpdateFailureMode::ChecksumMismatch);
        assert_eq!(failure.platform, "tauri-updater");
        assert_eq!(failure.platform_code, "checksum-mismatch");
        assert_eq!(
            state,
            AdapterState::Failed {
                category: FailureCategory::Download,
                code: UpdateFailureMode::ChecksumMismatch,
                platform_code: "checksum-mismatch".to_string(),
            }
        );
    }
}
