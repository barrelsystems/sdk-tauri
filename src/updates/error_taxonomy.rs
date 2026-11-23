use crate::updates::models::failure::{FailureCategory, Phase, UpdateFailure, UpdateFailureMode};
use crate::updates::models::state::UpdateState;

fn failure_mode_title(failure_mode: UpdateFailureMode) -> &'static str {
    use UpdateFailureMode::*;
    match failure_mode {
        ManifestUnreachable => "Could not reach the update manifest.",
        ManifestParseFailed => "The update manifest could not be parsed.",
        VersionInvalid => "The update manifest's version is invalid.",
        SignatureInvalid => "The update manifest's signature is invalid.",
        SignatureVersionMismatch => "The update's signed version doesn't match the manifest.",
        DownloadTransportFailed => "The update download failed at the network layer.",
        ExtractionFailed => "The downloaded update could not be extracted.",
        TargetNotFoundInManifest => "The expected update target is missing from the manifest.",
        TempStorageConstraint => "A temporary-storage constraint blocked the update.",
        InstallWriteFailed => "The update could not be written to disk.",
        InstallAuthRequiredOrFailed => "The update install needs or failed authorization.",
        ConfigInvalid => "The updater's configuration is invalid.",
        PlatformUnsupported => "This update isn't supported on the current platform.",
        ApplyNotConfirmed => {
            "The app quit to apply an update, but never came back up on the new version."
        }
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
) -> (UpdateFailure, UpdateState) {
    let failure = UpdateFailure {
        phase,
        failure_mode,
        title: failure_mode_title(failure_mode).to_string(),
        platform: platform.to_string(),
        platform_code: platform_code.to_string(),
    };
    let state = UpdateState::Failed {
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
            SignatureInvalid,
            SignatureVersionMismatch,
            DownloadTransportFailed,
            ExtractionFailed,
            TargetNotFoundInManifest,
            TempStorageConstraint,
            InstallWriteFailed,
            InstallAuthRequiredOrFailed,
            ConfigInvalid,
            PlatformUnsupported,
            ApplyNotConfirmed,
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
    fn builds_a_failure_and_a_matching_failed_update_state_from_one_classification() {
        let (failure, state) = build_failure_state(
            "tauri-updater",
            UpdateFailureMode::DownloadTransportFailed,
            Phase::Download,
            "download-transport-failed",
        );

        assert_eq!(failure.phase, Phase::Download);
        assert_eq!(
            failure.failure_mode,
            UpdateFailureMode::DownloadTransportFailed
        );
        assert_eq!(failure.platform, "tauri-updater");
        assert_eq!(failure.platform_code, "download-transport-failed");
        assert_eq!(
            state,
            UpdateState::Failed {
                category: FailureCategory::Download,
                code: UpdateFailureMode::DownloadTransportFailed,
                platform_code: "download-transport-failed".to_string(),
            }
        );
    }
}
