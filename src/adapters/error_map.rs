use crate::types::{Phase, UpdateFailureMode};
use tauri_plugin_updater::Error as UpdaterError;

/// Maps every real variant of `tauri_plugin_updater::Error` to the
/// canonical taxonomy, per the V1 spec's Tauri mapping table.
pub fn map_updater_error(error: &UpdaterError) -> (UpdateFailureMode, Phase, &'static str) {
    use UpdateFailureMode::*;
    match error {
        UpdaterError::EmptyEndpoints => (ManifestUnreachable, Phase::Check, "EmptyEndpoints"),
        UpdaterError::Io(_) => (DownloadTransportFailed, Phase::Download, "Io"),
        UpdaterError::Semver(_) => (VersionInvalid, Phase::Check, "Semver"),
        UpdaterError::Serialization(_) => (ManifestParseFailed, Phase::Check, "Serialization"),
        UpdaterError::ReleaseNotFound => (ManifestUnreachable, Phase::Check, "ReleaseNotFound"),
        UpdaterError::UnsupportedArch => (PlatformUnsupported, Phase::Config, "UnsupportedArch"),
        UpdaterError::UnsupportedOs => (PlatformUnsupported, Phase::Config, "UnsupportedOs"),
        UpdaterError::FailedToDetermineExtractPath => {
            (ConfigInvalid, Phase::Config, "FailedToDetermineExtractPath")
        }
        UpdaterError::UrlParse(_) => (ConfigInvalid, Phase::Config, "UrlParse"),
        UpdaterError::Reqwest(_) => (DownloadTransportFailed, Phase::Download, "Reqwest"),
        UpdaterError::TargetNotFound(_) => {
            (TargetNotFoundInManifest, Phase::Download, "TargetNotFound")
        }
        UpdaterError::TargetsNotFound(_) => {
            (TargetNotFoundInManifest, Phase::Download, "TargetsNotFound")
        }
        UpdaterError::Network(_) => (DownloadTransportFailed, Phase::Download, "Network"),
        UpdaterError::Minisign(_) => (SignatureInvalid, Phase::Verify, "Minisign"),
        UpdaterError::Base64(_) => (SignatureInvalid, Phase::Verify, "Base64"),
        UpdaterError::SignatureUtf8(_) => (SignatureInvalid, Phase::Verify, "SignatureUtf8"),
        UpdaterError::TempDirNotOnSameMountPoint => (
            TempStorageConstraint,
            Phase::Extract,
            "TempDirNotOnSameMountPoint",
        ),
        UpdaterError::PreviousAppNotRestored(_) => {
            (InstallWriteFailed, Phase::Install, "PreviousAppNotRestored")
        }
        UpdaterError::BinaryNotFoundInArchive => (
            TargetNotFoundInManifest,
            Phase::Download,
            "BinaryNotFoundInArchive",
        ),
        UpdaterError::TempDirNotFound => (TempStorageConstraint, Phase::Extract, "TempDirNotFound"),
        UpdaterError::AuthenticationFailed => (
            InstallAuthRequiredOrFailed,
            Phase::Install,
            "AuthenticationFailed",
        ),
        UpdaterError::DebInstallFailed => (InstallWriteFailed, Phase::Install, "DebInstallFailed"),
        UpdaterError::PackageInstallFailed => {
            (InstallWriteFailed, Phase::Install, "PackageInstallFailed")
        }
        UpdaterError::InvalidUpdaterFormat => {
            (ExtractionFailed, Phase::Extract, "InvalidUpdaterFormat")
        }
        UpdaterError::Http(_) => (DownloadTransportFailed, Phase::Download, "Http"),
        UpdaterError::InvalidHeaderValue(_) => (
            DownloadTransportFailed,
            Phase::Download,
            "InvalidHeaderValue",
        ),
        UpdaterError::InvalidHeaderName(_) => (
            DownloadTransportFailed,
            Phase::Download,
            "InvalidHeaderName",
        ),
        UpdaterError::FormatDate => (ConfigInvalid, Phase::Config, "FormatDate"),
        UpdaterError::InsecureTransportProtocol => {
            (ConfigInvalid, Phase::Config, "InsecureTransportProtocol")
        }
        UpdaterError::SignedVersionMismatch { .. } => (
            SignatureVersionMismatch,
            Phase::Verify,
            "SignedVersionMismatch",
        ),
        UpdaterError::MissingSignedVersion => (
            SignatureVersionMismatch,
            Phase::Verify,
            "MissingSignedVersion",
        ),
        UpdaterError::Tauri(_) => (Unknown, Phase::Unknown, "Tauri"),
        // `Extract` only exists under `cfg(windows, feature = "zip")`, which
        // this crate doesn't enable -- the wildcard below covers it (and any
        // future added variant) without needing that cfg combination here.
        #[allow(unreachable_patterns)]
        _ => (Unknown, Phase::Unknown, "unknown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use UpdateFailureMode::*;

    #[test]
    fn maps_a_real_differential_patch_incompatibility() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::InvalidUpdaterFormat);
        assert_eq!(mode, ExtractionFailed);
        assert_eq!(phase, Phase::Extract);
    }

    #[test]
    fn a_transport_error_classifies_as_download_regardless_of_when_it_occurs() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::Network("x".to_string()));
        assert_eq!(mode, DownloadTransportFailed);
        assert_eq!(phase, Phase::Download);
    }

    #[test]
    fn a_missing_signed_version_classifies_as_a_version_mismatch_not_plain_invalid() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::MissingSignedVersion);
        assert_eq!(mode, SignatureVersionMismatch);
        assert_eq!(phase, Phase::Verify);
    }

    #[test]
    fn an_opaque_tauri_error_falls_back_to_unknown_rather_than_misclassifying() {
        let (mode, phase, platform_code) = map_updater_error(&UpdaterError::Tauri(
            tauri::Error::AssetNotFound(String::new()),
        ));
        assert_eq!(mode, Unknown);
        assert_eq!(phase, Phase::Unknown);
        assert_eq!(platform_code, "Tauri");
    }

    #[test]
    fn platform_code_is_always_the_real_variant_name_never_a_message_string() {
        let (_, _, platform_code) = map_updater_error(&UpdaterError::UnsupportedOs);
        assert_eq!(platform_code, "UnsupportedOs");
    }

    #[test]
    fn unsupported_platform_classifies_as_config_not_check() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::UnsupportedArch);
        assert_eq!(mode, PlatformUnsupported);
        assert_eq!(phase, Phase::Config);

        let (mode, phase, _) = map_updater_error(&UpdaterError::UnsupportedOs);
        assert_eq!(mode, PlatformUnsupported);
        assert_eq!(phase, Phase::Config);
    }

    #[test]
    fn a_failed_extract_path_classifies_as_config_invalid_not_extraction_failed() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::FailedToDetermineExtractPath);
        assert_eq!(mode, ConfigInvalid);
        assert_eq!(phase, Phase::Config);
    }

    #[test]
    fn a_missing_manifest_target_classifies_as_download_not_check_or_extract() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::TargetNotFound("x".to_string()));
        assert_eq!(mode, TargetNotFoundInManifest);
        assert_eq!(phase, Phase::Download);

        let (mode, phase, _) =
            map_updater_error(&UpdaterError::TargetsNotFound(vec!["x".to_string()]));
        assert_eq!(mode, TargetNotFoundInManifest);
        assert_eq!(phase, Phase::Download);

        let (mode, phase, _) = map_updater_error(&UpdaterError::BinaryNotFoundInArchive);
        assert_eq!(mode, TargetNotFoundInManifest);
        assert_eq!(phase, Phase::Download);
    }

    #[test]
    fn a_temp_storage_constraint_classifies_as_extract_not_download() {
        let (mode, phase, _) = map_updater_error(&UpdaterError::TempDirNotOnSameMountPoint);
        assert_eq!(mode, TempStorageConstraint);
        assert_eq!(phase, Phase::Extract);

        let (mode, phase, _) = map_updater_error(&UpdaterError::TempDirNotFound);
        assert_eq!(mode, TempStorageConstraint);
        assert_eq!(phase, Phase::Extract);
    }
}
