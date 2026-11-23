//! Barrel's release-intelligence SDK for Tauri apps. Wraps
//! `tauri-plugin-updater`, reporting every check and update attempt back
//! to Barrel so you can see what's actually happening on your users' machines.
#![warn(missing_docs, missing_debug_implementations)]

mod errors;
mod init_config;
/// Conformance-only environment and native updater observations.
#[cfg(feature = "test-service")]
pub mod test_service;
/// Update status and manual controls -- namespaced rather than living at
/// the crate root, since a future Barrel module (telemetry, etc.) would
/// otherwise collide with generic names like `subscribe`/`download`.
pub mod updates;

use tauri::{plugin::TauriPlugin, Manager, RunEvent, Runtime};

use updates::runtime::{bootstrap, finish_quit_deferral, handle_window_event, shared_for};

const MAX_HOLD_CACHE_MAX_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

pub use errors::BarrelConfigError;
pub use init_config::InitConfig;
/// Bundles `base_url`/`credential`/`installation_id` for the protocol
/// calls this crate's own test service drives directly -- public only
/// under `test-service`, same as `protocol` itself.
#[cfg(feature = "test-service")]
pub use updates::protocol::wire_types::BackendConnection;

/// Call `.barrel_kit(config)` on your `tauri::Builder` instead of adding
/// this plugin directly -- full-wrap means `tauri-plugin-updater` is this
/// crate's own implementation detail, not something the host registers.
pub trait BarrelKitExt<R: Runtime> {
    /// Wires Barrel's updater into this builder. Call once, before `.build()`.
    fn barrel_kit(self, config: InitConfig) -> Self;
}

impl<R: Runtime> BarrelKitExt<R> for tauri::Builder<R> {
    fn barrel_kit(self, config: InitConfig) -> Self {
        let mut updater_builder = tauri_plugin_updater::Builder::new();
        if let Some(artifact_public_key) = config.updates.artifact_public_key.clone() {
            updater_builder = updater_builder.pubkey(artifact_public_key);
        }
        self.plugin(updater_builder.build())
            .plugin(tauri_plugin_store::Builder::default().build())
            .plugin(plugin(config))
    }
}

fn plugin<R: Runtime>(config: InitConfig) -> TauriPlugin<R> {
    tauri::plugin::Builder::new("barrel-kit")
        .setup(move |app, _api| {
            // A second `barrel_kit()` call must be a silent no-op, never a
            // validation error -- checked synchronously here, before any
            // config validation runs.

            // This way a second call with e.g. a blank credential still
            // no-ops instead of throwing.
            if app
                .try_state::<std::sync::Arc<updates::adapters::Adapter>>()
                .is_some()
            {
                return Ok(());
            }

            if config.credential.trim().is_empty() {
                return Err(Box::new(BarrelConfigError::new(
                    "Barrel: init() requires a non-empty `credential`.",
                )));
            }
            if let Err(err) = validate_hold_cache_max_age(config.updates.hold_cache_max_age_ms) {
                return Err(Box::new(err));
            }

            let adapter = std::sync::Arc::new(updates::adapters::Adapter::new());
            app.manage(adapter.clone());
            let app_handle = app.clone();
            let config = config.clone();
            tauri::async_runtime::spawn(async move {
                bootstrap(app_handle, config, adapter).await;
            });

            Ok(())
        })
        .on_event(|app, event| match event {
            RunEvent::WindowEvent { event, .. } => handle_window_event(event),
            RunEvent::ExitRequested { api, .. } => {
                if let Some(shared) = shared_for::<R>(app) {
                    if shared.host_control.has_pending_install() {
                        api.prevent_exit();
                        tauri::async_runtime::spawn(finish_quit_deferral(app.clone()));
                    }
                }
            }
            // The final, un-cancellable exit -- `ExitRequested` above can
            // still be prevented, so a fact recorded right before exit
            // needs this synchronous, guaranteed-to-run fallback.
            RunEvent::Exit => {
                if let Some(shared) = shared_for::<R>(app) {
                    shared
                        .installation_ids
                        .flush_sync(&shared.installation_id_path);
                    shared.facts.flush_sync();
                }
            }
            _ => {}
        })
        .build()
}

fn validate_hold_cache_max_age(
    hold_cache_max_age_ms: Option<u64>,
) -> Result<(), BarrelConfigError> {
    if let Some(ms) = hold_cache_max_age_ms {
        if ms > MAX_HOLD_CACHE_MAX_AGE_MS {
            return Err(BarrelConfigError::new(
                "Barrel: init() config's `hold_cache_max_age_ms` must not exceed 7 days.",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use updates::persistence::buffered_facts::BufferedFactsStore;
    use updates::persistence::version_expectation::VersionExpectationStore;
    use updates::runtime::check_version_expectation;

    #[test]
    fn plugin_setup_rejects_invalid_configuration_before_bootstrap() {
        for config in [
            InitConfig {
                credential: String::new(),
                ..Default::default()
            },
            InitConfig {
                credential: "  ".into(),
                ..Default::default()
            },
            InitConfig {
                credential: "cred".into(),
                updates: crate::updates::Config {
                    hold_cache_max_age_ms: Some(MAX_HOLD_CACHE_MAX_AGE_MS + 1),
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            let result = tauri::test::mock_builder()
                .plugin(plugin(config))
                .build(tauri::test::mock_context(tauri::test::noop_assets()));

            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("invalid configuration must fail before bootstrap"),
            };
            assert!(error.to_string().contains("Barrel:"));
        }
    }

    #[test]
    fn no_hold_cache_max_age_configured_passes() {
        assert!(validate_hold_cache_max_age(None).is_ok());
    }

    #[test]
    fn a_hold_cache_max_age_within_seven_days_passes() {
        assert!(validate_hold_cache_max_age(Some(MAX_HOLD_CACHE_MAX_AGE_MS)).is_ok());
    }

    #[test]
    fn a_hold_cache_max_age_above_seven_days_is_rejected() {
        assert!(validate_hold_cache_max_age(Some(MAX_HOLD_CACHE_MAX_AGE_MS + 1)).is_err());
    }

    fn unused_temp_path() -> std::path::PathBuf {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn check_version_expectation_clears_the_marker_silently_on_a_match() {
        let app = updates::persistence::test_support::mock_app();
        let running_version = app.package_info().version.to_string();
        let facts = BufferedFactsStore::new(app.handle().clone());
        let version_expectations = VersionExpectationStore::new(app.handle().clone());
        let path = unused_temp_path();
        version_expectations.set(&path, &running_version).await;

        check_version_expectation(app.handle(), &facts, &version_expectations, &path).await;

        assert_eq!(version_expectations.read(&path).await, None);
        assert!(facts.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn check_version_expectation_records_apply_not_confirmed_on_a_mismatch() {
        let app = updates::persistence::test_support::mock_app();
        let running_version = app.package_info().version.to_string();
        let facts = BufferedFactsStore::new(app.handle().clone());
        let version_expectations = VersionExpectationStore::new(app.handle().clone());
        let path = unused_temp_path();
        version_expectations
            .set(&path, &format!("{running_version}-not-running"))
            .await;

        check_version_expectation(app.handle(), &facts, &version_expectations, &path).await;

        assert_eq!(version_expectations.read(&path).await, None);
        let snapshot = facts.snapshot().await;
        assert_eq!(snapshot.len(), 1);
    }
}
