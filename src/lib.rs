mod adapter;
mod attributes;
mod cadence;
mod env;
mod error_taxonomy;
mod errors;
mod host_control;
mod internal_defects;
mod jitter;
mod keys;
mod persistence;
mod protocol;
mod types;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use tauri::{plugin::TauriPlugin, AppHandle, Manager, RunEvent, Runtime, WindowEvent};

use adapter::Adapter;
use cadence::{start_cadence, CadenceShared};
use host_control::HostControl;
use persistence::buffered_facts::BufferedFactsStore;
use persistence::installation_id::InstallationIdStore;
use persistence::key_rotation::TrustedKeyStore;

pub use errors::BarrelConfigError;
pub use types::{AdapterState, InitConfig, Policy, UpdateFailure, UpdateFailureMode};

static POLICY: Mutex<Option<Policy>> = Mutex::new(None);
static SHARED: OnceLock<Arc<dyn std::any::Any + Send + Sync>> = OnceLock::new();
static FOCUS_TRIGGER: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();

const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_HOLD_CACHE_MAX_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Synchronous getter. `None` before the first successful check-in --
/// never a synthesized default.
pub fn get_policy() -> Option<Policy> {
    POLICY.lock().unwrap().clone()
}

/// Synchronous getter. `None` if nothing has failed this session.
pub fn get_last_update_error<R: Runtime>(app: &AppHandle<R>) -> Option<UpdateFailure> {
    shared_for::<R>(app).and_then(|shared| shared.adapter.last_error())
}

/// Synchronous getter. `None` before the plugin's `setup` hook has run.
pub fn get_adapter_state<R: Runtime>(app: &AppHandle<R>) -> Option<AdapterState> {
    shared_for::<R>(app).map(|shared| shared.adapter.state())
}

/// Fires on every local adapter-state transition, in addition to -- never
/// instead of -- `get_adapter_state()`'s own polling. `None` before the
/// plugin's `setup` hook has run; call again once it has.
pub fn subscribe_adapter_state<R: Runtime>(
    app: &AppHandle<R>,
) -> Option<tokio::sync::watch::Receiver<AdapterState>> {
    shared_for::<R>(app).map(|shared| shared.adapter.subscribe_state())
}

/// Host-triggered earlier apply, ahead of the natural-quit default.
pub fn apply_update<R: Runtime>(app: &AppHandle<R>) {
    let Some(shared) = shared_for::<R>(app) else {
        return;
    };
    tauri::async_runtime::spawn(async move {
        let (base_url, trusted_key_pem) =
            cadence::resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
        let ctx = host_control::ActionContext {
            client: &shared.client,
            base_url: &base_url,
            credential: &shared.config.credential,
            installation_id: &shared.installation_id,
            trusted_key_pem: &trusted_key_pem,
            log: shared.log.as_ref(),
        };
        shared
            .host_control
            .apply_update(&ctx, &shared.adapter)
            .await;
    });
}

/// Host-triggered earlier download, for a host that set `auto_download:
/// false` and otherwise had no way to ever complete an update.
pub fn download_update<R: Runtime>(app: &AppHandle<R>) {
    let Some(shared) = shared_for::<R>(app) else {
        return;
    };
    tauri::async_runtime::spawn(async move {
        let (base_url, trusted_key_pem) =
            cadence::resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
        let ctx = host_control::ActionContext {
            client: &shared.client,
            base_url: &base_url,
            credential: &shared.config.credential,
            installation_id: &shared.installation_id,
            trusted_key_pem: &trusted_key_pem,
            log: shared.log.as_ref(),
        };
        shared
            .host_control
            .download_update(&ctx, &shared.adapter, &shared.facts, &shared.facts_path)
            .await;
    });
}

fn shared_for<R: Runtime>(_app: &AppHandle<R>) -> Option<Arc<CadenceShared<R>>> {
    SHARED.get()?.clone().downcast::<CadenceShared<R>>().ok()
}

/// Call `.barrel_kit(config)` on your `tauri::Builder` instead of adding
/// this plugin directly -- full-wrap means `tauri-plugin-updater` is this
/// crate's own implementation detail, not something the host registers.
pub trait BarrelKitExt<R: Runtime> {
    fn barrel_kit(self, config: InitConfig) -> Self;
}

impl<R: Runtime> BarrelKitExt<R> for tauri::Builder<R> {
    fn barrel_kit(self, config: InitConfig) -> Self {
        self.plugin(tauri_plugin_updater::Builder::new().build())
            .plugin(plugin(config))
    }
}

fn plugin<R: Runtime>(config: InitConfig) -> TauriPlugin<R> {
    tauri::plugin::Builder::new("barrel-kit")
        .setup(move |app, _api| {
            if config.credential.trim().is_empty() {
                return Err(Box::new(BarrelConfigError(
                    "Barrel: init() requires a non-empty `credential`.".to_string(),
                )));
            }
            if let Err(err) = validate_hold_cache_max_age(config.hold_cache_max_age_ms) {
                return Err(Box::new(err));
            }

            let app_handle = app.clone();
            let config = config.clone();
            tauri::async_runtime::spawn(async move {
                bootstrap(app_handle, config).await;
            });

            Ok(())
        })
        .on_event(|app, event| match event {
            RunEvent::ExitRequested { api, .. } => {
                if let Some(shared) = shared_for::<R>(app) {
                    if shared.host_control.has_pending_install() {
                        api.prevent_exit();
                        let app = app.clone();
                        tauri::async_runtime::spawn(async move {
                            apply_update(&app);
                        });
                    }
                }
            }
            // The final, un-cancellable exit -- `ExitRequested` above can
            // still be prevented, so a fact recorded right before exit
            // needs this synchronous, guaranteed-to-run fallback.
            RunEvent::Exit => {
                if let Some(shared) = shared_for::<R>(app) {
                    shared.facts.flush_sync(&shared.facts_path);
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
            return Err(BarrelConfigError(
                "Barrel: init() config's `hold_cache_max_age_ms` must not exceed 7 days."
                    .to_string(),
            ));
        }
    }
    Ok(())
}

async fn bootstrap<R: Runtime>(app: AppHandle<R>, config: InitConfig) {
    // A second `barrel_kit()` call must be a silent no-op, never a second
    // `Adapter`/cadence loop double-driving the native updater --
    // `SHARED.set()` alone doesn't prevent that, so this check comes first.
    if SHARED.get().is_some() {
        return;
    }

    let log: Box<dyn Fn(&str) + Send + Sync> = if config.debug {
        Box::new(|message: &str| eprintln!("[barrel-kit] {message}"))
    } else {
        Box::new(|_: &str| {})
    };

    let data_dir = app
        .path()
        .app_data_dir()
        .unwrap_or_else(|_| PathBuf::from("."));
    let installation_id_path = data_dir.join("barrel-installation.json");
    let facts_path = data_dir.join("barrel-buffered-facts.json");
    let trusted_key_path = data_dir.join("barrel-trusted-key.json");

    let installation_ids = InstallationIdStore::new();
    let installation_id = installation_ids.get_or_create(&installation_id_path).await;

    let facts = BufferedFactsStore::new();
    facts.configure(&facts_path).await;

    let trusted_keys = TrustedKeyStore::new();
    trusted_keys
        .get_or_create(&trusted_key_path, keys::PUBLIC_ED25519_KEY_PEM)
        .await;

    let client = reqwest::Client::new();

    let adapter = Adapter::new();
    if let Some(url) = config.update_feed_url.clone() {
        adapter.set_feed_url(url);
    }

    let shared = Arc::new(CadenceShared {
        app: app.clone(),
        client,
        adapter,
        facts,
        facts_path,
        trusted_keys,
        trusted_key_path,
        host_control: HostControl::new(),
        installation_id,
        config,
        sdk_version: SDK_VERSION.to_string(),
        log,
        in_flight: std::sync::atomic::AtomicBool::new(false),
    });

    if SHARED
        .set(shared.clone() as Arc<dyn std::any::Any + Send + Sync>)
        .is_err()
    {
        return; // Lost the race to a concurrent call -- let that one proceed instead.
    }

    let focus_trigger = start_cadence(shared);
    let _ = FOCUS_TRIGGER.set(focus_trigger.clone());

    // `on_window_event` is per-window, not app-wide -- every window this
    // app already has (and the plugin only ever runs after app setup, so
    // the main window already exists) gets the same focus listener.
    for window in app.webview_windows().values() {
        let focus_trigger = focus_trigger.clone();
        window.on_window_event(move |event| {
            if let WindowEvent::Focused(true) = event {
                focus_trigger.notify_one();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
