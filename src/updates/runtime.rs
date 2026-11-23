use super::adapters::Adapter;
use super::cadence::{send_check_in_now, start_cadence};
use super::host_control::{ActionContext, HostControl};
use super::persistence::buffered_facts::BufferedFactsStore;
use super::persistence::installation_id::InstallationIdStore;
use super::persistence::key_rotation::TrustedKeyStore;
use super::persistence::version_expectation::VersionExpectationStore;
use super::protocol::wire_types::BackendConnection;
#[cfg(feature = "test-service")]
use crate::test_service;
use crate::InitConfig;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, OnceLock};
use tauri::{AppHandle, Manager, Runtime, WindowEvent};

pub struct UpdateRuntime<R: Runtime> {
    pub app: AppHandle<R>,
    pub client: reqwest::Client,
    pub adapter: Arc<Adapter>,
    pub facts: BufferedFactsStore<R>,
    pub trusted_keys: TrustedKeyStore<R>,
    pub trusted_key_path: std::path::PathBuf,
    pub version_expectations: VersionExpectationStore<R>,
    pub version_expectation_path: std::path::PathBuf,
    pub host_control: HostControl,
    pub installation_id: String,
    pub installation_ids: crate::updates::persistence::installation_id::InstallationIdStore<R>,
    pub installation_id_path: std::path::PathBuf,
    pub config: InitConfig,
    pub sdk_version: String,
    pub log: Box<dyn Fn(&str) + Send + Sync>,
    /// A fresh tick while one is already in flight is skipped, not queued.
    pub in_flight: AtomicBool,
    pub last_attempted_revision: AtomicU64,
    /// Serializes vendor checks and automatic downloads; busy native work
    /// does not suppress independently guarded heartbeat sends.
    pub tick_lock: tokio::sync::Mutex<()>,
    /// Serialize normal sends independently of long-running updater actions; applying retains
    /// its bounded bypass.
    pub check_in_lock: tokio::sync::Mutex<()>,
}

impl<R: Runtime> UpdateRuntime<R> {
    /// Resolve the current connection fields before building the action
    /// context, including any key rotation from the last check-in.
    pub(crate) fn action_context<'a>(
        &'a self,
        base_url: &'a str,
        trusted_key_pem: &'a str,
    ) -> ActionContext<'a> {
        ActionContext {
            client: &self.client,
            connection: BackendConnection {
                base_url,
                credential: &self.config.credential,
                installation_id: &self.installation_id,
            },
            trusted_key_pem,
            log: self.log.as_ref(),
            // This lazy future runs only before apply and bypasses the heartbeat
            // guard so an unrelated request cannot suppress it.
            sync_before_apply: Box::pin(send_check_in_now(self, base_url, trusted_key_pem)),
        }
    }
}

static SHARED: OnceLock<Arc<dyn std::any::Any + Send + Sync>> = OnceLock::new();
static FOCUS_TRIGGER: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();

const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Share the classified apply result between manual actions and natural-quit coordination.
pub(crate) async fn perform_apply<R: Runtime>(
    shared: Arc<UpdateRuntime<R>>,
) -> Result<(), super::ActionError> {
    let (base_url, trusted_key_pem) =
        super::cadence::resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
    let ctx = shared.action_context(&base_url, &trusted_key_pem);
    let result = shared
        .host_control
        .apply_update(
            ctx,
            &shared.adapter,
            &shared.facts,
            &shared.version_expectations,
            &shared.version_expectation_path,
        )
        .await;
    #[cfg(feature = "test-service")]
    if test_service::runtime::directory().is_some() {
        return result;
    }

    // Windows exits through its installer; other desktop platforms need an
    // explicit restart request after the verified artifact was installed.
    #[cfg(not(target_os = "windows"))]
    if result.is_ok() {
        shared.app.request_restart();
    }

    result
}

/// An install denied during ordinary quit must complete the already-requested exit without
/// installing.
pub(crate) async fn finish_quit_deferral<R: Runtime>(app: AppHandle<R>) {
    let Some(shared) = shared_for::<R>(&app) else {
        return;
    };
    shared.host_control.wait_until_idle().await;
    if !matches!(shared.adapter.state(), super::State::Downloaded)
        || perform_apply(shared.clone()).await.is_err()
    {
        shared.host_control.clear_pending_install();
        app.exit(0);
    }
}

pub(crate) fn shared_for<R: Runtime>(_app: &AppHandle<R>) -> Option<Arc<UpdateRuntime<R>>> {
    SHARED.get()?.clone().downcast::<UpdateRuntime<R>>().ok()
}

/// Resolves what the previous session's apply attempt, if any,
/// expected to be running by now against what's actually running, then
/// clears the marker either way so this only ever fires once per attempt.
pub(crate) async fn check_version_expectation<R: Runtime>(
    app: &AppHandle<R>,
    facts: &BufferedFactsStore<R>,
    version_expectations: &VersionExpectationStore<R>,
    path: &std::path::Path,
) {
    let Some(expected_version) = version_expectations.read(path).await else {
        return;
    };

    let running_version = app.package_info().version.to_string();
    if expected_version != running_version {
        let (failure, _) = super::error_taxonomy::build_failure_state(
            "version-check",
            crate::updates::models::failure::UpdateFailureMode::ApplyNotConfirmed,
            crate::updates::models::failure::Phase::Install,
            &format!("expected {expected_version}, running {running_version}"),
        );
        facts.record_update_error(failure).await;
    }

    version_expectations.clear(path).await;
}

pub(crate) async fn bootstrap<R: Runtime>(
    app: AppHandle<R>,
    config: InitConfig,
    adapter: Arc<Adapter>,
) {
    // A second `barrel_kit()` call must be a silent no-op, never a second
    // `Adapter`/cadence loop double-driving the native updater --
    // `SHARED.set()` alone doesn't prevent that, so this check comes first.
    if SHARED.get().is_some() {
        return;
    }

    let log: Box<dyn Fn(&str) + Send + Sync> = if config.debug {
        Box::new(|message: &str| {
            #[cfg(feature = "test-service")]
            test_service::resilience_commands::record_log(message);
            eprintln!("[barrel-kit] {message}");
        })
    } else {
        Box::new(|_: &str| {})
    };

    let data_directory = app.path().app_data_dir().ok();
    #[cfg(feature = "test-service")]
    let data_directory = test_service::runtime::directory().or(data_directory);

    app.manage(super::persistence::disk_store::StorageDirectory(
        data_directory.clone(),
    ));
    // Paths remain identifiers in memory-only mode; DiskStore owns the I/O guard.
    let data_dir = data_directory.unwrap_or_default();

    let installation_id_path = data_dir.join("barrel-installation.json");
    let facts_path = data_dir.join("barrel-buffered-facts.json");
    let trusted_key_path = data_dir.join("barrel-trusted-key.json");
    let version_expectation_path = data_dir.join("barrel-version-expectation.json");

    let installation_ids = InstallationIdStore::new(app.clone());
    let installation_id = installation_ids.get_or_create(&installation_id_path).await;

    let facts = BufferedFactsStore::new(app.clone());
    facts.configure(&facts_path).await;

    let trusted_keys = TrustedKeyStore::new(app.clone());
    trusted_keys
        .get_or_create(&trusted_key_path, super::keys::PUBLIC_ED25519_KEY_PEM)
        .await;

    let version_expectations = VersionExpectationStore::new(app.clone());
    check_version_expectation(
        &app,
        &facts,
        &version_expectations,
        &version_expectation_path,
    )
    .await;

    let client = reqwest::Client::new();

    adapter.restore_error(facts.latest_update_error());
    if let Some(url) = config.updates.update_feed_url.clone() {
        adapter.set_feed_url(url);
    }

    let shared = Arc::new(UpdateRuntime {
        app: app.clone(),
        client,
        adapter,
        facts,
        trusted_keys,
        trusted_key_path,
        version_expectations,
        version_expectation_path,
        host_control: HostControl::new(),
        installation_id,
        installation_ids,
        installation_id_path,
        config,
        sdk_version: SDK_VERSION.to_string(),
        log,
        in_flight: std::sync::atomic::AtomicBool::new(false),
        last_attempted_revision: std::sync::atomic::AtomicU64::new(0),
        tick_lock: tokio::sync::Mutex::new(()),
        check_in_lock: tokio::sync::Mutex::new(()),
    });

    if SHARED
        .set(shared.clone() as Arc<dyn std::any::Any + Send + Sync>)
        .is_err()
    {
        return; // Lost the race to a concurrent call -- let that one proceed instead.
    }

    let focus_trigger = start_cadence(shared);
    let _ = FOCUS_TRIGGER.set(focus_trigger.clone());
}

pub(crate) fn handle_window_event(event: &WindowEvent) {
    if matches!(event, WindowEvent::Focused(true)) {
        if let Some(trigger) = FOCUS_TRIGGER.get() {
            trigger.notify_one();
        }
    }
}
