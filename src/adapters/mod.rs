pub mod error_map;

use std::sync::Mutex as SyncMutex;
use tauri::{AppHandle, Runtime};
use tauri_plugin_updater::{Update, UpdaterExt};
use tokio::sync::Mutex as AsyncMutex;

use self::error_map::map_updater_error;
use crate::error_taxonomy::build_failure_state;
use crate::persistence::buffered_facts::BufferedFactsStore;
use crate::types::{AdapterState, Phase, UpdateFailure, UpdateFailureMode};

const PLATFORM: &str = "tauri-updater";

/// Owns `tauri-plugin-updater`'s updater entirely -- the host never drives
/// it directly. Tauri's updater is a direct async call chain, so this
/// adapter drives it itself rather than subscribing to events.
pub struct Adapter {
    state: SyncMutex<AdapterState>,
    state_tx: tokio::sync::watch::Sender<AdapterState>,
    last_error: SyncMutex<Option<UpdateFailure>>,
    pending_update: AsyncMutex<Option<Update>>,
    downloaded_bytes: AsyncMutex<Option<Vec<u8>>>,
    feed_url: SyncMutex<Option<String>>,
}

impl Adapter {
    /// Starts idle, with no feed-URL override and nothing downloaded yet.
    pub fn new() -> Self {
        let (state_tx, _) = tokio::sync::watch::channel(AdapterState::Idle);
        Self {
            state: SyncMutex::new(AdapterState::Idle),
            state_tx,
            last_error: SyncMutex::new(None),
            pending_update: AsyncMutex::new(None),
            downloaded_bytes: AsyncMutex::new(None),
            feed_url: SyncMutex::new(None),
        }
    }

    /// Fires on every local adapter-state transition, in addition to --
    /// never instead of -- `state()`'s own polling. A `watch` channel,
    /// not a callback list, so each subscriber sees the latest state.
    pub fn subscribe_state(&self) -> tokio::sync::watch::Receiver<AdapterState> {
        self.state_tx.subscribe()
    }

    /// Where your own update feed lives. Overrides `tauri.conf.json`'s
    /// static `endpoints` at runtime; a no-op if never called.
    pub fn set_feed_url(&self, url: String) {
        *self.feed_url.lock().unwrap() = Some(url);
    }

    /// The adapter's current lifecycle state, read synchronously.
    pub fn state(&self) -> AdapterState {
        self.state.lock().unwrap().clone()
    }

    /// The most recent failure this session, or `None` if nothing's failed yet.
    pub fn last_error(&self) -> Option<UpdateFailure> {
        self.last_error.lock().unwrap().clone()
    }

    /// The version currently available/downloaded, if any -- `None` before
    /// `check_for_updates` has ever found one.
    pub async fn target_version(&self) -> Option<String> {
        self.pending_update
            .lock()
            .await
            .as_ref()
            .map(|update| update.version.clone())
    }

    async fn set_state<R: Runtime>(&self, facts: &BufferedFactsStore<R>, state: AdapterState) {
        *self.state.lock().unwrap() = state.clone();
        let _ = self.state_tx.send(state.clone());
        facts.record_adapter_state(state).await;
    }

    fn fail(&self, new_state: AdapterState, failure: UpdateFailure) {
        *self.last_error.lock().unwrap() = Some(failure);
        *self.state.lock().unwrap() = new_state.clone();
        let _ = self.state_tx.send(new_state);
    }

    /// Checks for an update. On success, leaves state at `Idle` or
    /// `Available`; the caller (host control) decides whether to download.
    pub async fn check_for_updates<R: Runtime>(
        &self,
        app: &AppHandle<R>,
        facts: &BufferedFactsStore<R>,
    ) {
        #[cfg(feature = "test-service")]
        crate::test_service::runtime::record_check();

        self.set_state(facts, AdapterState::Checking).await;

        let built = match self.feed_url.lock().unwrap().clone() {
            Some(url) => url
                .parse()
                .map_err(|_| "updateFeedURL is not a valid URL".to_string())
                .and_then(|endpoint| {
                    app.updater_builder()
                        .endpoints(vec![endpoint])
                        .map_err(|error| error.to_string())
                })
                .and_then(|builder| builder.build().map_err(|error| error.to_string())),
            None => app.updater().map_err(|error| error.to_string()),
        };
        let updater = match built {
            Ok(updater) => updater,
            Err(message) => {
                self.record_updater_error(facts, &message).await;
                return;
            }
        };

        match updater.check().await {
            Ok(None) => self.set_state(facts, AdapterState::Idle).await,
            Ok(Some(update)) => {
                let release_notes = update.body.clone();
                *self.pending_update.lock().await = Some(update);
                self.set_state(facts, AdapterState::Available { release_notes })
                    .await;
            }
            Err(error) => {
                let (failure_mode, phase, platform_code) = map_updater_error(&error);
                self.record_failure(facts, failure_mode, phase, platform_code)
                    .await;
            }
        }
    }

    /// Downloads the update `check_for_updates` found. A no-op if nothing
    /// is pending (already downloaded, or no update was ever available).
    pub async fn download_update<R: Runtime>(&self, facts: &BufferedFactsStore<R>) {
        let mut pending = self.pending_update.lock().await;
        let Some(update) = pending.take() else { return };

        #[cfg(feature = "test-service")]
        crate::test_service::runtime::record_download();

        self.set_state(facts, AdapterState::Downloading { progress: None })
            .await;

        let state_for_progress = &self.state;
        let state_tx_for_progress = &self.state_tx;
        let downloaded = std::sync::atomic::AtomicU64::new(0);
        let result = update
            .download(
                move |chunk_length, content_length| {
                    let total = downloaded
                        .fetch_add(chunk_length as u64, std::sync::atomic::Ordering::Relaxed)
                        + chunk_length as u64;
                    let progress = content_length
                        .filter(|&len| len > 0)
                        .map(|len| ((total as f64 / len as f64) * 100.0).round() as u32);
                    let new_state = AdapterState::Downloading { progress };
                    *state_for_progress.lock().unwrap() = new_state.clone();
                    let _ = state_tx_for_progress.send(new_state);
                },
                || {},
            )
            .await;

        match result {
            Ok(bytes) => {
                *self.downloaded_bytes.lock().await = Some(bytes);
                *pending = Some(update);
                self.set_state(facts, AdapterState::Downloaded).await;
            }
            Err(error) => {
                let (failure_mode, phase, platform_code) = map_updater_error(&error);
                self.record_failure(facts, failure_mode, phase, platform_code)
                    .await;
            }
        }
    }

    /// Installs the already-downloaded update. On Windows a successful
    /// install exits the process; on macOS/Linux the host must relaunch
    /// afterward.
    ///
    /// On a failed install, transitions to `Failed` instead of leaving the
    /// adapter stuck at `Downloaded`.
    pub async fn quit_and_install<R: Runtime>(&self, facts: &BufferedFactsStore<R>) {
        let update = self.pending_update.lock().await.take();
        let bytes = self.downloaded_bytes.lock().await.take();

        let (Some(update), Some(bytes)) = (update, bytes) else {
            // Install attempted before a successful download -- full-wrap
            // means this can only happen from a bug in Barrel's own
            // sequencing, never a real host/library interaction.

            // So it's a defect tally, not an `UpdateFailure`.
            crate::internal_defects::record_internal_defect("adapter:install-before-download");
            return;
        };

        #[cfg(feature = "test-service")]
        if crate::test_service::runtime::intercept_install() {
            return;
        }

        self.handle_install_result(facts, update.install(bytes))
            .await;
    }

    /// The `install()` error-handling half of `quit_and_install`, split out
    /// so it can be driven by a directly-constructed `Err` in tests without
    /// needing a real downloaded `Update` to force failure through.
    async fn handle_install_result<R: Runtime>(
        &self,
        facts: &BufferedFactsStore<R>,
        result: Result<(), tauri_plugin_updater::Error>,
    ) {
        if let Err(error) = result {
            let (failure_mode, phase, platform_code) = map_updater_error(&error);
            self.record_failure(facts, failure_mode, phase, platform_code)
                .await;
        }
    }

    /// Always `Phase::Unknown`/`UpdateFailureMode::Unknown` -- only fires when
    /// *building* the updater itself fails, never a real
    /// `tauri_plugin_updater::Error` the mapping table classifies.
    async fn record_updater_error<R: Runtime>(&self, facts: &BufferedFactsStore<R>, message: &str) {
        self.record_failure(facts, UpdateFailureMode::Unknown, Phase::Unknown, message)
            .await;
    }

    /// Classifies, tallies, and buffers one failure -- shared by every
    /// errored codepath above so the fail/record sequence lives in one place.
    async fn record_failure<R: Runtime>(
        &self,
        facts: &BufferedFactsStore<R>,
        failure_mode: UpdateFailureMode,
        phase: Phase,
        platform_code: &str,
    ) {
        let (failure, state) = build_failure_state(PLATFORM, failure_mode, phase, platform_code);
        self.fail(state.clone(), failure.clone());
        facts.record_update_error(failure).await;
        facts.record_adapter_state(state).await;
    }
}

impl Default for Adapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::mock_app;
    use crate::types::{Phase, UpdateFailureMode};

    #[test]
    fn subscribe_state_sees_a_transition_published_by_the_adapter() {
        let adapter = Adapter::new();
        let mut rx = adapter.subscribe_state();
        assert_eq!(*rx.borrow(), AdapterState::Idle);

        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure);

        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), state);
    }

    #[test]
    fn subscribe_state_supports_more_than_one_subscriber() {
        let adapter = Adapter::new();
        let mut first = adapter.subscribe_state();
        let mut second = adapter.subscribe_state();

        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure);

        assert_eq!(*first.borrow_and_update(), state);
        assert_eq!(*second.borrow_and_update(), state);
    }

    #[tokio::test]
    async fn quit_and_install_before_a_successful_download_records_an_internal_defect() {
        let before = crate::internal_defects::get_internal_defect_counts()
            .get("adapter:install-before-download")
            .copied()
            .unwrap_or(0);

        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        adapter.quit_and_install(&facts).await; // nothing pending -- out-of-sequence call

        let after = crate::internal_defects::get_internal_defect_counts()
            .get("adapter:install-before-download")
            .copied()
            .unwrap_or(0);
        assert_eq!(after, before + 1);
    }

    #[tokio::test]
    async fn a_failed_install_transitions_to_failed_instead_of_staying_stuck_at_downloaded() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        adapter.fail(
            AdapterState::Downloaded,
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "x").0,
        );
        assert_eq!(adapter.state(), AdapterState::Downloaded);

        adapter
            .handle_install_result(
                &facts,
                Err(tauri_plugin_updater::Error::InvalidUpdaterFormat),
            )
            .await;

        assert!(matches!(adapter.state(), AdapterState::Failed { .. }));
        let error = adapter.last_error().expect("a failure was recorded");
        assert_eq!(error.failure_mode, UpdateFailureMode::ExtractionFailed);
        assert_eq!(error.phase, Phase::Extract);
    }

    #[tokio::test]
    async fn available_state_carries_release_notes_through_to_subscribers() {
        let adapter = Adapter::new();
        let mut rx = adapter.subscribe_state();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        adapter
            .set_state(
                &facts,
                AdapterState::Available {
                    release_notes: Some("Fixed a thing.".to_string()),
                },
            )
            .await;

        assert!(rx.has_changed().unwrap());
        assert_eq!(
            *rx.borrow_and_update(),
            AdapterState::Available {
                release_notes: Some("Fixed a thing.".to_string())
            }
        );
    }
}
