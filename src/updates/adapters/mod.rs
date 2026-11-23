pub mod error_map;

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex as SyncMutex;
use tauri::{AppHandle, Runtime};
use tauri_plugin_updater::{Update, UpdaterExt};
use tokio::sync::Mutex as AsyncMutex;

use self::error_map::map_updater_error;
use crate::updates::error_taxonomy::build_failure_state;
use crate::updates::models::eligibility::UpdateEligibility;
use crate::updates::models::failure::{Phase, UpdateFailure, UpdateFailureMode};
use crate::updates::models::state::UpdateState;
use crate::updates::models::status::Status;
use crate::updates::persistence::buffered_facts::BufferedFactsStore;

const PLATFORM: &str = "tauri-updater";

/// Owns `tauri-plugin-updater`'s updater entirely -- the host never drives
/// it directly. Tauri's updater is a direct async call chain, so this
/// adapter drives it itself rather than subscribing to events.
pub struct Adapter {
    /// Mirrors `state`, except it never becomes `Checking` -- what every
    /// check-in actually reports. `state()` stays the richer, host-facing
    /// truth.
    reportable_state: SyncMutex<(UpdateState, u64)>,
    reportable_tx: tokio::sync::watch::Sender<UpdateState>,
    status_tx: tokio::sync::watch::Sender<Status>,
    pending_update: AsyncMutex<Option<Update>>,
    downloaded_bytes: AsyncMutex<Option<Vec<u8>>>,
    feed_url: SyncMutex<Option<String>>,
}

impl Adapter {
    /// Starts idle, with no feed-URL override and nothing downloaded yet.
    pub fn new() -> Self {
        let (reportable_tx, _) = tokio::sync::watch::channel(UpdateState::Idle);
        let (status_tx, _) = tokio::sync::watch::channel(Status {
            state: UpdateState::Idle,
            eligibility: None,
            last_error: None,
        });
        Self {
            reportable_state: SyncMutex::new((UpdateState::Idle, 0)),
            reportable_tx,
            status_tx,
            pending_update: AsyncMutex::new(None),
            downloaded_bytes: AsyncMutex::new(None),
            feed_url: SyncMutex::new(None),
        }
    }

    /// A separate stream keeps local `Checking` activity from overwriting
    /// an unread reportable transition in the cadence watcher.
    pub(crate) fn subscribe_reportable_state(&self) -> tokio::sync::watch::Receiver<UpdateState> {
        self.reportable_tx.subscribe()
    }

    /// Where your own update feed lives. Overrides `tauri.conf.json`'s
    /// static `endpoints` at runtime; a no-op if never called.
    pub fn set_feed_url(&self, url: String) {
        *self.feed_url.lock().unwrap() = Some(url);
    }

    /// The adapter's current lifecycle state, read synchronously. Includes
    /// `Checking`, for a host app's own UI -- never what a check-in reports.
    pub fn state(&self) -> UpdateState {
        self.status_tx.borrow().state.clone()
    }

    /// Installation state and its local revision are read atomically so
    /// a state-sync wait can detect when a heartbeat already included it.
    pub(crate) fn reportable_snapshot(&self) -> (UpdateState, u64) {
        self.reportable_state.lock().unwrap().clone()
    }

    fn update_reportable_state(&self, state: UpdateState) {
        let mut current = self.reportable_state.lock().unwrap();
        current.0 = state;
        current.1 += 1;
    }

    /// The most recent failure this session, or `None` if nothing's failed yet.
    pub fn last_error(&self) -> Option<UpdateFailure> {
        self.status_tx.borrow().last_error.clone()
    }

    /// Whether an update is available and this installation is eligible
    /// for it, per the backend's last verified decision. `None` before the
    /// first successful check-in -- never a synthesized default.
    pub fn eligibility(&self) -> Option<UpdateEligibility> {
        self.status_tx.borrow().eligibility.clone()
    }

    /// Called once per verified check-in response -- see
    /// `cadence::apply_check_in_response`.
    pub(crate) fn set_eligibility(&self, eligibility: UpdateEligibility) {
        self.status_tx
            .send_modify(|status| status.eligibility = Some(eligibility));
    }

    /// Fires on every change to state, eligibility, or the last known
    /// failure, as one combined stream -- what `updates::subscribe()`
    /// returns.
    pub fn subscribe_status(&self) -> tokio::sync::watch::Receiver<Status> {
        self.status_tx.subscribe()
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

    /// `pub(crate)` solely so other modules' tests (e.g. `host_control`'s)
    /// can drive the adapter into a given state without a real download.
    pub(crate) fn set_state<R: Runtime>(&self, facts: &BufferedFactsStore<R>, state: UpdateState) {
        // Not live, and not via `record_lifecycle_state`'s persisted
        // history either.
        if state.is_reportable() {
            self.update_reportable_state(state.clone());
            facts.record_lifecycle_state_now(state.clone());
            self.reportable_tx.send_replace(state.clone());
        }
        self.status_tx.send_modify(|status| status.state = state);
    }

    pub(crate) fn fail(&self, new_state: UpdateState, failure: UpdateFailure) {
        self.update_reportable_state(new_state.clone());
        self.reportable_tx.send_replace(new_state.clone());
        self.status_tx.send_modify(|status| {
            status.state = new_state;
            status.last_error = Some(failure);
        });
    }

    #[cfg(feature = "test-service")]
    pub(crate) fn reset_fixture(&self) {
        self.status_tx
            .send_modify(|status| status.state = UpdateState::Idle);
    }

    pub(crate) fn restore_error(&self, failure: Option<UpdateFailure>) {
        self.status_tx
            .send_modify(|status| status.last_error = failure);
    }

    /// Checks for an update. On success, leaves state at `Idle` or
    /// `Available`; the caller (host control) decides whether to download.
    pub async fn check_for_updates<R: Runtime>(
        &self,
        app: &AppHandle<R>,
        facts: &BufferedFactsStore<R>,
    ) {
        let claimed = self.status_tx.send_if_modified(|status| {
            if !matches!(
                status.state,
                UpdateState::Idle | UpdateState::Failed { .. } | UpdateState::UpdateCancelled
            ) {
                return false;
            }
            status.state = UpdateState::Checking;
            true
        });
        if !claimed {
            return;
        }

        #[cfg(feature = "test-service")]
        crate::test_service::runtime::record_check();

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
            Ok(None) => self.set_state(facts, UpdateState::Idle),
            Ok(Some(update)) => {
                let release_notes = update.body.clone();
                *self.pending_update.lock().await = Some(update);
                self.set_state(facts, UpdateState::Available { release_notes });
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

        self.set_state(facts, UpdateState::Downloading { progress: None });

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
                    let new_state = UpdateState::Downloading { progress };
                    self.set_state(facts, new_state);
                },
                || {},
            )
            .await;

        match result {
            Ok(bytes) => {
                *self.downloaded_bytes.lock().await = Some(bytes);
                *pending = Some(update);
                self.set_state(facts, UpdateState::Downloaded);
            }
            Err(error) => {
                let (failure_mode, phase, platform_code) = map_updater_error(&error);
                self.record_failure(facts, failure_mode, phase, platform_code)
                    .await;
            }
        }
    }

    /// Await the applying check-in before native installation can terminate
    /// the process. An install failure transitions to `Failed`.
    pub async fn quit_and_install<R: Runtime>(
        &self,
        facts: &BufferedFactsStore<R>,
        sync_before_install: Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>>,
    ) {
        let update = self.pending_update.lock().await.take();
        let bytes = self.downloaded_bytes.lock().await.take();

        let (Some(update), Some(bytes)) = (update, bytes) else {
            // Full-wrap means this can only happen from a bug in Barrel's
            // own sequencing, never a real host/library interaction.
            crate::updates::internal_defects::record_internal_defect(
                "adapter:install-before-download",
            );
            return;
        };

        // Mirrors `check_for_updates`/`download_update` setting their own
        // entry state at their own start.
        self.set_state(facts, UpdateState::Applying);
        // Awaited, not fire-and-forget: the real install call below can
        // tear this process down with no further chance to report anything.
        sync_before_install.await;

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
        facts.record_lifecycle_state(state).await;
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
    use crate::updates::models::failure::{Phase, UpdateFailureMode};
    use crate::updates::persistence::test_support::mock_app;

    #[tokio::test]
    async fn checking_cannot_hide_an_unread_reportable_transition() {
        let adapter = Adapter::new();
        let mut reportable = adapter.subscribe_reportable_state();
        let local = adapter.subscribe_status();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        adapter.set_state(&facts, UpdateState::Downloaded);
        adapter.set_state(&facts, UpdateState::Checking);

        assert_eq!(
            *local.borrow(),
            Status {
                state: UpdateState::Checking,
                eligibility: None,
                last_error: None
            }
        );
        assert!(reportable.has_changed().unwrap());
        assert_eq!(*reportable.borrow_and_update(), UpdateState::Downloaded);
    }

    #[tokio::test]
    async fn feed_checks_preserve_a_candidate_already_owned_by_an_action() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        for state in [
            UpdateState::Available {
                release_notes: None,
            },
            UpdateState::Downloading { progress: Some(50) },
            UpdateState::Downloaded,
            UpdateState::Applying,
        ] {
            adapter.set_state(&facts, state.clone());
            adapter.check_for_updates(app.handle(), &facts).await;
            assert_eq!(adapter.state(), state);
        }
    }

    #[test]
    fn subscribe_status_sees_a_failure_published_by_the_adapter() {
        let adapter = Adapter::new();
        let mut rx = adapter.subscribe_status();
        assert_eq!(
            *rx.borrow(),
            Status {
                state: UpdateState::Idle,
                eligibility: None,
                last_error: None
            }
        );

        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure.clone());

        assert!(rx.has_changed().unwrap());
        assert_eq!(
            *rx.borrow_and_update(),
            Status {
                state: state.clone(),
                eligibility: None,
                last_error: Some(failure)
            }
        );
    }

    #[test]
    fn subscribe_status_supports_more_than_one_subscriber() {
        let adapter = Adapter::new();
        let mut first = adapter.subscribe_status();
        let mut second = adapter.subscribe_status();

        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure.clone());

        assert_eq!(
            *first.borrow_and_update(),
            Status {
                state: state.clone(),
                eligibility: None,
                last_error: Some(failure.clone())
            }
        );
        assert_eq!(
            *second.borrow_and_update(),
            Status {
                state: state.clone(),
                eligibility: None,
                last_error: Some(failure)
            }
        );
    }

    #[test]
    fn eligibility_and_last_error_start_at_none_before_either_ever_happens() {
        let adapter = Adapter::new();
        assert_eq!(adapter.eligibility(), None);
        assert_eq!(adapter.last_error(), None);
        assert_eq!(
            adapter.subscribe_status().borrow().clone(),
            crate::updates::models::status::Status {
                state: UpdateState::Idle,
                eligibility: None,
                last_error: None,
            }
        );
    }

    #[test]
    fn a_late_subscriber_reads_the_current_snapshot_without_a_replayed_log() {
        let adapter = Adapter::new();
        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure.clone());

        // Late subscribers receive the complete current snapshot, including both failure
        // state and its classification.
        let late = adapter.subscribe_status();
        assert_eq!(
            *late.borrow(),
            Status {
                state: state.clone(),
                eligibility: None,
                last_error: Some(failure.clone())
            }
        );
        assert_eq!(
            adapter.subscribe_status().borrow().clone(),
            crate::updates::models::status::Status {
                state,
                eligibility: None,
                last_error: Some(failure),
            }
        );
    }

    #[tokio::test]
    async fn quit_and_install_before_a_successful_download_records_an_internal_defect() {
        let before = crate::updates::internal_defects::get_internal_defect_counts()
            .get("adapter:install-before-download")
            .copied()
            .unwrap_or(0);

        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        // Nothing pending -- out-of-sequence call, returns before the
        // sync future is ever polled.
        adapter.quit_and_install(&facts, Box::pin(async {})).await;

        let after = crate::updates::internal_defects::get_internal_defect_counts()
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
            UpdateState::Downloaded,
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "x").0,
        );
        assert_eq!(adapter.state(), UpdateState::Downloaded);

        adapter
            .handle_install_result(
                &facts,
                Err(tauri_plugin_updater::Error::InvalidUpdaterFormat),
            )
            .await;

        assert!(matches!(adapter.state(), UpdateState::Failed { .. }));
        let error = adapter.last_error().expect("a failure was recorded");
        assert_eq!(error.failure_mode, UpdateFailureMode::ExtractionFailed);
        assert_eq!(error.phase, Phase::Extract);
    }

    #[tokio::test]
    async fn reportable_state_mirrors_state_except_it_never_becomes_checking() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        adapter.set_state(&facts, UpdateState::Checking);
        assert_eq!(adapter.state(), UpdateState::Checking);
        assert_eq!(adapter.reportable_snapshot().0, UpdateState::Idle);

        adapter.set_state(&facts, UpdateState::Downloaded);
        assert_eq!(adapter.reportable_snapshot().0, UpdateState::Downloaded);

        adapter.set_state(&facts, UpdateState::Applying);
        assert_eq!(adapter.state(), UpdateState::Applying);
        assert_eq!(adapter.reportable_snapshot().0, UpdateState::Applying);

        // `Checking` again afterward still never overwrites it.
        adapter.set_state(&facts, UpdateState::Checking);
        assert_eq!(adapter.reportable_snapshot().0, UpdateState::Applying);
    }

    #[tokio::test]
    async fn checking_is_never_recorded_as_a_buffered_fact_either() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        adapter.set_state(&facts, UpdateState::Checking);
        assert_eq!(facts.snapshot().await.len(), 0);

        adapter.set_state(&facts, UpdateState::Downloaded);
        assert_eq!(facts.snapshot().await.len(), 1);
    }

    #[tokio::test]
    async fn a_failure_is_always_reportable_even_from_checking() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        adapter.set_state(&facts, UpdateState::Checking);

        let (failure, state) =
            build_failure_state(PLATFORM, UpdateFailureMode::Unknown, Phase::Unknown, "boom");
        adapter.fail(state.clone(), failure);

        assert_eq!(adapter.reportable_snapshot().0, state);
    }

    #[tokio::test]
    async fn late_subscribers_start_with_the_current_lifecycle_state() {
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        adapter.set_state(&facts, UpdateState::Downloaded);

        assert_eq!(
            *adapter.subscribe_status().borrow(),
            adapter.subscribe_status().borrow().clone()
        );
        assert_eq!(
            *adapter.subscribe_reportable_state().borrow(),
            adapter.reportable_snapshot().0
        );
    }

    #[tokio::test]
    async fn available_state_carries_release_notes_through_to_subscribers() {
        let adapter = Adapter::new();
        let mut rx = adapter.subscribe_status();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        adapter.set_state(
            &facts,
            UpdateState::Available {
                release_notes: Some("Fixed a thing.".to_string()),
            },
        );

        assert!(rx.has_changed().unwrap());
        assert_eq!(
            *rx.borrow_and_update(),
            Status {
                state: UpdateState::Available {
                    release_notes: Some("Fixed a thing.".to_string())
                },
                eligibility: None,
                last_error: None
            }
        );
    }
}

#[cfg(test)]
mod mapping_tests;
