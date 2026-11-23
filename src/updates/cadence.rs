use super::persistence::key_rotation::TrustedKeyStore;
use super::runtime::UpdateRuntime;
use crate::updates::clock::{now_epoch_millis, parse_iso8601_to_epoch_millis};
use crate::InitConfig;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::Runtime;
use tokio::sync::Notify;

use crate::updates::env::collect_environment;
use crate::updates::jitter::compute_jitter_fraction;
use crate::updates::models::eligibility::UpdateEligibility;
use crate::updates::persistence::buffered_facts::BufferedFact;
use crate::updates::protocol::check_in::{send_check_in, CheckInParams};
use crate::updates::protocol::update_eligibility_verification::verify_and_parse_update_eligibility;
use crate::updates::protocol::wire_types::{BackendConnection, CheckInResponse};

pub const DEFAULT_INTERVAL_MS: u64 = 30 * 60 * 1000;
pub const MIN_INTERVAL_MS: u64 = 15 * 60 * 1000;
/// Keeps the server's stuck-tier escalation meaningfully faster than its
/// own going-dark window regardless of configured interval -- this is the
/// SDK's own defensive clamp, separate from the backend's hard reject.
pub const MAX_INTERVAL_MS: u64 = 2 * 60 * 60 * 1000;
pub const DEFAULT_HOLD_CACHE_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;
/// Collapses a burst of reportable state transitions (chiefly
/// download-progress ticks) into one outgoing check-in reflecting only the
/// latest state, rather than one request per transition.
const STATE_SYNC_DEBOUNCE: Duration = Duration::from_secs(5);

/// Bounds `requested` to `[MIN_INTERVAL_MS, MAX_INTERVAL_MS]`, with no
/// logging of its own -- also what every check-in reports as its own
/// `checkInIntervalMs`.
fn clamp_interval_ms(requested: u64) -> u64 {
    requested.clamp(MIN_INTERVAL_MS, MAX_INTERVAL_MS)
}

/// Checks in on launch, then on a jittered interval, plus on
/// foreground/focus -- no OS sleep/resume hook, since Tauri core has no
/// cross-platform equivalent without pulling in a new dependency.
pub fn start_cadence<R: Runtime>(shared: Arc<UpdateRuntime<R>>) -> Arc<Notify> {
    let requested_interval = shared
        .config
        .updates
        .check_in_interval_ms
        .unwrap_or(DEFAULT_INTERVAL_MS);
    if requested_interval < MIN_INTERVAL_MS {
        (shared.log)(&format!(
            "cadence: requested interval {requested_interval}ms is below the {MIN_INTERVAL_MS}ms floor, clamped"
        ));
    }
    if requested_interval > MAX_INTERVAL_MS {
        (shared.log)(&format!(
            "cadence: requested interval {requested_interval}ms is above the {MAX_INTERVAL_MS}ms ceiling, clamped"
        ));
    }
    let interval_ms = clamp_interval_ms(requested_interval);
    let jitter_fraction = compute_jitter_fraction(&shared.installation_id);
    let jittered_interval_ms = (interval_ms as f64 * (1.0 + jitter_fraction)).round() as u64;

    let focus_trigger = Arc::new(Notify::new());

    #[cfg(feature = "test-service")]
    if crate::test_service::runtime::directory().is_some() {
        crate::test_service::resilience_commands::record_interval(jittered_interval_ms);
        if !crate::test_service::resilience_commands::automatic_cadence() {
            return focus_trigger;
        }
    }

    let shared_for_loop = shared.clone();
    let focus_trigger_for_loop = focus_trigger.clone();
    tauri::async_runtime::spawn(run_schedule(
        Duration::from_millis(jittered_interval_ms),
        focus_trigger_for_loop,
        move || {
            let shared = shared_for_loop.clone();
            tauri::async_runtime::spawn(async move {
                tick(&shared).await;
            });
        },
    ));

    tauri::async_runtime::spawn(run_state_sync_debounce(shared.clone()));

    focus_trigger
}

/// Watches every reportable adapter-state transition and syncs it to the
/// backend immediately, debounced -- independent of, and never instead of,
/// the periodic cadence above.
async fn run_state_sync_debounce<R: Runtime>(shared: Arc<UpdateRuntime<R>>) {
    let mut rx = shared.adapter.subscribe_reportable_state();
    let shared_for_fire = shared.clone();
    debounce_reportable_changes(&mut rx, STATE_SYNC_DEBOUNCE, move || {
        let shared = shared_for_fire.clone();
        tauri::async_runtime::spawn(async move {
            sync_state_now(&shared).await;
        });
    })
    .await;
}

/// Leading and trailing state sync; local `Checking` activity never
/// starts or extends the quiet window.
async fn debounce_reportable_changes(
    rx: &mut tokio::sync::watch::Receiver<crate::updates::models::state::UpdateState>,
    debounce: Duration,
    on_fire: impl Fn() + Send + 'static,
) {
    loop {
        loop {
            if rx.changed().await.is_err() {
                return; // the sender is gone
            }
            if rx.borrow().is_reportable() {
                break;
            }
        }

        // Leading edge: fires right away. An isolated transition such as
        // `Applying` must reach the backend before its own next step can
        // tear the process down -- there's no window it could wait out.
        on_fire();

        // Trailing edge: further reportable changes before the window
        // quiets collapse into one more fire of the settled value --
        // what actually bounds a progress-burst's request rate.
        let mut deadline = tokio::time::Instant::now() + debounce;
        let mut more_arrived = false;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                changed = rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if rx.borrow().is_reportable() {
                        deadline = tokio::time::Instant::now() + debounce;
                        more_arrived = true;
                    }
                    // `Checking`: ignore, deadline stays put, doesn't count.
                }
            }
        }

        if more_arrived {
            on_fire();
        }
    }
}

async fn run_schedule(period: Duration, focus: Arc<Notify>, tick: impl Fn() + Send + 'static) {
    tick();
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = timer.tick() => {}
            _ = focus.notified() => {}
        }
        tick();
    }
}

/// Resolve connection and trusted key once per operation; a verified rotation applies to the
/// next operation.
pub(crate) async fn resolve_connection_fields<R: Runtime>(
    config: &InitConfig,
    trusted_keys: &TrustedKeyStore<R>,
) -> (String, String) {
    let base_url = config
        .base_url
        .clone()
        .unwrap_or_else(|| crate::updates::keys::DEFAULT_BASE_URL.to_string());
    let trusted_key_pem = trusted_keys
        .current()
        .await
        .unwrap_or_else(|| crate::updates::keys::PUBLIC_ED25519_KEY_PEM.to_string());
    (base_url, trusted_key_pem)
}

/// Skip overlapping work rather than queueing it. The informational in-flight flag does not
/// own the lock.
async fn with_in_flight_guard(
    lock: &tokio::sync::Mutex<()>,
    in_flight: &AtomicBool,
    log: &(dyn Fn(&str) + Send + Sync),
    what: &str,
    body: impl Future<Output = ()>,
) {
    let Ok(_guard) = lock.try_lock() else {
        log(&format!("{what}: skipped, a check-in is already in flight"));
        return;
    };
    in_flight.store(true, Ordering::SeqCst);
    body.await;
    in_flight.store(false, Ordering::SeqCst);
}

pub(crate) async fn tick<R: Runtime>(shared: &Arc<UpdateRuntime<R>>) {
    if shared.tick_lock.try_lock().is_err() {
        // Native work can outlast several heartbeat intervals without losing connectivity.
        with_in_flight_guard(
            &shared.check_in_lock,
            &shared.in_flight,
            shared.log.as_ref(),
            "cadence: heartbeat",
            async {
                let (base_url, trusted_key_pem) =
                    resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
                send_check_in_now(shared, &base_url, &trusted_key_pem).await;
            },
        )
        .await;
        return;
    }

    with_in_flight_guard(
        &shared.tick_lock,
        &shared.in_flight,
        shared.log.as_ref(),
        "cadence: tick",
        async {
            shared
                .installation_ids
                .retry_persistence(&shared.installation_id_path);

            let (base_url, trusted_key_pem) =
                resolve_connection_fields(&shared.config, &shared.trusted_keys).await;

            // The vendor update-feed check piggybacks on this same cadence but
            // MUST NOT run while held -- a Hold gates the check itself, not
            // just the download/apply actions downstream of it.
            let hold_cache_max_age_ms = shared
                .config
                .updates
                .hold_cache_max_age_ms
                .unwrap_or(DEFAULT_HOLD_CACHE_MAX_AGE_MS);
            if !eligibility_holds_the_vendor_check(
                shared.adapter.eligibility().as_ref(),
                hold_cache_max_age_ms,
                now_epoch_millis(),
            ) {
                shared
                    .adapter
                    .check_for_updates(&shared.app, &shared.facts)
                    .await;
            }

            let ctx = shared.action_context(&base_url, &trusted_key_pem);
            shared
                .host_control
                .maybe_download(
                    &ctx,
                    &shared.adapter,
                    &shared.facts,
                    shared.config.updates.auto_download.unwrap_or(true),
                )
                .await;

            // Scoped to just the send itself, never to the download above --
            // a state-triggered sync must still be able to acquire this same
            // lock and report `downloading` while a download is in flight.
            let _send_guard = shared.check_in_lock.lock().await;
            send_check_in_now(shared, &base_url, &trusted_key_pem).await;
        },
    )
    .await;
}

/// "Report current state now" -- none of `tick`'s vendor-feed-check or
/// download steps.
async fn sync_state_now<R: Runtime>(shared: &UpdateRuntime<R>) {
    let revision = shared.adapter.reportable_snapshot().1;
    let _guard = shared.check_in_lock.lock().await;
    // The heartbeat may already have included the transition while we waited.
    if revision <= shared.last_attempted_revision.load(Ordering::SeqCst) {
        return;
    }
    shared.in_flight.store(true, Ordering::SeqCst);
    let (base_url, trusted_key_pem) =
        resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
    send_check_in_now(shared, &base_url, &trusted_key_pem).await;
    shared.in_flight.store(false, Ordering::SeqCst);
}

/// Builds, sends, and applies the response for one check-in reflecting
/// current state. `tick`/`sync_state_now` hold `in_flight` around this;
/// `action_context`'s `sync_before_apply` deliberately doesn't -- see there.
pub(super) async fn send_check_in_now<R: Runtime>(
    shared: &UpdateRuntime<R>,
    base_url: &str,
    trusted_key_pem: &str,
) {
    let env = collect_environment(&shared.app.package_info().version.to_string());
    // Snapshotting before the request goes out, and clearing exactly this
    // snapshot on ACK, is what keeps a fact recorded mid-flight from being
    // wiped alongside facts that were actually sent.
    let buffered_facts = shared.facts.snapshot().await;

    let (lifecycle_state, revision) = shared.adapter.reportable_snapshot();
    shared
        .last_attempted_revision
        .fetch_max(revision, Ordering::SeqCst);
    let response = send_check_in(
        &shared.client,
        CheckInParams {
            connection: BackendConnection {
                base_url,
                credential: &shared.config.credential,
                installation_id: &shared.installation_id,
            },
            sdk_version: &shared.sdk_version,
            channel: shared.config.updates.channel.as_deref().unwrap_or("stable"),
            observed_version: &env.observed_version,
            platform: &env.platform,
            arch: &env.arch,
            os_version: &env.os_version,
            attributes: &shared.config.updates.attributes,
            lifecycle_state,
            buffered_facts: buffered_facts.clone(),
            check_in_interval_ms: clamp_interval_ms(
                shared
                    .config
                    .updates
                    .check_in_interval_ms
                    .unwrap_or(DEFAULT_INTERVAL_MS),
            ),
        },
        &shared.log,
    )
    .await;

    if let Some(response) = &response {
        apply_check_in_response(shared, response, &buffered_facts, trusted_key_pem).await;
    }
}

async fn apply_check_in_response<R: Runtime>(
    shared: &UpdateRuntime<R>,
    response: &CheckInResponse,
    buffered_facts: &[BufferedFact],
    trusted_key_pem: &str,
) {
    shared.facts.clear(buffered_facts).await;

    if let Some(eligibility) = verify_and_parse_update_eligibility(response, trusted_key_pem) {
        shared.adapter.set_eligibility(eligibility);
        (shared.log)("update eligibility: verified and updated");
    } else {
        (shared.log)("update eligibility: verification failed");
    }

    shared
        .trusted_keys
        .apply_rotation_if_present(&shared.trusted_key_path, response)
        .await;
}

/// Only a confirmed `held` decision still within its ceiling holds the
/// check -- `None`/`unrestricted`/`eligible` all fail open. The ceiling is
/// the earlier of `hold_cache_max_age_ms` and the operator's own `until`.
fn eligibility_holds_the_vendor_check(
    eligibility: Option<&UpdateEligibility>,
    hold_cache_max_age_ms: u64,
    now_millis: i64,
) -> bool {
    let Some(UpdateEligibility::Held {
        issued_at, until, ..
    }) = eligibility
    else {
        return false;
    };
    let Some(issued_at_millis) = parse_iso8601_to_epoch_millis(issued_at) else {
        return false;
    };
    if (now_millis - issued_at_millis) as u64 > hold_cache_max_age_ms {
        return false;
    }
    if let Some(until) = until {
        if let Some(until_millis) = parse_iso8601_to_epoch_millis(until) {
            if now_millis > until_millis {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_interval_ms_raises_a_below_floor_request() {
        assert_eq!(clamp_interval_ms(MIN_INTERVAL_MS - 1), MIN_INTERVAL_MS);
    }

    #[test]
    fn clamp_interval_ms_lowers_an_above_ceiling_request() {
        assert_eq!(clamp_interval_ms(MAX_INTERVAL_MS + 1), MAX_INTERVAL_MS);
    }

    #[test]
    fn clamp_interval_ms_passes_an_in_range_request_through_unchanged() {
        let requested = MIN_INTERVAL_MS + 1;
        assert_eq!(clamp_interval_ms(requested), requested);
    }

    #[tokio::test]
    async fn a_scheduled_tick_reports_while_native_work_owns_the_tick_lock() {
        use crate::updates::persistence::{
            buffered_facts::BufferedFactsStore, installation_id::InstallationIdStore,
            key_rotation::TrustedKeyStore, version_expectation::VersionExpectationStore,
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let app = crate::updates::persistence::test_support::mock_app();
        let handle = app.handle().clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let received = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let count = socket.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
                if String::from_utf8_lossy(&request).contains("lifecycleState") {
                    break;
                }
            }
            socket.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let shared = Arc::new(UpdateRuntime {
            app: handle.clone(),
            client: reqwest::Client::new(),
            adapter: Arc::new(crate::updates::adapters::Adapter::new()),
            facts: BufferedFactsStore::new(handle.clone()),
            trusted_keys: TrustedKeyStore::new(handle.clone()),
            trusted_key_path: "unused-key.json".into(),
            version_expectations: VersionExpectationStore::new(handle.clone()),
            version_expectation_path: "unused-version.json".into(),
            host_control: crate::updates::host_control::HostControl::new(),
            installation_id: "heartbeat-test".into(),
            installation_ids: InstallationIdStore::new(handle),
            installation_id_path: "unused-installation.json".into(),
            config: crate::InitConfig {
                base_url: Some(base_url),
                ..Default::default()
            },
            sdk_version: "test".into(),
            log: Box::new(|_| {}),
            in_flight: AtomicBool::new(true),
            last_attempted_revision: std::sync::atomic::AtomicU64::new(0),
            tick_lock: tokio::sync::Mutex::new(()),
            check_in_lock: tokio::sync::Mutex::new(()),
        });
        shared.adapter.set_state(
            &shared.facts,
            crate::updates::State::Downloading { progress: Some(25) },
        );
        let native_work = shared.tick_lock.lock().await;
        tokio::time::timeout(Duration::from_secs(2), tick(&shared))
            .await
            .unwrap();
        let request = received.await.unwrap();
        assert!(request.contains("downloading"));
        assert!(shared.tick_lock.try_lock().is_err());
        drop(native_work);
    }

    #[tokio::test(start_paused = true)]
    async fn focus_does_not_reset_the_fixed_timer_and_missed_ticks_do_not_burst() {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let focus = Arc::new(Notify::new());
        let observed = count.clone();
        let task = tokio::spawn(run_schedule(
            Duration::from_secs(100),
            focus.clone(),
            move || {
                observed.fetch_add(1, Ordering::Relaxed);
            },
        ));
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_secs(40)).await;
        focus.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 2);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 3);
        tokio::time::advance(Duration::from_secs(350)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 4);
        task.abort();
        let _ = task.await;
    }

    fn millis(iso8601: &str) -> i64 {
        parse_iso8601_to_epoch_millis(iso8601).unwrap()
    }

    #[test]
    fn a_held_decision_within_its_ceiling_holds_the_vendor_check() {
        let held = UpdateEligibility::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: None,
        };
        let now = millis("2026-01-01T01:00:00.000Z"); // 1h later, under the 24h default
        assert!(eligibility_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }

    #[test]
    fn no_eligibility_decision_yet_does_not_hold_the_vendor_check() {
        assert!(!eligibility_holds_the_vendor_check(
            None,
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            millis("2026-01-01T00:00:00.000Z")
        ));
    }

    #[test]
    fn an_unrestricted_decision_does_not_hold_the_vendor_check() {
        let unrestricted = UpdateEligibility::Unrestricted {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
        };
        assert!(!eligibility_holds_the_vendor_check(
            Some(&unrestricted),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            millis("2026-01-01T00:00:00.000Z")
        ));
    }

    #[test]
    fn a_held_decision_past_its_ceiling_fails_open() {
        let held = UpdateEligibility::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: None,
        };
        let now = millis("2026-01-03T00:00:00.000Z"); // 2 days later, past the 24h default
        assert!(!eligibility_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn an_isolated_transition_fires_immediately_with_no_trailing_followup() {
        use crate::updates::models::state::UpdateState;

        let (tx, mut rx) = tokio::sync::watch::channel(UpdateState::Idle);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = count.clone();
        let task = tokio::spawn(async move {
            debounce_reportable_changes(&mut rx, Duration::from_secs(5), move || {
                observed.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        });

        // The leading edge: `Applying` must reach the backend before its
        // own next step -- the real native install call -- can tear the
        // process down, so this cannot wait out any window.
        tx.send(UpdateState::Applying).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        // Nothing else arrives -- no spurious trailing fire.
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        task.abort();
        let _ = task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_fires_immediately_then_once_more_after_it_settles() {
        use crate::updates::models::state::UpdateState;

        let (tx, mut rx) = tokio::sync::watch::channel(UpdateState::Idle);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = count.clone();
        let task = tokio::spawn(async move {
            debounce_reportable_changes(&mut rx, Duration::from_secs(5), move || {
                observed.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        });

        tx.send(UpdateState::Downloading { progress: Some(10) })
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1); // leading fire, no wait

        tokio::time::advance(Duration::from_secs(3)).await;
        tx.send(UpdateState::Downloading { progress: Some(20) })
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        // 6s since the first change, but only 3s since the second -- the
        // second change should have pushed the trailing deadline out.
        assert_eq!(count.load(Ordering::Relaxed), 1);

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        // Trailing fire: the burst collapses to exactly one more call.
        assert_eq!(count.load(Ordering::Relaxed), 2);

        task.abort();
        let _ = task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn checking_never_starts_resets_or_counts_as_a_trailing_arrival() {
        use crate::updates::models::state::UpdateState;

        let (tx, mut rx) = tokio::sync::watch::channel(UpdateState::Idle);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = count.clone();
        let task = tokio::spawn(async move {
            debounce_reportable_changes(&mut rx, Duration::from_secs(5), move || {
                observed.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        });

        // `Checking` alone, however long, never fires.
        tx.send(UpdateState::Checking).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 0);

        // A reportable change fires immediately (leading edge).
        tx.send(UpdateState::Downloading { progress: None })
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        // `Checking` during the trailing window must not push the deadline
        // out, nor count as a further arrival worth a trailing fire.
        tokio::time::advance(Duration::from_secs(3)).await;
        tx.send(UpdateState::Checking).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn a_held_decisions_own_until_expiring_fails_open_even_within_the_ceiling() {
        let held = UpdateEligibility::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: Some("2026-01-01T12:00:00.000Z".to_string()),
        };
        // 1h past `until`, but well within the 24h default ceiling.
        let now = millis("2026-01-01T13:00:00.000Z");
        assert!(!eligibility_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }
}
