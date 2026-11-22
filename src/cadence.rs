use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, Runtime};
use tokio::sync::Notify;

use crate::adapters::Adapter;
use crate::env::collect_environment;
use crate::host_control::{ActionContext, HostControl};
use crate::jitter::compute_jitter_fraction;
use crate::persistence::buffered_facts::{BufferedFact, BufferedFactsStore};
use crate::persistence::key_rotation::TrustedKeyStore;
use crate::persistence::version_expectation::VersionExpectationStore;
use crate::protocol::check_in::{
    now_epoch_millis, parse_iso8601_to_epoch_millis, send_check_in, CheckInParams,
};
use crate::protocol::policy::verify_and_parse_policy;
use crate::types::{BackendConnection, CheckInResponse, InitConfig, Policy};

pub const DEFAULT_INTERVAL_MS: u64 = 30 * 60 * 1000;
pub const MIN_INTERVAL_MS: u64 = 15 * 60 * 1000;
pub const DEFAULT_HOLD_CACHE_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

pub struct CadenceShared<R: Runtime> {
    pub app: AppHandle<R>,
    pub client: reqwest::Client,
    pub adapter: Adapter,
    pub facts: BufferedFactsStore<R>,
    pub trusted_keys: TrustedKeyStore<R>,
    pub trusted_key_path: std::path::PathBuf,
    pub version_expectations: VersionExpectationStore<R>,
    pub version_expectation_path: std::path::PathBuf,
    pub host_control: HostControl,
    pub installation_id: String,
    pub config: InitConfig,
    pub sdk_version: String,
    pub log: Box<dyn Fn(&str) + Send + Sync>,
    /// A fresh tick while one is already in flight is skipped, not queued.
    pub in_flight: AtomicBool,
}

impl<R: Runtime> CadenceShared<R> {
    /// Every call site resolves `base_url`/`trusted_key_pem` fresh via
    /// `resolve_connection_fields` first, then builds its context from here.
    ///
    /// A method on `CadenceShared` itself, rather than a free function
    /// reaching into it from another module -- the two modules no longer
    /// need to know about each other's internals this way.
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
        }
    }
}

/// Checks in on launch, then on a jittered interval, plus on
/// foreground/focus -- no OS sleep/resume hook, since Tauri core has no
/// cross-platform equivalent without pulling in a new dependency.
pub fn start_cadence<R: Runtime>(shared: Arc<CadenceShared<R>>) -> Arc<Notify> {
    let requested_interval = shared
        .config
        .check_in_interval_ms
        .unwrap_or(DEFAULT_INTERVAL_MS);
    if requested_interval < MIN_INTERVAL_MS {
        (shared.log)(&format!(
            "cadence: requested interval {requested_interval}ms is below the {MIN_INTERVAL_MS}ms floor, clamped"
        ));
    }
    let interval_ms = requested_interval.max(MIN_INTERVAL_MS);
    let jitter_fraction = compute_jitter_fraction(&shared.installation_id);
    let jittered_interval_ms = (interval_ms as f64 * (1.0 + jitter_fraction)).round() as u64;

    let focus_trigger = Arc::new(Notify::new());

    let shared_for_loop = shared.clone();
    let focus_trigger_for_loop = focus_trigger.clone();
    tauri::async_runtime::spawn(async move {
        tick(&shared_for_loop).await;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(jittered_interval_ms)) => {}
                _ = focus_trigger_for_loop.notified() => {}
            }
            tick(&shared_for_loop).await;
        }
    });

    focus_trigger
}

/// Resolves `base_url`/`trusted_key_pem`, falling back to the SDK's own
/// embedded defaults -- shared by every call site that builds an
/// `ActionContext`.
///
/// Any key rotation a check-in response carries takes effect from the
/// next resolution on, never retroactively within one call.
pub(crate) async fn resolve_connection_fields<R: Runtime>(
    config: &InitConfig,
    trusted_keys: &TrustedKeyStore<R>,
) -> (String, String) {
    let base_url = config
        .base_url
        .clone()
        .unwrap_or_else(|| crate::keys::DEFAULT_BASE_URL.to_string());
    let trusted_key_pem = trusted_keys
        .current()
        .await
        .unwrap_or_else(|| crate::keys::PUBLIC_ED25519_KEY_PEM.to_string());
    (base_url, trusted_key_pem)
}

async fn tick<R: Runtime>(shared: &Arc<CadenceShared<R>>) {
    if shared.in_flight.swap(true, Ordering::SeqCst) {
        (shared.log)("cadence: tick skipped, a check-in is already in flight");
        return;
    }

    let (base_url, trusted_key_pem) =
        resolve_connection_fields(&shared.config, &shared.trusted_keys).await;

    // The vendor update-feed check piggybacks on this same cadence but MUST
    // NOT run while held -- a Hold gates the check itself, not just the
    // download/apply actions downstream of it.
    let hold_cache_max_age_ms = shared
        .config
        .hold_cache_max_age_ms
        .unwrap_or(DEFAULT_HOLD_CACHE_MAX_AGE_MS);
    if !policy_holds_the_vendor_check(
        crate::POLICY.lock().unwrap().as_ref(),
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
            shared.config.auto_download.unwrap_or(true),
        )
        .await;

    let env = collect_environment(&shared.app.package_info().version.to_string());
    // Snapshotting before the request goes out, and clearing exactly this
    // snapshot on ACK, is what keeps a fact recorded mid-flight from being
    // wiped alongside facts that were actually sent.
    let buffered_facts = shared.facts.snapshot().await;

    let response = send_check_in(
        &shared.client,
        CheckInParams {
            connection: BackendConnection {
                base_url: &base_url,
                credential: &shared.config.credential,
                installation_id: &shared.installation_id,
            },
            sdk_version: &shared.sdk_version,
            channel: shared.config.channel.as_deref().unwrap_or("stable"),
            observed_version: &env.observed_version,
            platform: &env.platform,
            arch: &env.arch,
            os_version: &env.os_version,
            attributes: &shared.config.attributes,
            adapter_state: shared.adapter.state(),
            buffered_facts: buffered_facts.clone(),
        },
        &shared.log,
    )
    .await;

    if let Some(response) = &response {
        apply_check_in_response(shared, response, &buffered_facts, &trusted_key_pem).await;
    }

    shared.in_flight.store(false, Ordering::SeqCst);
}

async fn apply_check_in_response<R: Runtime>(
    shared: &Arc<CadenceShared<R>>,
    response: &CheckInResponse,
    buffered_facts: &[BufferedFact],
    trusted_key_pem: &str,
) {
    shared.facts.clear(buffered_facts).await;

    match verify_and_parse_policy(response, trusted_key_pem) {
        Some(policy) => {
            (shared.log)("policy: verified and updated");
            crate::POLICY.lock().unwrap().replace(policy);
        }
        None => (shared.log)("policy: verification failed"),
    }

    shared
        .trusted_keys
        .apply_rotation_if_present(&shared.trusted_key_path, response)
        .await;
}

/// Only a confirmed `held` Policy still within its ceiling holds the check --
/// `None`/`current`/`updateEligible` all fail open. The ceiling is the
/// earlier of `hold_cache_max_age_ms` and the operator's own `until`.
fn policy_holds_the_vendor_check(
    policy: Option<&Policy>,
    hold_cache_max_age_ms: u64,
    now_millis: i64,
) -> bool {
    let Some(Policy::Held {
        issued_at, until, ..
    }) = policy
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

    fn millis(iso8601: &str) -> i64 {
        parse_iso8601_to_epoch_millis(iso8601).unwrap()
    }

    #[test]
    fn a_held_policy_within_its_ceiling_holds_the_vendor_check() {
        let held = Policy::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: None,
        };
        let now = millis("2026-01-01T01:00:00.000Z"); // 1h later, under the 24h default
        assert!(policy_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }

    #[test]
    fn no_policy_yet_does_not_hold_the_vendor_check() {
        assert!(!policy_holds_the_vendor_check(
            None,
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            millis("2026-01-01T00:00:00.000Z")
        ));
    }

    #[test]
    fn a_current_policy_does_not_hold_the_vendor_check() {
        let current = Policy::Current {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
        };
        assert!(!policy_holds_the_vendor_check(
            Some(&current),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            millis("2026-01-01T00:00:00.000Z")
        ));
    }

    #[test]
    fn a_held_policy_past_its_ceiling_fails_open() {
        let held = Policy::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: None,
        };
        let now = millis("2026-01-03T00:00:00.000Z"); // 2 days later, past the 24h default
        assert!(!policy_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }

    #[test]
    fn a_held_policys_own_until_expiring_fails_open_even_within_the_ceiling() {
        let held = Policy::Held {
            issued_at: "2026-01-01T00:00:00.000Z".to_string(),
            environment: "production".to_string(),
            reason: None,
            until: Some("2026-01-01T12:00:00.000Z".to_string()),
        };
        // 1h past `until`, but well within the 24h default ceiling.
        let now = millis("2026-01-01T13:00:00.000Z");
        assert!(!policy_holds_the_vendor_check(
            Some(&held),
            DEFAULT_HOLD_CACHE_MAX_AGE_MS,
            now
        ));
    }
}
