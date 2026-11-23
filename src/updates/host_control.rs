use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use tauri::Runtime;

use crate::updates::adapters::Adapter;
use crate::updates::models::eligibility::UpdateEligibility;
use crate::updates::models::state::UpdateState;
use crate::updates::persistence::buffered_facts::BufferedFactsStore;
use crate::updates::persistence::version_expectation::VersionExpectationStore;
use crate::updates::protocol::pre_action_check::{check_pre_action, PreActionCheckParams, Stage};
use crate::updates::protocol::wire_types::BackendConnection;
use crate::updates::ActionError;

/// Everything a pre-action-check needs, bundled once per cadence tick
/// rather than threaded through as separate positional arguments.
pub struct ActionContext<'a> {
    pub client: &'a reqwest::Client,
    pub connection: BackendConnection<'a>,
    pub trusted_key_pem: &'a str,
    pub log: &'a (dyn Fn(&str) + Send + Sync),
    /// Awaited inline before `Applying`'s real native install call --
    /// the one check-in that must land before the process can disappear.
    pub sync_before_apply: Pin<Box<dyn Future<Output = ()> + Send + Sync + 'a>>,
}

/// Gates every download and apply on a fresh `pre-action-check`, not just
/// the cadence's own 30-minute eligibility decision -- a Hold placed since
/// the last check-in must still land in time to stop either action.
pub struct HostControl {
    pending_install: AtomicBool,
    action_in_progress: AtomicBool,
    action_idle: tokio::sync::Notify,
}

/// A fresh pre-action-check for `stage`, gating the caller on whatever
/// eligibility decision Barrel returns right now -- not the cadence's own
/// cached one.
async fn can_proceed(ctx: &ActionContext<'_>, adapter: &Adapter, stage: Stage) -> bool {
    let eligibility = check_pre_action(
        ctx.client,
        PreActionCheckParams {
            connection: ctx.connection,
            stage,
            trusted_key_pem: ctx.trusted_key_pem,
        },
        ctx.log,
    )
    .await;

    let held = matches!(eligibility, Some(UpdateEligibility::Held { .. }));
    if let Some(eligibility) = eligibility {
        adapter.set_eligibility(eligibility);
    }
    !held
}

impl HostControl {
    pub fn new() -> Self {
        Self {
            pending_install: AtomicBool::new(false),
            action_in_progress: AtomicBool::new(false),
            action_idle: tokio::sync::Notify::new(),
        }
    }

    pub(crate) async fn wait_until_idle(&self) {
        loop {
            let notified = self.action_idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.action_in_progress.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    pub fn has_pending_install(&self) -> bool {
        self.pending_install.load(Ordering::SeqCst)
    }

    /// For the quit-deferral path only, once it's decided to finish exiting
    /// without installing -- lets the re-triggered `ExitRequested` pass
    /// straight through instead of deferring on this same pending install.
    pub fn clear_pending_install(&self) {
        self.pending_install.store(false, Ordering::SeqCst);
    }

    /// Call after `Adapter::check_for_updates` -- downloads only if the
    /// adapter landed on `Available` and the host has `auto_download` on.
    pub async fn maybe_download<R: Runtime>(
        &self,
        ctx: &ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore<R>,
        auto_download: bool,
    ) {
        if auto_download {
            let _ = self.download_update(ctx, adapter, facts).await;
        }
    }

    /// A host-triggered download is gated by a fresh eligibility check and its required
    /// lifecycle state.
    pub async fn download_update<R: Runtime>(
        &self,
        ctx: &ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore<R>,
    ) -> Result<(), ActionError> {
        let _reservation = reserve_action(self)?;
        if !matches!(adapter.state(), UpdateState::Available { .. }) {
            return Err(ActionError::NoUpdateAvailable);
        }

        if !can_proceed(ctx, adapter, Stage::PreDownload).await {
            return Err(ActionError::Held);
        }

        adapter.download_update(facts).await;
        if adapter.state() == UpdateState::Downloaded {
            self.pending_install.store(true, Ordering::SeqCst);
        }
        match adapter.state() {
            UpdateState::Downloaded => Ok(()),
            UpdateState::UpdateCancelled => Err(ActionError::Cancelled),
            _ => Err(ActionError::Failed(adapter.last_error())),
        }
    }

    /// Authorize a verified download immediately before apply and await the bounded applying
    /// report before native handoff.
    #[allow(clippy::too_many_arguments)]
    pub async fn apply_update<R: Runtime>(
        &self,
        ctx: ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore<R>,
        version_expectations: &VersionExpectationStore<R>,
        version_expectation_path: &Path,
    ) -> Result<(), ActionError> {
        let _reservation = reserve_action(self)?;
        if adapter.state() != UpdateState::Downloaded {
            return Err(ActionError::NothingDownloadedYet);
        }

        if !can_proceed(&ctx, adapter, Stage::PreApply).await {
            return Err(ActionError::Held);
        }

        // Awaited, not fire-and-forget: a successful Windows install
        // can exit the process immediately once `quit_and_install`
        // returns, so this must land on disk before that call starts.
        if let Some(version) = adapter.target_version().await {
            version_expectations
                .set(version_expectation_path, &version)
                .await;
        }

        self.pending_install.store(false, Ordering::SeqCst);
        adapter.quit_and_install(facts, ctx.sync_before_apply).await;

        // The classified failure already explains this apply attempt;
        // clear its marker to prevent a duplicate report on next launch.
        if matches!(adapter.state(), UpdateState::Failed { .. }) {
            self.pending_install.store(false, Ordering::SeqCst);
            version_expectations.clear(version_expectation_path).await;
            return Err(ActionError::Failed(adapter.last_error()));
        }

        Ok(())
    }
}

struct ActionReservation<'a>(&'a HostControl);

impl Drop for ActionReservation<'_> {
    fn drop(&mut self) {
        self.0.action_in_progress.store(false, Ordering::SeqCst);
        self.0.action_idle.notify_waiters();
    }
}

fn reserve_action(host: &HostControl) -> Result<ActionReservation<'_>, ActionError> {
    host.action_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .map(|_| ActionReservation(host))
        .map_err(|_| ActionError::AlreadyInProgress)
}

impl Default for HostControl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::persistence::buffered_facts::BufferedFactsStore;
    use crate::updates::persistence::test_support::mock_app;

    // An unreachable address, never actually dialed: every test here exercises
    // only the guard clause that returns before `can_proceed`'s network call.
    fn unreachable_ctx() -> ActionContext<'static> {
        static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        ActionContext {
            client: CLIENT.get_or_init(reqwest::Client::new),
            connection: BackendConnection {
                base_url: "http://127.0.0.1:0",
                credential: "unused",
                installation_id: "unused",
            },
            trusted_key_pem: "unused",
            log: &(|_: &str| {}),
            // Every test below hits a guard clause before this is ever
            // polled -- a real future is only needed past that point.
            sync_before_apply: Box::pin(async {}),
        }
    }

    #[tokio::test]
    async fn download_update_is_a_no_op_when_the_adapter_is_not_available() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        assert_eq!(
            host_control
                .download_update(&unreachable_ctx(), &adapter, &facts)
                .await,
            Err(ActionError::NoUpdateAvailable)
        );

        assert_eq!(adapter.state(), UpdateState::Idle);
        assert!(!host_control.has_pending_install());
    }

    #[tokio::test]
    async fn apply_update_is_a_no_op_when_nothing_has_downloaded() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        let version_expectations = VersionExpectationStore::new(app.handle().clone());
        let version_expectation_path = std::env::temp_dir().join("unused-version-expectation.json");

        let applied = host_control
            .apply_update(
                unreachable_ctx(),
                &adapter,
                &facts,
                &version_expectations,
                &version_expectation_path,
            )
            .await;

        // No panic, no state change, and a reported non-apply is the whole
        // assertion -- there's nothing yet to apply, so this must stay a
        // quiet no-op the caller can also tell wasn't a real apply.
        assert!(applied.is_err());
        assert_eq!(adapter.state(), UpdateState::Idle);
    }

    #[tokio::test]
    async fn apply_update_is_denied_without_installing_when_a_fresh_check_finds_a_hold() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use ed25519_dalek::{Signer, SigningKey};
        use pkcs8::EncodePublicKey;
        use std::io::{Read, Write};

        let signing_key = SigningKey::from_bytes(&[99; 32]);
        let trusted_key_pem = signing_key
            .verifying_key()
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .unwrap();
        let eligibility_json =
            r#"{"type":"held","issuedAt":"2026-01-01T00:00:00.000Z","environment":"production"}"#;
        let signature = STANDARD.encode(signing_key.sign(eligibility_json.as_bytes()).to_bytes());
        let response_body = format!(
            r#"{{"protocolVersion":1,"updateEligibility":{},"signature":"{}"}}"#,
            serde_json::to_string(eligibility_json).unwrap(),
            signature,
        );

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut bytes = [0; 4096];
            loop {
                let count = socket.read(&mut bytes).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&bytes[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = entered_tx.send(());
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .unwrap();
        });

        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        // Only `Adapter::set_state` can reach `Downloaded` outside a real
        // download -- widened to `pub(crate)` for exactly this.
        adapter.set_state(&facts, UpdateState::Downloaded);
        let version_expectations = VersionExpectationStore::new(app.handle().clone());
        let version_expectation_path =
            std::env::temp_dir().join("unused-version-expectation-held.json");

        let client = reqwest::Client::new();
        let ctx = ActionContext {
            client: &client,
            connection: BackendConnection {
                base_url: &base_url,
                credential: "cred",
                installation_id: "install",
            },
            trusted_key_pem: &trusted_key_pem,
            log: &(|_: &str| {}),
            // Never polled -- `can_proceed`'s denial returns before it would be.
            sync_before_apply: Box::pin(async {}),
        };

        let operation = host_control.apply_update(
            ctx,
            &adapter,
            &facts,
            &version_expectations,
            &version_expectation_path,
        );
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("authorization unexpectedly completed: {result:?}"),
            _ = entered_rx => {}
        }
        let concurrent = host_control
            .apply_update(
                unreachable_ctx(),
                &adapter,
                &facts,
                &version_expectations,
                &version_expectation_path,
            )
            .await;
        assert_eq!(concurrent, Err(ActionError::AlreadyInProgress));
        release_tx.send(()).unwrap();
        let applied = operation.await;

        server.join().unwrap();

        // A verified Hold leaves the update downloaded; quit coordination must still finish
        // the requested application exit.
        assert_eq!(applied, Err(ActionError::Held));
        assert!(matches!(
            adapter.subscribe_status().borrow().eligibility,
            Some(UpdateEligibility::Held { .. })
        ));
        assert_eq!(adapter.state(), UpdateState::Downloaded);
    }

    #[tokio::test]
    async fn maybe_download_does_nothing_when_auto_download_is_off() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let app = mock_app();
        let facts = BufferedFactsStore::new(app.handle().clone());

        host_control
            .maybe_download(&unreachable_ctx(), &adapter, &facts, false)
            .await;

        assert_eq!(adapter.state(), UpdateState::Idle);
    }
}
