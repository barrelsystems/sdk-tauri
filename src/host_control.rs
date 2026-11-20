use std::sync::atomic::{AtomicBool, Ordering};

use crate::adapters::Adapter;
use crate::persistence::buffered_facts::BufferedFactsStore;
use crate::protocol::pre_action_check::{check_pre_action, PreActionCheckParams, Stage};
use crate::types::{AdapterState, Policy};

/// Everything a pre-action-check needs, bundled once per cadence tick
/// rather than threaded through as separate positional arguments.
pub struct ActionContext<'a> {
    pub client: &'a reqwest::Client,
    pub base_url: &'a str,
    pub credential: &'a str,
    pub installation_id: &'a str,
    pub trusted_key_pem: &'a str,
    pub log: &'a (dyn Fn(&str) + Send + Sync),
}

/// Gates every download and apply on a fresh `pre-action-check`, not just
/// the cadence's own 30-minute Policy -- a Hold placed since the last
/// check-in must still land in time to stop either action.
pub struct HostControl {
    pending_install: AtomicBool,
}

/// A fresh pre-action-check for `stage`, gating the caller on whatever
/// Policy Barrel returns right now -- not the cadence's own cached one.
async fn can_proceed(ctx: &ActionContext<'_>, stage: Stage) -> bool {
    let policy = check_pre_action(
        ctx.client,
        PreActionCheckParams {
            base_url: ctx.base_url,
            credential: ctx.credential,
            installation_id: ctx.installation_id,
            stage,
            trusted_key_pem: ctx.trusted_key_pem,
        },
        ctx.log,
    )
    .await;

    !matches!(policy, Some(Policy::Held { .. }))
}

impl HostControl {
    pub fn new() -> Self {
        Self {
            pending_install: AtomicBool::new(false),
        }
    }

    pub fn has_pending_install(&self) -> bool {
        self.pending_install.load(Ordering::SeqCst)
    }

    /// Call after `Adapter::check_for_updates` -- downloads only if the
    /// adapter landed on `Available` and the host has `auto_download` on.
    pub async fn maybe_download(
        &self,
        ctx: &ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore,
        auto_download: bool,
    ) {
        if auto_download {
            self.download_update(ctx, adapter, facts).await;
        }
    }

    /// Host-triggered earlier download, for a host that set `auto_download:
    /// false` and otherwise had no way to ever complete an update. A no-op,
    /// not an error, unless the adapter is currently `Available`.
    pub async fn download_update(
        &self,
        ctx: &ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore,
    ) {
        if !matches!(adapter.state(), AdapterState::Available { .. }) {
            return;
        }

        if can_proceed(ctx, Stage::PreDownload).await {
            adapter.download_update(facts).await;
            if adapter.state() == AdapterState::Downloaded {
                self.pending_install.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Host-triggered earlier apply, ahead of the natural-quit default. A
    /// no-op, not an error, before anything has actually downloaded --
    /// there's nothing yet to apply.
    pub async fn apply_update(
        &self,
        ctx: &ActionContext<'_>,
        adapter: &Adapter,
        facts: &BufferedFactsStore,
    ) {
        if adapter.state() != AdapterState::Downloaded {
            return;
        }

        if can_proceed(ctx, Stage::PreApply).await {
            adapter.quit_and_install(facts).await;

            // A failed install leaves the adapter in `Failed`, not stuck
            // at `Downloaded`.

            // Clearing the flag here is what stops `RunEvent::ExitRequested`
            // from retrying the same failing install forever on every
            // subsequent exit attempt.
            if matches!(adapter.state(), AdapterState::Failed { .. }) {
                self.pending_install.store(false, Ordering::SeqCst);
            }
        }
    }
}

impl Default for HostControl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::buffered_facts::BufferedFactsStore;

    // An unreachable address, never actually dialed: every test here exercises
    // only the guard clause that returns before `can_proceed`'s network call.
    fn unreachable_ctx() -> ActionContext<'static> {
        static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        ActionContext {
            client: CLIENT.get_or_init(reqwest::Client::new),
            base_url: "http://127.0.0.1:0",
            credential: "unused",
            installation_id: "unused",
            trusted_key_pem: "unused",
            log: &(|_: &str| {}),
        }
    }

    #[tokio::test]
    async fn download_update_is_a_no_op_when_the_adapter_is_not_available() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let facts = BufferedFactsStore::new();

        host_control
            .download_update(&unreachable_ctx(), &adapter, &facts)
            .await;

        assert_eq!(adapter.state(), AdapterState::Idle);
        assert!(!host_control.has_pending_install());
    }

    #[tokio::test]
    async fn apply_update_is_a_no_op_when_nothing_has_downloaded() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let facts = BufferedFactsStore::new();

        host_control
            .apply_update(&unreachable_ctx(), &adapter, &facts)
            .await;

        // No panic and no state change is the whole assertion -- there's
        // nothing yet to apply, so this must stay a quiet no-op.
        assert_eq!(adapter.state(), AdapterState::Idle);
    }

    #[tokio::test]
    async fn maybe_download_does_nothing_when_auto_download_is_off() {
        let host_control = HostControl::new();
        let adapter = Adapter::new();
        let facts = BufferedFactsStore::new();

        host_control
            .maybe_download(&unreachable_ctx(), &adapter, &facts, false)
            .await;

        assert_eq!(adapter.state(), AdapterState::Idle);
    }
}
