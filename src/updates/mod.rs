//! Update status, subscriptions, and manual actions.

pub(crate) mod adapters;
pub(crate) mod attributes;
pub(crate) mod cadence;
pub(crate) mod clock;
pub(crate) mod env;
pub(crate) mod error_taxonomy;
pub(crate) mod host_control;
pub(crate) mod internal_defects;
pub(crate) mod jitter;
pub(crate) mod keys;
pub(crate) mod models;
pub(crate) mod persistence;
/// Wire helpers available to the development-only conformance service.
#[cfg(feature = "test-service")]
pub mod protocol;
#[cfg(not(feature = "test-service"))]
pub(crate) mod protocol;
pub(crate) mod runtime;

use self::runtime::UpdateRuntime;
use self::runtime::{perform_apply, shared_for};
use std::sync::Arc;
use tauri::{AppHandle, Runtime};

pub use crate::updates::models::configuration::Config;
pub use crate::updates::models::eligibility::UpdateEligibility as Eligibility;
pub use crate::updates::models::failure::{
    FailureCategory, Phase, UpdateFailure as Failure, UpdateFailureMode as FailureMode,
};
pub use crate::updates::models::state::UpdateState as State;
pub use crate::updates::models::status::Status;

pub use models::action_error::ActionError;

/// Complete snapshots, initially current and subsequently coalesced by the receiver.
pub fn subscribe<R: Runtime>(app: &AppHandle<R>) -> Option<tokio::sync::watch::Receiver<Status>> {
    use tauri::Manager;
    app.try_state::<Arc<adapters::Adapter>>()
        .map(|adapter| adapter.subscribe_status())
}

/// Completes after verified download, or returns a classified action error.
pub async fn download<R: Runtime>(app: &AppHandle<R>) -> Result<(), ActionError> {
    perform_download(shared_for::<R>(app).ok_or(ActionError::NotReady)?).await
}

pub(crate) async fn perform_download<R: Runtime>(
    shared: Arc<UpdateRuntime<R>>,
) -> Result<(), ActionError> {
    let (base_url, trusted_key_pem) =
        cadence::resolve_connection_fields(&shared.config, &shared.trusted_keys).await;
    let ctx = shared.action_context(&base_url, &trusted_key_pem);
    shared
        .host_control
        .download_update(&ctx, &shared.adapter, &shared.facts)
        .await
}

/// Completes at installer handoff if the process survives; it cannot confirm relaunch.
pub async fn apply<R: Runtime>(app: &AppHandle<R>) -> Result<(), ActionError> {
    perform_apply(shared_for::<R>(app).ok_or(ActionError::NotReady)?).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tauri::Manager;

    #[test]
    fn public_subscriptions_deliver_current_snapshots_independently() {
        let app = tauri::test::mock_builder()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();
        assert!(subscribe(app.handle()).is_none());

        let adapter = Arc::new(adapters::Adapter::new());
        app.manage(adapter.clone());
        let first = subscribe(app.handle()).unwrap();
        assert_eq!(first.borrow().state, State::Idle);
        assert_eq!(first.borrow().eligibility, None);
        assert_eq!(first.borrow().last_error, None);

        let held = Eligibility::Held {
            issued_at: "2026-01-01T00:00:00Z".into(),
            environment: "test".into(),
            reason: None,
            until: None,
        };
        adapter.set_eligibility(held.clone());
        let mut late = subscribe(app.handle()).unwrap();
        assert_eq!(late.borrow_and_update().eligibility, Some(held));
        drop(first);

        let unrestricted = Eligibility::Unrestricted {
            issued_at: "2026-01-01T00:00:01Z".into(),
            environment: "test".into(),
        };
        adapter.set_eligibility(unrestricted.clone());
        assert!(late.has_changed().unwrap());
        assert_eq!(late.borrow_and_update().eligibility, Some(unrestricted));
    }

    #[tokio::test]
    async fn public_actions_report_not_ready_without_a_runtime() {
        let app = tauri::test::mock_builder()
            .build(tauri::test::mock_context(tauri::test::noop_assets()))
            .unwrap();

        assert!(matches!(
            download(app.handle()).await,
            Err(ActionError::NotReady)
        ));
        assert!(matches!(
            apply(app.handle()).await,
            Err(ActionError::NotReady)
        ));
    }
}
