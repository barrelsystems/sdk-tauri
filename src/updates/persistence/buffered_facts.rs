use super::persistence_writer::PersistenceWriter;
use crate::updates::clock::now_iso8601;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tauri::{AppHandle, Runtime};

use super::disk_store::DiskStore;
use crate::updates::models::failure::UpdateFailure;
use crate::updates::models::state::UpdateState;

const MAX_BUFFERED_FACTS: usize = 32;

fn is_terminal_update_state(state: &UpdateState) -> bool {
    matches!(
        state,
        UpdateState::Downloaded
            | UpdateState::Applying
            | UpdateState::Failed { .. }
            | UpdateState::UpdateCancelled
    )
}

// Snapshot clones retain identity; disk and wire payloads only carry fact values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum BufferedFact {
    #[serde(rename = "lifecycleState")]
    LifecycleState {
        #[serde(skip, default = "uuid::Uuid::new_v4")]
        observation_id: uuid::Uuid,
        state: UpdateState,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
    #[serde(rename = "updateError")]
    UpdateError {
        #[serde(skip, default = "uuid::Uuid::new_v4")]
        observation_id: uuid::Uuid,
        #[serde(with = "persisted_failure")]
        failure: UpdateFailure,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
}

impl BufferedFact {
    fn discriminant(&self) -> String {
        match self {
            BufferedFact::UpdateError { failure, .. } => format!(
                "updateError:{:?}",
                (
                    failure.phase,
                    failure.failure_mode,
                    &failure.platform,
                    &failure.platform_code
                )
            ),
            BufferedFact::LifecycleState { state, .. } => format!("lifecycleState:{state:?}"),
        }
    }

    fn priority(&self) -> u8 {
        match self {
            BufferedFact::UpdateError { .. } => 1,
            BufferedFact::LifecycleState { state, .. } => u8::from(is_terminal_update_state(state)),
        }
    }

    fn last_observed_at(&self) -> &str {
        match self {
            BufferedFact::LifecycleState {
                last_observed_at, ..
            }
            | BufferedFact::UpdateError {
                last_observed_at, ..
            } => last_observed_at,
        }
    }

    fn first_observed_at(&self) -> &str {
        match self {
            BufferedFact::LifecycleState {
                first_observed_at, ..
            }
            | BufferedFact::UpdateError {
                first_observed_at, ..
            } => first_observed_at,
        }
    }

    fn occurrence_count(&self) -> u32 {
        match self {
            BufferedFact::LifecycleState {
                occurrence_count, ..
            }
            | BufferedFact::UpdateError {
                occurrence_count, ..
            } => *occurrence_count,
        }
    }

    fn with_merge(mut self, existing: &BufferedFact) -> Self {
        let first_observed_at = existing.first_observed_at().to_string();
        let occurrence_count = existing.occurrence_count() + 1;
        match &mut self {
            BufferedFact::LifecycleState {
                first_observed_at: f,
                occurrence_count: o,
                ..
            }
            | BufferedFact::UpdateError {
                first_observed_at: f,
                occurrence_count: o,
                ..
            } => {
                *f = first_observed_at;
                *o = occurrence_count;
            }
        }
        self
    }
}

pub struct BufferedFactsStore<R: Runtime> {
    buffer: Mutex<Vec<BufferedFact>>,
    disk: Arc<DiskStore<R>>,
    writer: PersistenceWriter<R>,
    // Bound once via `configure()` and never changed afterward -- every
    // other method reads this instead of taking its own `path` argument.
    path: std::sync::OnceLock<PathBuf>,
}

impl<R: Runtime> BufferedFactsStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        let disk = Arc::new(DiskStore::new(app));
        let writer = PersistenceWriter::new(disk.clone());

        Self {
            buffer: Mutex::new(Vec::new()),
            disk,
            writer,
            path: std::sync::OnceLock::new(),
        }
    }

    /// Binds this store's on-disk path and hydrates the in-memory buffer
    /// from it. Call once at startup, before any fact is recorded.
    pub async fn configure(&self, path: &Path) {
        let _ = self.path.set(path.to_path_buf());

        let Some(value) = self.disk.read(path, "facts").await else {
            return;
        };
        let Ok(facts) = serde_json::from_value::<Vec<BufferedFact>>(value) else {
            return;
        };
        *self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = facts;
    }

    /// A snapshot of the current facts, for inclusion in a check-in
    /// request. Never the live buffer.
    pub async fn snapshot(&self) -> Vec<BufferedFact> {
        self.buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Acknowledge only the sent observations; preserve newer equal-valued or concurrently
    /// recorded facts.
    pub async fn clear(&self, sent: &[BufferedFact]) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buffer.retain(|fact| !sent.contains(fact));
        self.persist_locked(&buffer);
    }

    pub async fn record_lifecycle_state(&self, state: UpdateState) {
        self.record_lifecycle_state_now(state);
    }

    pub fn record_lifecycle_state_now(&self, state: UpdateState) {
        if !state.is_reportable() {
            return;
        }

        let now = now_iso8601();
        self.upsert(BufferedFact::LifecycleState {
            observation_id: uuid::Uuid::new_v4(),
            state,
            first_observed_at: now.clone(),
            last_observed_at: now,
            occurrence_count: 1,
        });
    }

    pub async fn record_update_error(&self, failure: UpdateFailure) {
        let now = now_iso8601();
        self.upsert(BufferedFact::UpdateError {
            observation_id: uuid::Uuid::new_v4(),
            failure,
            first_observed_at: now.clone(),
            last_observed_at: now,
            occurrence_count: 1,
        });
    }

    /// `getLastUpdateError()`'s fallback source on a fresh process: the
    /// most recently observed buffered `updateError` fact, picked by
    /// `last_observed_at`.
    pub fn latest_update_error(&self) -> Option<UpdateFailure> {
        // Synchronous and self-contained -- dropped well before any disk
        // I/O this store does elsewhere.
        let buffer = self.buffer.lock().ok()?;
        buffer
            .iter()
            .filter_map(|fact| match fact {
                BufferedFact::UpdateError {
                    failure,
                    last_observed_at,
                    ..
                } => Some((last_observed_at.as_str(), failure)),
                BufferedFact::LifecycleState { .. } => None,
            })
            .max_by_key(|(last_observed_at, _)| *last_observed_at)
            // A full `UpdateError` already carries its own resolved `title`,
            // so no further lookup is needed here.
            .map(|(_, failure)| failure.clone())
    }

    fn upsert(&self, candidate: BufferedFact) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let discriminant = candidate.discriminant();

        if let Some(existing) = buffer
            .last()
            .filter(|fact| fact.discriminant() == discriminant)
        {
            let merged = candidate.with_merge(existing);
            let existing_index = buffer.len() - 1;
            buffer[existing_index] = merged;
            self.persist_locked(&buffer);
            return;
        }

        if buffer.len() >= MAX_BUFFERED_FACTS {
            let Some(eviction_index) = find_eviction_candidate_index(&buffer) else {
                return;
            };
            // A full buffer of terminal facts is never displaced by an
            // incoming intermediate one -- the incoming fact is dropped.
            if buffer[eviction_index].priority() > candidate.priority() {
                return;
            }
            buffer.remove(eviction_index);
        }

        buffer.push(candidate);
        self.persist_locked(&buffer);
    }

    fn persist_locked(&self, buffer: &[BufferedFact]) {
        let Some(path) = self.path.get() else { return };
        let Ok(value) = serde_json::to_value(buffer) else {
            return;
        };
        self.writer.persist(path, "facts", value);
    }

    /// Waits only at explicit durability boundaries, never while holding buffer state.
    #[cfg(any(test, feature = "test-service"))]
    pub async fn wait_for_pending_writes(&self) {
        self.writer.wait_for_pending_writes().await;
    }

    /// Serializes exit with any running writer and persists the latest snapshot.
    pub fn flush_sync(&self) {
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.persist_locked(&buffer);
        self.writer.flush_sync();
    }
}

fn find_eviction_candidate_index(buffer: &[BufferedFact]) -> Option<usize> {
    buffer
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.priority()
                .cmp(&b.priority())
                .then_with(|| a.last_observed_at().cmp(b.last_observed_at()))
        })
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::models::failure::{FailureCategory, UpdateFailureMode};
    use crate::updates::persistence::test_support::mock_app;
    use tempfile::NamedTempFile;

    fn failed_state() -> UpdateState {
        UpdateState::Failed {
            category: FailureCategory::Download,
            code: UpdateFailureMode::Unknown,
            platform_code: "x".to_string(),
        }
    }

    fn update_error(platform_code: &str) -> UpdateFailure {
        UpdateFailure {
            phase: crate::updates::models::failure::Phase::Download,
            failure_mode: UpdateFailureMode::Unknown,
            title: "x".to_string(),
            platform: "tauri-updater".to_string(),
            platform_code: platform_code.to_string(),
        }
    }

    #[tokio::test]
    async fn failure_titles_are_derived_after_reload_and_never_persisted() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("facts.json");
        store.configure(&path).await;
        store.record_update_error(update_error("persisted")).await;
        store.wait_for_pending_writes().await;
        let disk: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(disk["facts"][0]["failure"].get("title").is_none());
        store.wait_for_pending_writes().await;
        let second_app = mock_app();
        let second = BufferedFactsStore::new(second_app.handle().clone());
        second.configure(&path).await;
        let failure = second.latest_update_error().unwrap();
        assert_eq!(failure.platform_code, "persisted");
        assert_eq!(failure.title, "An unclassified update failure occurred.");
    }

    #[tokio::test]
    async fn merges_a_repeated_fact_instead_of_duplicating_it() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_lifecycle_state(UpdateState::Idle).await;
        store.record_lifecycle_state(UpdateState::Idle).await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].occurrence_count(), 2);
    }

    #[tokio::test]
    async fn clear_only_removes_facts_present_in_the_sent_snapshot() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_lifecycle_state(UpdateState::Checking).await;
        let sent = store.snapshot().await;
        store.record_lifecycle_state(failed_state()).await; // recorded mid-flight

        store.clear(&sent).await;

        let remaining = store.snapshot().await;
        assert_eq!(remaining.len(), 1);
        assert!(matches!(
            remaining[0],
            BufferedFact::LifecycleState {
                state: UpdateState::Failed { .. },
                ..
            }
        ));
    }

    fn fixed_fact(state: UpdateState) -> BufferedFact {
        BufferedFact::LifecycleState {
            observation_id: uuid::Uuid::new_v4(),
            state,
            first_observed_at: "2026-01-01T00:00:00.000Z".into(),
            last_observed_at: "2026-01-01T00:00:00.000Z".into(),
            occurrence_count: 1,
        }
    }

    #[tokio::test]
    async fn acknowledgment_preserves_an_unsent_equal_valued_observation() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        store.upsert(fixed_fact(UpdateState::Checking));
        let sent = store.snapshot().await;
        store.upsert(fixed_fact(UpdateState::Downloaded));
        store.upsert(fixed_fact(UpdateState::Checking));
        let before = store.snapshot().await;
        assert_eq!(
            serde_json::to_value(&before[0]).unwrap(),
            serde_json::to_value(&before[2]).unwrap()
        );

        store.clear(&sent).await;
        let remaining = store.snapshot().await;
        assert_eq!(remaining, before[1..]);

        store.clear(&sent).await;
        assert_eq!(store.snapshot().await, remaining);
    }

    #[tokio::test]
    async fn acknowledgment_cannot_clear_an_equal_replacement_after_eviction() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        store.upsert(fixed_fact(UpdateState::Checking));
        let sent = store.snapshot().await;
        for progress in 0..32 {
            store.upsert(fixed_fact(UpdateState::Downloading {
                progress: Some(progress),
            }));
        }
        store.upsert(fixed_fact(UpdateState::Checking));
        let before = store.snapshot().await;

        store.clear(&sent).await;
        assert_eq!(store.snapshot().await, before);
    }

    #[tokio::test]
    async fn a_full_buffer_of_terminal_facts_drops_an_incoming_intermediate_one() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        for i in 0..32 {
            store
                .record_update_error(update_error(&format!("code-{i}")))
                .await;
        }
        assert_eq!(store.snapshot().await.len(), 32);

        store.record_lifecycle_state(UpdateState::Checking).await;
        assert_eq!(store.snapshot().await.len(), 32); // dropped, not evicted-in
    }

    #[tokio::test]
    async fn latest_update_error_picks_the_most_recently_observed_one() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_update_error(update_error("first")).await;
        store.record_update_error(update_error("second")).await;

        let latest = store.latest_update_error().expect("a fact was buffered");
        assert_eq!(latest.platform_code, "second");
    }

    #[tokio::test]
    async fn latest_update_error_is_none_when_nothing_is_buffered() {
        let app = mock_app();
        let store = BufferedFactsStore::new(app.handle().clone());
        assert!(store.latest_update_error().is_none());
    }

    #[tokio::test]
    async fn configure_reads_back_facts_persisted_by_a_fresh_instance_across_a_simulated_restart() {
        let app = mock_app();
        let first = BufferedFactsStore::new(app.handle().clone());
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        first.configure(&path).await;
        first.record_update_error(update_error("first")).await;

        first.wait_for_pending_writes().await;

        // A separate mock app -- its own `AppHandle` and resource table,
        // so this can only pass by genuinely reading `path` back.
        let second_app = mock_app();
        let second = BufferedFactsStore::new(second_app.handle().clone());
        second.configure(&path).await;

        let snapshot = second.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(second.latest_update_error().unwrap().platform_code, "first");
    }
}

mod persisted_failure {
    use crate::updates::models::failure::{Phase, UpdateFailure, UpdateFailureMode};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StoredFailure {
        phase: Phase,
        failure_mode: UpdateFailureMode,
        platform: String,
        platform_code: String,
    }

    pub fn serialize<S: Serializer>(
        failure: &UpdateFailure,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        StoredFailure {
            phase: failure.phase,
            failure_mode: failure.failure_mode,
            platform: failure.platform.clone(),
            platform_code: failure.platform_code.clone(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<UpdateFailure, D::Error> {
        let stored = StoredFailure::deserialize(deserializer)?;
        Ok(crate::updates::error_taxonomy::build_failure_state(
            &stored.platform,
            stored.failure_mode,
            stored.phase,
            &stored.platform_code,
        )
        .0)
    }
}
