use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::Mutex;

use super::disk_store::DiskStore;
use crate::types::{AdapterState, UpdateFailure};

const MAX_BUFFERED_FACTS: usize = 32;

fn is_terminal_adapter_state(state: &AdapterState) -> bool {
    matches!(
        state,
        AdapterState::Downloaded | AdapterState::Failed { .. } | AdapterState::UpdateCancelled
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum BufferedFact {
    #[serde(rename = "adapterState")]
    AdapterState {
        state: AdapterState,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
    #[serde(rename = "updateError")]
    UpdateError {
        failure: UpdateFailure,
        first_observed_at: String,
        last_observed_at: String,
        occurrence_count: u32,
    },
}

impl BufferedFact {
    fn discriminant(&self) -> String {
        match self {
            BufferedFact::UpdateError { failure, .. } => {
                format!(
                    "updateError:{:?}:{}",
                    failure.failure_mode, failure.platform_code
                )
            }
            BufferedFact::AdapterState {
                state:
                    AdapterState::Failed {
                        code,
                        platform_code,
                        ..
                    },
                ..
            } => {
                format!("adapterState:failed:{code:?}:{platform_code}")
            }
            BufferedFact::AdapterState { state, .. } => format!("adapterState:{state:?}"),
        }
    }

    fn priority(&self) -> u8 {
        match self {
            BufferedFact::UpdateError { .. } => 1,
            BufferedFact::AdapterState { state, .. } => u8::from(is_terminal_adapter_state(state)),
        }
    }

    fn last_observed_at(&self) -> &str {
        match self {
            BufferedFact::AdapterState {
                last_observed_at, ..
            }
            | BufferedFact::UpdateError {
                last_observed_at, ..
            } => last_observed_at,
        }
    }

    fn first_observed_at(&self) -> &str {
        match self {
            BufferedFact::AdapterState {
                first_observed_at, ..
            }
            | BufferedFact::UpdateError {
                first_observed_at, ..
            } => first_observed_at,
        }
    }

    fn occurrence_count(&self) -> u32 {
        match self {
            BufferedFact::AdapterState {
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
            BufferedFact::AdapterState {
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

pub struct BufferedFactsStore {
    buffer: Mutex<Vec<BufferedFact>>,
    disk: DiskStore,
    // Bound once via `configure()` and never changed afterward -- every
    // other method reads this instead of taking its own `path` argument.
    path: std::sync::OnceLock<PathBuf>,
}

impl BufferedFactsStore {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
            disk: DiskStore::new(),
            path: std::sync::OnceLock::new(),
        }
    }

    /// Binds this store's on-disk path and hydrates the in-memory buffer
    /// from it. Call once at startup, before any fact is recorded.
    pub async fn configure(&self, path: &PathBuf) {
        let _ = self.path.set(path.clone());

        let Ok(raw) = tokio::fs::read_to_string(path).await else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<FactsOnDisk>(&raw) else {
            return;
        };
        *self.buffer.lock().await = parsed.facts;
    }

    /// A snapshot of the current facts, for inclusion in a check-in
    /// request. Never the live buffer.
    pub async fn snapshot(&self) -> Vec<BufferedFact> {
        self.buffer.lock().await.clone()
    }

    /// Clears exactly the facts in `sent` (a snapshot `snapshot()` returned
    /// earlier) -- call only once ingestion ACKs the check-in that carried
    /// them, never optimistically.
    ///
    /// A fact recorded after `sent` was taken must survive this call.
    pub async fn clear(&self, sent: &[BufferedFact]) {
        let mut buffer = self.buffer.lock().await;
        buffer.retain(|fact| !sent.contains(fact));
        self.persist_locked(&buffer).await;
    }

    pub async fn record_adapter_state(&self, state: AdapterState) {
        let now = now_iso8601();
        self.upsert(BufferedFact::AdapterState {
            state,
            first_observed_at: now.clone(),
            last_observed_at: now,
            occurrence_count: 1,
        })
        .await;
    }

    pub async fn record_update_error(&self, failure: UpdateFailure) {
        let now = now_iso8601();
        self.upsert(BufferedFact::UpdateError {
            failure,
            first_observed_at: now.clone(),
            last_observed_at: now,
            occurrence_count: 1,
        })
        .await;
    }

    /// `getLastUpdateError()`'s fallback source on a fresh process: the
    /// most recently observed buffered `updateError` fact, picked by
    /// `last_observed_at`.
    ///
    /// A full `UpdateError` already carries its own `title`, resolved at
    /// the time it was recorded, so no further lookup is needed here.
    ///
    /// `None` if nothing's buffered, or if the buffer happens to be locked
    /// by a concurrent async write at this exact synchronous-call instant
    /// -- fail-open, same posture as every other read here.
    pub fn latest_update_error(&self) -> Option<UpdateFailure> {
        let buffer = self.buffer.try_lock().ok()?;
        buffer
            .iter()
            .filter_map(|fact| match fact {
                BufferedFact::UpdateError {
                    failure,
                    last_observed_at,
                    ..
                } => Some((last_observed_at.as_str(), failure)),
                BufferedFact::AdapterState { .. } => None,
            })
            .max_by_key(|(last_observed_at, _)| *last_observed_at)
            .map(|(_, failure)| failure.clone())
    }

    async fn upsert(&self, candidate: BufferedFact) {
        let mut buffer = self.buffer.lock().await;
        let discriminant = candidate.discriminant();

        if let Some(existing_index) = buffer
            .iter()
            .position(|fact| fact.discriminant() == discriminant)
        {
            let merged = candidate.with_merge(&buffer[existing_index]);
            buffer[existing_index] = merged;
            self.persist_locked(&buffer).await;
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
        self.persist_locked(&buffer).await;
    }

    async fn persist_locked(&self, buffer: &[BufferedFact]) {
        let Some(path) = self.path.get() else { return };
        let snapshot = serde_json::to_string(&FactsOnDisk {
            facts: buffer.to_vec(),
        })
        .unwrap_or_default();
        self.disk.persist(path, &snapshot).await;
    }

    /// The reserved synchronous path for process exit (`RunEvent::Exit`),
    /// which can't `.await`.
    ///
    /// Best-effort: if the buffer is locked by an in-flight async write at
    /// this exact instant, this silently skips -- fail-open, same posture
    /// as every other persistence path here.
    pub fn flush_sync(&self) {
        let Some(path) = self.path.get() else { return };
        let Ok(buffer) = self.buffer.try_lock() else {
            return;
        };
        let Ok(snapshot) = serde_json::to_string(&FactsOnDisk {
            facts: buffer.to_vec(),
        }) else {
            return;
        };
        let _ = std::fs::write(path, snapshot);
    }
}

impl Default for BufferedFactsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize)]
struct FactsOnDisk {
    facts: Vec<BufferedFact>,
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

// A thin forward, not a duplicate -- `check_in` already imports
// `BufferedFact` from here, so importing its `now_iso8601` back would
// make the two modules depend on each other both ways.
fn now_iso8601() -> String {
    crate::protocol::check_in::now_iso8601()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FailureCategory, UpdateFailureMode};
    use tempfile::NamedTempFile;

    fn failed_state() -> AdapterState {
        AdapterState::Failed {
            category: FailureCategory::Download,
            code: UpdateFailureMode::ChecksumMismatch,
            platform_code: "x".to_string(),
        }
    }

    fn update_error(platform_code: &str) -> UpdateFailure {
        UpdateFailure {
            phase: crate::types::Phase::Download,
            failure_mode: UpdateFailureMode::Unknown,
            title: "x".to_string(),
            platform: "tauri-updater".to_string(),
            platform_code: platform_code.to_string(),
        }
    }

    #[tokio::test]
    async fn merges_a_repeated_fact_instead_of_duplicating_it() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_adapter_state(AdapterState::Idle).await;
        store.record_adapter_state(AdapterState::Idle).await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].occurrence_count(), 2);
    }

    #[tokio::test]
    async fn clear_only_removes_facts_present_in_the_sent_snapshot() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_adapter_state(AdapterState::Checking).await;
        let sent = store.snapshot().await;
        store.record_adapter_state(failed_state()).await; // recorded mid-flight

        store.clear(&sent).await;

        let remaining = store.snapshot().await;
        assert_eq!(remaining.len(), 1);
        assert!(matches!(
            remaining[0],
            BufferedFact::AdapterState {
                state: AdapterState::Failed { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_full_buffer_of_terminal_facts_drops_an_incoming_intermediate_one() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        for i in 0..32 {
            store
                .record_update_error(update_error(&format!("code-{i}")))
                .await;
        }
        assert_eq!(store.snapshot().await.len(), 32);

        store.record_adapter_state(AdapterState::Checking).await;
        assert_eq!(store.snapshot().await.len(), 32); // dropped, not evicted-in
    }

    #[tokio::test]
    async fn latest_update_error_picks_the_most_recently_observed_one() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();
        store.configure(&path).await;

        store.record_update_error(update_error("first")).await;
        store.record_update_error(update_error("second")).await;

        let latest = store.latest_update_error().expect("a fact was buffered");
        assert_eq!(latest.platform_code, "second");
    }

    #[tokio::test]
    async fn latest_update_error_is_none_when_nothing_is_buffered() {
        let store = BufferedFactsStore::new();
        assert!(store.latest_update_error().is_none());
    }
}
