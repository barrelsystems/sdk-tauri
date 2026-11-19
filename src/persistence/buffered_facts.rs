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
}

impl BufferedFactsStore {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
            disk: DiskStore::new(),
        }
    }

    /// Hydrates the in-memory buffer from disk. Call once at startup,
    /// before any fact is recorded.
    pub async fn configure(&self, path: &PathBuf) {
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
    /// them, never optimistically: a fact recorded after `sent` was taken
    /// must survive.
    pub async fn clear(&self, path: &PathBuf, sent: &[BufferedFact]) {
        let mut buffer = self.buffer.lock().await;
        buffer.retain(|fact| !sent.contains(fact));
        self.persist_locked(path, &buffer).await;
    }

    pub async fn record_adapter_state(&self, path: &PathBuf, state: AdapterState) {
        let now = now_iso8601();
        self.upsert(
            path,
            BufferedFact::AdapterState {
                state,
                first_observed_at: now.clone(),
                last_observed_at: now,
                occurrence_count: 1,
            },
        )
        .await;
    }

    pub async fn record_update_error(&self, path: &PathBuf, failure: UpdateFailure) {
        let now = now_iso8601();
        self.upsert(
            path,
            BufferedFact::UpdateError {
                failure,
                first_observed_at: now.clone(),
                last_observed_at: now,
                occurrence_count: 1,
            },
        )
        .await;
    }

    async fn upsert(&self, path: &PathBuf, candidate: BufferedFact) {
        let mut buffer = self.buffer.lock().await;
        let discriminant = candidate.discriminant();

        if let Some(existing_index) = buffer
            .iter()
            .position(|fact| fact.discriminant() == discriminant)
        {
            let merged = candidate.with_merge(&buffer[existing_index]);
            buffer[existing_index] = merged;
            self.persist_locked(path, &buffer).await;
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
        self.persist_locked(path, &buffer).await;
    }

    async fn persist_locked(&self, path: &PathBuf, buffer: &[BufferedFact]) {
        let snapshot = serde_json::to_string(&FactsOnDisk {
            facts: buffer.to_vec(),
        })
        .unwrap_or_default();
        self.disk.persist(path, &snapshot).await;
    }

    /// The reserved synchronous path for process exit (`RunEvent::Exit`),
    /// which can't `.await`. Best-effort: if the buffer is locked by an
    /// in-flight async write at this exact instant, this silently skips --
    /// fail-open, same posture as every other persistence path here.
    pub fn flush_sync(&self, path: &PathBuf) {
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

    #[tokio::test]
    async fn merges_a_repeated_fact_instead_of_duplicating_it() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();

        store.record_adapter_state(&path, AdapterState::Idle).await;
        store.record_adapter_state(&path, AdapterState::Idle).await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].occurrence_count(), 2);
    }

    #[tokio::test]
    async fn clear_only_removes_facts_present_in_the_sent_snapshot() {
        let store = BufferedFactsStore::new();
        let path = NamedTempFile::new().unwrap().path().to_path_buf();

        store
            .record_adapter_state(&path, AdapterState::Checking)
            .await;
        let sent = store.snapshot().await;
        store.record_adapter_state(&path, failed_state()).await; // recorded mid-flight

        store.clear(&path, &sent).await;

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

        for i in 0..32 {
            store
                .record_update_error(
                    &path,
                    UpdateFailure {
                        phase: crate::types::Phase::Download,
                        failure_mode: UpdateFailureMode::Unknown,
                        title: "x".to_string(),
                        platform: "tauri-updater".to_string(),
                        platform_code: format!("code-{i}"),
                    },
                )
                .await;
        }
        assert_eq!(store.snapshot().await.len(), 32);

        store
            .record_adapter_state(&path, AdapterState::Checking)
            .await;
        assert_eq!(store.snapshot().await.len(), 32); // dropped, not evicted-in
    }
}
