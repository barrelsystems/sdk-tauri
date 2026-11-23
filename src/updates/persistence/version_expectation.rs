use std::path::Path;
use tauri::{AppHandle, Runtime};

use super::disk_store::DiskStore;

const KEY: &str = "expectedVersion";

/// Persists which version an apply attempt expected to be
/// running, so the next startup can confirm it actually landed.
pub struct VersionExpectationStore<R: Runtime> {
    disk: DiskStore<R>,
}

impl<R: Runtime> VersionExpectationStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        Self {
            disk: DiskStore::new(app),
        }
    }

    /// Call once at startup, before deciding whether the previous
    /// session's apply attempt actually landed.
    pub async fn read(&self, path: &Path) -> Option<String> {
        self.disk
            .read(path, KEY)
            .await
            .and_then(|value| value.as_str().map(str::to_string))
    }

    /// Call immediately before triggering apply, with the version being applied.
    pub async fn set(&self, path: &Path, version: &str) {
        self.disk.write(path, KEY, serde_json::json!(version)).await;
    }

    /// Call once the current startup's expectation has been resolved either way.
    pub async fn clear(&self, path: &Path) {
        self.disk.write(path, KEY, serde_json::Value::Null).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::persistence::test_support::mock_app;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn returns_none_when_nothing_was_ever_expected() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let app = mock_app();
        let store = VersionExpectationStore::new(app.handle().clone());
        assert_eq!(store.read(&path).await, None);
    }

    #[tokio::test]
    async fn persists_an_expected_version_and_reads_it_back_after_a_simulated_restart() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let first_app = mock_app();
        let first = VersionExpectationStore::new(first_app.handle().clone());
        first.set(&path, "2.0.0").await;

        // A separate mock app/AppHandle -- no in-memory cache survives
        // from `first`, so this can only pass by genuinely reading `path`.
        let second_app = mock_app();
        let second = VersionExpectationStore::new(second_app.handle().clone());
        assert_eq!(second.read(&path).await, Some("2.0.0".to_string()));
    }

    #[tokio::test]
    async fn clearing_the_expectation_persists_across_a_restart() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let app = mock_app();
        let store = VersionExpectationStore::new(app.handle().clone());
        store.set(&path, "2.0.0").await;
        store.clear(&path).await;

        let second_app = mock_app();
        let second = VersionExpectationStore::new(second_app.handle().clone());
        assert_eq!(second.read(&path).await, None);
    }
}
