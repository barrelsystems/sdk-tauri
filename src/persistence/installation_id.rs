use std::path::Path;
use tauri::{AppHandle, Runtime};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::disk_store::DiskStore;

pub struct InstallationIdStore<R: Runtime> {
    cached: Mutex<Option<String>>,
    disk: DiskStore<R>,
}

impl<R: Runtime> InstallationIdStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        Self {
            cached: Mutex::new(None),
            disk: DiskStore::new(app),
        }
    }

    /// Reads the persisted installation ID, or generates and persists a
    /// fresh one. Every I/O call is async and fails open -- a write
    /// failure keeps the in-memory ID for this process's lifetime rather
    /// than regenerating.
    pub async fn get_or_create(&self, path: &Path) -> String {
        let mut cached = self.cached.lock().await;
        if let Some(id) = cached.as_ref() {
            return id.clone();
        }

        let existing = self
            .disk
            .read(path, "installationId")
            .await
            .and_then(|value| value.as_str().map(str::to_string));

        let id = match existing {
            Some(existing) => existing,
            None => {
                let generated = Uuid::new_v4().to_string();
                self.disk
                    .write(path, "installationId", serde_json::json!(generated))
                    .await;
                generated
            }
        };

        *cached = Some(id.clone());
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::mock_app;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn generates_and_persists_a_fresh_id_when_nothing_exists_yet() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let app = mock_app();
        let store = InstallationIdStore::new(app.handle().clone());
        let id = store.get_or_create(&path).await;

        assert!(!id.is_empty());
        assert_eq!(store.get_or_create(&path).await, id); // cached, not regenerated
    }

    #[tokio::test]
    async fn reads_back_an_id_persisted_by_a_fresh_instance_across_a_simulated_restart() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let first_app = mock_app();
        let first = InstallationIdStore::new(first_app.handle().clone());
        let generated = first.get_or_create(&path).await;

        // A separate mock app/AppHandle -- no in-memory cache survives
        // from `first`, so this can only pass by genuinely reading `path`.
        let second_app = mock_app();
        let second = InstallationIdStore::new(second_app.handle().clone());
        assert_eq!(second.get_or_create(&path).await, generated);
    }
}
