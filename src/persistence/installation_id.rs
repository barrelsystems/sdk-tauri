use std::path::Path;
use std::sync::{Arc, OnceLock};
use tauri::{AppHandle, Runtime};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::disk_store::DiskStore;

pub struct InstallationIdStore<R: Runtime> {
    cached: OnceLock<String>,
    resolution: Mutex<()>,
    saving: Arc<Mutex<()>>,
    disk: Arc<DiskStore<R>>,
}

impl<R: Runtime> InstallationIdStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        Self {
            cached: OnceLock::new(),
            resolution: Mutex::new(()),
            saving: Arc::new(Mutex::new(())),
            disk: Arc::new(DiskStore::new(app)),
        }
    }

    pub async fn get_or_create(&self, path: &Path) -> String {
        let _resolution = self.resolution.lock().await;
        let id = match self.cached.get() {
            Some(id) => id.clone(),
            None => {
                let id = self
                    .disk
                    .read(path, "installationId")
                    .await
                    .and_then(|value| value.as_str().map(str::to_string))
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let _ = self.cached.set(id.clone());
                id
            }
        };
        self.retry_persistence(path);
        self.wait_for_pending_persistence().await;
        id
    }

    // Reserve the save before spawning so completion waits also see a job
    // that has not been polled yet. A blocked save never queues more retries.
    pub fn retry_persistence(&self, path: &Path) {
        let Some(id) = self.cached.get().cloned() else {
            return;
        };
        let Ok(guard) = self.saving.clone().try_lock_owned() else {
            return;
        };
        let disk = self.disk.clone();
        let path = path.to_path_buf();
        tauri::async_runtime::spawn(async move {
            disk.write(&path, "installationId", serde_json::json!(id))
                .await;
            drop(guard);
        });
    }

    pub async fn wait_for_pending_persistence(&self) {
        let _guard = self.saving.lock().await;
    }

    pub fn flush_sync(&self, path: &Path) {
        if let Some(id) = self.cached.get() {
            self.disk
                .write_sync(path, "installationId", serde_json::json!(id));
        }
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
    async fn retries_the_same_cached_id_when_storage_recovers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("installation.json");
        std::fs::create_dir(&path).unwrap();
        let app = mock_app();
        let store = InstallationIdStore::new(app.handle().clone());
        let first = store.get_or_create(&path).await;
        std::fs::remove_dir(&path).unwrap();

        assert_eq!(store.get_or_create(&path).await, first);
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["installationId"], first);
    }

    #[tokio::test]
    async fn exit_flush_recovers_the_original_id_without_another_async_lookup() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("installation.json");
        std::fs::create_dir(&path).unwrap();
        let app = mock_app();
        let store = InstallationIdStore::new(app.handle().clone());
        let first = store.get_or_create(&path).await;
        std::fs::remove_dir(&path).unwrap();

        store.flush_sync(&path);
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["installationId"], first);
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
