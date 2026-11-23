use std::path::Path;
use tauri::{AppHandle, Manager, Runtime};
use tauri_plugin_store::StoreExt;

pub(crate) struct StorageDirectory(pub Option<std::path::PathBuf>);

/// Async store operations run on a blocking thread; only process exit
/// uses synchronous I/O. Unavailable storage keeps the runtime memory-only.
pub struct DiskStore<R: Runtime> {
    app: AppHandle<R>,
    persistent: bool,
}

impl<R: Runtime> DiskStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        let persistent = app
            .try_state::<StorageDirectory>()
            .is_none_or(|directory| directory.0.is_some());
        Self { app, persistent }
    }

    /// Reads `key` from the store at `path`. Always reloads from disk
    /// first, so this never serves a stale cached value -- the only read
    /// path these stores have. `None` on any failure.
    pub async fn read(&self, path: &Path, key: &str) -> Option<serde_json::Value> {
        if !self.persistent {
            return None;
        }

        let app = self.app.clone();
        let path = path.to_path_buf();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let store = app.store_builder(&path).disable_auto_save().build().ok()?;
            let _ = store.reload();
            store.get(&key)
        })
        .await
        .ok()?
    }

    /// Writes `key` -> `value` and saves immediately. Fails open:
    /// memory remains authoritative when saving fails.
    pub async fn write(&self, path: &Path, key: &str, value: serde_json::Value) {
        if !self.persistent {
            return;
        }

        let app = self.app.clone();
        let path = path.to_path_buf();
        let key = key.to_string();
        let _ = tokio::task::spawn_blocking(move || -> Option<()> {
            let store = app.store_builder(&path).disable_auto_save().build().ok()?;
            store.set(key, value);
            store.save().ok()
        })
        .await;
    }

    /// The synchronous counterpart to `write`, for the one call site that
    /// can't `.await` (`RunEvent::Exit`). Fails open, same as `write`.
    pub fn write_sync(&self, path: &Path, key: &str, value: serde_json::Value) {
        if !self.persistent {
            return;
        }

        let Ok(store) = self.app.store_builder(path).disable_auto_save().build() else {
            return;
        };
        store.set(key.to_string(), value);
        let _ = store.save();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::persistence::test_support::mock_app;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn unavailable_storage_preserves_one_in_memory_installation_without_writing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("installation.json");
        let app = mock_app();
        app.manage(StorageDirectory(None));
        let store = crate::updates::persistence::installation_id::InstallationIdStore::new(
            app.handle().clone(),
        );
        let first = store.get_or_create(&path).await;

        assert_eq!(store.get_or_create(&path).await, first);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn persists_and_the_write_is_readable_back() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        let app = mock_app();
        let store = DiskStore::new(app.handle().clone());
        store
            .write(&path, "greeting", serde_json::json!("hello"))
            .await;

        assert_eq!(
            store.read(&path, "greeting").await,
            Some(serde_json::json!("hello"))
        );
    }

    #[tokio::test]
    async fn fails_open_without_panicking_when_the_parent_path_cannot_be_a_directory() {
        // `path`'s parent is itself a plain file, so `create_dir_all` can
        // never succeed here -- the real shape of a permanently blocked write.
        let blocking_file = NamedTempFile::new().unwrap();
        let path = blocking_file.path().join("subdir").join("data.json");

        let app = mock_app();
        let store = DiskStore::new(app.handle().clone());
        store
            .write(&path, "greeting", serde_json::json!("hello"))
            .await; // must not panic
    }
}
