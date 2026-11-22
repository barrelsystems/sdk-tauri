use std::path::Path;
use tauri::{AppHandle, Runtime};
use tauri_plugin_store::StoreExt;

/// A single key-value JSON file, backed by `tauri-plugin-store`. Every
/// async read/write runs on a blocking-pool thread, since the plugin's
/// own store calls are synchronous -- `write_sync` is the one exception.
pub struct DiskStore<R: Runtime> {
    app: AppHandle<R>,
}

impl<R: Runtime> DiskStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        Self { app }
    }

    /// Reads `key` from the store at `path`. Always reloads from disk
    /// first, so this never serves a stale cached value -- the only read
    /// path these stores have. `None` on any failure.
    pub async fn read(&self, path: &Path, key: &str) -> Option<serde_json::Value> {
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
    /// swallows every error, same as the code this replaces.
    pub async fn write(&self, path: &Path, key: &str, value: serde_json::Value) {
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
    use crate::persistence::test_support::mock_app;
    use tempfile::NamedTempFile;

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
