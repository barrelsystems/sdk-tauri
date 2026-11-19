use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Mirrors an in-memory value to disk: a non-blocking, fail-open write on
/// `persist()`. Concurrent writers serialize onto one mutex.
pub struct DiskStore {
    mutex: Arc<Mutex<()>>,
}

impl DiskStore {
    pub fn new() -> Self {
        Self {
            mutex: Arc::new(Mutex::new(())),
        }
    }

    pub async fn persist(&self, path: &PathBuf, snapshot: &str) {
        let _guard = self.mutex.lock().await;

        let Some(parent) = path.parent() else { return };
        if tokio::fs::create_dir_all(parent).await.is_err() {
            return; // Fail open: disk full, permission denied, etc.
        }
        let _ = tokio::fs::write(path, snapshot).await;
    }
}

impl Default for DiskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn persists_and_the_write_is_readable_back() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        DiskStore::new().persist(&path, "hello").await;

        assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn fails_open_without_panicking_when_the_parent_path_cannot_be_a_directory() {
        // `path`'s parent is itself a plain file, so `create_dir_all` can
        // never succeed here -- the real shape of a permanently blocked write.
        let blocking_file = NamedTempFile::new().unwrap();
        let path = blocking_file.path().join("subdir").join("data.json");

        DiskStore::new().persist(&path, "hello").await; // must not panic
    }
}
