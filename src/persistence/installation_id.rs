use std::path::PathBuf;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::disk_store::DiskStore;

pub struct InstallationIdStore {
    cached: Mutex<Option<String>>,
    disk: DiskStore,
}

impl InstallationIdStore {
    pub fn new() -> Self {
        Self {
            cached: Mutex::new(None),
            disk: DiskStore::new(),
        }
    }

    /// Reads the persisted installation ID, or generates and persists a
    /// fresh one. Every I/O call is async and fails open -- a write
    /// failure keeps the in-memory ID for this process's lifetime rather
    /// than regenerating.
    pub async fn get_or_create(&self, path: &PathBuf) -> String {
        let mut cached = self.cached.lock().await;
        if let Some(id) = cached.as_ref() {
            return id.clone();
        }

        let id = match read_existing(path).await {
            Some(existing) => existing,
            None => {
                let generated = Uuid::new_v4().to_string();
                let snapshot = serde_json::json!({ "installationId": generated }).to_string();
                self.disk.persist(path, &snapshot).await;
                generated
            }
        };

        *cached = Some(id.clone());
        id
    }
}

impl Default for InstallationIdStore {
    fn default() -> Self {
        Self::new()
    }
}

async fn read_existing(path: &PathBuf) -> Option<String> {
    let raw = tokio::fs::read_to_string(path).await.ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).ok()?;
    parsed.get("installationId")?.as_str().map(str::to_string)
}
