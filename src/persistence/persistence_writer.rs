use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tauri::Runtime;
use tokio::sync::watch;

use super::disk_store::DiskStore;

#[derive(Clone)]
struct PendingWrite {
    revision: u64,
    path: PathBuf,
    key: String,
    value: serde_json::Value,
}

pub(crate) struct PersistenceWriter<R: Runtime> {
    disk: Arc<DiskStore<R>>,
    pending: watch::Sender<Option<PendingWrite>>,
    completed: watch::Sender<u64>,
    io: Arc<Mutex<()>>,
    exiting: Arc<AtomicBool>,
}

impl<R: Runtime> PersistenceWriter<R> {
    pub fn new(disk: Arc<DiskStore<R>>) -> Self {
        let (pending, mut updates) = watch::channel::<Option<PendingWrite>>(None);
        let (completed, _) = watch::channel(0);
        let io = Arc::new(Mutex::new(()));
        let exiting = Arc::new(AtomicBool::new(false));
        let worker_disk = disk.clone();
        let worker_io = io.clone();
        let worker_exiting = exiting.clone();
        let worker_completed = completed.clone();

        tauri::async_runtime::spawn(async move {
            while updates.changed().await.is_ok() {
                let Some(job) = updates.borrow_and_update().clone() else {
                    continue;
                };
                let revision = job.revision;
                let disk = worker_disk.clone();
                let io = worker_io.clone();
                let exiting = worker_exiting.clone();

                let _ = tauri::async_runtime::spawn_blocking(move || {
                    let _guard = io.lock().unwrap_or_else(|error| error.into_inner());
                    if exiting.load(Ordering::Acquire) {
                        return;
                    }

                    #[cfg(feature = "test-service")]
                    crate::test_service::buffer_persistence_control::before_buffer_write(&job.path);

                    if !exiting.load(Ordering::Acquire) {
                        disk.write_sync(&job.path, &job.key, job.value);
                    }
                })
                .await;

                worker_completed.send_modify(|done| *done = (*done).max(revision));
            }
        });

        Self {
            disk,
            pending,
            completed,
            io,
            exiting,
        }
    }

    pub fn persist(&self, path: &Path, key: &str, value: serde_json::Value) {
        // Each value is a complete snapshot; coalescing queued snapshots
        // keeps pending memory bounded without losing current observations.
        self.pending.send_modify(|pending| {
            let revision = pending
                .as_ref()
                .map_or(1, |job| job.revision.saturating_add(1));
            *pending = Some(PendingWrite {
                revision,
                path: path.to_path_buf(),
                key: key.to_string(),
                value,
            });
        });
    }

    #[cfg(any(test, feature = "test-service"))]
    pub async fn wait_for_pending_writes(&self) {
        let revision = self.pending.borrow().as_ref().map_or(0, |job| job.revision);
        let mut completed = self.completed.subscribe();
        let _ = completed.wait_for(|done| *done >= revision).await;
    }

    pub fn flush_sync(&self) {
        self.exiting.store(true, Ordering::Release);
        let _guard = self.io.lock().unwrap_or_else(|error| error.into_inner());
        let Some(job) = self.pending.borrow().clone() else {
            return;
        };

        self.disk.write_sync(&job.path, &job.key, job.value);
        self.completed
            .send_modify(|done| *done = (*done).max(job.revision));
    }
}
