//! Controlled native disk conditions for isolated conformance workers.

use std::sync::Mutex;
use tauri::{AppHandle, Runtime};

use super::buffer_commands::wait_for_buffer_writes;
use super::runtime::directory;

/// Controllable persistence conditions at the test worker's I/O boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BufferPersistenceMode {
    /// Native disk operations proceed normally.
    Normal,
    /// The background writer waits before touching disk.
    Blocked,
    /// The buffer destination is temporarily unwritable.
    Failed,
}

struct WriteGate {
    mode: BufferPersistenceMode,
    waiting: bool,
}

static WRITE_GATE: (Mutex<WriteGate>, std::sync::Condvar) = (
    Mutex::new(WriteGate {
        mode: BufferPersistenceMode::Normal,
        waiting: false,
    }),
    std::sync::Condvar::new(),
);

pub(crate) fn before_buffer_write(path: &std::path::Path) {
    if directory().is_none_or(|directory| path != directory.join("barrel-buffered-facts.json")) {
        return;
    }

    let (lock, changed) = &WRITE_GATE;
    let mut gate = lock.lock().unwrap();
    while gate.mode == BufferPersistenceMode::Blocked {
        gate.waiting = true;
        gate = changed.wait(gate).unwrap();
    }
    gate.waiting = false;
}

/// Whether the real background writer is currently held at the disk boundary.
pub fn buffer_write_blocked() -> bool {
    WRITE_GATE.0.lock().unwrap().waiting
}

/// Releases the controlled writer without requiring the async runtime to progress.
pub fn release_buffer_write() {
    WRITE_GATE.0.lock().unwrap().mode = BufferPersistenceMode::Normal;
    WRITE_GATE.1.notify_all();
}

/// Changes only the isolated worker's disk conditions, not buffer behavior.
pub async fn set_buffer_persistence<R: Runtime>(app: &AppHandle<R>, mode: BufferPersistenceMode) {
    let previous = WRITE_GATE.0.lock().unwrap().mode;
    release_buffer_write();
    wait_for_buffer_writes(app).await;
    let path = directory()
        .expect("test storage")
        .join("barrel-buffered-facts.json");
    if previous == BufferPersistenceMode::Failed {
        let _ = std::fs::remove_dir(&path);
    }
    if mode == BufferPersistenceMode::Failed {
        let _ = std::fs::remove_file(&path);
        std::fs::create_dir(&path).expect("failed-write fixture");
    }
    WRITE_GATE.0.lock().unwrap().mode = mode;
}
