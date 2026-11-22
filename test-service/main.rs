mod buffer_exit_probe;
mod dispatch;
mod mock_update_server;
mod routes;
mod runtime;
mod wire_types;

/// `tauri_plugin_barrel_kit`'s shared state is a process-wide `OnceLock`.
/// A second `barrel_kit()` call in one process is a silent no-op by
/// design, so one process can only ever host one adapter -- `routes::run`
/// spawns a fresh child per `createAdapter` instead.
pub(crate) const WORKER_ENV_VAR: &str = "BARREL_TEST_SERVICE_WORKER_CONFIG";

#[tokio::main]
async fn main() {
    if let Ok(config) = std::env::var(buffer_exit_probe::ENV) {
        buffer_exit_probe::run_child(config).await;
        return;
    }
    if let Ok(config_json) = std::env::var(WORKER_ENV_VAR) {
        runtime::run(config_json).await;
        return;
    }

    routes::run().await;
}

fn test_context() -> tauri::Context<tauri::test::MockRuntime> {
    tauri::generate_context!("test-service/tauri.conf.json")
}
