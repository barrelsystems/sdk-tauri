mod buffer_exit_probe;
mod dispatch;
mod mock_update_server;
mod routes;
mod runtime;
mod wire_types;

/// The runtime is process-scoped; each adapter needs an isolated child process.
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
