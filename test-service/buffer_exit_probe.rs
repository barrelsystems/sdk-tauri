use serde::{Deserialize, Serialize};
use tauri_plugin_barrel_kit::{
    test_service::{self, buffer_commands::FactObservation},
    BarrelKitExt, InitConfig,
};

pub const ENV: &str = "BARREL_BUFFER_EXIT_PROBE";

#[derive(Serialize, Deserialize)]
struct Config {
    directory: std::path::PathBuf,
    facts: Vec<FactObservation>,
    mode: String,
    reload: bool,
}

pub async fn run_probe(mode: String, facts: Vec<FactObservation>) -> serde_json::Value {
    let directory =
        std::env::temp_dir().join(format!("barrel-buffer-exit-{}", uuid::Uuid::new_v4()));
    let config = serde_json::to_string(&Config {
        directory: directory.clone(),
        facts,
        mode,
        reload: false,
    })
    .expect("probe config");
    let executable = std::env::current_exe().expect("test service executable");
    let output = tokio::process::Command::new(&executable)
        .env(ENV, &config)
        .kill_on_drop(true)
        .output()
        .await
        .expect("probe process");
    assert!(
        output.status.success(),
        "buffer exit probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = std::fs::read(directory.join("barrel-buffered-facts.json"))
        .expect("exit restored the buffer");
    let mut result: serde_json::Value = serde_json::from_slice(&data).expect("buffer JSON");
    let mut reload: Config = serde_json::from_str(&config).expect("probe config");
    reload.reload = true;
    let loaded = tokio::process::Command::new(executable)
        .env(ENV, serde_json::to_string(&reload).unwrap())
        .kill_on_drop(true)
        .output()
        .await
        .expect("reload process");
    assert!(loaded.status.success(), "buffer reload failed");
    result["recoveredFacts"] = serde_json::from_slice(&loaded.stdout).expect("reloaded facts");
    result["pendingWriteObserved"] = serde_json::Value::Bool(output.stdout.starts_with(b"pending"));
    let _ = std::fs::remove_dir_all(directory);
    result
}

pub async fn run_child(config: String) {
    let config: Config = serde_json::from_str(&config).expect("probe config");
    test_service::runtime::configure(config.directory, "").expect("probe storage");
    let app = tauri::test::mock_builder()
        .barrel_kit(InitConfig {
            credential: "test-service".into(),
            base_url: Some("http://127.0.0.1:0".into()),
            auto_download: Some(false),
            ..Default::default()
        })
        .build(crate::test_context())
        .expect("probe app");
    test_service::runtime::wait_until_ready(app.handle()).await;
    if config.reload {
        println!(
            "{}",
            test_service::buffer_commands::buffered_facts(app.handle()).await
        );
        std::process::exit(0);
    }
    if config.mode == "pending" {
        test_service::buffer_persistence_control::set_buffer_persistence(
            app.handle(),
            test_service::buffer_persistence_control::BufferPersistenceMode::Blocked,
        )
        .await;
    }
    for fact in config.facts {
        test_service::buffer_commands::record_fact(app.handle(), fact).await;
        if config.mode == "pending" {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !test_service::buffer_persistence_control::buffer_write_blocked() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "writer must reach blocked boundary"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    }
    if config.mode == "pending" {
        use std::io::Write;
        println!("pending");
        std::io::stdout().flush().unwrap();
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(300));
            test_service::buffer_persistence_control::release_buffer_write();
        });
    } else {
        test_service::buffer_commands::wait_for_buffer_writes(app.handle()).await;
        test_service::buffer_commands::prepare_buffer_exit(app.handle());
    }
    let window = tauri::WebviewWindowBuilder::new(
        &app,
        "buffer-exit",
        tauri::WebviewUrl::App("index.html".into()),
    )
    .build()
    .expect("probe window");
    // Closing the final mock window dispatches the real plugin Exit event.
    let code = app.run_return(move |_, event| {
        if matches!(event, tauri::RunEvent::Ready) {
            window.destroy().expect("close probe window");
        }
    });
    std::process::exit(code);
}
