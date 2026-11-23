use super::*;
use base64::Engine;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

struct FeedOptions {
    known_length: bool,
    valid_signature: bool,
}

struct UpdateFeed {
    url: String,
    public_key: String,
    stop: Arc<AtomicBool>,
    current: Arc<AtomicBool>,
    unreachable: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl UpdateFeed {
    fn start(options: FeedOptions) -> Self {
        let FeedOptions {
            known_length,
            valid_signature,
        } = options;
        let keypair = minisign::KeyPair::generate_unencrypted_keypair().unwrap();
        let public_key = base64::engine::general_purpose::STANDARD
            .encode(keypair.pk.to_box().unwrap().to_string());
        let signature = minisign::sign(
            None,
            &keypair.sk,
            b"test-update-bytes".as_slice(),
            Some("timestamp:0\tfile:update.bin\tversion:99.0.0"),
            None,
        )
        .unwrap();
        let signature = if valid_signature {
            base64::engine::general_purpose::STANDARD.encode(signature.into_string())
        } else {
            "!".to_string()
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/update", listener.local_addr().unwrap());
        let binary_url = url.replace("/update", "/binary");
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let current = Arc::new(AtomicBool::new(false));
        let worker_current = current.clone();
        let unreachable = Arc::new(AtomicBool::new(false));
        let worker_unreachable = unreachable.clone();
        let thread = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("test feed: {error}"),
                };
                // Accepted sockets can inherit the listener's nonblocking
                // mode; read the complete request before choosing a response.
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).unwrap_or(0) == 0 {
                        break;
                    }
                    request.push(byte[0]);
                }
                if worker_unreachable.load(Ordering::Acquire)
                    && request.starts_with(b"GET /update ")
                {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                } else if worker_current.load(Ordering::Acquire)
                    && request.starts_with(b"GET /update ")
                {
                    socket
                        .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                        .unwrap();
                } else if request.starts_with(b"GET /binary ") {
                    let headers = if known_length {
                        "Content-Length: 17"
                    } else {
                        "Transfer-Encoding: chunked"
                    };
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\n{headers}\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    for chunk in [
                        b"test-".as_slice(),
                        b"update-".as_slice(),
                        b"bytes".as_slice(),
                    ] {
                        if !known_length {
                            write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                        }
                        socket.write_all(chunk).unwrap();
                        if !known_length {
                            socket.write_all(b"\r\n").unwrap();
                        }
                        socket.flush().unwrap();
                        std::thread::sleep(Duration::from_millis(40));
                    }
                    if !known_length {
                        socket.write_all(b"0\r\n\r\n").unwrap();
                    }
                } else {
                    let body = serde_json::json!({ "version": "99.0.0", "notes": "Native notes", "url": binary_url, "signature": signature }).to_string();
                    write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            }
        });
        Self {
            url,
            public_key,
            stop,
            current,
            unreachable,
            thread: Some(thread),
        }
    }

    fn app(&self) -> tauri::App<tauri::test::MockRuntime> {
        let mut context = tauri::test::mock_context(tauri::test::noop_assets());
        context.config_mut().plugins.0.insert(
            "updater".into(),
            serde_json::json!({
                "pubkey": self.public_key, "dangerousInsecureTransportProtocol": true
            }),
        );
        tauri::test::mock_builder()
            .plugin(tauri_plugin_store::Builder::default().build())
            .plugin(tauri_plugin_updater::Builder::new().build())
            .build(context)
            .unwrap()
    }
}

impl Drop for UpdateFeed {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn native_download_callbacks_report_only_real_progress_and_verified_completion() {
    for known_length in [true, false] {
        let feed = UpdateFeed::start(FeedOptions {
            known_length,
            valid_signature: true,
        });
        let app = feed.app();
        let facts = BufferedFactsStore::new(app.handle().clone());
        let adapter = Adapter::new();
        adapter.set_feed_url(feed.url.clone());
        assert_eq!(adapter.state(), UpdateState::Idle);
        assert!(adapter.last_error().is_none());

        adapter.check_for_updates(app.handle(), &facts).await;
        assert_eq!(
            adapter.state(),
            UpdateState::Available {
                release_notes: Some("Native notes".into())
            }
        );
        assert_eq!(adapter.target_version().await.as_deref(), Some("99.0.0"));
        let mut states = adapter.subscribe_reportable_state();
        let download = adapter.download_update(&facts);
        let observe = async {
            let mut progress_values = Vec::new();
            loop {
                states.changed().await.unwrap();
                let state = states.borrow_and_update().clone();
                match state {
                    UpdateState::Downloading { progress } => progress_values.push(progress),
                    UpdateState::Downloaded => break,
                    UpdateState::Failed { .. } => {
                        panic!("download failed: {:?}", adapter.last_error())
                    }
                    _ => {}
                }
            }
            progress_values
        };
        let (_, progress) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(download, observe)
        })
        .await
        .unwrap();
        assert!(!progress.is_empty());
        if known_length {
            assert!(progress
                .iter()
                .flatten()
                .any(|value| *value > 0 && *value < 100));
            assert!(progress.iter().flatten().all(|value| *value <= 100));
        } else {
            assert!(progress.iter().all(Option::is_none));
        }
        let recorded = facts.snapshot().await;
        assert_eq!(
            recorded.iter().any(|fact| matches!(
                fact,
                crate::updates::persistence::buffered_facts::BufferedFact::LifecycleState {
                    state: UpdateState::Downloading { progress: Some(_) },
                    ..
                }
            )),
            known_length
        );
        assert_eq!(adapter.state(), UpdateState::Downloaded);
        assert!(adapter.last_error().is_none());
    }
}

#[tokio::test]
async fn receiving_all_bytes_does_not_mark_an_unverified_download_as_downloaded() {
    let feed = UpdateFeed::start(FeedOptions {
        known_length: true,
        valid_signature: false,
    });
    let app = feed.app();
    let facts = BufferedFactsStore::new(app.handle().clone());
    let adapter = Adapter::new();
    adapter.set_feed_url(feed.url.clone());
    adapter.check_for_updates(app.handle(), &facts).await;

    adapter.download_update(&facts).await;

    let failure = adapter.last_error().unwrap();
    assert_eq!(failure.failure_mode, UpdateFailureMode::SignatureInvalid);
    assert_eq!(failure.phase, Phase::Verify);
    assert_eq!(failure.platform, "tauri-updater");
    assert_eq!(failure.platform_code, "Base64");
    assert!(matches!(adapter.state(), UpdateState::Failed { .. }));
    let snapshot = facts.snapshot().await;
    assert!(
        !snapshot.iter().any(|fact| matches!(
            fact,
            crate::updates::persistence::buffered_facts::BufferedFact::LifecycleState {
                state: UpdateState::Downloaded,
                ..
            }
        )),
        "receiving bytes must not publish downloaded before verification"
    );
    let disk = serde_json::to_value(snapshot).unwrap();
    assert!(!disk.to_string().contains("\"title\""));
}

#[tokio::test]
async fn a_normal_no_update_cycle_is_idle_and_keeps_the_last_failure() {
    let feed = UpdateFeed::start(FeedOptions {
        known_length: true,
        valid_signature: false,
    });
    let app = feed.app();
    let facts = BufferedFactsStore::new(app.handle().clone());
    let adapter = Adapter::new();
    adapter.set_feed_url(feed.url.clone());
    adapter.check_for_updates(app.handle(), &facts).await;
    adapter.download_update(&facts).await;
    let failure = adapter.last_error().unwrap();
    feed.current.store(true, Ordering::Release);

    adapter.check_for_updates(app.handle(), &facts).await;

    assert_eq!(adapter.state(), UpdateState::Idle);
    assert_eq!(
        adapter.last_error().unwrap().platform_code,
        failure.platform_code
    );
    let states: Vec<_> = facts
        .snapshot()
        .await
        .into_iter()
        .filter_map(|fact| match fact {
            crate::updates::persistence::buffered_facts::BufferedFact::LifecycleState {
                state,
                ..
            } => Some(state),
            _ => None,
        })
        .collect();
    assert_eq!(states.last(), Some(&UpdateState::Idle));
    // `Checking` is local SDK activity, never a fact the backend learns
    // about -- not even via the buffered-facts history, despite two
    // `check_for_updates` calls each passing through it.
    assert_eq!(
        states
            .iter()
            .filter(|state| **state == UpdateState::Checking)
            .count(),
        0
    );
}

#[tokio::test]
async fn a_native_check_failure_reports_its_real_identifier_and_phase() {
    let feed = UpdateFeed::start(FeedOptions {
        known_length: true,
        valid_signature: true,
    });
    let app = feed.app();
    let facts = BufferedFactsStore::new(app.handle().clone());
    let adapter = Adapter::new();
    adapter.set_feed_url(feed.url.clone());
    feed.unreachable.store(true, Ordering::Release);

    adapter.check_for_updates(app.handle(), &facts).await;

    let failure = adapter.last_error().unwrap();
    assert_eq!(failure.failure_mode, UpdateFailureMode::ManifestUnreachable);
    assert_eq!(failure.phase, Phase::Check);
    assert_eq!(failure.platform_code, "ReleaseNotFound");
    assert_eq!(failure.platform, "tauri-updater");
    assert_eq!(
        adapter.state(),
        UpdateState::Failed {
            category: crate::updates::models::failure::FailureCategory::Check,
            code: UpdateFailureMode::ManifestUnreachable,
            platform_code: "ReleaseNotFound".into(),
        }
    );
}
