use crate::updates::clock::now_iso8601;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use super::protocol_request::{post_protocol_request, PostProtocolRequestParams};
use crate::updates::attributes::bound_attributes;
use crate::updates::models::state::UpdateState;
use crate::updates::persistence::buffered_facts::BufferedFact;
use crate::updates::protocol::wire_types::PROTOCOL_VERSION;
use crate::updates::protocol::wire_types::{
    BackendConnection, CheckInRequest, CheckInResponse, WireBufferedFact,
};

const CHECK_IN_TIMEOUT: Duration = Duration::from_secs(10);

/// Parameters for one `send_check_in` call.
#[derive(Debug)]
pub struct CheckInParams<'a> {
    /// Where this check-in is sent, and how it authenticates.
    pub connection: BackendConnection<'a>,
    /// This SDK's own version.
    pub sdk_version: &'a str,
    /// The release channel reported by the host app.
    pub channel: &'a str,
    /// The app version this installation is currently running.
    pub observed_version: &'a str,
    /// The host OS platform name.
    pub platform: &'a str,
    /// The host CPU architecture.
    pub arch: &'a str,
    /// The host OS version.
    pub os_version: &'a str,
    /// Arbitrary key-value pairs sent with this check-in.
    pub attributes: &'a HashMap<String, String>,
    /// The updater's current lifecycle state, as reported in this check-in.
    pub lifecycle_state: UpdateState,
    /// Facts buffered since the last successful check-in.
    pub buffered_facts: Vec<BufferedFact>,
    /// This SDK's own configured cadence, already floor/ceiling-clamped.
    pub check_in_interval_ms: u64,
}

/// Sends one check-in attempt with a bounded timeout. There is no retry
/// loop here by design -- any failure (network error, non-2xx, timeout)
/// resolves to `None`, and the caller's next scheduled tick is the retry.
pub async fn send_check_in(
    client: &reqwest::Client,
    params: CheckInParams<'_>,
    log: &impl Fn(&str),
) -> Option<CheckInResponse> {
    let body = CheckInRequest {
        protocol_version: PROTOCOL_VERSION,
        installation_id: params.connection.installation_id.to_string(),
        event_id: Uuid::new_v4().to_string(),
        checked_in_at: now_iso8601(),
        sdk_version: params.sdk_version.to_string(),
        channel: params.channel.to_string(),
        observed_version: params.observed_version.to_string(),
        platform: params.platform.to_string(),
        arch: params.arch.to_string(),
        os_version: params.os_version.to_string(),
        attributes: bound_attributes(params.attributes),
        lifecycle_state: params.lifecycle_state,
        buffered_facts: params
            .buffered_facts
            .iter()
            .map(WireBufferedFact::from)
            .collect(),
        check_in_interval_ms: params.check_in_interval_ms,
    };

    let parsed = post_protocol_request(
        client,
        PostProtocolRequestParams {
            base_url: params.connection.base_url,
            path: "/check-in",
            credential: params.connection.credential,
            timeout: CHECK_IN_TIMEOUT,
            operation: "check-in",
        },
        &body,
        log,
    )
    .await?;

    log("check-in: succeeded");
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sends_the_host_channel_on_check_in() {
        use crate::init_config::InitConfig;
        use std::io::{Read, Write};

        for configured in [None, Some("Beta / Custom".to_string())] {
            let config = InitConfig {
                updates: crate::updates::Config {
                    channel: configured.clone(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let expected = configured.as_deref().unwrap_or("stable").to_string();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();

                let mut request = Vec::new();
                let mut bytes = [0; 4096];

                loop {
                    let count = socket.read(&mut bytes).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&bytes[..count]);

                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        if let Ok(body) =
                            serde_json::from_slice::<serde_json::Value>(&request[end + 4..])
                        {
                            assert_eq!(body["channel"], expected);
                            break;
                        }
                    }
                }

                let body = r#"{"protocolVersion":1,"updateEligibility":"{}","signature":"sig"}"#;
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            });

            let response = send_check_in(
                &reqwest::Client::new(),
                CheckInParams {
                    connection: BackendConnection {
                        base_url: &base_url,
                        credential: "cred",
                        installation_id: "install",
                    },
                    sdk_version: "1.0.0",
                    channel: config.updates.channel.as_deref().unwrap_or("stable"),
                    observed_version: "1.0.0",
                    platform: "test",
                    arch: "test",
                    os_version: "test",
                    attributes: &HashMap::new(),
                    lifecycle_state: UpdateState::Idle,
                    buffered_facts: vec![],
                    check_in_interval_ms: 30 * 60 * 1000,
                },
                &|_| {},
            )
            .await;

            assert!(response.is_some());
            server.join().unwrap();
        }
    }

    #[tokio::test]
    async fn sends_the_configured_check_in_interval_on_check_in() {
        use std::io::{Read, Write};

        let expected = 45 * 60 * 1000_u64;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();

            let mut request = Vec::new();
            let mut bytes = [0; 4096];

            loop {
                let count = socket.read(&mut bytes).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&bytes[..count]);

                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    if let Ok(body) =
                        serde_json::from_slice::<serde_json::Value>(&request[end + 4..])
                    {
                        assert_eq!(body["checkInIntervalMs"], expected);
                        break;
                    }
                }
            }

            let body = r#"{"protocolVersion":1,"updateEligibility":"{}","signature":"sig"}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });

        let response = send_check_in(
            &reqwest::Client::new(),
            CheckInParams {
                connection: BackendConnection {
                    base_url: &base_url,
                    credential: "cred",
                    installation_id: "install",
                },
                sdk_version: "1.0.0",
                channel: "stable",
                observed_version: "1.0.0",
                platform: "test",
                arch: "test",
                os_version: "test",
                attributes: &HashMap::new(),
                lifecycle_state: UpdateState::Idle,
                buffered_facts: vec![],
                check_in_interval_ms: expected,
            },
            &|_| {},
        )
        .await;

        assert!(response.is_some());
        server.join().unwrap();
    }
}
