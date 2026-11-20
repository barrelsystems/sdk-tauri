use std::time::Duration;

use crate::types::{CheckInResponse, PROTOCOL_VERSION};

pub(crate) struct PostProtocolRequestParams<'a> {
    pub base_url: &'a str,
    pub path: &'a str,
    pub credential: &'a str,
    pub timeout: Duration,
    pub operation: &'a str,
}

/// Posts a JSON body per `params`, then validates the response envelope: a
/// transport failure, a non-2xx status, a parse failure, or an unrecognized
/// `protocolVersion` all fail open to `None`, logged under `params.operation`.
///
/// Shared by `check_in` and `pre_action_check` -- both return the identical
/// `{protocolVersion, policy, signature}` envelope shape.
pub(crate) async fn post_protocol_request(
    client: &reqwest::Client,
    params: PostProtocolRequestParams<'_>,
    body: &impl serde::Serialize,
    log: &(impl Fn(&str) + ?Sized),
) -> Option<CheckInResponse> {
    let operation = params.operation;
    let result = client
        .post(format!("{}{}", params.base_url, params.path))
        .bearer_auth(params.credential)
        .json(body)
        .timeout(params.timeout)
        .send()
        .await;

    let response = match result {
        Ok(response) => response,
        Err(error) => {
            log(&format!("{operation}: failed ({error})"));
            return None;
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        log(&format!(
            "{operation}: received a non-2xx response ({status}): {text}"
        ));
        return None;
    }

    let parsed = match response.json::<CheckInResponse>().await {
        Ok(parsed) => parsed,
        Err(error) => {
            log(&format!(
                "{operation}: failed to parse the response ({error})"
            ));
            return None;
        }
    };

    if parsed.protocol_version != PROTOCOL_VERSION {
        log(&format!(
            "{operation}: rejected unrecognized protocolVersion {}",
            parsed.protocol_version
        ));
        return None;
    }

    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Starts a tiny raw-TCP server that accepts exactly one connection,
    /// drains the request, writes back `raw_response` verbatim, then
    /// exits -- enough to drive `post_protocol_request` through a
    /// real HTTP round trip without a mocking dependency.
    async fn serve_once(raw_response: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let _ = stream.write_all(raw_response.as_bytes()).await;
            }
        });
        format!("http://{addr}")
    }

    fn http_ok(json_body: &serde_json::Value) -> String {
        let body = json_body.to_string();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn noop_log(_: &str) {}

    #[tokio::test]
    async fn succeeds_on_a_valid_envelope() {
        let body = json!({ "protocolVersion": 1, "policy": "{}", "signature": "sig" });
        let raw = http_ok(&body);
        let base_url = serve_once(Box::leak(raw.into_boxed_str())).await;

        let result = post_protocol_request(
            &reqwest::Client::new(),
            PostProtocolRequestParams {
                base_url: &base_url,
                path: "/check-in",
                credential: "cred",
                timeout: Duration::from_secs(5),
                operation: "check-in",
            },
            &json!({}),
            &noop_log,
        )
        .await;

        assert!(result.is_some());
        assert_eq!(result.unwrap().protocol_version, 1);
    }

    #[tokio::test]
    async fn fails_open_on_a_non_2xx_response() {
        let raw =
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let base_url = serve_once(raw).await;

        let result = post_protocol_request(
            &reqwest::Client::new(),
            PostProtocolRequestParams {
                base_url: &base_url,
                path: "/check-in",
                credential: "cred",
                timeout: Duration::from_secs(5),
                operation: "check-in",
            },
            &json!({}),
            &noop_log,
        )
        .await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn fails_open_on_an_unrecognized_protocol_version() {
        let body = json!({ "protocolVersion": 99, "policy": "{}", "signature": "sig" });
        let raw = http_ok(&body);
        let base_url = serve_once(Box::leak(raw.into_boxed_str())).await;

        let result = post_protocol_request(
            &reqwest::Client::new(),
            PostProtocolRequestParams {
                base_url: &base_url,
                path: "/check-in",
                credential: "cred",
                timeout: Duration::from_secs(5),
                operation: "check-in",
            },
            &json!({}),
            &noop_log,
        )
        .await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn fails_open_on_a_transport_error() {
        // Bind then immediately drop -- nothing is listening at this
        // address, guaranteeing a connection-refused transport failure.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let result = post_protocol_request(
            &reqwest::Client::new(),
            PostProtocolRequestParams {
                base_url: &format!("http://{addr}"),
                path: "/check-in",
                credential: "cred",
                timeout: Duration::from_secs(5),
                operation: "check-in",
            },
            &json!({}),
            &noop_log,
        )
        .await;

        assert!(result.is_none());
    }
}
