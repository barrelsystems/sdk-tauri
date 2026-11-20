use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use super::protocol_request::{post_protocol_request, PostProtocolRequestParams};
use crate::attributes::bound_attributes;
use crate::persistence::buffered_facts::BufferedFact;
use crate::types::{
    AdapterState, CheckInRequest, CheckInResponse, WireBufferedFact, PROTOCOL_VERSION,
};

const CHECK_IN_TIMEOUT: Duration = Duration::from_secs(10);

/// Parameters for one `send_check_in` call.
#[derive(Debug)]
pub struct CheckInParams<'a> {
    /// The check-in endpoint's base URL.
    pub base_url: &'a str,
    /// Sent as a bearer token.
    pub credential: &'a str,
    /// Stable per-install identifier.
    pub installation_id: &'a str,
    /// This SDK's own version.
    pub sdk_version: &'a str,
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
    /// The adapter's current lifecycle state, as reported in this check-in.
    pub adapter_state: AdapterState,
    /// Facts buffered since the last successful check-in.
    pub buffered_facts: Vec<BufferedFact>,
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
        installation_id: params.installation_id.to_string(),
        event_id: Uuid::new_v4().to_string(),
        checked_in_at: now_iso8601(),
        sdk_version: params.sdk_version.to_string(),
        observed_version: params.observed_version.to_string(),
        platform: params.platform.to_string(),
        arch: params.arch.to_string(),
        os_version: params.os_version.to_string(),
        attributes: bound_attributes(params.attributes),
        adapter_state: params.adapter_state,
        buffered_facts: params
            .buffered_facts
            .iter()
            .map(WireBufferedFact::from)
            .collect(),
    };

    let parsed = post_protocol_request(
        client,
        PostProtocolRequestParams {
            base_url: params.base_url,
            path: "/check-in",
            credential: params.credential,
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

/// No `chrono` dependency for one timestamp -- `SystemTime` plus a minimal
/// RFC 3339 formatter covers it.
pub fn now_iso8601() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();

    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let (hours, minutes, seconds) = (
        time_of_day / 3600,
        (time_of_day % 3600) / 60,
        time_of_day % 60,
    );
    let (year, month, day) = civil_from_days(days as i64);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Milliseconds since the Unix epoch, for comparing against a parsed
/// `issuedAt`/`until` timestamp without pulling in `chrono`.
pub fn now_epoch_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Inverse of `now_iso8601`'s formatter -- parses the same fixed
/// "YYYY-MM-DDTHH:MM:SS.mmmZ" shape this SDK always produces back into
/// milliseconds since the Unix epoch.
///
/// That fixed shape is also the only one a trusted Policy response's
/// `issuedAt`/`until` ever takes.
pub fn parse_iso8601_to_epoch_millis(s: &str) -> Option<i64> {
    if s.len() != 24 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let minute: i64 = s.get(14..16)?.parse().ok()?;
    let second: i64 = s.get(17..19)?.parse().ok()?;
    let millis: i64 = s.get(20..23)?.parse().ok()?;
    let days = days_from_civil(year, month, day);
    Some(days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1000 + millis)
}

/// Howard Hinnant's days-from-civil algorithm -- the forward counterpart
/// to `civil_from_days` below, dependency-free for the same reason.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// Howard Hinnant's days-from-civil algorithm, inverted -- a small, exact,
/// dependency-free way to turn a Unix day count into a proleptic Gregorian
/// (year, month, day), valid for any date this SDK will ever observe.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_timestamp_into_the_right_epoch_millis() {
        // 2026-01-01T00:00:00.000Z is 1767225600000ms since the epoch --
        // a value independently computable, not just a round trip.
        assert_eq!(
            parse_iso8601_to_epoch_millis("2026-01-01T00:00:00.000Z"),
            Some(1_767_225_600_000)
        );
    }

    #[test]
    fn rejects_a_string_of_the_wrong_length() {
        assert_eq!(parse_iso8601_to_epoch_millis("2026-01-01T00:00:00Z"), None);
    }

    #[test]
    fn rejects_non_numeric_components() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("20XX-01-01T00:00:00.000Z"),
            None
        );
    }

    #[test]
    fn civil_from_days_and_days_from_civil_round_trip_across_a_leap_day() {
        for (year, month, day) in [(2024, 2, 29), (2000, 2, 29), (1900, 3, 1), (2026, 12, 31)] {
            let days = days_from_civil(year, month, day);
            assert_eq!(civil_from_days(days), (year, month, day));
        }
    }

    #[test]
    fn now_iso8601_produces_a_string_now_epoch_millis_and_the_parser_agree_on() {
        let formatted = now_iso8601();
        let via_formatter_and_parser = parse_iso8601_to_epoch_millis(&formatted).unwrap();
        let via_direct_clock_read = now_epoch_millis();

        // Both reads happen within the same test; a correct formatter/parser
        // pair can never disagree by more than a second of real elapsed time.
        assert!((via_formatter_and_parser - via_direct_clock_read).abs() < 1000);
    }
}
