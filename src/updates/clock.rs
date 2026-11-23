use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// RFC 3339 timestamp at the current instant, in UTC, always to
/// millisecond precision -- the fixed shape every check-in/eligibility
/// decision uses.
pub fn now_iso8601() -> String {
    format_fixed(OffsetDateTime::now_utc())
}

fn format_fixed(dt: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.millisecond(),
    )
}

/// Milliseconds since the Unix epoch, for comparing against a parsed
/// `issuedAt`/`until` timestamp.
pub fn now_epoch_millis() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// Accept only the canonical UTC millisecond timestamp used on the wire, not every valid RFC
/// 3339 spelling.
pub fn parse_iso8601_to_epoch_millis(s: &str) -> Option<i64> {
    if s.len() != 24 || !s.ends_with('Z') || s.as_bytes().get(19) != Some(&b'.') {
        return None;
    }
    let parsed = OffsetDateTime::parse(s, &Rfc3339).ok()?;
    Some((parsed.unix_timestamp_nanos() / 1_000_000) as i64)
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
    fn formats_and_parses_agree_across_a_leap_day() {
        for fixed in [
            "2024-02-29T12:00:00.000Z",
            "2000-02-29T00:00:00.000Z",
            "1900-03-01T00:00:00.000Z",
            "2026-12-31T23:59:59.999Z",
        ] {
            let millis = parse_iso8601_to_epoch_millis(fixed).expect("fixed timestamp parses");
            let formatted = format_fixed(
                OffsetDateTime::from_unix_timestamp_nanos(millis as i128 * 1_000_000)
                    .expect("millis back into a valid instant"),
            );
            assert_eq!(formatted, fixed);
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
