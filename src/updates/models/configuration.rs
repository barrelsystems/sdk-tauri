/// Update-specific configuration; all fields have platform defaults.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Nominal cadence in milliseconds, clamped to 15 minutes–2 hours.
    pub check_in_interval_ms: Option<u64>,
    /// Application-provided reporting attributes.
    pub attributes: std::collections::HashMap<String, String>,
    /// Observed release track, default stable; does not select a feed.
    pub channel: Option<String>,
    /// Enable automatic downloads, default true.
    pub auto_download: Option<bool>,
    /// Override the native release feed.
    pub update_feed_url: Option<String>,
    /// Public key used by the native updater to verify release artifacts.
    /// Defaults to the updater configuration; unrelated to eligibility verification.
    pub artifact_public_key: Option<String>,
    /// Cached Hold lifetime in milliseconds; default 24 hours, maximum seven days.
    pub hold_cache_max_age_ms: Option<u64>,
}
