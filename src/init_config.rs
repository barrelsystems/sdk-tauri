/// Shared SDK connection settings and update behavior.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    /// Project SDK credential.
    pub credential: String,
    /// Override the reporting endpoint.
    pub base_url: Option<String>,
    /// Enable diagnostic logging.
    pub debug: bool,
    /// Update-specific settings.
    pub updates: crate::updates::Config,
}
