use std::fmt;

/// Invalid initialization settings that the application can correct.
#[derive(Debug)]
pub struct BarrelConfigError(String);

impl BarrelConfigError {
    /// Builds a config error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for BarrelConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BarrelConfigError {}
