use std::fmt;

/// Returned synchronously by `init()`, and only by `init()` -- every later
/// failure fails open instead.
#[derive(Debug)]
pub struct BarrelConfigError(pub String);

impl fmt::Display for BarrelConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BarrelConfigError {}
