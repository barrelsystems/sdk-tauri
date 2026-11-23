use std::process::Command;

pub struct Environment {
    pub observed_version: String,
    pub platform: String,
    pub arch: String,
    pub os_version: String,
}

pub fn collect_environment(app_version: &str) -> Environment {
    Environment {
        observed_version: app_version.to_string(),
        platform: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        os_version: os_version(),
    }
}

#[cfg(unix)]
fn os_version() -> String {
    Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_string())
        .unwrap_or_default()
}

#[cfg(windows)]
fn os_version() -> String {
    Command::new("cmd")
        .args(["/C", "ver"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_string())
        .unwrap_or_default()
}
