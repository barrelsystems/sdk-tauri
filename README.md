# Barrel for Tauri

Release reporting and update eligibility for Tauri applications on macOS, Windows and Linux. Requires Tauri 2.

## Install

There is no tagged release yet. Install from source:

```sh
cargo add tauri-plugin-barrel-kit --git https://github.com/barrelsystems/sdk-tauri --branch master
```

## Quick start

```rust
use tauri_plugin_barrel_kit::{BarrelKitExt, InitConfig, updates};

fn main() -> tauri::Result<()> {
    tauri::Builder::default()
        .barrel_kit(InitConfig {
            credential: "your-project-sdk-credential".into(),
            updates: updates::Config { auto_download: Some(false), ..Default::default() },
            ..Default::default()
        })
        .run(tauri::generate_context!())
}
```

With an initialized `AppHandle` named `app`:

```rust
let mut subscription = updates::subscribe(&app).ok_or("plugin not registered")?;
loop {
    let status = subscription.borrow_and_update().clone();
    // Render the complete initial snapshot and subsequent changes.
    render_status(status);
    if subscription.changed().await.is_err() { break; }
}
```

From application action handlers:

```rust
updates::download(&app).await?;
updates::apply(&app).await?;
```

## Application UI

The SDK is a Rust utility. Menus, background tasks and other native code can use
`updates::subscribe`, `updates::download` and `updates::apply` directly. No webview
or frontend package is required.

The [example](examples/README.md) shows the framework transport explicitly:
application-owned Tauri commands call the Rust utility, a native `Channel` forwards
subscription snapshots, and the webview uses `invoke` to request installation.
The SDK itself exports no frontend bridge or commands.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `credential` | Required | Project SDK credential. |
| `base_url` | Barrel endpoint | Reporting endpoint override for development. |
| `debug` | `false` | Diagnostic logging. |
| `updates` | Defaults below | Update-specific configuration. |

Fields inside `updates`:

| Setting | Default | Purpose |
| --- | --- | --- |
| `check_in_interval_ms` | 30 minutes | Nominal cadence in milliseconds, clamped to 15 minutes–2 hours. |
| `channel` | `"stable"` | Observed release track. |
| `attributes` | Empty | String attributes included in reporting. |
| `auto_download` | `true` | Download available updates automatically. |
| `update_feed_url` | Updater configuration | Override the release feed. |
| `artifact_public_key` | Updater configuration | Verify release artifacts with this public key; not an eligibility key. |
| `hold_cache_max_age_ms` | 24 hours | Cached Hold lifetime in milliseconds; maximum seven days. |

## Update controls

Status contains `state`, `eligibility`, and `last_error`. Subscriptions receive a
complete initial snapshot and subsequent snapshots; intermediate transitions may
coalesce. Subscriptions are `tokio::sync::watch` receivers; drop a receiver to unsubscribe.
Subscriptions return `None` when the plugin is absent. Registered plugins
publish an initial idle snapshot before asynchronous setup completes. Read the
receiver’s current value before awaiting `changed()`; no separate getter is needed.
The example forwards this native stream to its webview using a Tauri channel.

Manual download completes after verified download. Manual apply completes at
installer handoff if the process survives long enough to return; it cannot confirm
a successful relaunch. Commands report classified errors for an unready SDK,
invalid state, a concurrent action, Hold, cancellation, or native failure.
Background reporting remains fail open.

Manual commands return `Result<(), updates::ActionError>`. Barrel registers the
updater and storage plugins; do not register them twice. Feed/signing overrides
fall back to your existing updater configuration when omitted.

## Behavior

- Reporting, verification failures, and unavailable storage fail open. A previously
  verified Hold remains effective within its configured cache lifetime.
- Downloads run automatically by default. A fresh verified Hold can defer a
  download or installation; it never prevents an ordinary application quit.
- Your application owns its release feed and artifacts. The native updater
  verifies artifacts and installs them; Barrel verifies eligibility decisions.
- Reports version, operating system, architecture, update state/errors, channel,
  configured attributes, and a locally generated installation identifier.
- `channel` is an observed, case-sensitive release track; it does not select a feed.
  Attributes are limited to 20 keys and 256 UTF-8 bytes per value.
- Debug logging is disabled by default. Reporting is best effort.

## Examples and support

- [Tauri example](examples/basic-tauri-app)
- [Report a problem](https://github.com/barrelsystems/sdk-tauri/issues)

See the [example data flow](examples/README.md) for native subscriptions, transport
and renderer button handling.
