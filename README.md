# tauri-plugin-barrel-kit

Barrel's release-intelligence SDK for Tauri apps. It wraps
`tauri-plugin-updater` and reports what actually happened on a check,
download, or install. It doesn't replace your updater or touch your
binaries — your release feed stays on your own infrastructure.

## Install

```sh
cargo add tauri-plugin-barrel-kit
```

## Use it

```rust
use tauri_plugin_barrel_kit::BarrelKitExt;

tauri::Builder::default()
    .barrel_kit(tauri_plugin_barrel_kit::InitConfig {
        credential: std::env::var("BARREL_CREDENTIAL").unwrap(),
        update_feed_url: std::env::var("BARREL_UPDATE_FEED_URL").ok(), // omit to use tauri.conf.json's own static endpoints
        ..Default::default()
    })
    .run(tauri::generate_context!())
    .expect("error while running the app");

// later, anywhere you have an AppHandle:
let policy = tauri_plugin_barrel_kit::get_policy(); // Option<Policy>
let state = tauri_plugin_barrel_kit::get_adapter_state(&app); // Option<AdapterState>
let last_error = tauri_plugin_barrel_kit::get_last_update_error(&app); // Option<UpdateFailure>

// subscribe instead of polling:
if let Some(mut rx) = tauri_plugin_barrel_kit::subscribe_adapter_state(&app) {
    tauri::async_runtime::spawn(async move {
        while rx.changed().await.is_ok() {
            let state = rx.borrow_and_update().clone();
            // re-render on every transition
        }
    });
}

tauri_plugin_barrel_kit::download_update(&app); // manual trigger, needed only with auto_download: false
tauri_plugin_barrel_kit::apply_update(&app); // host-triggered install, ahead of the natural quit-time default
```

That's the whole surface. Setup runs inside the plugin's own setup hook and
fails open after that: a network blip or a bad signature from us never
blocks an update on your end.

## Behavior

Checks in on launch, then every 30 minutes (15-minute floor), jittered per
install, plus on window focus. Every Policy response is verified with
`ed25519-dalek`; a failed verification just keeps the last good Policy
around instead of erroring out.

Every check-in reports the channel key in `InitConfig.channel` verbatim
(default: `"stable"`), without changing the updater's feed. Keys are
case-sensitive; use the configured key rather than its display name.

Facts recorded while offline get buffered to disk and flushed on the next
successful check-in. Repeats get merged rather than duplicated, and if the
buffer fills up, the oldest non-terminal entry gets dropped first. Downloads
and installs both re-check Policy immediately beforehand, not just on the
30-minute cadence, so a Hold placed in between still has time to land.

Updates download automatically once found. Set `auto_download: false` in
`InitConfig` and call `download_update(&app)` yourself if you'd rather
control the timing. The plugin's feed URL gets fully wrapped too — pass
`update_feed_url` to override `tauri.conf.json`'s static endpoints at
runtime, or leave it unset to use whatever's already there.

`AdapterState` reports the real lifecycle
(`idle`/`checking`/`available`/`downloading`/`downloaded`/`failed`), and any
failure classifies into one `failureMode` shared across every Barrel SDK —
pull it with `get_last_update_error()` or subscribe live with
`subscribe_adapter_state()`. Release notes come through on
`AdapterState::Available.release_notes` when the feed has any, `None` when
it doesn't.

A few smaller things: the SDK supports server-directed rotation of the
trust key it ships with, keeps a random installation ID on disk, and bounds
whatever custom attributes you attach so a bug in your own code can't
balloon a check-in past what we'll accept. Debug logging is off unless you
set `debug: true`, in which case it goes to `eprintln!`.

## What stays yours

Targeting and rollout live on Barrel's dashboard. The feed itself and
whatever binaries it serves are yours; `update_feed_url` just points at them.

## Dependencies

`tauri` + `tauri-plugin-updater` (the thing being wrapped), `serde`/`serde_json`
for the wire protocol, `reqwest` with `rustls-tls`, `tokio` (already a
`tauri` dependency), `ed25519-dalek`/`pkcs8`/`base64` for signatures, `uuid`
for installation IDs. `url` comes in free via `tauri-plugin-updater`'s own
tree. Everything here ships inside your binary, so we keep this list short
on purpose.
