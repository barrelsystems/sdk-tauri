# Tauri example

Set `BARREL_CREDENTIAL` and, for local development, `BARREL_BASE_URL` and
`BARREL_UPDATE_FEED_URL`. Run your normal Tauri development command from
`basic-tauri-app/src-tauri`; no frontend build or package is needed.

```text
Rust: updates::subscribe → Tauri Channel → webview status display
Restart click → invoke("apply_update") → Rust command → updates::apply(&app)
```

The [Rust entry](basic-tauri-app/src-tauri/src/main.rs) demonstrates
`updates::subscribe` and `updates::apply`.
It owns the command handlers and the subscription task's lifetime. The [webview](basic-tauri-app/src/main.js)
uses framework `Channel`/`invoke` directly to display status and send a request.
The SDK owns no frontend state or transport abstraction.

Automatic download runs in the native utility. The view shows one status message,
download progress, and “Restart to update” when ready. Native menus can call
`updates::apply(&app).await` or `updates::download(&app).await` without a webview;
observe the same Rust subscription to drive native menu state.

The [capability](basic-tauri-app/src-tauri/capabilities/main.json) grants application
commands only to the local main window. The [build script](basic-tauri-app/src-tauri/build.rs)
registers those commands with Tauri's permission system.
