//! A mock Tauri app with the store plugin already registered, so
//! persistence tests can build a real `AppHandle` without a running
//! application. Shared by every test module that constructs these stores.

pub(crate) fn mock_app() -> tauri::App<tauri::test::MockRuntime> {
    tauri::test::mock_builder()
        .plugin(tauri_plugin_store::Builder::default().build())
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap()
}
