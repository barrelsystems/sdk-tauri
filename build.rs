fn main() {
    // Only the test-service binary is a real Tauri app needing codegen
    // (`generate_context!`) -- the shipped plugin never calls it.

    // A runtime check isn't enough: `tauri_build` isn't even linked into
    // this build script's own crate without the feature, so the call
    // itself must be compiled out, not just skipped at runtime.
    #[cfg(feature = "test-service")]
    tauri_build::try_build(
        tauri_build::Attributes::new().config_path("test-service/tauri.conf.json"),
    )
    .expect("tauri_build succeeds");
}
