fn main() {
    #[cfg(not(feature = "test-service"))]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
        && std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc")
    {
        // Library tests bypass app resource generation, but still load
        // native controls that require the version-6 activation context.
        let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/windows-app-manifest.xml");
        println!("cargo:rerun-if-changed={}", manifest.display());
        println!("cargo:rustc-link-arg=/MANIFEST:EMBED");
        println!("cargo:rustc-link-arg=/MANIFESTINPUT:{}", manifest.display());
    }

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
