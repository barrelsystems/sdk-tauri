# Contributing

Thanks for your interest in improving this SDK.

## Reporting a bug or requesting a feature

Open a [GitHub issue](https://github.com/barrelsystems/sdk-tauri/issues).
For a bug, include your OS, Tauri version, the SDK version, and a minimal
reproduction. For a feature request, describe the use case, not just the
desired API.

## Local development setup

You'll need a stable Rust toolchain (`rustup` recommended) with the
`clippy` and `rustfmt` components, plus Tauri's own platform prerequisites
for the OS you're building on (see the
[Tauri prerequisites guide](https://v2.tauri.app/start/prerequisites/)).
On Linux, you'll also need:

```sh
sudo apt-get install -y libwebkit2gtk-4.1-dev libayatana-appindicator3-dev librsvg2-dev
```

Clone the repo and build:

```sh
cargo build
```

## Before opening a pull request

Run the same checks CI runs, from the repo root:

```sh
cargo fmt --check
cargo clippy --all-targets --features test-service -- -D warnings
cargo test
cargo build
```

The example app under `examples/basic-tauri-app/src-tauri` builds
separately and is also checked in CI:

```sh
cd examples/basic-tauri-app/src-tauri && cargo build
```

CI additionally runs the shared cross-SDK conformance suite against this
SDK's test service; you don't need to run that locally, but a protocol or
lifecycle change is likely to affect it.

## Pull request process

Open your PR against `master`, not `main`. Keep the change focused and
describe the behavior change, not just the implementation. Link the issue
it addresses, if any. A maintainer will review and may ask for changes
before merging.

## Code of Conduct

Participation in this project is governed by our
[Code of Conduct](CODE_OF_CONDUCT.md).
