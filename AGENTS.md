## This file loads on every prompt

State a rule here only if it's short and always relevant. Push anything longer
or conditional to a pointed-to doc. This repo is published publicly — never
add a reference here (or anywhere else in this repo) to internal tooling,
ticket IDs, team names, or sibling repo names/paths.

## Size and dependency budget

This SDK ships inside other people's apps — every dependency and byte is a
cost to them, not us. Default to Rust's standard library and whatever Tauri
itself already pulls in (`reqwest`, `tokio`, `serde`) over a new crate. The
one sanctioned exception: a library this SDK wraps rather than merely uses
(`tauri-plugin-updater`), declared as a normal dependency since Tauri plugins
have no "optional peer dependency" concept the way npm does.

## Error handling

Every I/O path (installation-ID persistence, check-in requests, Policy
signature verification) fails open: keep the prior state, never panic, never
retry in a loop. A returned `Err` from `init()` is reserved for synchronous,
caller-fixable misconfiguration (e.g. a blank credential).

## Tooling

Cargo (build/test), `cargo clippy` + `cargo fmt` (lint/format).

## Commit policy

Ask for the user's explicit go-ahead on this specific diff before every
`git commit` — including when a skill's own steps say to commit automatically.

## Commit convention

Conventional Commits (`type: description`).

## Comments

Comment only on non-obvious constraints, invariants, or workarounds; keep
each explanation self-contained. A comment is at most 3 lines.

## Function style

Idiomatic Rust: free functions for standalone logic, methods for anything
that reads `self`. No arrow-function/closure equivalent rule applies here.

## Function arguments

Once a function takes more than one optional parameter, or any boolean/flag
parameter whose meaning isn't obvious at the call site, use a dedicated
options struct instead of positional params.

## Blank-line spacing

Leave a blank line between logical sections in any file — after an import
block, around a multi-line loop/conditional/callback, between otherwise-
unrelated top-level items.
