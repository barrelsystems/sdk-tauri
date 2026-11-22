# Source organization

Modules are grouped by responsibility; native language naming and package layouts apply.

| Module | Responsibility |
| --- | --- |
| Buffered facts | Observation identity, adjacent merging, bounded eviction, snapshots, and acknowledged clearing. |
| Persistence writer | Background write ordering, completion waits, failure handling, and synchronous exit coordination. |
| Protocol | Request encoding, transport, signature verification, and Policy decoding. |
| Runtime | SDK initialization, cadence, and updater action orchestration. |
| Adapters | Native updater integration and platform error mapping. |

Buffered facts publishes complete snapshots to the persistence writer. Ordinary
observations never wait for disk; callers wait explicitly where durability is
required. Snapshot identities are process-local and excluded from disk and wire
encoding. Native queues and locks may differ while preserving these guarantees.

The conformance service separates runtime setup, buffer commands, controlled disk
conditions, and process-exit probes. HTTP/IPC entry points route requests; buffer
commands use the production store rather than a parallel implementation.

`src/persistence/disk_store.rs` is the native plugin-store wrapper used by the
writer and by startup/durable stores. Feature-gated support in `src/test_service/`
provides access to private runtime state; `test-service/` owns HTTP routing,
worker lifetime, vendor fixtures, and exit-process orchestration.
