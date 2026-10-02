# Toolchain and performance changes

Development uses Rust 1.99.0, pinned in `rust-toolchain.toml`, with the Rust 2024
edition. Both packages now inherit the workspace minimum Rust version (1.98.1).

## Changes

- Engine.IO binary packets encode base64 into the final buffer, allocated to the
  encoded size. Polling transports reuse this encoder.
- Engine.IO polling payloads encode into one buffer without per-packet clones,
  intermediate buffers, or a trailing separator. Empty payload encoding is safe.
- Polling cache tokens use a shared atomic counter instead of formatting and
  hashing the system clock. Tokens are opaque and unique within a process.
- Async polling reads a complete response body, validates HTTP status, and uses
  the shared current URL on each request. HTTP chunks are not packet boundaries.
- Async packet processing iterates decoded payloads directly. Stream polling pins
  lock futures on the stack, and WebSocket text sends reuse validated `Bytes`.
- Socket.IO text events serialize arguments without an intermediate argument Vec
  or copied event name. Packet buffers and attachment lists reserve capacity.
- Ack removal uses `swap_remove`. Async callbacks run after releasing the ack
  lock, allowing them to register another acknowledgement without deadlocking.
- Unused callback lifetime bounds and outdated idioms were cleaned up. Clippy
  checks all targets with warnings denied, with and without default features.

## Local measurement

Criterion on Apple Silicon, optimized build, Rust 1.98.1, 20 samples with a 0.2 s
warmup and 0.5 s measurement per case. Before and after used the same compiler,
benchmark, machine, and options. These measure packet encoding, not network
throughput. Text encoding showed no statistically significant change.

| Binary input | Before | After | Approximate speedup |
| --- | ---: | ---: | ---: |
| 32 bytes | 218 ns | 39 ns | 5.6x |
| 1 KiB | 4.13 us | 0.326 us | 12.7x |
| 64 KiB | 259 us | 19.3 us | 13.4x |

The benchmark is independent of servers:

```sh
cargo bench -p rust_engineio --bench codec
```

For a controlled comparison, save a Criterion baseline before applying changes
and compare after, using the same toolchain for both runs:

```sh
cargo bench -p rust_engineio --bench codec -- --save-baseline before
cargo bench -p rust_engineio --bench codec -- --baseline before
```

## Verification

The full suite includes polling, WebSocket, TLS, auth, binary attachments, acks,
and reconnection against the Node servers in `ci/`. Regression tests additionally
cover HTTP chunk assembly, session URL updates, HTTP error responses, reentrant
ack callbacks, base64 padding, empty payloads, JSON escaping, and ack ID parsing.
Verification passed on Rust 1.99.0: 82 workspace tests and 35 doctests with all
features, plus 33 Engine.IO tests and one doctest without default features.
Clippy passed with warnings denied for all features, no default features, and
callbacks-only configurations of both crates. The all-feature suite also passed
on Rust 1.98.1 before the final stack-pinning and zero-copy WebSocket changes.

Use `make test-fast` for the codec tests; the full suite requires the test servers
as described in `ci/README.md`.
