#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

# Apply the native deployment target even when invoked from the SDK root.
cargo test --config native/.cargo/config.toml --locked --manifest-path native/Cargo.toml -- --nocapture
cargo clippy --config native/.cargo/config.toml --locked --manifest-path native/Cargo.toml --all-targets -- -D warnings
cargo build --config native/.cargo/config.toml --locked --manifest-path native/Cargo.toml --release
node node_modules/vitest/vitest.mjs run test/native-abi.test.ts test/native-parser.test.ts test/native-session.test.ts
