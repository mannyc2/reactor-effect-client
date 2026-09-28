#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

if [ "$(uname -s)" = Linux ]; then
  cc=${CC:-clang-21}
  cxx=${CXX:-clang++-21}
  for compiler in "$cc" "$cxx"; do
    "$compiler" --version >/dev/null 2>&1 || {
      echo "native-linux-toolchain-missing: '$compiler' cannot run; set CC/CXX explicitly or opt in to scripts/install-linux-toolchain.sh in a supported Docker/CI environment" >&2
      exit 2
    }
  done
  export CC="$cc" CXX="$cxx"
fi

# Apply the native deployment target even when invoked from the SDK root.
# Lint levels live in rust/Cargo.toml's [lints] table; warnings fail here.
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --all-targets -- --nocapture
cargo clippy --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --no-deps --document-private-items
# The media load tests receive from a libwebrtc far peer on the same pinned
# reactor-webrtc. It is a Cargo example, so it never enters the staged library.
cargo build --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --release --example far_peer
REACTOR_NATIVE_FAR_PEER="${CARGO_TARGET_DIR:-$root/rust/target}/release/examples/far_peer"
export REACTOR_NATIVE_FAR_PEER
# The preceding native:build owns staging; the tests load the staged addon.
# Bun has its own Node-API implementation, so the suite runs on both runtimes.
"${NODE_BINARY:-node}" node_modules/vitest/vitest.mjs run
"${BUN_BINARY:-bun}" --bun node_modules/vitest/vitest.mjs run
