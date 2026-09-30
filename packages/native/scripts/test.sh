#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

case "${1:-}" in
  ""|rust|node) ;;
  *) echo "usage: test.sh [rust|node]" >&2; exit 2 ;;
esac
if [ "$#" -gt 1 ]; then
  echo "usage: test.sh [rust|node]" >&2
  exit 2
fi

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
# build.rs reads the locked graph offline, which needs every platform's crates.
cargo fetch --locked --manifest-path rust/Cargo.toml
cargo test --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --all-targets -- --nocapture
cargo clippy --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --no-deps --document-private-items
# The media load tests receive from a libwebrtc far peer on the same pinned
# reactor-webrtc. It is a Cargo example, so it never enters the staged library.
cargo build --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --release --example far_peer
# `test.sh rust` stops here, for CI, which runs the suite below on each runtime
# in a job of its own against the addon this build staged.
if [ "${1:-}" = rust ]; then
  exit 0
fi
REACTOR_NATIVE_FAR_PEER="${CARGO_TARGET_DIR:-$root/rust/target}/release/examples/far_peer"
export REACTOR_NATIVE_FAR_PEER
# The preceding native:build owns staging; the tests load the staged addon.
# The full gate also checks Bun's Node-API implementation; the local Node tier
# stops after the first runtime.
"${NODE_BINARY:-node}" node_modules/vitest/vitest.mjs run
if [ "${1:-}" = node ]; then
  exit 0
fi
"${BUN_BINARY:-bun}" --bun node_modules/vitest/vitest.mjs run
