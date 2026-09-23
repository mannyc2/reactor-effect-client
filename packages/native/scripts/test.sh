#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

if [ "$(uname -s)" = Linux ]; then
  cc=${CC:-clang-21}
  cxx=${CXX:-clang++-21}
  command -v "$cc" >/dev/null 2>&1 || {
    echo "native-linux-toolchain-missing: C compiler '$cc' not found; set CC/CXX explicitly or opt in to scripts/install-linux-toolchain.sh in a supported Docker/CI environment" >&2
    exit 2
  }
  command -v "$cxx" >/dev/null 2>&1 || {
    echo "native-linux-toolchain-missing: C++ compiler '$cxx' not found; set CC/CXX explicitly or opt in to scripts/install-linux-toolchain.sh in a supported Docker/CI environment" >&2
    exit 2
  }
  "$cc" --version >/dev/null 2>&1 || { echo "native-linux-toolchain-invalid: CC '$cc' cannot execute" >&2; exit 2; }
  "$cxx" --version >/dev/null 2>&1 || { echo "native-linux-toolchain-invalid: CXX '$cxx' cannot execute" >&2; exit 2; }
  export CC="$cc" CXX="$cxx"
fi

# Apply the native deployment target even when invoked from the SDK root.
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml -- --nocapture
cargo clippy --config rust/.cargo/config.toml --locked --manifest-path rust/Cargo.toml --all-targets -- -D warnings
# The preceding native:build owns staging. Tests load only the staged artifact
# and reject a missing or mismatched identity instead of rebuilding another copy.
"${NODE_BINARY:-node}" node_modules/vitest/vitest.mjs run
