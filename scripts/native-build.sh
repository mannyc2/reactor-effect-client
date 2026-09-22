#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ "$(uname -s)" = Linux ]; then
  cc=${CC:-clang-21}
  cxx=${CXX:-clang++-21}
  command -v "$cc" >/dev/null 2>&1 || {
    echo "native-linux-toolchain-missing: C compiler '$cc' not found; set CC/CXX explicitly or opt in to native/install-linux-toolchain.sh in a supported Docker/CI environment" >&2
    exit 2
  }
  command -v "$cxx" >/dev/null 2>&1 || {
    echo "native-linux-toolchain-missing: C++ compiler '$cxx' not found; set CC/CXX explicitly or opt in to native/install-linux-toolchain.sh in a supported Docker/CI environment" >&2
    exit 2
  }
  "$cc" --version >/dev/null 2>&1 || { echo "native-linux-toolchain-invalid: CC '$cc' cannot execute" >&2; exit 2; }
  "$cxx" --version >/dev/null 2>&1 || { echo "native-linux-toolchain-invalid: CXX '$cxx' cannot execute" >&2; exit 2; }
  export CC="$cc" CXX="$cxx"
fi

# Cargo discovers configuration from its working directory, not --manifest-path.
cargo build --config "$root/native/.cargo/config.toml" --locked --manifest-path "$root/native/Cargo.toml" --release

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin-arm64; library=libreactor_effect_native.dylib ;;
  Darwin-x86_64) platform=darwin-x64; library=libreactor_effect_native.dylib ;;
  Linux-x86_64) platform=linux-x64; library=libreactor_effect_native.so ;;
  Linux-aarch64|Linux-arm64) platform=linux-arm64; library=libreactor_effect_native.so ;;
  *) echo "unsupported native build host: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

"$root/scripts/native-package.sh" "native/target/release/$library" "$platform"
