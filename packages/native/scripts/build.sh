#!/bin/sh
# Builds the addon for this host with the napi CLI, then stages it into its
# platform package and writes the TypeScript declarations of its binding.
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

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin-arm64 ;;
  Linux-x86_64) platform=linux-x64-gnu ;;
  *) echo "unsupported native build host: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

out=$(mktemp -d "${TMPDIR:-/tmp}/reactor-native.XXXXXX")
trap 'rm -rf "$out"' EXIT HUP INT TERM
# build.rs reads the locked graph offline, which needs every platform's crates.
cargo fetch --locked --manifest-path rust/Cargo.toml
node_modules/.bin/napi build --platform --release --no-const-enum \
  --manifest-path rust/Cargo.toml --package-json-path package.json \
  --output-dir "$out" --no-js --dts binding.d.ts
"${NODE_BINARY:-node}" scripts/stage.mjs "$out/reactor-effect-native.$platform.node" "$platform" "$out/binding.d.ts"
