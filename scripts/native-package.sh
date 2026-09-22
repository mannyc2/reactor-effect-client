#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_path=${1:-}
platform=${2:-}

if [ -z "$source_path" ] || [ -z "$platform" ]; then
  echo "usage: scripts/native-package.sh <shared-library> <platform-arch>" >&2
  echo "example: scripts/native-package.sh native/target/release/libreactor_effect_native.dylib darwin-arm64" >&2
  exit 2
fi

case "$platform" in
  darwin-*) library=libreactor_effect_native.dylib ;;
  linux-*) library=libreactor_effect_native.so ;;
  win32-*) library=reactor_effect_native.dll ;;
  *) echo "unsupported package platform: $platform" >&2; exit 2 ;;
esac

case "$source_path" in
  /*) source=$source_path ;;
  *) source=$root/$source_path ;;
esac

if [ ! -f "$source" ]; then
  echo "native library not found: $source" >&2
  exit 1
fi

exec "${NODE_BINARY:-node}" "$root/native/stage.mjs" "$source" "$platform"
