#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bun_binary=${BUN_BINARY:-bun}
output=$root/.check/browser-native

rm -rf "$output"
mkdir -p "$output"

node "$root/scripts/build.mjs"
"$bun_binary" build "$root/integration/browser/native-connectivity.ts" --target=browser --outfile="$output/browser.js"

if grep -Eq '(^|[^[:alnum:]_])(koffi|reactor_effect_abi_version|native-bridge)([^[:alnum:]_]|$)|node:buffer' "$output/browser.js"; then
  echo "browser bundle unexpectedly retained a native/Koffi dependency" >&2
  exit 1
fi

node "$root/scripts/browser-native.mjs" "$output/browser.js"
