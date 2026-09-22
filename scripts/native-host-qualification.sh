#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
node_binary=${NODE_BINARY:-node}
bun_binary=${BUN_BINARY:-bun}

# Focused production adapter closure. The release owner separately runs the
# complete SDK build/package gate after every public API migration is finished.
"$node_binary" node_modules/typescript/bin/tsc --ignoreConfig --outDir dist --rootDir src \
  --declaration --sourceMap --target ES2022 --module NodeNext --strict --skipLibCheck \
  --types node --exactOptionalPropertyTypes --noUncheckedIndexedAccess \
  src/index.ts src/native/index.ts src/browser/index.ts
"$node_binary" node_modules/typescript/bin/tsc --ignoreConfig --noEmit --target ES2022 \
  --module NodeNext --strict --skipLibCheck --types node --exactOptionalPropertyTypes \
  --noUncheckedIndexedAccess integration/browser/native-connectivity.ts
"$node_binary" node_modules/vitest/vitest.mjs run \
  --project native

mkdir -p "$root/.check"
output=$(mktemp -d "$root/.check/native-public-host-XXXXXXXX")
"$bun_binary" --no-env-file build integration/browser/native-connectivity.ts --target=browser --outfile="$output/browser.js"
if grep -E 'koffi|reactor_effect_peer_|native-bridge|native-peer' "$output/browser.js" >/dev/null; then
  echo 'browser public entry unexpectedly includes a native dependency' >&2
  exit 1
fi
"$node_binary" "$root/scripts/browser-native.mjs" "$output/browser.js"
