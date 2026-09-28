#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
workspace=$(CDPATH= cd -- "$root/.." && pwd)
bun_binary=${BUN_BINARY:-bun}
node_binary=${NODE_BINARY:-node}
mkdir -p "$workspace/.check"
output=$(mktemp -d "$workspace/.check/browser-native-XXXXXXXX")

# The public packages under test are the built workspace packages, never source.
(cd "$workspace" && "$bun_binary" run build)
"$bun_binary" build "$root/browser/native-connectivity.ts" --target=browser --outfile="$output/browser.js"

if grep -Eq 'reactor-effect-native|(^|[^[:alnum:]_])(takeVideo|buildIdentity)([^[:alnum:]_]|$)|node:buffer' "$output/browser.js"; then
  echo "browser bundle unexpectedly retained a native dependency" >&2
  exit 1
fi

"$node_binary" "$root/scripts/browser-native.ts" "$output/browser.js"
