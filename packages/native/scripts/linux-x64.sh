#!/bin/sh
# Builds and checks the linux-x64 addon in Docker, then stages it here.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
context=${DOCKER_CONTEXT:-}

if [ -z "$context" ]; then
  echo "DOCKER_CONTEXT must name the explicit daemon/context used for the isolated Linux build" >&2
  exit 2
fi

output=$(mktemp -d "${TMPDIR:-/tmp}/reactor-native-linux.XXXXXX")
trap 'rm -rf "$output"' EXIT HUP INT TERM

docker --context "$context" build \
  --platform linux/amd64 \
  --file "$root/Dockerfile" \
  --output "type=local,dest=$output" \
  "$root"

"${NODE_BINARY:-node}" "$root/scripts/stage.mjs" \
  "$output/reactor-effect-native.linux-x64-gnu.node" linux-x64-gnu "$output/binding.d.ts"
