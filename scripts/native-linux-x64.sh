#!/bin/sh
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
  --file "$root/native/Dockerfile" \
  --output "type=local,dest=$output" \
  "$root"

"$root/scripts/native-package.sh" "$output/libreactor_effect_native.so" linux-x64
