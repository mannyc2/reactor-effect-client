#!/bin/sh
# Run the measured spike matrix sequentially on a quiet host. Each run writes
# <name>.json (the harness result) and <name>.err under $OUT.
set -u
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
OUT=${OUT:-$HERE/results}
mkdir -p "$OUT"
run() { # name runtime script args...
  name=$1; rt=$2; script=$3; shift 3
  echo "== $name ($(date -u +%H:%M:%S))"
  timeout 300 "$rt" "$HERE/js/$script" "$@" > "$OUT/$name.json" 2> "$OUT/$name.err"
  echo "   rc=$?"
  pkill -x far_peer 2>/dev/null; pkill -x ffmpeg 2>/dev/null
  sleep 3
}
run node-A-1        node spike-a-bridge.mjs
run node-B-1        node spike-b-bridge.mjs
run node-F-stock    node spike-f-werift.mjs
run node-F-twcc     node spike-f-werift.mjs --fix-twcc
run node-F-forced   node spike-f-werift.mjs --fix-twcc --min-kbps 6000
run bun-A-1         bun  spike-a-bridge.mjs
run bun-B-1         bun  spike-b-bridge.mjs
run bun-F-forced    bun  spike-f-werift.mjs --fix-twcc --min-kbps 6000
run node-A-2        node spike-a-bridge.mjs --sessions 2
run node-B-2        node spike-b-bridge.mjs --sessions 2
run bun-A-2         bun  spike-a-bridge.mjs --sessions 2
run bun-B-2         bun  spike-b-bridge.mjs --sessions 2
run node-A-2-stall2s node spike-a-bridge.mjs --sessions 2 --block-ms 2000
run node-B-2-stall2s node spike-b-bridge.mjs --sessions 2 --block-ms 2000
run node-A-lossy    node spike-a-bridge.mjs --loss 0.02 --delay-ms 40
run node-B-lossy    node spike-b-bridge.mjs --loss 0.02 --delay-ms 40
run node-F-lossy    node spike-f-werift.mjs --fix-twcc --min-kbps 6000 --loss 0.02 --delay-ms 40
