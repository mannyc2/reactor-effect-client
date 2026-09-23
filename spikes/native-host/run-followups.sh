#!/bin/sh
# Follow-up measurements after run-spikes.sh: FFI cost without Effect,
# renewal through the shipped artifact, and Koffi async call cost.
set -u
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
OUT=${OUT:-$HERE/results}
mkdir -p "$OUT"
for rt in node bun; do
  echo "== $rt-A-raw"; timeout 300 $rt "$HERE/js/spike-a-raw.mjs" > "$OUT/$rt-A-raw.json" 2> "$OUT/$rt-A-raw.err"; echo "   rc=$?"
  pkill -x far_peer 2>/dev/null; sleep 3
  echo "== $rt-renewal"; timeout 600 $rt "$HERE/js/renewal-stress.mjs" --cycles 30 > "$OUT/$rt-renewal.json" 2> "$OUT/$rt-renewal.err"; echo "   rc=$?"
  pkill -x far_peer 2>/dev/null; sleep 3
  echo "== $rt-koffi-async"; timeout 120 $rt "$HERE/js/koffi-async-bench.mjs" > "$OUT/$rt-koffi-async.txt" 2>&1; echo "   rc=$?"
done
