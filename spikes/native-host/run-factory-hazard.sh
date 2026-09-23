#!/bin/sh
# Repeat each factory_hazard mode in fresh processes and record exit status.
# A signal exit (rc >= 128) is a crash; 101 is a Rust panic (e.g. a failed
# connect assertion). Output: TSV on stdout, per-run logs under $OUT.
set -u
BIN=${BIN:-$(dirname "$0")/rust/target/release/factory_hazard}
OUT=${OUT:-$(mktemp -d)}
RUNS=${RUNS:-20}
echo "mode\targs\trun\trc\tseconds" 
for spec in "shared 50" "per-peer 50" "renewal 30" "drop-live 30" "seq 2000" "churn 500 4"; do
  i=0
  while [ "$i" -lt "$RUNS" ]; do
    start=$(date +%s.%N)
    timeout 600 "$BIN" $spec > "$OUT/$(echo "$spec" | tr ' ' _)-$i.out" 2> "$OUT/$(echo "$spec" | tr ' ' _)-$i.err"
    rc=$?
    end=$(date +%s.%N)
    printf '%s\t%s\t%s\t%s\t%.1f\n' "${spec%% *}" "${spec#* }" "$i" "$rc" "$(echo "$end - $start" | bc)"
    i=$((i + 1))
  done
done
