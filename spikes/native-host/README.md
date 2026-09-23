# Native-host spikes

Throwaway evidence for [`native-host-decision.md`](../../native-host-decision.md). Nothing here is a workspace member, a package input or a CI job. Do not merge it.

## Layout

| Path                                                         | What it is                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rust/`                                                      | Crate over reactor-webrtc `bebf63e` (the bridge's pin). `far_peer` is a libwebrtc answerer that plays Reactor's server. `factory_hazard` reproduces the one-factory-per-process claim.                                                                                                             |
| `bridge-b/`                                                  | Spike B, the shrunk bridge: `packages/native/rust` with typed media queues, a readiness notifier, synchronous single-copy takes and one factory per process (ABI 3)                                                                                                                                |
| `js/harness.mjs`                                             | Shared schedule and measurements: 60 s of 1344×768 at 24 fps plus 48 kHz PCM, a data-channel round trip every second, a 250 ms JS stall at 20 s, an interruption at 40 s, then shutdown. It samples CPU, RSS, event-loop lag, the latency of a libuv-threadpool `fs.stat`, and end-to-end latency. |
| `js/spike-a-bridge.mjs`                                      | Spike A: today's bridge exactly as shipped (`NativePeer` → `NativeBridge` → Koffi async polls)                                                                                                                                                                                                     |
| `js/spike-a-raw.mjs`                                         | Today's `NativeBridge` and parsers without Effect delivery, which isolates the FFI and copy cost                                                                                                                                                                                                   |
| `js/spike-b-bridge.mjs`                                      | Spike B host: a Koffi readiness callback plus synchronous takes                                                                                                                                                                                                                                    |
| `js/spike-f-werift.mjs`                                      | Spike F: werift 0.24.4, the metadata trailer parsed in TypeScript, and ffmpeg decoding (VP8 via IVF, Opus via Ogg). `--fix-twcc` swaps in a conforming TWCC feedback encoder; `--min-kbps` forces the far peer's minimum bitrate.                                                                  |
| `js/smoke-g-node-datachannel.mjs`, `js/smoke-h-wrtc.mjs`     | Short checks for G and H                                                                                                                                                                                                                                                                           |
| `js/renewal-stress.mjs`                                      | The-show's renewal through the shipped artifact, repeated                                                                                                                                                                                                                                          |
| `js/koffi-*-bench.mjs`, `js/copy-bench.mjs`                  | Micro-benchmarks behind the pool-saturation and Bun findings                                                                                                                                                                                                                                       |
| `run-factory-hazard.sh`, `run-spikes.sh`, `run-followups.sh` | The measured matrices                                                                                                                                                                                                                                                                              |
| `tables.py`                                                  | Renders the result rows from `results/*.json`                                                                                                                                                                                                                                                      |
| `prstats.py`                                                 | Per-author PR merge latency from `refs/pull/*` (the upstream-likelihood estimate)                                                                                                                                                                                                                  |
| `results/`                                                   | The JSON each run printed, `factory-summary.tsv`, and the benchmark output                                                                                                                                                                                                                         |
| `upstream/`                                                  | Draft reactor-webrtc patches, as `git format-patch` output against `bebf63e`, plus `check/`, a crate that exercises them against libwebrtc                                                                                                                                                         |

## Reproduce

Install Node 24.15.0, Bun 1.4.2, Rust 1.90.0, Clang 21, `zstd` and ffmpeg. On Linux x64 the digest-checked Node and Bun installer in `effect-agent-browserbase/tools/pinned-toolchain.sh` works. apt.llvm.org was unreachable here, so LLVM 21.1.8 came from its GitHub release. Then:

```sh
bun install --frozen-lockfile && bun run build && bun run native:build && bun run native:test
cd spikes/native-host
npm install                                   # werift 0.24.4, node-datachannel 0.33.4, @roamhq/wrtc 0.10.0
for p in effect koffi reactor-effect-client; do ln -sfn "$(readlink -f ../../packages/native/node_modules/$p)" node_modules/$p; done
(cd rust && CC=clang-21 CXX=clang++-21 cargo build --release)
(cd bridge-b && CC=clang-21 CXX=clang++-21 cargo build --release)

OUT=/tmp/factory RUNS=20 ./run-factory-hazard.sh   # 120 processes; prints TSV, exit status per run
OUT=results ./run-spikes.sh                        # 17 runs of about 65 s each
OUT=results ./run-followups.sh                     # A-raw, renewal stress, Koffi async benchmark on both runtimes
python3 tables.py results                          # the rows used in the decision document
node js/spike-f-werift.mjs --fix-twcc --recover --jitter-ms 300 --min-kbps 6000 --loss 0.02 --delay-ms 40
node js/smoke-g-node-datachannel.mjs; bun js/smoke-g-node-datachannel.mjs
node js/smoke-h-wrtc.mjs; bun js/smoke-h-wrtc.mjs
```

`SPIKE_DEBUG=/tmp/trace.log` makes the harness and spikes B and F append phase markers and counters to that file.

Checking the upstream drafts: clone reactor-webrtc into `upstream/reactor-webrtc` beside this repository (the `path` in `upstream/check/Cargo.toml`), run `git checkout bebf63e && git am …/upstream/*.patch`, then `(cd upstream/check && CC=clang-21 CXX=clang++-21 cargo run --release)`.

The lossy runs use the far peer's own UDP relay (`--loss 0.02 --delay-ms 40`), because the host kernel had no netem. The relay drops each packet with probability 0.02 and delays it by 40 ms in each direction. It sits in front of every far-peer host candidate, and the offer's own candidates are withheld so that it is the only path.
