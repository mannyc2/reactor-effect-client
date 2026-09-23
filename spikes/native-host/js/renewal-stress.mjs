// The-show's renewal through the shipped artifact: open a replacement
// NativePeer (a new PeerConnectionFactory inside the bridge) while the current
// one streams, then shut the old one down while the replacement streams.
// Repeats --cycles times against one far peer, and exits nonzero on a stall.
//   node|bun js/renewal-stress.mjs [--cycles 30]
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { resolveNativeBridge } from "../../../packages/native/dist/_internal/bridge.js";
import { NativePeer } from "../../../packages/native/dist/_internal/peer.js";
import { arg, farPeer, runtime, sleep } from "./harness.mjs";

const cycles = arg("--cycles", 30);
const far = farPeer(["--width", "640", "--height", "360", "--fps", "24"]);
await far.ready;
const path = await resolveNativeBridge();
const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
];

let serial = 0;
const open = async () => {
  const id = `r${serial++}`;
  const peer = new NativePeer(path);
  const scope = Effect.runSync(Scope.make());
  let resolveOpen,
    channels = 0;
  const opened = new Promise((r) => (resolveOpen = r));
  const emit = (e) => {
    if (e.type === "ice" && e.candidate) far.candidate(id, e.candidate);
    else if (e.type === "channel" && e.open && ++channels === 2) resolveOpen();
  };
  const prepared = await Effect.runPromise(Scope.provide(scope)(peer.prepare([], tracks, emit)));
  await Effect.runPromise(peer.answer(await far.answer(id, prepared.sdp)));
  await opened;
  const session = { id, peer, scope, frames: 0 };
  session.reader = Effect.runFork(
    Stream.runForEach(peer.rawMedia.video("main_video"), () => Effect.sync(() => session.frames++)),
  );
  return session;
};
const close = async (s) => {
  const started = performance.now();
  await Effect.runPromise(Fiber.interrupt(s.reader));
  await Effect.runPromise(s.peer.shutdown());
  await Effect.runPromise(Scope.close(s.scope, Exit.void));
  await far.close(s.id);
  return performance.now() - started;
};

const shutdowns = [];
let current = await open();
while (current.frames < 3) await sleep(20);
for (let i = 0; i < cycles; i++) {
  const next = await open();
  const deadline = performance.now() + 10_000;
  while (next.frames < 3) {
    if (performance.now() > deadline) throw new Error(`cycle ${i}: replacement never streamed`);
    await sleep(20);
  }
  const before = next.frames;
  shutdowns.push(await close(current));
  await sleep(200);
  if (next.frames <= before) throw new Error(`cycle ${i}: replacement stalled during drain`);
  current = next;
}
shutdowns.push(await close(current));
await far.quit();
const sorted = [...shutdowns].sort((a, b) => a - b);
console.log(
  JSON.stringify({
    label: "renewal stress via shipped bridge",
    runtime,
    cycles,
    factoriesCreated: serial,
    shutdownMs: {
      p50: Math.round(sorted[Math.floor(sorted.length / 2)]),
      max: Math.round(sorted.at(-1)),
    },
    ok: true,
  }),
);
process.exit(0);
