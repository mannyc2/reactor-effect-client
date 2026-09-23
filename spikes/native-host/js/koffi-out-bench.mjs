// Today's bridge poll signature, called async with timeout 0 (never blocks):
// isolates Koffi's async path with an _Out_ size_t* JS-array argument.
import koffi from "koffi";
import { fileURLToPath } from "node:url";
const lib = koffi.load(
  fileURLToPath(
    new URL("../../../packages/native/lib/linux-x64/libreactor_effect_native.so", import.meta.url),
  ),
);
const create = lib.func("void *reactor_effect_peer_create(void)");
const poll = lib.func(
  "int reactor_effect_peer_poll_event(void *peer, uint32_t timeout_ms, _Out_ uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
);
const pollPlain = lib.func(
  "int reactor_effect_peer_poll_event(void *peer, uint32_t timeout_ms, uint8_t *out, size_t out_cap, size_t *out_len)",
);
const destroy = lib.func("void reactor_effect_peer_destroy(void *peer)");
const peer = create();
const call = (fn, ...a) =>
  new Promise((res, rej) => fn.async(...a, (e, r) => (e ? rej(e) : res(r))));
const time = async (label, f, reps = 2000) => {
  for (let i = 0; i < 50; i++) await f();
  const t = performance.now();
  for (let i = 0; i < reps; i++) await f();
  console.log(label, (((performance.now() - t) / reps) * 1000).toFixed(0), "us per call");
};
await time("async, _Out_ size_t* as JS array [0]", () => call(poll, peer, 0, null, 0, [0]));
const lenBuf = new BigUint64Array(1);
await time("async, size_t* as BigUint64Array", () => call(pollPlain, peer, 0, null, 0, lenBuf));
await time("sync,  _Out_ size_t* as JS array [0]", async () => poll(peer, 0, null, 0, [0]));
destroy(peer);
