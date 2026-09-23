// Cost of passing a fresh 4.1 MB Buffer to a Koffi call, sync and async: the
// shape of today's bridge's copy poll (bridge.ts:508-511).
import koffi from "koffi";
const libc = koffi.load("libc.so.6");
const memset = libc.func("void *memset(uint8_t *s, int c, size_t n)");
const n = 1344 * 768 * 4 + 64;
const call = (fn, ...a) =>
  new Promise((res, rej) => fn.async(...a, (e, r) => (e ? rej(e) : res(r))));
const time = async (label, f, reps = 40) => {
  await f();
  const t = performance.now();
  for (let i = 0; i < reps; i++) await f();
  console.log(label, ((performance.now() - t) / reps).toFixed(2), "ms per call");
};
await time("sync  memset(fresh 4 MB Buffer)", () => memset(Buffer.allocUnsafe(n), 1, n));
await time("async memset(fresh 4 MB Buffer)", () => call(memset, Buffer.allocUnsafe(n), 1, n));
const reused = Buffer.allocUnsafe(n);
await time("async memset(reused 4 MB Buffer)", () => call(memset, reused, 1, n));
await time("async memset(fresh 4 KB Buffer)", () =>
  call(memset, Buffer.allocUnsafe(4096), 1, 4096),
);
