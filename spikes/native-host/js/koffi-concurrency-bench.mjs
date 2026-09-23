// Completions per second when N loops each keep one Koffi async call in flight
// (today's bridge keeps three per peer). Each call sleeps 1 ms natively.
import koffi from "koffi";
const libc = koffi.load("libc.so.6");
const usleep = libc.func("int usleep(uint32_t us)");
const call = (fn, ...a) =>
  new Promise((res, rej) => fn.async(...a, (e, r) => (e ? rej(e) : res(r))));
for (const loops of [1, 3, 6]) {
  let done = 0,
    stop = false;
  const workers = Array.from({ length: loops }, async () => {
    while (!stop) {
      await call(usleep, 1000);
      done++;
    }
  });
  const t = performance.now();
  await new Promise((r) => setTimeout(r, 3000));
  stop = true;
  await Promise.all(workers);
  const s = (performance.now() - t) / 1000;
  console.log(
    `${loops} concurrent loops: ${Math.round(done / s)} completions/s (ideal ${(loops * 1000 * 0.9) | 0}+)`,
  );
}
