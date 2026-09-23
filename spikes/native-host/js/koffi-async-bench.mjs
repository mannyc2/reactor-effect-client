// Koffi .async round-trip throughput, alone and next to three calls that block
// 100 ms each (today's bridge keeps three such polls in flight per peer).
import koffi from "koffi";
const libc = koffi.load("libc.so.6");
const getpid = libc.func("int getpid(void)");
const usleep = libc.func("int usleep(uint32_t us)");
const call = (fn, ...a) =>
  new Promise((res, rej) => fn.async(...a, (e, r) => (e ? rej(e) : res(r))));
const burst = async (label, n) => {
  const t = performance.now();
  for (let i = 0; i < n; i++) await call(getpid);
  const ms = performance.now() - t;
  console.log(label, `${((ms / n) * 1000).toFixed(0)} us per sequential async call`);
};
await burst("idle pool:", 2000);
let stop = false;
const blockers = [0, 1, 2].map(async () => {
  while (!stop) await call(usleep, 100000);
});
await new Promise((r) => setTimeout(r, 50));
await burst("3 blocking 100 ms polls in flight:", 200);
const six = [0, 1, 2].map(async () => {
  while (!stop) await call(usleep, 100000);
});
await new Promise((r) => setTimeout(r, 50));
await burst("6 blocking 100 ms polls in flight (two peers):", 100);
stop = true;
await Promise.all([...blockers, ...six]);
