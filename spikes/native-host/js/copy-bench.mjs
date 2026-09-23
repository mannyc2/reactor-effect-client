const n = 1344 * 768 * 4 + 64;
const src = new Uint8Array(n).fill(7);
const time = (label, f) => {
  f();
  const t = performance.now();
  for (let i = 0; i < 20; i++) f();
  console.log(label, ((performance.now() - t) / 20).toFixed(2), "ms per 4.1 MB copy");
};
time("Uint8Array.from(typed)", () => Uint8Array.from(src));
time("Uint8Array.from(subarray)", () => Uint8Array.from(src.subarray(4)));
time("slice()", () => src.slice());
time("new Uint8Array + set", () => {
  const d = new Uint8Array(n);
  d.set(src);
});
time("Buffer.allocUnsafe", () => Buffer.allocUnsafe(n));
