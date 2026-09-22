function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
export function object(value: unknown): Record<string, unknown> {
  if (!isObject(value)) throw new Error("expected object");
  return value;
}
export function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
export function text(value: unknown): string { assert(typeof value === "string", "expected string"); return value; }
export function number(value: unknown): number { assert(typeof value === "number" && Number.isFinite(value), "expected finite number"); return value; }
export function list(value: unknown): unknown[] { assert(Array.isArray(value), "expected array"); return value; }
export const stringify = (value: unknown): string => JSON.stringify(value, (_key, item: unknown) => typeof item === "bigint" ? item.toString() : item, 2);
export async function request(path: string, body?: unknown, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, { method: body === undefined ? "GET" : "POST", credentials: "omit", redirect: "error",
    signal: signal === undefined ? AbortSignal.timeout(6000) : AbortSignal.any([signal, AbortSignal.timeout(6000)]),
    ...(body === undefined ? {} : { body: JSON.stringify(body), headers: { "Content-Type": "application/json" } }) });
  if (!response.ok) throw new Error(`fixture HTTP ${response.status}: ${await response.text()}`);
  if (response.status === 204) return undefined;
  const data: unknown = await response.json(); return data;
}
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const finish = (): void => { signal?.removeEventListener("abort", abort); resolve(); };
    const timer = setTimeout(finish, ms);
    const abort = (): void => { clearTimeout(timer); signal?.removeEventListener("abort", abort); reject(new Error("fixture interrupted")); };
    if (signal?.aborted) abort(); else signal?.addEventListener("abort", abort, { once: true });
  });
}
export async function until(check: () => boolean, ms: number, label: string, signal?: AbortSignal): Promise<void> {
  const end = performance.now() + ms;
  while (!check()) { if (performance.now() > end) throw new Error(label + ": deadline"); await delay(25, signal); }
}
export function hash(data: Uint8Array | Uint8ClampedArray): string {
  let n = 2166136261;
  for (const b of data) n = Math.imul(n ^ b, 16777619);
  return (n >>> 0).toString(16);
}
export async function sha256(bytes: Uint8Array<ArrayBuffer>): Promise<string> {
  return [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map((b) => b.toString(16).padStart(2, "0")).join("");
}
export function unhex(value: string): Uint8Array<ArrayBuffer> {
  assert(value.length % 2 === 0 && /^[a-f0-9]*$/i.test(value), "invalid independent oracle hex");
  return new Uint8Array(Array.from({ length: value.length / 2 }, (_, i) => parseInt(value.slice(i * 2, i * 2 + 2), 16)));
}
export function rms(values: Float32Array<ArrayBuffer>): number {
  return Math.sqrt(values.reduce((sum, x) => sum + x * x, 0) / Math.max(values.length, 1));
}
export function element<A extends HTMLElement>(id: string, check: (value: HTMLElement) => value is A): A {
  const item = document.getElementById(id); assert(item !== null && check(item), "missing element " + id); return item;
}
/** Container parsing, not decoding. Validate every top-level ISO BMFF length and required boxes. */
export function mp4Boxes(bytes: Uint8Array<ArrayBuffer>): readonly { type: string; size: number }[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), boxes: { type: string; size: number }[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    assert(bytes.length - offset >= 8, "truncated MP4 box header");
    let size = view.getUint32(offset), header = 8;
    const type = String.fromCharCode(...bytes.subarray(offset + 4, offset + 8));
    if (size === 1) { assert(bytes.length - offset >= 16, "truncated extended box size"); const big = view.getBigUint64(offset + 8); assert(big <= BigInt(Number.MAX_SAFE_INTEGER), "oversize MP4 box"); size = Number(big); header = 16; }
    if (size === 0) size = bytes.length - offset;
    assert(size >= header && size <= bytes.length - offset, "invalid MP4 box extent");
    boxes.push({ type, size }); offset += size;
  }
  for (const type of ["ftyp", "moov", "moof", "mdat"]) assert(boxes.some((box) => box.type === type), "missing " + type);
  return boxes;
}

/** Development runner deadline. Browser operations themselves may be uncancellable;
 * the owning peer is closed on failure and each subsequent mutation checks liveness. */
export const bounded = async <A>(work: Promise<A>, milliseconds: number, label: string, signal: AbortSignal): Promise<A> => {
  let timer: ReturnType<typeof setTimeout> | undefined, abort: (() => void) | undefined;
  try { return await Promise.race([work, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(label + " deadline")), milliseconds);
    abort = () => reject(new Error(label + " aborted"));
    if (signal.aborted) abort(); else signal.addEventListener("abort", abort, { once: true });
  })]); } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (abort !== undefined) signal.removeEventListener("abort", abort);
  }
};
