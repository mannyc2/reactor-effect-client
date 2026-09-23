/** Conservative retained-size estimate of one value; shared objects inside it count once. */
export const sizeOf = (input: unknown): number => {
  const pending: unknown[] = [input],
    seen = new Set<object>();
  let bytes = 0,
    nodes = 0;
  while (pending.length > 0) {
    if (++nodes > 1_000_000) return Number.MAX_SAFE_INTEGER;
    const value = pending.pop();
    if (typeof value === "string") bytes += value.length * 3;
    else if (value !== null && typeof value === "object") {
      if (seen.has(value)) continue;
      seen.add(value);
      if (ArrayBuffer.isView(value)) bytes += value.byteLength;
      else if (value instanceof ArrayBuffer) bytes += value.byteLength;
      else if (value instanceof Map) {
        bytes += value.size * 32;
        for (const [key, child] of value) {
          pending.push(key);
          pending.push(child);
        }
      } else if (value instanceof Set) {
        bytes += value.size * 16;
        for (const child of value) pending.push(child);
      } else {
        bytes += 32;
        for (const child of Object.values(value)) pending.push(child);
      }
    } else bytes += 8;
  }
  return bytes;
};

/**
 * The reducer's retained bytes as a running total, so checking the bound costs
 * nothing per event. Each retained object is sized once, when its first holder
 * takes it, and subtracted when its last holder drops it: one reply source
 * behind every clip of a queue snapshot counts once. The total is an upper
 * bound of `sizeOf` over everything held, since an object reachable from two
 * held roots (a clip in both the queue and the clip table) counts twice.
 */
export class Retained {
  private readonly held = new Map<object, { count: number; readonly bytes: number }>();
  private total = 0;

  get bytes(): number {
    return this.total;
  }

  /** Bytes not tied to an object: entry overheads and keys. */
  add(bytes: number): void {
    this.total += bytes;
  }

  hold(value: object | null | undefined): void {
    if (value === null || value === undefined) return;
    const entry = this.held.get(value);
    if (entry !== undefined) {
      entry.count++;
      return;
    }
    const bytes = sizeOf(value);
    this.held.set(value, { count: 1, bytes });
    this.total += bytes;
  }

  drop(value: object | null | undefined): void {
    if (value === null || value === undefined) return;
    const entry = this.held.get(value);
    if (entry === undefined) return;
    if (--entry.count > 0) return;
    this.held.delete(value);
    this.total -= entry.bytes;
  }

  /** Replace one held reference with another; holding first keeps a shared object sized once. */
  swap(previous: object | null | undefined, next: object | null | undefined): void {
    this.hold(next);
    this.drop(previous);
  }
}
