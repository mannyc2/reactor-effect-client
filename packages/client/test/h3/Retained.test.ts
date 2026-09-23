/** The H3 reducer's running retained-byte total. */
import { describe, expect, test } from "vitest";
import { Retained, sizeOf } from "../../src/h3/_internal/retained.js";

describe("retained-byte accounting", () => {
  test("an object held by several entries counts once, until its last holder drops it", () => {
    const retained = new Retained();
    const source = { data: { prompt: "a shared reply" } };
    retained.hold(source);
    retained.hold(source);
    expect(retained.bytes).toBe(sizeOf(source));
    retained.drop(source);
    expect(retained.bytes).toBe(sizeOf(source));
    retained.drop(source);
    expect(retained.bytes).toBe(0);
  });

  test("the total bounds the size of everything held from above", () => {
    const retained = new Retained();
    const clip = { clip_id: "c", prompt: "p".repeat(100) };
    const queue = { playout: [clip] };
    retained.hold(clip);
    retained.hold(queue);
    expect(retained.bytes).toBeGreaterThanOrEqual(sizeOf({ clip, queue }));
  });

  test("swapping to the same object keeps its size, and swapping away releases it", () => {
    const retained = new Retained();
    const state = { playing: true };
    retained.swap(undefined, state);
    retained.swap(state, state);
    expect(retained.bytes).toBe(sizeOf(state));
    retained.swap(state, undefined);
    expect(retained.bytes).toBe(0);
  });
});
