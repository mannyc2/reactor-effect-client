import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { afterEach, vi } from "vitest";
import * as BrowserMedia from "../src/BrowserMedia.js";

class FakeTrack {
  readyState: "live" | "ended" = "live";
  readonly clones: FakeTrack[] = [];
  readonly kind = "video";
  clone(): FakeTrack {
    const clone = new FakeTrack();
    this.clones.push(clone);
    return clone;
  }
  stop(): void {
    this.readyState = "ended";
  }
}

/** A media element whose `play` never settles (an empty race) unless a test says otherwise. */
const element = (play: () => Promise<void> = () => Promise.race<void>([])) => {
  const fake: Parameters<typeof BrowserMedia.play>[1] & { paused: number } = {
    srcObject: null,
    paused: 0,
    getAttribute: () => null,
    play,
    pause: () => {
      fake.paused++;
    },
  };
  return fake;
};

// The tests hand a fake track to functions typed for the DOM's.
const asTrack = (track: FakeTrack) => track as unknown as MediaStreamTrack;

afterEach(() => {
  vi.unstubAllGlobals();
});

it.effect("a play that never starts fails at its deadline and detaches its clone", () =>
  Effect.gen(function* () {
    vi.stubGlobal("MediaStream", class {});
    const track = new FakeTrack();
    const media = element();
    const playing = yield* BrowserMedia.play(asTrack(track), media, {
      playTimeout: "2 seconds",
    }).pipe(Effect.scoped, Effect.flip, Effect.forkChild);
    yield* TestClock.adjust("2 seconds");
    const error = yield* Fiber.join(playing);
    assert.strictEqual(error.reason._tag, "Timeout");
    assert.strictEqual(track.clones[0]?.readyState, "ended");
    assert.strictEqual(media.srcObject, null);
    assert.strictEqual(track.readyState, "live");
  }),
);

it.effect("a media element that cannot take the stream stops the clone it was given", () =>
  Effect.gen(function* () {
    vi.stubGlobal(
      "MediaStream",
      class {
        constructor() {
          throw new Error("MediaStream is not supported here");
        }
      },
    );
    const track = new FakeTrack();
    const error = yield* BrowserMedia.play(asTrack(track), element()).pipe(
      Effect.scoped,
      Effect.flip,
    );
    assert.strictEqual(error.reason._tag, "UnsupportedCapability");
    assert.strictEqual(track.clones[0]?.readyState, "ended");
  }),
);

it.effect("an element that already has a source is refused before anything is cloned", () =>
  Effect.gen(function* () {
    const track = new FakeTrack();
    const media = element();
    media.getAttribute = () => "clip.mp4";
    const error = yield* BrowserMedia.play(asTrack(track), media).pipe(Effect.scoped, Effect.flip);
    assert.strictEqual(error.reason._tag, "InvalidState");
    assert.strictEqual(track.clones.length, 0);
  }),
);

it.effect("playback that started holds its clone until the scope closes", () =>
  Effect.gen(function* () {
    vi.stubGlobal("MediaStream", class {});
    const track = new FakeTrack();
    const media = element(() => Promise.resolve());
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* BrowserMedia.play(asTrack(track), media);
        assert.strictEqual(track.clones[0]?.readyState, "live");
        assert.notStrictEqual(media.srcObject, null);
      }),
    );
    assert.strictEqual(track.clones[0]?.readyState, "ended");
    assert.strictEqual(media.srcObject, null);
    assert.strictEqual(media.paused, 1);
  }),
);
