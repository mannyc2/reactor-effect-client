/**
 * A scripted stand-in for the addon, without libwebrtc, so tests control
 * timing, failures and lifetime. It has the addon's shape; like the addon, it
 * wakes the host on a later turn of the event loop and never waits for it.
 *
 * Files in its directory steer it from any process: while `hold-stats` or
 * `hold-shutdown` exists, that call does not answer. Every call it receives,
 * each call's answer and each item taken from a queue is appended to
 * `calls.log`, so a test sees what reached a child and what it sent back.
 */
// A stand-in for a Node-API module has no Effect services: it reads and appends the files
// another process steers it with synchronously, as the addon's own calls return.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { appendFileSync, existsSync } from "node:fs";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import type * as Binding from "../../src/internal/binding.js";

const mapping: Array<Binding.Mapping> = [
  { name: "main_video", kind: "video", direction: "recvonly", mid: "0" },
  { name: "main_audio", kind: "audio", direction: "recvonly", mid: "1" },
  { name: "input_audio", kind: "audio", direction: "sendonly", mid: "2" },
];

/** The addon's module, steered by the files in `directory`. */
export const make = (directory: string) => {
  const log = (call: string) => appendFileSync(`${directory}/calls.log`, `${call}\n`);
  /** `value`, once no `hold-<call>` file is in the directory: a promise, as the addon answers. */
  const release = <A,>(call: string, value: A): Promise<A> =>
    Effect.runPromise(
      Effect.suspend(() =>
        existsSync(`${directory}/hold-${call}`) ? Effect.fail("held") : Effect.succeed(value),
      ).pipe(Effect.retry({ schedule: Schedule.spaced("5 millis") })),
    );
  /** In-process controls: the peers made, and a fault to inject. */
  const controls = { peers: [] as Array<NativePeer>, fault: false };

  class NativePeer implements Binding.NativePeer {
    closed = false;
    events: Array<Binding.PeerEvent> = [];
    video: Array<Binding.Video> = [];
    audio: Array<Binding.Audio> = [];
    readonly calls = new Set<Promise<Binding.Reply>>();
    readonly ready: (bits: number) => void;

    constructor(ready: (bits: number) => void) {
      this.ready = ready;
      controls.peers.push(this);
    }

    wake(bits: number): void {
      setImmediate(() => {
        if (!this.closed) this.ready(bits);
      });
    }

    call(
      name: string,
      answer: () => Binding.Reply | Promise<Binding.Reply>,
    ): Promise<Binding.Reply> {
      log(name);
      if (this.closed) return Promise.resolve({ failure: { class: "Closed", message: "closed" } });
      const call = Promise.resolve(answer()).finally(() => log(`${name} answered`));
      this.calls.add(call);
      return call.finally(() => this.calls.delete(call));
    }

    prepare(): Promise<Binding.Reply> {
      return this.call("prepare", () => ({ prepared: { sdp: "fixture native offer", mapping } }));
    }

    answer(): Promise<Binding.Reply> {
      return this.call("answer", () => {
        this.events.push(
          { type: "state", state: "connected" },
          { type: "channel", channel: "control", open: true },
          { type: "channel", channel: "data", open: true },
        );
        this.wake(1);
        return {};
      });
    }

    direction(): Promise<Binding.Reply> {
      return this.call("direction", () => ({}));
    }

    maxBitrate(): Promise<Binding.Reply> {
      return this.call("maxBitrate", () => ({}));
    }

    stats(): Promise<Binding.Reply> {
      return this.call("stats", () => release("stats", { stats: [] }));
    }

    /** A data-channel send takes 50 ms. */
    send(channel: Binding.Channel): Promise<Binding.Reply> {
      return this.call("send", () =>
        channel === "data" ? Effect.runPromise(Effect.as(Effect.sleep("50 millis"), {})) : {},
      );
    }

    /** Queue a failed connection, which the host classifies from statistics. */
    fail(): void {
      this.events.push({ type: "state", state: "failed" });
      this.wake(1);
    }

    take<A>(queue: string, items: Array<A>): A | null {
      const item = this.closed ? undefined : items.shift();
      if (item === undefined) return null;
      log(`take ${queue}`);
      return item;
    }

    takeEvent(): Binding.PeerEvent | null {
      return this.take("event", this.events);
    }

    takeVideo(): Binding.Video | null {
      return this.take("video", this.video);
    }

    takeAudio(): Binding.Audio | null {
      return this.take("audio", this.audio);
    }

    /**
     * The snapshot is also a test gate: it releases one frame and one audio
     * block, so readers subscribe before media arrives. A fault names the
     * frame's track by main_audio's index, which is not a video receiver.
     */
    pressure(): Binding.Pressure {
      if (!this.closed) {
        this.video.push({
          track: controls.fault ? 1 : 0,
          width: 1,
          height: 1,
          frameId: 18446744073709551615n,
          timestampUs: 9007199254740993n,
          sequence: 9007199254740993n,
          data: Uint8Array.of(1, 2, 3, 4),
          metadata: Uint8Array.of(9, 8, 7),
        });
        this.audio.push({
          track: 1,
          sampleRate: 48000,
          channels: 2,
          sequence: 3n,
          samples: Int16Array.of(1, -2, 300, -400),
        });
        this.wake(6);
      }
      return {
        closed: this.closed,
        pendingRequests: this.calls.size,
        queuedControl: this.events.length,
        queuedVideo: this.video.length,
        queuedAudio: this.audio.length,
        queuedBytes: 0,
        droppedVideo: 0n,
        droppedAudio: 0n,
        deliveredVideo: 0n,
        deliveredAudio: 0n,
      };
    }

    close(): void {
      this.closed = true;
      this.events = [];
      this.video = [];
      this.audio = [];
    }

    /** Like the addon's owner join, shutdown waits for every admitted call. */
    shutdown(): Promise<Binding.Reply> {
      log("shutdown");
      this.close();
      return Promise.allSettled([...this.calls]).then(() => release("shutdown", {}));
    }
  }

  return { NativePeer, buildIdentity: () => "{}", controls };
};
