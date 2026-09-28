/**
 * What the native tests share: the scripted fake addon, the far peer process
 * the media tests receive from, and small assertions. The far peer and the
 * load measurements are process and clock harnesses, so this module keeps
 * the package's lenient diagnostics.
 */
import { spawn } from "node:child_process";
import type { ChildProcessByStdio } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import type * as Duration from "effect/Duration";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as Reactor from "reactor-effect-client/Reactor";
import type { IceCandidate } from "reactor-effect-client/Coordinator";
import type { DecodedMedia } from "reactor-effect-client/Media";
import type { Peer } from "reactor-effect-client/Peer";
import * as NativePeer from "../src/NativePeer.js";
import { load } from "../src/internal/addon.js";
import type { Addon } from "../src/internal/addon.js";
import { local } from "../src/internal/local.js";
import * as InProcess from "../src/internal/peer.js";
import { make } from "./fixtures/addon.mjs";

const fixture = fileURLToPath(new URL("./fixtures/addon.mts", import.meta.url));

/** Poll `condition` on the live clock until it holds, or fail with `message`. */
export const eventually = (
  condition: () => boolean,
  message: string,
  timeout: Duration.Input = "5 seconds",
) =>
  Effect.suspend(() => (condition() ? Effect.void : Effect.fail(new Error(message)))).pipe(
    Effect.retry({ schedule: Schedule.spaced("5 millis") }),
    Effect.timeoutOrElse({ duration: timeout, orElse: () => Effect.die(new Error(message)) }),
    Effect.orDie,
  );

/**
 * The scripted fake addon in a directory of its own. `path` is a module
 * another process can load; `module` is its twin in this process. Holding a
 * call keeps it from answering in either.
 */
export const makeFakeAddon = () => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-addon-"));
  const path = join(directory, "addon.cjs");
  writeFileSync(path, `module.exports = require(${JSON.stringify(fixture)}).make(__dirname);\n`);
  const marker = (name: string) => join(directory, name);
  const calls = (): ReadonlyArray<string> =>
    existsSync(marker("calls.log"))
      ? readFileSync(marker("calls.log"), "utf8")
          .split("\n")
          .filter((line) => line !== "")
      : [];
  return {
    directory,
    path,
    module: make(directory),
    hold: (call: "stats" | "shutdown", held: boolean): void =>
      held
        ? writeFileSync(marker(`hold-${call}`), "")
        : rmSync(marker(`hold-${call}`), { force: true }),
    calls,
    /** Wait until `count` calls named `call` have reached the fake. */
    reached: (call: string, count = 1) =>
      eventually(
        () => calls().filter((name) => name === call).length >= count,
        `${call} never reached the fake addon`,
      ),
    remove: () => rmSync(directory, { recursive: true, force: true }),
  };
};

/** The fake addon, removed with the scope. */
export const fakeAddon = Effect.acquireRelease(Effect.sync(makeFakeAddon), (addon) =>
  Effect.sync(addon.remove),
);

export type FakeAddon = Effect.Success<typeof fakeAddon>;

/** A native peer on `addon`, the installed one by default, made as the layer makes it. */
export const nativePeer = (addon?: Addon, shutdownTimeout?: Duration.Duration) =>
  (addon === undefined ? load(undefined) : Effect.succeed(addon)).pipe(
    Effect.flatMap(local),
    Effect.flatMap((handle) => InProcess.make(handle, shutdownTimeout)),
  );

export const until = async (
  condition: () => boolean,
  message: string,
  timeoutMs = 5000,
): Promise<void> => {
  const deadline = performance.now() + timeoutMs;
  while (!condition()) {
    if (performance.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

/**
 * The canonical factory over the native peer, with its host layer built in the
 * caller's scope, as `Reactor.layer(configuration).pipe(Layer.provide(NativePeer.layer(options)))`
 * does for an application.
 */
export const nativeClient = (
  settings: Coordinator.Options & Reactor.Options = {},
  options: NativePeer.Options = {},
) =>
  Layer.build(Layer.merge(NativePeer.layer(options), Coordinator.layer(settings))).pipe(
    Effect.flatMap((services) => Reactor.make(settings).pipe(Effect.provide(services))),
  );

/** The test far peer that `bun run native:test` builds, or `REACTOR_NATIVE_FAR_PEER`. */
export const farPeerPath =
  process.env.REACTOR_NATIVE_FAR_PEER ??
  fileURLToPath(new URL("../rust/target/release/examples/far_peer", import.meta.url));

export type Message = Readonly<Record<string, unknown>>;

export const record = (value: unknown): Message =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Message) : {};

/**
 * The far peer process: a libwebrtc sender on the pinned reactor-webrtc that
 * answers offers, sends 1344x768 BGRA at 24 fps with per-frame metadata plus
 * 48 kHz PCM, and echoes both channels, one session per id.
 */
export class FarPeer {
  private readonly waiters: {
    readonly op: string;
    readonly id: string;
    readonly resolve: (message: Message) => void;
  }[] = [];
  private readonly exited: Promise<unknown>;

  private constructor(private readonly child: ChildProcessByStdio<Writable, Readable, null>) {
    this.exited = new Promise((resolve) => child.once("exit", resolve));
    createInterface({ input: child.stdout }).on("line", (line) => {
      const message = JSON.parse(line) as Message;
      const index = this.waiters.findIndex(
        (waiter) => waiter.op === message.op && waiter.id === (message.id ?? ""),
      );
      if (index >= 0) this.waiters.splice(index, 1)[0]?.resolve(message);
    });
  }

  static async start(): Promise<FarPeer> {
    if (!existsSync(farPeerPath))
      throw new Error(
        `missing far peer ${farPeerPath}; build it with cargo build --release --example far_peer (scripts/test.sh does)`,
      );
    const far = new FarPeer(spawn(farPeerPath, [], { stdio: ["pipe", "pipe", "inherit"] }));
    await far.next("ready", "");
    return far;
  }

  private next(op: string, id: string): Promise<Message> {
    return new Promise((resolve) => this.waiters.push({ op, id, resolve }));
  }

  private send(message: Message): void {
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  async answer(id: string, sdp: string): Promise<string> {
    const reply = this.next("answer", id);
    this.send({ op: "offer", id, sdp });
    return String((await reply).sdp);
  }

  candidate(id: string, candidate: IceCandidate): void {
    this.send({
      op: "candidate",
      id,
      candidate: candidate.candidate,
      sdpMid: candidate.sdp_mid,
      sdpMLineIndex: candidate.sdp_mline_index,
    });
  }

  /** One session's stats as far_peer.rs reports them: pacing, encoder and path. */
  async stats(id: string): Promise<Message> {
    const reply = this.next("stats", id);
    this.send({ op: "stats", id });
    return record((await reply).stats);
  }

  /** Frames the far peer's encoder actually sent, and their size. */
  async sent(id: string): Promise<{ frames: number; width: number; height: number }> {
    const video = record((await this.stats(id)).video);
    return {
      frames: Number(video.framesSent ?? 0),
      width: Number(video.frameWidth ?? 0),
      height: Number(video.frameHeight ?? 0),
    };
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  async close(id: string): Promise<void> {
    const reply = this.next("closed", id);
    this.send({ op: "close", id });
    await reply;
  }

  async quit(): Promise<void> {
    this.child.stdin.end();
    await this.exited;
  }
}

/** An error with its Redacted diagnostic detail revealed, for assertions on what it recorded. */
export const revealed = <E extends { readonly context: { readonly detail?: unknown } }>(
  error: E,
) => {
  const detail = error.context.detail;
  return {
    ...error,
    reason: (error as { readonly reason?: unknown }).reason,
    message: (error as { readonly message?: unknown }).message,
    context: {
      ...error.context,
      detail: Redacted.isRedacted(detail) ? Redacted.value(detail) : detail,
    },
  };
};

/** A peer's decoded media: every native peer has it. */
export const decoded = (peer: Peer): Omit<DecodedMedia, "generation" | "tracks" | "retired"> => {
  if (peer.media._tag !== "Decoded") throw new Error("expected a peer with decoded media");
  return peer.media;
};

/**
 * Each frame's bytes are the whole of an ArrayBuffer of their own: offset 0,
 * no slack and no buffer shared with another frame, so a transfer moves only
 * that frame.
 */
export const assertExactFrames = <A>(
  frames: ReadonlyArray<A>,
  bytes: (frame: A) => ArrayBufferView,
): void => {
  const seen = new Set<ArrayBufferLike>();
  frames.forEach((frame, index) => {
    const view = bytes(frame);
    if (view.byteOffset !== 0 || view.buffer.byteLength !== view.byteLength)
      throw new Error(`frame ${index} is not the whole of its buffer`);
    if (seen.has(view.buffer)) throw new Error(`frame ${index} shares its buffer with another`);
    seen.add(view.buffer);
  });
};
