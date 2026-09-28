import { spawn, spawnSync } from "node:child_process";
import type { ChildProcessByStdio } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Redacted from "effect/Redacted";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import type * as Duration from "effect/Duration";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as Reactor from "reactor-effect-client/Reactor";
import type { IceCandidate } from "reactor-effect-client/Coordinator";
import type { DecodedMedia } from "reactor-effect-client/Media";
import type { Peer } from "reactor-effect-client/Peer";
import * as NativePeer from "../src/NativePeer.js";
import * as Library from "../src/internal/library.js";
import * as InProcess from "../src/internal/peer.js";

export const libraryName =
  process.platform === "darwin"
    ? "libreactor_effect_native.dylib"
    : process.platform === "win32"
      ? "reactor_effect_native.dll"
      : "libreactor_effect_native.so";
export const libraryPath = fileURLToPath(
  new URL(`../lib/${process.platform}-${process.arch}/${libraryName}`, import.meta.url),
);
const include = fileURLToPath(new URL("../rust/include", import.meta.url));
const fixtureSource = fileURLToPath(new URL("./session-fixture.c", import.meta.url));

/** Compile C source against the ABI header into a shared library in a fresh directory. */
export const compileLibrary = (
  source: string,
  name: string,
): { readonly directory: string; readonly path: string } => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-"));
  const file = join(directory, `${name}.c`);
  const path = join(directory, `lib${name}.${process.platform === "darwin" ? "dylib" : "so"}`);
  writeFileSync(file, source);
  const compiler = process.env.CC ?? "cc";
  const flags = process.platform === "darwin" ? ["-dynamiclib"] : ["-shared", "-fPIC"];
  const result = spawnSync(
    compiler,
    ["-std=c11", "-D_DEFAULT_SOURCE", "-pthread", `-I${include}`, ...flags, file, "-o", path],
    { encoding: "utf8" },
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`${compiler} failed: ${result.stderr}`);
  return { directory, path };
};

/** The scripted ABI 4 fixture; `prefix` is prepended to its source. */
export const compileFixture = (
  prefix = "",
): { readonly directory: string; readonly path: string } =>
  compileLibrary(`${prefix}${readFileSync(fixtureSource, "utf8")}`, "fixture");

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

/** A native peer on the library at `path`, loaded as the layer loads it. */
export const nativePeer = (path: string = libraryPath, shutdownTimeout?: Duration.Duration) =>
  Effect.flatMap(Library.load(path), (library) => InProcess.make(library, shutdownTimeout));

/**
 * A native peer on its own scope, for a test that drives it across awaits:
 * `close` closes the scope, whose finalizer joins the peer if nothing else did.
 */
export const openPeer = async (path: string = libraryPath) => {
  const scope = await Effect.runPromise(Scope.make());
  const peer = await Effect.runPromise(nativePeer(path).pipe(Scope.provide(scope)));
  return {
    peer,
    media: decoded(peer),
    scope,
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  };
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
