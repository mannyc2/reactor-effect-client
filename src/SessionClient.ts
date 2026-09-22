import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Http from "effect/unstable/http/HttpClient";
import type * as Schema from "effect/Schema";
import { HttpClient } from "./http.js";
import { Session } from "./session.js";
import type { SessionEvent, SessionOptions } from "./session.js";
import { Observations } from "./observation.js";
import { ReactorError, errorOf } from "./errors.js";
import type { PeerFactoryShape } from "./PeerFactory.js";
import type { AudioFrame, VideoFrame, Client, ControlEvent, Options, Snapshot, UploadReference } from "./Model.js";
import type * as Wire from "./wire.generated.js";

interface Buffered<A> { readonly value: A; readonly bytes: number }
const maxQueuedBytes = 32 * 1024 * 1024;
const maxUploadBytes = 64 * 1024 * 1024;
const emptyPressure: Snapshot = {
  closed: false, queuedControl: 0, queuedVideo: 0, queuedAudio: 0, queuedBytes: 0,
  droppedVideo: 0n, droppedAudio: 0n, pendingRequests: 0, deliveredVideo: 0n, deliveredAudio: 0n,
};

/**
 * A decoded-media view over the independent protocol Session. It translates
 * model messages for the H3 adapter; it does not allocate a second session owner.
 * Browser-only code does not import this filesystem convenience boundary.
 */
export const make = (peers: PeerFactoryShape, options: Options): Effect.Effect<
  Client, ReactorError, Scope.Scope | Http.HttpClient | FileSystem.FileSystem | Path.Path | Crypto.Crypto
> => Effect.gen(function* () {
  const scope = yield* Effect.scope;
  const http = yield* Http.HttpClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  yield* Effect.try({
    try: () => {
      if (!Redacted.isRedacted(options.jwt) || typeof options.modelName !== "string" || options.modelName.length === 0) {
        throw new ReactorError("InvalidInput", "a model name and Redacted token are required", { outcome: "not-submitted" });
      }
    }, catch: errorOf,
  });
  yield* peers.check;

  const gate = yield* Semaphore.make(1);
  const observations = new Observations<ControlEvent>();
  const video = yield* Queue.dropping<Buffered<VideoFrame>, ReactorError>(2);
  const audio = yield* Queue.dropping<Buffered<AudioFrame>, ReactorError>(32);
  let implementation: Session | undefined;
  let closing = false;
  let closed = false;
  let queuedBytes = 0;
  let droppedVideo = 0n, droppedAudio = 0n;
  let deliveredVideo = 0n, deliveredAudio = 0n;
  let mediaScope: Scope.Closeable | undefined;
  let reportedId: string | undefined;
  let pressure = emptyPressure;
  let retiredGeneration: bigint | undefined;
  let retiredVideoDrops = 0n, retiredAudioDrops = 0n;

  const requireSession = () => {
    if (closing || closed) throw new ReactorError("Closed", "client is closing or closed", { outcome: "not-submitted" });
    if (implementation === undefined) throw new ReactorError("InvalidState", "connect the session first", { outcome: "not-submitted" });
    return implementation;
  };
  const attempt = <A>(f: () => A) => Effect.try({ try: f, catch: errorOf });

  const emit = (event: ControlEvent) => {
    const encoded = JSON.stringify(event);
    observations.emit(event, new TextEncoder().encode(encoded).byteLength);
  };
  const project = (event: SessionEvent) => Effect.sync(() => {
    const id = implementation?.id;
    if (id !== undefined && id !== reportedId) { reportedId = id; emit({ _tag: "Session", sessionId: id }); }
    switch (event.type) {
      case "status": emit({ _tag: "Status", status: event.status }); break;
      case "model":
        if (event.reply.kind === "message") emit({ _tag: "Message", message: {
          type: event.reply.type,
          ...(event.reply.data === undefined ? {} : { data: event.reply.data }),
        } });
        break;
      case "track": emit({ _tag: "Track", name: event.name, mid: event.mid }); break;
      case "diagnostic": emit({ _tag: "Error", error: {
        code: event.error.context.remoteCode ?? event.error.code,
        message: event.error.message,
        recoverable: event.error.code === "Disconnected" || event.error.code === "Timeout",
      } }); break;
      default: break;
    }
  });

  const discard = <A>(queue: Queue.Queue<Buffered<A>, ReactorError>) => {
    const entry = Queue.takeUnsafe(queue);
    if (entry !== undefined && Exit.isSuccess(entry)) queuedBytes -= entry.value.bytes;
  };
  const offerVideo = (frame: VideoFrame) => Effect.sync(() => {
    if (closing) return;
    const bytes = frame.data.byteLength + frame.metadata.byteLength;
    while (Queue.sizeUnsafe(video) > 0 && (Queue.sizeUnsafe(video) >= 2 || queuedBytes + bytes > maxQueuedBytes)) {
      discard(video); droppedVideo++;
    }
    if (queuedBytes + bytes > maxQueuedBytes || !Queue.offerUnsafe(video, { value: frame, bytes })) droppedVideo++;
    else queuedBytes += bytes;
  });
  const offerAudio = (frame: AudioFrame) => Effect.sync(() => {
    if (closing) return;
    const bytes = frame.samples.byteLength;
    if (queuedBytes + bytes > maxQueuedBytes || !Queue.offerUnsafe(audio, { value: frame, bytes })) droppedAudio++;
    else queuedBytes += bytes;
  });
  const channel = <A>(name: string, queue: Queue.Queue<Buffered<A>, ReactorError>, delivered: () => void) => {
    let reading = false;
    return Stream.unwrap(Effect.gen(function* () {
      yield* Effect.acquireRelease(attempt(() => {
        if (closing) throw new ReactorError("Closed", "client is closed");
        if (reading) throw new ReactorError("AlreadyReading", `${name} already has a reader`);
        reading = true;
      }), () => Effect.sync(() => { reading = false; }));
      return Stream.fromEffectRepeat(Effect.uninterruptibleMask((restore) => restore(Queue.take(queue)).pipe(
        Effect.map((entry) => { queuedBytes -= entry.bytes; delivered(); return entry.value; }),
      )));
    }));
  };

  const startMedia = (session: Session) => Effect.gen(function* () {
    if (mediaScope !== undefined) yield* Scope.close(mediaScope, Exit.void);
    mediaScope = yield* Scope.fork(scope);
    const port = session.rawMedia;
    if (port === undefined) return yield* Effect.fail(new ReactorError("UnsupportedCapability", "peer has no decoded-media capability"));
    const generation = session.snapshot.generation;
    const tracks = session.snapshot.remote?.descriptor?.capabilities?.tracks ?? [];
    const pump = <A>(stream: Stream.Stream<A, ReactorError>, receive: (value: A) => Effect.Effect<void>) => stream.pipe(
      Stream.runForEach((value) => session.snapshot.generation === generation ? receive(value) : Effect.void),
      Effect.catch((error) => Effect.sync(() => {
        if (!closing && session.snapshot.generation === generation) emit({ _tag: "Error", error: { code: error.code, message: error.message, recoverable: false } });
      })),
      Effect.forkIn(mediaScope!, { startImmediately: true }),
    );
    for (const track of tracks) {
      if (track.direction !== "recvonly") continue;
      if (track.kind === "video") yield* pump(port.video(track.name), offerVideo);
      else yield* pump(port.audio(track.name), offerAudio);
    }
  });

  const retireMedia = (session: Session) => Effect.gen(function* () {
    if (mediaScope !== undefined) {
      yield* Scope.close(mediaScope, Exit.void);
      mediaScope = undefined;
    }
    const generation = session.snapshot.generation;
    if (generation !== retiredGeneration) {
      const port = session.rawMedia;
      if (port !== undefined) pressure = yield* port.snapshot.pipe(Effect.catch(() => Effect.succeed(pressure)));
      // Retain observed losses across connection generations. A failed snapshot
      // cannot establish how much additional upstream media was lost.
      retiredVideoDrops += pressure.droppedVideo;
      retiredAudioDrops += pressure.droppedAudio;
      retiredGeneration = generation;
      pressure = emptyPressure;
    }
    // Buffers not yet handed to a reader belong to the retired generation. They
    // must never be delivered as if they arrived on the replacement connection.
    while (Queue.sizeUnsafe(video) > 0) { discard(video); droppedVideo++; }
    while (Queue.sizeUnsafe(audio) > 0) { discard(audio); droppedAudio++; }
  });

  const close = yield* Effect.cached(Effect.uninterruptible(Effect.gen(function* () {
    closing = true;
    const mediaExit = mediaScope === undefined ? Exit.void : yield* Effect.exit(Scope.close(mediaScope, Exit.void));
    const sessionExit = yield* Effect.exit(implementation === undefined ? Effect.succeed(undefined) : implementation.close());
    observations.end();
    while (Queue.sizeUnsafe(video) > 0) discard(video);
    while (Queue.sizeUnsafe(audio) > 0) discard(audio);
    Queue.failCauseUnsafe(video, Cause.fail(new ReactorError("Closed", "client closed")));
    Queue.failCauseUnsafe(audio, Cause.fail(new ReactorError("Closed", "client closed")));
    if (Exit.isFailure(mediaExit) || Exit.isFailure(sessionExit)) return yield* Effect.fail(new ReactorError("Shutdown", "local cleanup failed", {
      detail: { media: mediaExit, session: sessionExit },
    }));
    const report = sessionExit.value;
    if (report !== undefined && !report.localClosed) return yield* Effect.fail(new ReactorError("Shutdown", "native cleanup did not establish local closure", { detail: report }));
    closed = true;
  })));
  yield* Effect.addFinalizer(() => close.pipe(Effect.orDie));

  const connect: Client["connect"] = (input = {}) => gate.withPermit(Effect.gen(function* () {
    if (closing || implementation !== undefined) return yield* Effect.fail(new ReactorError("InvalidState", "client already connected or closed", { outcome: "not-submitted" }));
    const bytes = yield* crypto.randomBytes(16).pipe(Effect.mapError((cause) => new ReactorError("InvalidState", "request identity allocation failed", { detail: cause, outcome: "not-submitted" })));
    const configured = yield* attempt((): SessionOptions => ({
      apiUrl: options.apiUrl ?? "https://api.reactor.inc",
      credential: Effect.sync(() => Redacted.value(options.jwt)),
      requestNamespace: Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""),
      maxPending: 32,
      maxUploadBytes,
      ...(input.sessionId === undefined ? { model: { name: options.modelName } } : { attach: { sessionId: input.sessionId }, autoResumeTracks: false }),
    }));
    const session = yield* attempt(() => new Session(configured, peers.make, new HttpClient(configured, http)));
    implementation = session;
    const observation = yield* session.observe({ capacity: 256 }).pipe(Scope.provide(scope));
    yield* observation.events.pipe(Stream.runForEach(project), Effect.catchCause((cause) => Effect.sync(() => {
      if (!closing) emit({ _tag: "Error", error: { code: "Protocol", message: "session observation failed", recoverable: false } });
      void cause;
    })), Effect.forkIn(scope, { startImmediately: true }));
    yield* session.start();
    yield* startMedia(session);
  }));

  const uploadFile: Client["uploadFile"] = (file) => Effect.gen(function* () {
    const session = yield* attempt(requireSession);
    const chunks: Uint8Array[] = [];
    let size = 0;
    yield* fs.stream(file).pipe(Stream.runForEach((bytes) => attempt(() => {
      size += bytes.byteLength;
      if (size > maxUploadBytes) throw new ReactorError("InvalidInput", "upload exceeds its byte limit", { outcome: "not-submitted" });
      chunks.push(bytes);
    })), Effect.mapError((cause) => cause instanceof ReactorError ? cause : new ReactorError("InvalidInput", "upload file could not be read", { detail: cause, outcome: "not-submitted" })));
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    const name = path.basename(file);
    const extension = path.extname(name).toLowerCase();
    const mimeType = extension === ".png" ? "image/png" : extension === ".webp" ? "image/webp" : [".jpg", ".jpeg"].includes(extension) ? "image/jpeg" : "application/octet-stream";
    const result = yield* session.upload(name, mimeType, bytes);
    return { ...result.file, size: Number(result.file.size) } satisfies UploadReference;
  });

  return {
    connect,
    send: (command, args = {}, uploads) => Effect.gen(function* () {
      const session = yield* attempt(requireSession);
      const files = yield* Effect.try({
        try: () => {
          const files = new Map<string, Wire.UploadReference>();
          for (const [name, upload] of Object.entries(uploads ?? {})) {
            if (name.length === 0 || !Number.isSafeInteger(upload.size) || upload.size < 0 ||
              ![upload.upload_id, upload.name, upload.mime_type].every((value) => typeof value === "string" && value.length > 0)) {
              throw new Error("Invalid upload reference");
            }
            files.set(name, { ...upload, size: BigInt(upload.size) });
          }
          return files;
        },
        catch: (cause) => new ReactorError("InvalidInput", "command uploads must contain valid upload references", {
          outcome: "not-submitted", detail: cause,
        }),
      });
      const reply = yield* session.command(command, args, files);
      return reply.kind === "ack" ? undefined : {
        type: reply.type, ...(reply.data === undefined ? {} : { data: reply.data }),
      } satisfies Schema.Json;
    }),
    uploadFile,
    reconnect: gate.withPermit(Effect.gen(function* () {
      const session = yield* attempt(requireSession);
      yield* retireMedia(session);
      yield* session.reconnect();
      yield* startMedia(session);
    })),
    disconnect: close,
    close,
    stats: attempt(requireSession).pipe(Effect.flatMap((session) => session.rawStats()), Effect.flatMap((stats) => attempt(() => JSON.parse(JSON.stringify(stats)) as Schema.Json))),
    requestSchema: attempt(requireSession).pipe(Effect.flatMap((session) => session.schema()), Effect.map((value) => value.openapi)),
    events: observations.stream({ capacity: 256 }),
    video: channel("video", video, () => { deliveredVideo++; }),
    audio: channel("audio", audio, () => { deliveredAudio++; }),
    snapshot: Effect.gen(function* () {
      const media = implementation?.rawMedia;
      const generation = implementation?.snapshot.generation;
      if (media !== undefined && !closed && generation !== retiredGeneration) {
        const observed = yield* media.snapshot.pipe(Effect.catch(() => Effect.succeed(pressure)));
        if (!closed && implementation?.snapshot.generation === generation && generation !== retiredGeneration) pressure = observed;
      }
      return {
        ...pressure, closed,
        queuedVideo: Queue.sizeUnsafe(video) + (closed ? 0 : pressure.queuedVideo),
        queuedAudio: Queue.sizeUnsafe(audio) + (closed ? 0 : pressure.queuedAudio),
        queuedBytes: queuedBytes + (closed ? 0 : pressure.queuedBytes),
        droppedVideo: droppedVideo + retiredVideoDrops + pressure.droppedVideo,
        droppedAudio: droppedAudio + retiredAudioDrops + pressure.droppedAudio,
        pendingRequests: (implementation?.snapshot.pending.data ?? 0) + (implementation?.snapshot.pending.control ?? 0),
        deliveredVideo, deliveredAudio,
      };
    }),
  };
});
