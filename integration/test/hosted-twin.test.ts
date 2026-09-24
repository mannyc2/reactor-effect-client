/**
 * The hosted twin, driven end to end by the public client over loopback: the
 * vertical and takeover flows of the hosted qualification, each fault, and
 * the refusals, with no credential and no traffic beyond 127.0.0.1.
 */
import { Buffer } from "node:buffer";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import { mediaGeneration } from "reactor-effect-client/host";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Testing from "reactor-effect-client/testing";
import { startTwin, twinPeers } from "../hosted/twin/index.js";
import type { Twin, TwinOptions } from "../hosted/twin/index.js";

const prompt = "A slow camera move across a sunlit table with a glass of water.";

/** What openH3 needs beside the client; a prompt-only clip reads no file. */
const services = Layer.mergeAll(
  Path.layer,
  FileSystem.layerNoop({}),
  Layer.sync(Crypto.Crypto, () =>
    Crypto.make({
      randomBytes: (size) => new Uint8Array(randomBytes(size)),
      digest: (algorithm, data) =>
        Effect.sync(
          () => new Uint8Array(createHash(algorithm.replace("-", "")).update(data).digest()),
        ),
    }),
  ),
);

/** The qualification's client layer, with twin peers in place of the native host. */
const clientLayer = (apiUrl: string) =>
  Reactor.layer({ apiUrl }).pipe(
    Layer.provideMerge(Layer.mergeAll(Reactor.FetchHttp.layer, services, twinPeers(apiUrl))),
  );
type Services = Layer.Success<ReturnType<typeof clientLayer>>;

/** One scope and one client, as one process of the qualification has. */
const run = <A, E>(twin: Twin, body: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
  Effect.runPromise(body.pipe(Effect.scoped, Effect.provide(clientLayer(twin.url))));

const coordinator = (twin: Twin, jwt?: Redacted.Redacted<string>) =>
  Reactor.Coordinator.make({
    apiUrl: twin.url,
    ...(jwt === undefined ? {} : { credential: Effect.succeed(jwt) }),
  });

const mint = (twin: Twin, apiKey = twin.apiKey) =>
  coordinator(twin).pipe(
    Effect.flatMap((client) =>
      client.mintToken({
        apiKey: Redacted.make(apiKey),
        modelName: H3.modelName,
        maxSessionDuration: "60 seconds",
        expiresAfter: "120 seconds",
      }),
    ),
  );

/** Allocate, record the owner, connect and bind H3, as the qualification opens a session. */
const open = (grant: Reactor.Coordinator.TokenGrant, source?: Orchestration.SessionSourceOptions) =>
  Effect.gen(function* () {
    let owned: Orchestration.Allocated | undefined;
    const opened = yield* Orchestration.openH3({
      mint: Effect.succeed(grant),
      onAllocated: (allocated) =>
        Effect.sync(() => {
          owned = allocated;
        }),
      source,
    });
    return {
      session: owned!.session,
      allocation: owned!.allocation,
      provider: opened.source.provider,
    };
  });

/**
 * Turn autoplay on, which H3 documents as off by default. The provider
 * resolves a command before it reduces the state_update the twin sends right
 * after the reply, and refuses a submission while synchronizing, so this
 * refreshes before anything is enqueued.
 */
const autoplay = (provider: H3.Provider) =>
  provider.setAutoplay(true).pipe(Effect.andThen(provider.refresh));

const enqueue = (provider: H3.Provider, seconds = 5, text = prompt) =>
  Effect.gen(function* () {
    const submission = yield* provider.prepare({ prompt: text, seconds });
    const acceptance = yield* submission.submit;
    return { acceptance, operation: yield* provider.operation(submission) };
  });

const play = (provider: H3.Provider, seconds = 5) =>
  Effect.gen(function* () {
    yield* autoplay(provider);
    const clip = yield* enqueue(provider, seconds);
    yield* clip.operation.reached("started").pipe(Effect.timeout("10 seconds"));
    return clip;
  });

const frames = (session: Reactor.Session, count: number) =>
  mediaGeneration(session).pipe(
    Effect.flatMap((media) =>
      media
        .video("main_video")
        .pipe(Stream.take(count), Stream.runCollect, Effect.timeout("5 seconds")),
    ),
  );

const eventually = <A, E, R>(effect: Effect.Effect<A, E, R>, done: (value: A) => boolean) =>
  Effect.gen(function* () {
    while (true) {
      const value = yield* effect;
      if (done(value)) return value;
      yield* Effect.sleep("20 millis");
    }
  }).pipe(Effect.timeout("5 seconds"));

/** What the vertical check requires of frames: not all black, and changing. */
const lit = (frame: { readonly data: Uint8Array }): boolean => {
  for (let index = 0; index < frame.data.length; index += 4)
    if (frame.data[index] !== 0 || frame.data[index + 1] !== 0 || frame.data[index + 2] !== 0)
      return true;
  return false;
};
const changing = (all: readonly { readonly data: Uint8Array }[]): boolean =>
  all.some((frame) => frame.data.some((byte, index) => byte !== all[0]!.data[index]));

const status = (error: { readonly reason: Reactor.ReactorErrorReason }) =>
  error.reason._tag === "Http" ? error.reason.status : undefined;

const until = async (condition: () => boolean, timeoutMs: number): Promise<number> => {
  const started = performance.now();
  while (!condition()) {
    if (performance.now() - started > timeoutMs) throw new Error("the twin never got there");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return performance.now() - started;
};

const withTwin = async <A>(body: (twin: Twin) => Promise<A>, options?: TwinOptions) => {
  const twin = await startTwin(options);
  try {
    return await body(twin);
  } finally {
    await twin.close();
  }
};

/** The takeover's owner: allocate, stream, report, then wait to be killed. */
const owner = (apiUrl: string, stored: string) =>
  Effect.gen(function* () {
    const grant = JSON.parse(stored) as {
      readonly jwt: string;
      readonly expiresAt: number;
      readonly granted: { readonly maxSessions: 1; readonly maxSessionSeconds: number };
    };
    const { session, allocation, provider } = yield* open({
      ...grant,
      jwt: Redacted.make(grant.jwt),
    });
    const playing = yield* play(provider, 10);
    // The clip after it waits in the playout queue while the first one plays. As
    // after autoplay, the provider has not yet reduced the snapshots after clip_started.
    yield* provider.refresh;
    const queued = yield* enqueue(provider, 5, `${prompt} The glass tips over.`);
    yield* queued.operation.reached("generated").pipe(Effect.timeout("10 seconds"));
    yield* frames(session, 24);
    yield* Console.log(
      `owner-streaming ${session.id} ${playing.acceptance.clip.clip_id} ${queued.acceptance.clip.clip_id} ${allocation.endsAt}`,
    );
    return yield* Effect.never;
  }).pipe(Effect.scoped, Effect.provide(clientLayer(apiUrl)));

// The takeover test runs this file as its owner process, as qualify.ts runs
// itself. The owner never returns, so no test registers in that process.
const ownerArgument = process.argv.indexOf("--twin-owner");
if (ownerArgument >= 0)
  await Effect.runPromise(owner(process.argv[ownerArgument + 1]!, process.env.TWIN_OWNER_GRANT!));

/** The owner's report: its session, the clip it plays and the clip waiting after it. */
const streaming = (
  child: ChildProcess,
): Promise<{
  readonly sessionId: string;
  readonly playing: string;
  readonly queued: string;
  readonly endsAt: number;
}> =>
  new Promise((resolve, reject) => {
    createInterface({ input: child.stdout! }).on("line", (line) => {
      const [tag, sessionId, playing, queued, endsAt] = line.split(" ");
      if (tag === "owner-streaming" && sessionId && playing && queued && endsAt)
        resolve({ sessionId, playing, queued, endsAt: Number(endsAt) });
    });
    child.once("exit", (code) => reject(new Error(`the owner exited early (${code})`)));
  });

test(
  "the vertical check runs end to end against the twin",
  () =>
    withTwin(async (twin) => {
      const started = performance.now();
      const result = await run(
        twin,
        Effect.gen(function* () {
          const client = yield* coordinator(twin);
          const rate = yield* Reactor.Coordinator.modelRate(yield* client.pricing, H3.modelName);
          const grant = yield* mint(twin);
          const { session, provider } = yield* open(grant);
          yield* autoplay(provider);
          const submitted = performance.now();
          const { acceptance, operation } = yield* enqueue(provider);
          yield* operation.reached("generated").pipe(Effect.timeout("5 seconds"));
          const generatedMs = performance.now() - submitted;
          yield* operation.reached("started").pipe(Effect.timeout("5 seconds"));
          const startedMs = performance.now() - submitted;
          const media = yield* mediaGeneration(session);
          const video = yield* media
            .video("main_video")
            .pipe(Stream.take(24), Stream.runCollect, Effect.timeout("5 seconds"));
          const audio = yield* media
            .audio("main_audio")
            .pipe(Stream.take(50), Stream.runCollect, Effect.timeout("5 seconds"));
          const pressure = yield* media.snapshot;
          const stats = yield* session.stats;
          // The qualification closes here; this run first lets the clip end.
          const verticalMs = performance.now() - started;
          yield* operation.ended.pipe(Effect.timeout("10 seconds"));
          const endedMs = performance.now() - submitted;
          const facts = yield* operation.facts;
          const closed = yield* session.close;
          return {
            rate,
            grant,
            acceptance,
            facts,
            tracks: media.tracks,
            video,
            audio,
            pressure,
            stats,
            closed,
            sessionId: session.id,
            generatedMs,
            startedMs,
            endedMs,
            verticalMs,
          };
        }),
      );
      // At the default rate, the whole capped 60 s session costs US$0.10.
      expect((result.rate.creditsPerSecond * 60) / result.rate.creditsPerDollar).toBeCloseTo(0.1);
      expect(result.grant.granted).toEqual({ maxSessions: 1, maxSessionSeconds: 60 });
      expect(result.acceptance.evidence.kind).toBe("correlated");
      expect(result.acceptance.clip.frames).toBe(124);
      expect(result.facts.generated?.message).toBe("clip_generated");
      expect(result.facts.started?.message).toBe("clip_started");
      expect(result.facts.ended?.message).toBe("clip_finished");
      expect(result.generatedMs).toBeGreaterThan(400);
      expect(result.startedMs).toBeGreaterThan(900);
      expect(result.startedMs).toBeLessThan(4000);
      expect(result.endedMs - result.startedMs).toBeGreaterThan(4800);
      expect(result.endedMs - result.startedMs).toBeLessThan(8000);
      expect(result.verticalMs).toBeLessThan(15_000);
      expect(result.tracks).toEqual([
        { name: "main_video", kind: "video", direction: "recvonly" },
        { name: "main_audio", kind: "audio", direction: "recvonly" },
      ]);
      expect(result.video).toHaveLength(24);
      for (const frame of result.video)
        expect(frame).toMatchObject({
          format: "BGRA",
          width: 160,
          height: 90,
          track: "main_video",
        });
      expect(result.video.every(lit)).toBe(true);
      expect(changing(result.video)).toBe(true);
      // Admission sequences run without a gap: nothing was dropped.
      expect(
        result.video.every(
          (frame, index) =>
            index === 0 || frame.sequence === result.video[index - 1]!.sequence + 1n,
        ),
      ).toBe(true);
      expect(result.audio).toHaveLength(50);
      for (const block of result.audio) {
        expect(block).toMatchObject({ sampleRate: 48_000, channels: 1, track: "main_audio" });
        expect(block.samples).toHaveLength(480);
      }
      expect(result.audio.some((block) => block.samples.some((sample) => sample !== 0))).toBe(true);
      expect(result.pressure).toMatchObject({
        closed: false,
        readerOverflows: 0n,
        droppedVideo: 0n,
      });
      expect(result.pressure.deliveredVideo >= 24n).toBe(true);
      // As on the native host, the pair names its local candidate only, and it
      // is the direct pair carrying the media, not the relay pair ICE left.
      expect(result.stats.pair).toMatchObject({ localCandidateType: "host" });
      expect(result.stats.pair?.remoteCandidateType).toBeUndefined();
      expect((result.stats.pair?.bytesReceived ?? 0n) > 0n).toBe(true);
      expect(result.stats.roundTripTimeSeconds).toBeGreaterThan(0);
      expect(result.stats.framesPerSecond).toBeGreaterThan(12);
      expect(result.stats.jitterSeconds).toBeGreaterThanOrEqual(0);
      expect(result.stats.lossRatio).toBe(0);
      expect(result.closed.remote).toMatchObject({
        attempted: true,
        deleteStatus: 202,
        confirmed: true,
        evidence: "terminal",
        state: "CLOSED",
      });
      expect([twin.sessionsCreated, twin.deletes, twin.enqueues]).toEqual([1, 1, 1]);
      expect(twin.sessions.get(result.sessionId)).toEqual({ state: "CLOSED", connected: false });
    }),
  30_000,
);

test("the twin mints recognisable credentials: an HS256 JWT the twin plainly signed", () =>
  withTwin(async (twin) => {
    const grant = await run(twin, mint(twin));
    const [header, payload] = Redacted.value(grant.jwt).split(".");
    const decode = (part: string | undefined): unknown =>
      JSON.parse(Buffer.from(part ?? "", "base64url").toString("utf8"));
    expect(twin.apiKey.startsWith("twin-api-key-")).toBe(true);
    expect(decode(header)).toEqual({ alg: "HS256", typ: "JWT", kid: "reactor-twin" });
    expect(decode(payload)).toMatchObject({ iss: "reactor-twin", sub: "reactor-twin-account" });
    expect(grant.expiresAt * 1000 - Date.now()).toBeGreaterThan(90_000);
  }));

test("a reference image uploads through the twin before its clip is queued", () =>
  withTwin(async (twin) => {
    // The smallest complete PNG: one pixel.
    const png = Uint8Array.from(
      Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
        "base64",
      ),
    );
    const acceptance = await run(
      twin,
      Effect.gen(function* () {
        const { provider } = yield* open(yield* mint(twin));
        const submission = yield* provider.prepare({
          prompt,
          references: [{ _tag: "Bytes", bytes: png }],
        });
        return yield* submission.submit;
      }),
    );
    expect(acceptance.clip).toMatchObject({ has_reference_image: true, reference_image_count: 1 });
  }));

test("reference audio uploads through the twin and its clip reports it; audio alone is refused", () =>
  withTwin(async (twin) => {
    const image = { _tag: "Bytes" as const, bytes: Testing.pngBytes(64, 48) };
    const voice = { _tag: "Bytes" as const, bytes: Testing.wavBytes(3) };
    const { acceptance, contract } = await run(
      twin,
      Effect.gen(function* () {
        const { provider } = yield* open(yield* mint(twin));
        const submission = yield* provider.prepare({
          prompt: `${prompt} Audio 1 is the narrator's voice.`,
          references: [image],
          audio: [voice],
        });
        return { acceptance: yield* submission.submit, contract: provider.contract };
      }),
    );
    expect(contract.referenceAudio).toBe(true);
    expect(acceptance.clip).toMatchObject({
      has_reference_image: true,
      reference_image_count: 1,
      has_reference_audio: true,
      reference_audio_count: 1,
    });
    // The twin refuses what the client never sends: audio with no image or continuation.
    const refused = await run(
      twin,
      Effect.gen(function* () {
        const session = (yield* open(yield* mint(twin))).session;
        const upload = yield* session.upload("voice.wav", "audio/wav", voice.bytes);
        return yield* session.command("enqueue", {
          prompt,
          reference_images: [],
          reference_audios: [{ ...upload.file, size: Number(upload.file.size) }],
          metadata: "",
        });
      }),
    );
    expect(refused).toMatchObject({ kind: "message", type: "command_error" });
  }));

test("a relay twin selects a relay candidate pair", () =>
  withTwin(
    async (twin) => {
      const stats = await run(
        twin,
        Effect.gen(function* () {
          const { session } = yield* open(yield* mint(twin));
          return yield* session.stats;
        }),
      );
      expect(stats.pair).toMatchObject({ localCandidateType: "relay" });
    },
    { relay: true },
  ));

test(
  "a second client attaches with the persisted token, sees the queued clip and replaces the first connection",
  () =>
    withTwin(async (twin) => {
      const result = await run(
        twin,
        Effect.gen(function* () {
          const grant = yield* mint(twin);
          const { session: first, provider } = yield* open(grant);
          const { acceptance, operation } = yield* enqueue(provider);
          yield* operation.reached("generated").pipe(Effect.timeout("5 seconds"));
          // Another Client instance, as another process builds one, with the same token.
          const second = yield* Effect.gen(function* () {
            const client = yield* Reactor.Client;
            const session = yield* client.attachConnected({ sessionId: first.id, jwt: grant.jwt });
            const attached = yield* H3.make(session);
            const snapshot = yield* attached.current;
            yield* attached.play(acceptance.clip.clip_id);
            const playing = yield* eventually(
              attached.current,
              (current) =>
                current._tag === "Ready" &&
                current.state.playing_clip_id === acceptance.clip.clip_id,
            );
            const video = yield* frames(session, 12);
            return { snapshot, playing, video, closed: yield* session.close };
          }).pipe(Effect.scoped, Effect.provide(clientLayer(twin.url)));
          const replaced = yield* eventually(
            first.current,
            (current) => current.status !== "ready",
          );
          return { acceptance, second, replaced, closed: yield* first.close };
        }),
      );
      const queued = result.second.snapshot.clips.find(
        (entry) => entry.clip.clip_id === result.acceptance.clip.clip_id,
      );
      expect(queued?.clip).toMatchObject({
        metadata: result.acceptance.clip.metadata,
        prompt,
        ready: true,
      });
      expect(result.second.playing._tag).toBe("Ready");
      expect(result.second.video.some(lit)).toBe(true);
      expect(changing(result.second.video)).toBe(true);
      // An attached session leaves termination to its owner.
      expect(result.second.closed.remote.attempted).toBe(false);
      expect(result.replaced.status).toBe("disconnected");
      expect(result.replaced.lastError?.reason._tag).toBe("ChannelClosed");
      expect(result.closed.remote).toMatchObject({ confirmed: true, evidence: "terminal" });
    }),
  30_000,
);

test(
  "another process takes the session over after its owner is killed",
  () =>
    withTwin(async (twin) => {
      const grant = await run(twin, mint(twin));
      const child = spawn(
        process.execPath,
        [fileURLToPath(import.meta.url), "--twin-owner", twin.url],
        {
          env: {
            ...process.env,
            TWIN_OWNER_GRANT: JSON.stringify({ ...grant, jwt: Redacted.value(grant.jwt) }),
          },
          stdio: ["ignore", "pipe", "inherit"],
        },
      );
      try {
        const { sessionId, playing, queued } = await streaming(child);
        child.kill("SIGKILL");
        const killedAt = performance.now();
        // The twin sees the owner stop polling: disconnected, while the session and its clip go on.
        await until(() => twin.sessions.get(sessionId)?.connected === false, 4000);
        expect(twin.sessions.get(sessionId)?.state).toBe("ACTIVE");
        const taken = await run(
          twin,
          Effect.gen(function* () {
            const client = yield* Reactor.Client;
            const session = yield* client.attachConnected({ sessionId, jwt: grant.jwt });
            const attachMs = performance.now() - killedAt;
            const provider = yield* H3.make(session);
            const snapshot = yield* provider.current;
            const fresh = yield* frames(session, 12);
            return { attachMs, snapshot, fresh, closed: yield* session.close };
          }),
        );
        expect(taken.attachMs).toBeLessThan(5000);
        // H3 lists no playing clip in its queues: state names it, and queued clips carry metadata.
        expect(taken.snapshot._tag === "Ready" && taken.snapshot.state.playing_clip_id).toBe(
          playing,
        );
        const clips = taken.snapshot.clips.map((entry) => entry.clip.clip_id);
        expect(clips).toContain(queued);
        expect(clips).not.toContain(playing);
        expect(taken.fresh.every(lit)).toBe(true);
        expect(changing(taken.fresh)).toBe(true);
        expect(taken.closed.remote.attempted).toBe(false);
        // The durable owner record terminates it with the persisted token.
        const termination = await run(
          twin,
          coordinator(twin, grant.jwt).pipe(
            Effect.flatMap((client) => client.terminate(sessionId)),
          ),
        );
        expect(termination).toMatchObject({ confirmed: true, evidence: "terminal" });
      } finally {
        child.kill("SIGKILL");
      }
    }),
  60_000,
);

/** Start the owner process and kill it once it streams; the twin keeps its session going. */
const killedOwner = async (twin: Twin, grant: Reactor.Coordinator.TokenGrant) => {
  const child = spawn(
    process.execPath,
    [fileURLToPath(import.meta.url), "--twin-owner", twin.url],
    {
      env: {
        ...process.env,
        TWIN_OWNER_GRANT: JSON.stringify({ ...grant, jwt: Redacted.value(grant.jwt) }),
      },
      stdio: ["ignore", "pipe", "inherit"],
    },
  );
  try {
    const owned = await streaming(child);
    child.kill("SIGKILL");
    await until(() => twin.sessions.get(owned.sessionId)?.connected === false, 4000);
    return owned;
  } finally {
    child.kill("SIGKILL");
  }
};

test(
  "a killed owner's session resumes through Orchestration, which only reads it and terminates it on close",
  () =>
    withTwin(async (twin) => {
      const grant = await run(twin, mint(twin));
      const { sessionId, playing, queued, endsAt } = await killedOwner(twin, grant);
      expect(twin.sessions.get(sessionId)?.state).toBe("ACTIVE");
      const enqueued = twin.enqueues;
      const allocation: Orchestration.Allocation = {
        sessionId,
        ownership: "owned",
        model: H3.modelName,
        expiresAt: grant.expiresAt,
        endsAt,
      };
      const resumed = await run(
        twin,
        Effect.gen(function* () {
          let opens = 0;
          const handle = yield* Orchestration.make({
            lead: "1 second",
            // This check has no replacement: only the recorded session resumes.
            open: Effect.gen(function* () {
              if (opens++ > 0)
                return yield* Reactor.ReactorError.fromCode("InvalidState", "no replacement");
              return yield* Orchestration.resumeH3({ allocation, jwt: grant.jwt });
            }),
          });
          const state = yield* eventually(
            handle.engine.state,
            (current) => current.availability === "Ready" && current.playing._tag === "Some",
          );
          const fresh = yield* handle.media.video.pipe(
            Stream.take(12),
            Stream.runCollect,
            Effect.timeout("5 seconds"),
          );
          return { state, fresh, cleanup: yield* handle.close };
        }),
      );
      const playingNow = resumed.state.playing;
      expect(playingNow._tag === "Some" ? String(playingNow.value.clipId) : undefined).toBe(
        playing,
      );
      const listed = [...resumed.state.queued, ...resumed.state.ready].find(
        (clip) => clip.clipId === queued,
      );
      expect(listed?.provider.prompt).toBe(`${prompt} The glass tips over.`);
      expect(resumed.fresh.every(lit)).toBe(true);
      expect(changing(resumed.fresh)).toBe(true);
      expect(twin.enqueues).toBe(enqueued);
      // The adopted session is owned: the orchestration's close terminated it.
      expect(resumed.cleanup.sessions).toHaveLength(1);
      expect(resumed.cleanup.sessions[0]!.lease).toMatchObject({
        ownership: "owned",
        sessionId,
        remote: { attempted: true, confirmed: true, evidence: "terminal", state: "CLOSED" },
      });
      expect(twin.sessions.get(sessionId)?.state).toBe("CLOSED");
    }),
  60_000,
);

test("overgrant: a token for two sessions is refused before any allocation", () =>
  withTwin(
    async (twin) => {
      const error = await run(twin, Effect.flip(mint(twin)));
      expect(error.reason._tag).toBe("Protocol");
      expect(error.message).toContain("session grant");
      expect(twin.sessionsCreated).toBe(0);
    },
    { faults: { overgrant: true } },
  ));

test(
  "slowDelete: the close cannot confirm, and a later read sees the session end",
  () =>
    withTwin(
      async (twin) => {
        const grant = await run(twin, mint(twin));
        const { closed, sessionId } = await run(
          twin,
          Effect.gen(function* () {
            const { session } = yield* open(grant);
            return { closed: yield* session.close, sessionId: session.id };
          }),
        );
        expect(closed.remote).toMatchObject({
          attempted: true,
          deleteStatus: 202,
          confirmed: false,
          evidence: null,
          state: "STOPPING",
        });
        expect(
          await until(() => twin.sessions.get(sessionId)?.state === "CLOSED", 4000),
        ).toBeGreaterThan(1000);
        const inspection = await run(
          twin,
          coordinator(twin, grant.jwt).pipe(Effect.flatMap((client) => client.inspect(sessionId))),
        );
        expect(inspection.state).toBe("CLOSED");
      },
      { faults: { slowDelete: true } },
    ),
  20_000,
);

test(
  "ignoreDelete: the session runs on until the cap ends it",
  () =>
    withTwin(
      async (twin) => {
        const created = performance.now();
        const { closed, sessionId } = await run(
          twin,
          Effect.gen(function* () {
            const { session } = yield* open(yield* mint(twin));
            return { closed: yield* session.close, sessionId: session.id };
          }),
        );
        expect(closed.remote).toMatchObject({
          deleteStatus: 202,
          confirmed: false,
          state: "ACTIVE",
        });
        expect(twin.sessions.get(sessionId)?.state).toBe("ACTIVE");
        await until(() => twin.sessions.get(sessionId)?.state === "CLOSED", 5000);
        expect(performance.now() - created).toBeGreaterThan(1500);
      },
      { capSeconds: 2, faults: { ignoreDelete: true } },
    ),
  20_000,
);

test(
  "the cap ends a connected session on its own, and its connection closes",
  () =>
    withTwin(
      async (twin) => {
        const result = await run(
          twin,
          Effect.gen(function* () {
            const { session } = yield* open(yield* mint(twin));
            const ended = yield* eventually(
              session.current,
              (current) => current.status !== "ready",
            );
            return { ended, closed: yield* session.close, sessionId: session.id };
          }),
        );
        expect(result.ended.lastError?.reason._tag).toBe("ChannelClosed");
        expect(twin.sessions.get(result.sessionId)?.state).toBe("CLOSED");
        expect(result.closed.remote).toMatchObject({ confirmed: true, evidence: "terminal" });
      },
      { capSeconds: 2 },
    ),
  20_000,
);

test(
  "dropEnqueueReply: the enqueue arrives, is never answered, and its outcome is unknown",
  () =>
    withTwin(
      async (twin) => {
        const result = await run(
          twin,
          Effect.gen(function* () {
            const { provider } = yield* open(yield* mint(twin), {
              provider: { replyTimeout: "500 millis", reconcileWindow: "250 millis" },
            });
            const submission = yield* provider.prepare({ prompt, seconds: 5 });
            const failure = yield* Effect.flip(submission.submit);
            return { failure, queue: (yield* provider.getQueue).value };
          }),
        );
        expect(result.failure.context.outcome).toBe("unknown");
        expect(twin.enqueues).toBe(1);
        expect(result.queue.generation).toHaveLength(0);
      },
      { faults: { dropEnqueueReply: true } },
    ),
  20_000,
);

test(
  "blackFrames: a playing clip's frames are all black",
  () =>
    withTwin(
      async (twin) => {
        const video = await run(
          twin,
          Effect.gen(function* () {
            const { session, provider } = yield* open(yield* mint(twin));
            yield* play(provider);
            return yield* frames(session, 12);
          }),
        );
        expect(video).toHaveLength(12);
        expect(video.some(lit)).toBe(false);
      },
      { faults: { blackFrames: true } },
    ),
  20_000,
);

test(
  "frozenFrames: a playing clip's frames never change",
  () =>
    withTwin(
      async (twin) => {
        const video = await run(
          twin,
          Effect.gen(function* () {
            const { session, provider } = yield* open(yield* mint(twin));
            yield* play(provider);
            return yield* frames(session, 12);
          }),
        );
        expect(video.every(lit)).toBe(true);
        expect(changing(video)).toBe(false);
      },
      { faults: { frozenFrames: true } },
    ),
  20_000,
);

test(
  "noAudio: the session offers audio and sends none",
  () =>
    withTwin(
      async (twin) => {
        const result = await run(
          twin,
          Effect.gen(function* () {
            const { session, provider } = yield* open(yield* mint(twin));
            yield* play(provider);
            const media = yield* mediaGeneration(session);
            const audio = yield* media
              .audio("main_audio")
              .pipe(Stream.take(1), Stream.runCollect, Effect.timeout("1 second"), Effect.result);
            return { tracks: media.tracks, audio, video: yield* frames(session, 6) };
          }),
        );
        expect(result.tracks).toContainEqual({
          name: "main_audio",
          kind: "audio",
          direction: "recvonly",
        });
        expect(result.audio._tag).toBe("Failure");
        expect(result.video).toHaveLength(6);
      },
      { faults: { noAudio: true } },
    ),
  20_000,
);

test("a wrong API key, a missing or foreign token and a spent grant are refused", () =>
  withTwin((other) =>
    withTwin(async (twin) => {
      const result = await run(
        twin,
        Effect.gen(function* () {
          const wrongKey = yield* Effect.flip(mint(twin, "twin-api-key-wrong"));
          const grant = yield* mint(twin);
          const foreign = yield* mint(other);
          const client = yield* Reactor.Client;
          const session = yield* client.create({ model: H3.modelName, jwt: grant.jwt });
          const inspect = (jwt?: Redacted.Redacted<string>) =>
            coordinator(twin, jwt).pipe(Effect.flatMap((each) => each.inspect(session.id)));
          return {
            wrongKey,
            missing: yield* Effect.flip(inspect()),
            foreignRead: yield* Effect.flip(inspect(foreign.jwt)),
            foreignCreate: yield* Effect.flip(
              client.create({ model: H3.modelName, jwt: foreign.jwt }),
            ),
            spent: yield* Effect.flip(client.create({ model: H3.modelName, jwt: grant.jwt })),
            inspected: yield* inspect(grant.jwt),
          };
        }),
      );
      expect(status(result.wrongKey)).toBe(401);
      expect(status(result.missing)).toBe(401);
      expect(status(result.foreignRead)).toBe(401);
      expect(status(result.foreignCreate)).toBe(401);
      // The token granted exactly one session.
      expect(status(result.spent)).toBe(403);
      expect(result.inspected).toMatchObject({ state: "ACTIVE", hasCapabilities: true });
      expect(twin.sessionsCreated).toBe(1);
    }),
  ));
