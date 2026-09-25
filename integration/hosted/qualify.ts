/**
 * The hosted qualification: paid checks against hosted Reactor, run only by a
 * maintainer who authorizes the spend, and a free rehearsal of the same checks
 * against a local twin. README.md says what each check costs and gathers, why,
 * and how the evidence is shared.
 *
 *   bun integration/hosted/qualify.ts preflight --total-budget-usd=1.50 --ledger=<dir>
 *   bun integration/hosted/qualify.ts rehearse <check> [--faults=<a,b>] [--ledger=<dir>]
 *   bun integration/hosted/qualify.ts <vertical|takeover|turn|audio|resume> --budget-usd=0.75 \
 *     --total-budget-usd=1.50 --ledger=<dir> --network="<where>" --i-authorize-paid-sessions
 *   bun integration/hosted/qualify.ts scheduler --budget-usd=1.50 \
 *     --total-budget-usd=3.75 --ledger=<dir> --network="<where>" --i-authorize-paid-sessions
 *   bun integration/hosted/qualify.ts summarize <evidence file or ledger>...
 *
 * `REACTOR_API_KEY` mints one token per session, each capped at 50 s
 * server-side; `scheduler` uses two. `REACTOR_API_URL` overrides the coordinator. A paid check
 * refuses before allocating unless the published rate fits its budget, the
 * ledger's earlier runs leave room for its worst case, and the returned token
 * grants no more than it asked for. It saves its evidence (never a token) at
 * every milestone, stops at the first unknown outcome or unconfirmed
 * termination, and never repeats by itself.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { release, tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { deflateSync } from "node:zlib";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import type { Mutable } from "effect/Types";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import type { MediaGeneration } from "reactor-effect-client/host";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Testing from "reactor-effect-client/testing";
import * as Native from "reactor-effect-native";
import {
  AudioReader,
  ContractTally,
  VideoReader,
  readInto,
  sampleStats,
  since,
  spanRecorder,
  tallyReply,
  terminationTrail,
} from "./collect.js";
import type { Pressure, StatsSample } from "./evidence.js";
import { Writer, conclude, format, lockLedger, readLedger, reservedUsd } from "./evidence.js";
import type { Draft, LedgerEntry } from "./evidence.js";
import {
  Refused,
  acceptGrant,
  admit,
  admitRelayCheck,
  admitTotal,
  authorize,
  billedUsd,
  checks,
  liveClipVideo,
  liveVideo,
  maxCheckUsd,
  maxSchedulerUsd,
  maxTotalUsd,
  options,
  sessionSeconds,
  sessionsFor,
  tokenSeconds,
  workSeconds,
  worstCaseUsd,
} from "./gates.js";
import type { Check } from "./gates.js";
import { summarize } from "./report.js";

const script = fileURLToPath(import.meta.url);
const prompt = "A slow camera move across a sunlit table with a glass of water.";
const tracks = H3.h3ReferenceTurboRealtime.tracks;

/** A solid mid-gray PNG: an image reference that cannot make the clip black. */
const grayPng = (width: number, height: number): Uint8Array => {
  const crcTable = Array.from({ length: 256 }, (_, n) => {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    return c >>> 0;
  });
  const chunk = (type: string, data: Uint8Array) => {
    const out = new Uint8Array(12 + data.length);
    const view = new DataView(out.buffer);
    view.setUint32(0, data.length);
    out.set(new TextEncoder().encode(type), 4);
    out.set(data, 8);
    let c = 0xffffffff;
    for (const b of out.subarray(4, 8 + data.length)) c = crcTable[(c ^ b) & 0xff]! ^ (c >>> 8);
    view.setUint32(8 + data.length, (c ^ 0xffffffff) >>> 0);
    return out;
  };
  const header = new Uint8Array(13);
  const view = new DataView(header.buffer);
  view.setUint32(0, width);
  view.setUint32(4, height);
  header.set([8, 2, 0, 0, 0], 8); // 8-bit RGB
  const rows = new Uint8Array(height * (1 + width * 3)).fill(128);
  for (let row = 0; row < height; row++) rows[row * (1 + width * 3)] = 0; // filter: none
  const parts = [
    Uint8Array.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", header),
    chunk("IDAT", new Uint8Array(deflateSync(rows))),
    chunk("IEND", new Uint8Array()),
  ];
  const png = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    png.set(part, offset);
    offset += part.length;
  }
  return png;
};

/**
 * What the `audio` check submits: one image, because H3 takes audio only with
 * an image or a continuation, and one 3 s tone as the audio reference.
 */
const audioRequest = (marker: string): H3.Request => ({
  prompt: `Picture 1 is a plain gray backdrop. Audio 1 is a low, steady hum under the scene. ${prompt}`,
  seconds: 5,
  metadata: marker,
  references: [{ _tag: "Bytes", bytes: grayPng(256, 144) }],
  audio: [{ _tag: "Bytes", bytes: Testing.wavBytes(3, { sampleRate: 48_000, frequency: 220 }) }],
});

/** Where a check runs: hosted Reactor for money, or the local twin for free. */
interface Target {
  readonly mode: "paid" | "rehearsal";
  readonly apiUrl: string;
  readonly apiKey: Redacted.Redacted<string>;
  readonly peers: Layer.Layer<Reactor.PeerFactory, Reactor.ReactorError>;
  readonly network: string;
  /** What points a child owner process at the same target. */
  readonly ownerArgs: readonly string[];
  /**
   * How long a check reads media once its clip started, and how long the
   * attacher reads fresh frames: long enough on hosted Reactor to measure
   * pacing, short against the twin, whose pacing is synthetic.
   */
  readonly windowMs: number;
}

interface Budget {
  readonly checkUsd: number;
  readonly totalUsd: number;
  readonly reservedUsd: number;
}

/** One run: its evidence, the spans it records, and the secrets its evidence must never hold. */
interface Run {
  readonly origin: number;
  readonly evidence: Draft;
  readonly spans: ReturnType<typeof spanRecorder>;
  readonly secrets: string[];
  readonly writer: Writer;
}

const save = (run: Run): void => {
  run.evidence.spans = run.spans.records();
  run.writer.save(run.evidence);
};

const mark = (run: Run, step: string, detail?: string) =>
  Effect.sync(() => {
    run.evidence.milestones.push({
      atMs: since(run.origin),
      step,
      ...(detail === undefined ? {} : { detail }),
    });
    save(run);
  });

const judge = (run: Run, name: string, failure: string | undefined): void => {
  run.evidence.criteria.push({
    name,
    passed: failure === undefined,
    ...(failure === undefined ? {} : { detail: failure }),
  });
};

const refusal = (cause: unknown): Refused =>
  cause instanceof Refused ? cause : new Refused({ message: String(cause) });
const gate = <A>(evaluate: () => A) => Effect.try({ try: evaluate, catch: refusal });

const outcomeOf = (error: unknown) =>
  Reactor.isReactorFailure(error) ? error.context.outcome : undefined;

/** A command whose remote outcome, when it fails, the stop rules must see. */
const recorded = <A, E, R>(run: Run, command: Effect.Effect<A, E, R>) =>
  command.pipe(
    Effect.tapError((error) =>
      Effect.sync(() => {
        const outcome = outcomeOf(error);
        if (outcome !== undefined) run.evidence.outcomes.push(outcome);
      }),
    ),
  );

/** A failure as the evidence states it: the library's own message, never provider text. */
const failureText = (cause: unknown): string => {
  if (Cause.isCause(cause))
    return Cause.hasInterruptsOnly(cause)
      ? "the check was interrupted"
      : failureText(Cause.squash(cause));
  if (cause instanceof Refused) return cause.message;
  if (Cause.isTimeoutError(cause)) return "a step ran past its deadline";
  if (Reactor.isReactorFailure(cause))
    return `${cause.reason._tag}: ${cause.message}${cause.context.outcome === undefined ? "" : ` (outcome ${cause.context.outcome})`}`;
  return `unexpected: ${String(cause).slice(0, 300)}`;
};

const round = (value: number, places = 4) => Math.round(value * 10 ** places) / 10 ** places;

const clientLayer = (target: Target) =>
  Reactor.layer({ apiUrl: target.apiUrl }).pipe(
    Layer.provideMerge(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, target.peers)),
  );

/** The version of `name` as it resolves from here, read from its own manifest. */
const packageDirectory = (name: string): string | undefined => {
  try {
    let directory = dirname(fileURLToPath(import.meta.resolve(name)));
    for (;;) {
      const manifest = join(directory, "package.json");
      if (
        existsSync(manifest) &&
        (JSON.parse(readFileSync(manifest, "utf8")) as { name?: string }).name === name
      )
        return directory;
      const parent = dirname(directory);
      if (parent === directory) return undefined;
      directory = parent;
    }
  } catch {
    return undefined;
  }
};

const environment = (target: Target): Draft["environment"] => {
  const packages: Record<string, string> = {};
  for (const name of [
    "reactor-effect-client",
    "reactor-effect-native",
    "effect",
    "@effect/platform-node",
  ]) {
    const directory = packageDirectory(name);
    if (directory !== undefined)
      packages[name] = (
        JSON.parse(readFileSync(join(directory, "package.json"), "utf8")) as { version: string }
      ).version;
  }
  const git = (...args: string[]) =>
    spawnSync("git", args, { cwd: dirname(script), encoding: "utf8", timeout: 5_000 });
  const head = git("rev-parse", "HEAD");
  const status = git("status", "--porcelain", "--untracked-files=no");
  const nativeDirectory = packageDirectory("reactor-effect-native");
  const identityFile =
    nativeDirectory === undefined
      ? undefined
      : join(nativeDirectory, "lib", `${process.platform}-${process.arch}`, "native-identity.json");
  const identity =
    target.mode === "paid" && identityFile !== undefined && existsSync(identityFile)
      ? (JSON.parse(readFileSync(identityFile, "utf8")) as {
          platform: string;
          library: string;
          sha256: string;
          build: { abiVersion: number; sourceSha256: string; webrtcPrebuilt: string };
        })
      : undefined;
  return {
    runtime: typeof Bun === "undefined" ? `node ${process.version}` : `bun ${Bun.version}`,
    os: `${process.platform} ${process.arch} ${release()}`,
    ...(head.status === 0 ? { commit: head.stdout.trim() } : {}),
    ...(status.status === 0 ? { dirty: status.stdout.trim().length > 0 } : {}),
    packages,
    ...(identity === undefined
      ? {}
      : {
          native: {
            platform: identity.platform,
            library: identity.library,
            sha256: identity.sha256,
            abiVersion: identity.build.abiVersion,
            sourceSha256: identity.build.sourceSha256,
            webrtcPrebuilt: identity.build.webrtcPrebuilt,
          },
        }),
    network: target.network,
    apiOrigin: new URL(target.apiUrl).origin,
  };
};

/**
 * The spending gates, in order: the published rate must fit this check's
 * budget, and its worst case what the ledger has left. The evidence records
 * that worst case before any token exists, so the ledger reserves it even if
 * this process dies next. Then the first token is minted and must grant no more
 * than asked for; a two-session check reserves both before that point.
 */
const admitted = (target: Target, run: Run, budget: Budget, sessionCount = 1) =>
  Effect.gen(function* () {
    const coordinator = yield* Reactor.Coordinator.make({ apiUrl: target.apiUrl });
    const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
    run.evidence.budget.rate = rate;
    const worst = yield* gate(() => admit(rate, budget.checkUsd, sessionCount));
    yield* gate(() => admitTotal([budget.reservedUsd], worst, budget.totalUsd));
    run.evidence.budget.worstCaseUsd = round(worst);
    yield* mark(run, "admitted", `worst case $${worst.toFixed(4)}`);
    const grant = yield* coordinator.mintToken({
      apiKey: target.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
      expiresAfter: `${tokenSeconds} seconds`,
    });
    run.secrets.push(Redacted.value(grant.jwt));
    yield* gate(() => acceptGrant(grant.granted));
    run.evidence.grant = { ...grant.granted, expiresAt: grant.expiresAt };
    yield* mark(run, "minted");
    return { grant, rate };
  });

/** The time left until `deadline`, a Clock time in milliseconds, never negative. */
const until = (deadline: number) =>
  Clock.currentTimeMillis.pipe(Effect.map((now) => Duration.millis(Math.max(0, deadline - now))));

/**
 * After the session: follow the coordinator's view of it until it is terminal
 * or gone, bounded by the token's validity, and price what it used.
 */
const afterSession = (
  target: Target,
  run: Run,
  jwt: Redacted.Redacted<string>,
  rate: { readonly creditsPerSecond: number; readonly creditsPerDollar: number },
) =>
  Effect.gen(function* () {
    const session = run.evidence.session;
    if (session === undefined) return;
    const coordinator = yield* Reactor.Coordinator.make({
      apiUrl: target.apiUrl,
      credential: Effect.succeed(jwt),
    });
    const expires = (run.evidence.grant?.expiresAt ?? 0) * 1000 - 5_000;
    const { trail, terminalMs } = yield* terminationTrail(
      coordinator,
      session.id,
      run.origin,
      Math.min(expires, (yield* Clock.currentTimeMillis) + 20_000),
    );
    const termination = run.evidence.termination;
    if (termination !== undefined)
      run.evidence.termination = {
        ...termination,
        trail,
        ...(terminalMs === undefined ? {} : { terminalMs }),
      };
    // Reactor bills from `ready` until the session ends, by the minute. The
    // estimate counts from allocation, which precedes ready, to the library's
    // confirmed report or else the first terminal read, which follow the end,
    // so it bills no less than Reactor does.
    const endedMs = termination?.confirmed === true ? termination.reportedMs : terminalMs;
    if (endedMs !== undefined) {
      run.evidence.session = { ...session, endedMs };
      run.evidence.budget.estimatedUsd = round(
        billedUsd(rate, (endedMs - session.allocatedMs) / 1000) +
          (run.evidence.scheduler?.replacement.estimatedUsd ?? 0),
      );
    }
    judge(
      run,
      "confirmed termination",
      termination === undefined
        ? "termination was never requested"
        : termination.confirmed
          ? undefined
          : `the library could not confirm the end${terminalMs === undefined ? "" : `; the coordinator reported it terminal ${Math.round(terminalMs - termination.requestedMs)} ms after the request`}`,
    );
    yield* mark(run, "trail", trail.map((entry) => entry.state).join(" > "));
  }).pipe(Effect.ignore);

/** The session's close, recorded as the library reports it. */
const closeSession = (run: Run, session: Reactor.Session) =>
  Effect.gen(function* () {
    const requestedMs = since(run.origin);
    const report = yield* session.close;
    run.evidence.termination = {
      requestedMs,
      reportedMs: since(run.origin),
      confirmed: report.remote.confirmed,
      close: report,
      trail: [],
    };
    yield* mark(run, "closed", report.remote.confirmed ? "confirmed" : "unconfirmed");
  });

/**
 * One paid session through the public opener, observed end to end. The `turn`
 * check also requires a relay pair; the `audio` check submits a reference
 * image and a reference audio clip, and requires the clip to report the audio.
 */
const vertical = (target: Target, run: Run, budget: Budget, check: "vertical" | "turn" | "audio") =>
  Effect.gen(function* () {
    const relay = check === "turn";
    const { grant, rate } = yield* admitted(target, run, budget);
    const marker = `hosted-qualification:${run.evidence.runId}`;
    const tally = new ContractTally();
    const video = new VideoReader();
    const audio = new AudioReader();
    const samples: StatsSample[] = [];
    const lifecycle: Mutable<NonNullable<Draft["lifecycle"]>> = {};
    let media: MediaGeneration | undefined;
    let pressure: Pressure | undefined;
    let deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
    // What the collectors saw, written into the evidence however the check ends.
    const collected = Effect.gen(function* () {
      if (media !== undefined)
        pressure = yield* media.snapshot.pipe(
          Effect.map((snapshot) => ({
            deliveredVideo: String(snapshot.deliveredVideo),
            deliveredAudio: String(snapshot.deliveredAudio),
            droppedVideo: String(snapshot.droppedVideo),
            droppedAudio: String(snapshot.droppedAudio),
            readerOverflows: String(snapshot.readerOverflows),
          })),
          Effect.orElseSucceed(() => pressure),
        );
      if (run.evidence.contract !== undefined)
        run.evidence.contract = {
          ...run.evidence.contract,
          duplicates: tally.duplicates,
          stale: tally.stale,
          ...(tally.lastState === undefined ? {} : { lastState: tally.lastState }),
        };
      if (media !== undefined) {
        const audioOffered = media.tracks.some(
          (track) => track.kind === "audio" && track.direction === "recvonly",
        );
        const firstClipFrameMs =
          lifecycle.started === undefined ? undefined : video.firstAfter(lifecycle.started.atMs);
        run.evidence.media = {
          audioOffered,
          ...(firstClipFrameMs === undefined ? {} : { firstClipFrameMs }),
          video: video.summary(),
          ...(audioOffered ? { audio: audio.summary() } : {}),
          ...(pressure === undefined ? {} : { pressure }),
        };
      }
      // The pair that carried the media: the last sample that named its local
      // candidate while it received. The native host names no remote
      // candidate, because reactor-webrtc reports only the local one, which is
      // what says whether this side went through TURN.
      const paired = samples.findLast(
        (sample) => sample.local !== undefined && (sample.receivedKbps ?? 0) > 0,
      );
      run.evidence.network = {
        samples,
        ...(paired === undefined
          ? {}
          : { pair: { local: paired.local ?? null, remote: paired.remote ?? null } }),
      };
      save(run);
    });
    const body = Effect.scoped(
      Effect.gen(function* () {
        let allocated: Reactor.Session | undefined;
        const opened = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          onAllocated: ({ session }) =>
            Effect.gen(function* () {
              allocated = session;
              deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
              run.evidence.session = { id: session.id, allocatedMs: since(run.origin), tracks: [] };
              yield* mark(run, "allocated");
            }),
        });
        const session = allocated!;
        // Finalizers run last-added first: the collectors' snapshot, then the close.
        yield* Effect.addFinalizer(() => closeSession(run, session).pipe(Effect.ignore));
        yield* Effect.addFinalizer(() => collected.pipe(Effect.ignore));
        yield* mark(run, "connected");
        const provider = opened.source.provider;
        // How the data channel answers the commands that follow, before the first is sent.
        const replies: Record<string, number> = {};
        yield* session.observe({ capacity: 4096 }).pipe(
          Effect.flatMap((observation) =>
            observation.events.pipe(
              Stream.runForEach((event) => Effect.sync(() => tallyReply(replies, event))),
              Effect.ignore,
              Effect.forkScoped,
            ),
          ),
          Effect.ignore,
        );
        yield* sampleStats(session, run.origin, samples).pipe(Effect.forkScoped);
        yield* provider.events({ capacity: 4096 }).pipe(
          Stream.runForEach((event) => Effect.sync(() => tally.add(event))),
          Effect.catch((error) => Effect.sync(() => tally.failed(error))),
          Effect.forkScoped,
        );
        const contract = provider.contract;
        run.evidence.contract = {
          modelName: contract.modelName,
          documentedVersion: contract.documentedVersion,
          deploymentTitle: contract.deployment.title,
          deploymentVersion: contract.deployment.version,
          messages: tally.messages,
          unknown: tally.unknown,
          duplicates: 0,
          stale: 0,
          diagnostics: tally.diagnostics,
          referenceAudio: contract.referenceAudio,
        };
        const inspector = yield* Reactor.Coordinator.make({
          apiUrl: target.apiUrl,
          credential: Effect.succeed(grant.jwt),
        });
        const inspection = yield* inspector.inspect(session.id);
        run.evidence.server = {
          cluster: inspection.cluster,
          zone: inspection.zone,
          serverVersion: inspection.serverVersion,
          transport:
            inspection.selectedTransport === null
              ? null
              : `${inspection.selectedTransport.protocol}/${inspection.selectedTransport.version}`,
          additional: inspection.additional,
        };
        const generation = yield* Native.media(session);
        media = generation;
        const audioOffered = generation.tracks.some(
          (track) => track.kind === "audio" && track.direction === "recvonly",
        );
        run.evidence.session = {
          ...run.evidence.session!,
          tracks: generation.tracks.map(({ name, kind, direction }) => ({ name, kind, direction })),
        };
        yield* readInto(Reactor.recorder(generation.video(tracks.video)), video, run.origin).pipe(
          Effect.ignore,
          Effect.forkScoped,
        );
        if (audioOffered)
          yield* readInto(Reactor.recorder(generation.audio(tracks.audio)), audio, run.origin).pipe(
            Effect.ignore,
            Effect.forkScoped,
          );
        // H3 holds a generated clip until it is played, unless autoplay is on.
        yield* recorded(run, provider.setAutoplay(true));
        const request: H3.Request =
          check === "audio" ? audioRequest(marker) : { prompt, seconds: 5, metadata: marker };
        const submission = yield* provider.prepare(request);
        const submitMs = since(run.origin);
        const acceptance = yield* recorded(run, submission.submit);
        run.evidence.outcomes.push("replied");
        tally.watch(acceptance.clip.clip_id, marker);
        const clip = acceptance.clip;
        run.evidence.acceptance = {
          clipId: clip.clip_id,
          evidence: acceptance.evidence.kind,
          transportGeneration: String(acceptance.evidence.source.generation),
          submitMs,
          acceptedMs: since(run.origin),
          metadataEchoes: tally.echoes,
          ...(check === "audio"
            ? {
                references: {
                  images: request.references?.length ?? 0,
                  audio: request.audio?.length ?? 0,
                  reportedImages: clip.reference_image_count ?? null,
                  reportedAudio: clip.reference_audio_count ?? null,
                  hasReferenceAudio: clip.has_reference_audio ?? null,
                },
              }
            : {}),
        };
        run.evidence.lifecycle = lifecycle;
        const proof = acceptance.evidence.source;
        yield* mark(
          run,
          "accepted",
          `by ${proof.kind === "ack" ? "ack" : proof.type} ${proof.correlation}`,
        );
        const operation = yield* provider.operation(submission);
        const reached = (phase: "generated" | "started" | "ended") => (fact: H3.ClipFact) =>
          Effect.sync(() => {
            lifecycle[phase] ??= {
              atMs: since(run.origin),
              message: fact.message,
              transportGeneration: String(fact.transportGeneration),
            };
          });
        yield* operation
          .reached("generated")
          .pipe(Effect.flatMap(reached("generated")), Effect.ignore, Effect.forkScoped);
        yield* operation.ended.pipe(
          Effect.flatMap(reached("ended")),
          Effect.ignore,
          Effect.forkScoped,
        );
        yield* operation
          .reached("started")
          .pipe(Effect.flatMap(reached("started")), Effect.timeout(yield* until(deadline)));
        yield* mark(run, "clip started");
        yield* Effect.sleep(Duration.min(Duration.millis(target.windowMs), yield* until(deadline)));
        yield* collected;
        yield* mark(
          run,
          "observed",
          `replies ${Object.entries(replies)
            .map(([reply, count]) => `${reply} ${count}`)
            .join(", ")}`,
        );
        judge(
          run,
          "correlated acceptance",
          acceptance.evidence.kind === "correlated"
            ? undefined
            : `acceptance was only by ${acceptance.evidence.kind}`,
        );
        judge(
          run,
          "lifecycle progression",
          lifecycle.generated !== undefined && lifecycle.started !== undefined
            ? undefined
            : "generated and started were not both observed",
        );
        // Started is a provider fact: idle frames can still arrive before the
        // media update. Require continued motion at the end of this window.
        judge(run, "live video", liveClipVideo(video.seenSince(lifecycle.started!.atMs)));
        judge(
          run,
          "audio when offered",
          !audioOffered || (audio.count > 0 && audio.summary().peakRms > 0)
            ? undefined
            : "the session offered audio and no sound arrived",
        );
        judge(
          run,
          "metadata preserved",
          Object.keys(tally.echoes).some((type) => type !== "clip_queued")
            ? undefined
            : "no provider message after the reply listed the clip with its metadata",
        );
        if (check === "audio") {
          const references = run.evidence.acceptance.references!;
          judge(
            run,
            "reference audio reported",
            references.hasReferenceAudio === true && references.reportedAudio === references.audio
              ? undefined
              : `the accepted clip reports has_reference_audio ${references.hasReferenceAudio ?? "absent"} and ${references.reportedAudio ?? "no"} audio reference(s) for ${references.audio} sent`,
          );
        }
        const pair = run.evidence.network?.pair;
        judge(
          run,
          relay ? "relay pair selected" : "ICE pair selected",
          pair === undefined
            ? "no stats sample named a pair that was receiving"
            : relay && pair.local !== "relay" && pair.remote !== "relay"
              ? `the pair carrying the media was ${pair.local ?? "?"}${pair.remote === null ? "" : ` to ${pair.remote}`}`
              : undefined,
        );
      }),
    ).pipe(
      Effect.provide(clientLayer(target)),
      Effect.tapError((error) =>
        Effect.sync(() => {
          // A failed open already closed its session; its report is the evidence.
          if (Reactor.AcquisitionFailure.is(error) && run.evidence.session !== undefined) {
            const at = since(run.origin);
            run.evidence.termination = {
              requestedMs: at,
              reportedMs: at,
              confirmed: error.cleanup.remote.confirmed,
              close: error.cleanup,
              trail: [],
            };
          }
        }),
      ),
    );
    return yield* body.pipe(Effect.ensuring(afterSession(target, run, grant.jwt, rate)));
  });

/** Two bounded H3 sessions that measure the provider facts scheduler policy depends on. */
const scheduler = (target: Target, run: Run, budget: Budget) =>
  Effect.gen(function* () {
    // Reserve both server-capped sessions before either token is minted.
    const { grant, rate } = yield* admitted(target, run, budget, sessionsFor("scheduler"));
    run.evidence.scheduler = {
      replacement: {},
      builds: [],
      latencyByRequestedSeconds: [],
      metadata: { observed: {}, mismatched: {} },
    };
    const coordinator = yield* Reactor.Coordinator.make({ apiUrl: target.apiUrl });
    const nextGrant = yield* coordinator.mintToken({
      apiKey: target.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
      expiresAfter: `${tokenSeconds} seconds`,
    });
    run.secrets.push(Redacted.value(nextGrant.jwt));
    yield* gate(() => acceptGrant(nextGrant.granted));
    run.evidence.scheduler = {
      ...run.evidence.scheduler,
      replacement: {
        grant: { ...nextGrant.granted, expiresAt: nextGrant.expiresAt },
      },
    };
    yield* mark(run, "replacement minted");

    const marker = `hosted-qualification:${run.evidence.runId}:scheduler`;
    const observed: { type: string; clipId: string; metadata: string; atMs: number }[] = [];
    const watch = (provider: H3.Provider) =>
      provider.events({ capacity: 4096 }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event._tag !== "Message" || event.disposition !== "applied") return;
            const message = event.message;
            if (message.type === "unknown" || !("clip" in message.data)) return;
            if (observed.length < 256)
              observed.push({
                type: message.type,
                clipId: message.data.clip.clip_id,
                metadata: message.data.clip.metadata,
                atMs: since(run.origin),
              });
          }),
        ),
        Effect.forkScoped,
      );
    const oldVideo = new VideoReader();
    const newVideo = new VideoReader();
    let deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
    const body = Effect.scoped(
      Effect.gen(function* () {
        let oldSession: Reactor.Session | undefined;
        const old = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          onAllocated: ({ session }) =>
            Effect.gen(function* () {
              oldSession = session;
              deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
              run.evidence.session = { id: session.id, allocatedMs: since(run.origin), tracks: [] };
              yield* mark(run, "allocated");
            }),
        });
        yield* Effect.addFinalizer(() => closeSession(run, oldSession!).pipe(Effect.ignore));
        yield* watch(old.source.provider);
        const oldMedia = yield* Native.media(oldSession!);
        run.evidence.session = {
          ...run.evidence.session!,
          tracks: oldMedia.tracks.map(({ name, kind, direction }) => ({ name, kind, direction })),
        };
        yield* readInto(Reactor.recorder(oldMedia.video(tracks.video)), oldVideo, run.origin).pipe(
          Effect.ignore,
          Effect.forkScoped,
        );
        const provider = old.source.provider;
        yield* recorded(run, provider.setAutoplay(false));
        const submit = (seconds: number, name: string, position?: number) =>
          Effect.gen(function* () {
            const submission = yield* provider.prepare({
              prompt,
              seconds,
              metadata: `${marker}:${name}`,
              ...(position === undefined ? {} : { position }),
            });
            const submittedMs = since(run.origin);
            const acceptance = yield* recorded(run, submission.submit);
            run.evidence.outcomes.push("replied");
            const operation = yield* provider.operation(submission);
            run.evidence.scheduler = {
              ...run.evidence.scheduler!,
              builds: [
                ...run.evidence.scheduler!.builds,
                {
                  clipId: acceptance.clip.clip_id,
                  requestedSeconds: seconds,
                  submittedMs,
                },
              ],
            };
            yield* mark(run, `${name} accepted`);
            return { acceptance, operation, submittedMs };
          });
        // The second request asks for position zero while the first is building.
        const first = yield* submit(5, "building");
        const next = yield* submit(15, "position-zero", 0);
        const queued = yield* recorded(run, provider.getQueue);
        run.evidence.outcomes.push("replied");
        run.evidence.scheduler = {
          ...run.evidence.scheduler!,
          positionZero: {
            buildingClipId: first.acceptance.clip.clip_id,
            requestedClipId: next.acceptance.clip.clip_id,
            generationOrder: queued.value.generation.map((clip) => clip.clip_id),
          },
        };
        judge(
          run,
          "position zero while building",
          queued.value.generation[0]?.clip_id === first.acceptance.clip.clip_id &&
            queued.value.generation[1]?.clip_id === next.acceptance.clip.clip_id
            ? undefined
            : "the building clip and position-zero request were not observed in that order",
        );
        for (const entry of [first, next]) {
          yield* entry.operation.reached("generated").pipe(Effect.timeout(yield* until(deadline)));
          const readyQueue = yield* recorded(run, provider.getQueue);
          run.evidence.outcomes.push("replied");
          const readyClip = readyQueue.value.playout.find(
            (clip) => clip.clip_id === entry.acceptance.clip.clip_id,
          );
          const readyMs = since(run.origin);
          run.evidence.scheduler = {
            ...run.evidence.scheduler,
            builds: run.evidence.scheduler.builds.map((build) =>
              build.clipId === entry.acceptance.clip.clip_id
                ? {
                    ...build,
                    readyMs,
                    ...(readyClip === undefined ? {} : { readySeconds: readyClip.seconds }),
                    submitToReadyMs: readyMs - entry.submittedMs,
                  }
                : build,
            ),
          };
          judge(
            run,
            `Ready duration ${entry.acceptance.clip.clip_id}`,
            readyClip === undefined
              ? "the generated clip was not listed in the Ready queue"
              : undefined,
          );
        }
        const builds = run.evidence.scheduler.builds.filter((build) => build.readyMs !== undefined);
        run.evidence.scheduler = {
          ...run.evidence.scheduler,
          latencyByRequestedSeconds: [
            ...new Set(builds.map((build) => build.requestedSeconds)),
          ].map((seconds) => {
            const values = builds
              .filter((build) => build.requestedSeconds === seconds)
              .map((build) => build.submitToReadyMs!)
              .sort((a, b) => a - b);
            return {
              requestedSeconds: seconds,
              count: values.length,
              p50Ms: values[Math.floor((values.length - 1) * 0.5)]!,
              p95Ms: values[Math.floor((values.length - 1) * 0.95)]!,
            };
          }),
        };
        // Move only after both clips are Ready. The reply records latency and queue.
        const moveStart = since(run.origin);
        const moved = yield* recorded(run, provider.move(next.acceptance.clip.clip_id, 0));
        run.evidence.outcomes.push("replied");
        const moveEnd = since(run.origin);
        run.evidence.scheduler = {
          ...run.evidence.scheduler,
          readyMove: {
            clipId: next.acceptance.clip.clip_id,
            replyMs: moveEnd,
            elapsedMs: moveEnd - moveStart,
            queue: moved.value.queue,
            position: moved.value.position,
          },
        };
        judge(
          run,
          "Ready move reply",
          moved.value.queue === "playout" && moved.value.position === 0
            ? undefined
            : "move did not place the Ready clip first in playout",
        );
        // Pop a queued build. A bounded observation can show later messages, not
        // prove that the provider saved compute or that no picture was rendered.
        const popped = yield* submit(10, "popped");
        const beforePop = yield* recorded(run, provider.getQueue);
        run.evidence.outcomes.push("replied");
        const wasGeneration =
          beforePop.value.generation[0]?.clip_id === popped.acceptance.clip.clip_id;
        yield* recorded(run, provider.pop(popped.acceptance.clip.clip_id));
        run.evidence.outcomes.push("replied");
        const poppedMs = since(run.origin);
        yield* Effect.sleep(
          Duration.min(
            Duration.millis(target.mode === "rehearsal" ? 700 : 2_000),
            yield* until(deadline),
          ),
        );
        const untilMs = since(run.origin);
        const poppedEvents = observed.filter(
          (event) => event.clipId === popped.acceptance.clip.clip_id && event.atMs > poppedMs,
        );
        run.evidence.scheduler = {
          ...run.evidence.scheduler,
          poppedBuild: {
            clipId: popped.acceptance.clip.clip_id,
            wasGeneration,
            poppedMs,
            observedUntilMs: untilMs,
            generatedAfterPop: poppedEvents.some((event) => event.type === "clip_generated"),
            startedAfterPop: poppedEvents.some((event) => event.type === "clip_started"),
          },
        };
        judge(
          run,
          "pop generation head",
          !wasGeneration
            ? "the clip was not at the head of the generation queue before pop"
            : poppedEvents.some(
                  (event) => event.type === "clip_generated" || event.type === "clip_started",
                )
              ? "the popped clip generated or started after the pop reply"
              : undefined,
        );
        yield* recorded(run, provider.setAutoplay(true));
        yield* next.operation.reached("started").pipe(Effect.timeout(yield* until(deadline)));
        yield* mark(run, "old clip started");

        let replacementSession: Reactor.Session | undefined;
        const replacement = yield* Orchestration.openH3({
          mint: Effect.succeed(nextGrant),
          onAllocated: ({ session }) =>
            Effect.gen(function* () {
              replacementSession = session;
              run.evidence.scheduler = {
                ...run.evidence.scheduler!,
                replacement: {
                  ...run.evidence.scheduler!.replacement,
                  session: { id: session.id, allocatedMs: since(run.origin) },
                },
              };
              yield* mark(run, "replacement allocated");
            }),
        }).pipe(Effect.timeout(yield* until(deadline)));
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            const requestedMs = since(run.origin);
            const report = yield* replacementSession!.close;
            const reportedMs = since(run.origin);
            const previous = run.evidence.scheduler!.replacement;
            const allocation = previous.session!;
            run.evidence.scheduler = {
              ...run.evidence.scheduler!,
              replacement: {
                ...previous,
                session: { ...allocation, endedMs: reportedMs },
                termination: { requestedMs, reportedMs, confirmed: report.remote.confirmed },
                estimatedUsd: round(billedUsd(rate, (reportedMs - allocation.allocatedMs) / 1000)),
              },
            };
            judge(
              run,
              "replacement confirmed termination",
              report.remote.confirmed ? undefined : "the replacement session end was not confirmed",
            );
            yield* mark(
              run,
              "replacement closed",
              report.remote.confirmed ? "confirmed" : "unconfirmed",
            );
          }).pipe(Effect.ignore),
        );
        yield* watch(replacement.source.provider);
        const newMedia = yield* Native.media(replacementSession!);
        yield* readInto(Reactor.recorder(newMedia.video(tracks.video)), newVideo, run.origin).pipe(
          Effect.ignore,
          Effect.forkScoped,
        );
        const newProvider = replacement.source.provider;
        yield* recorded(run, newProvider.setAutoplay(false));
        const newSubmission = yield* newProvider.prepare({
          prompt,
          seconds: 5,
          metadata: `${marker}:replacement`,
        });
        const newAcceptance = yield* recorded(run, newSubmission.submit);
        run.evidence.outcomes.push("replied");
        const newOperation = yield* newProvider.operation(newSubmission);
        yield* newOperation.reached("generated").pipe(Effect.timeout(yield* until(deadline)));
        yield* recorded(run, provider.stop);
        const oldStopMs = since(run.origin);
        yield* recorded(run, newProvider.setAutoplay(true));
        yield* newOperation.reached("started").pipe(Effect.timeout(yield* until(deadline)));
        const newStartMs = since(run.origin);
        yield* Effect.sleep(Duration.min(Duration.millis(target.windowMs), yield* until(deadline)));
        const oldLastFrameMs = oldVideo.lastBefore(oldStopMs);
        const replacementFirstFrameMs = newVideo.firstAfter(newStartMs);
        if (oldLastFrameMs !== undefined && replacementFirstFrameMs !== undefined)
          run.evidence.scheduler = {
            ...run.evidence.scheduler,
            decodedHandoff: {
              oldLastFrameMs,
              replacementFirstFrameMs,
              gapMs: replacementFirstFrameMs - oldLastFrameMs,
            },
          };
        judge(
          run,
          "decoded handoff observation",
          oldLastFrameMs !== undefined && replacementFirstFrameMs !== undefined
            ? undefined
            : "both sides did not deliver a decoded frame around the switch",
        );
        const watched = new Set([
          first.acceptance.clip.clip_id,
          next.acceptance.clip.clip_id,
          popped.acceptance.clip.clip_id,
          newAcceptance.clip.clip_id,
        ]);
        const counts: Record<string, number> = {};
        const mismatched: Record<string, number> = {};
        for (const event of observed) {
          if (!watched.has(event.clipId)) continue;
          counts[event.type] = (counts[event.type] ?? 0) + 1;
          if (!event.metadata.includes(marker))
            mismatched[event.type] = (mismatched[event.type] ?? 0) + 1;
        }
        run.evidence.scheduler = {
          ...run.evidence.scheduler,
          metadata: { observed: counts, mismatched },
        };
        judge(
          run,
          "observed clip metadata",
          Object.keys(counts).length > 0 && Object.keys(mismatched).length === 0
            ? undefined
            : "a watched clip message had missing metadata, or none was observed",
        );
        yield* mark(run, "scheduler observed");
      }),
    ).pipe(Effect.provide(clientLayer(target)));
    return yield* body.pipe(Effect.ensuring(afterSession(target, run, grant.jwt, rate)));
  });

interface OwnerRecord {
  readonly sessionId: string;
  readonly jwt: string;
  readonly expiresAt: number;
  readonly allocatedAt: number;
  /** The library's owner record, which `resumeH3` resumes from. */
  readonly allocation: Orchestration.Allocation;
}

/**
 * The owner of the takeover, in a child process: allocate, record the durable
 * owner record, play a long clip with a short one queued behind it, stream,
 * report, then wait to be killed. It holds the grant, never the API key.
 */
const owner = (target: Target, grantFile: string, recordFile: string, marker: string) =>
  Effect.gen(function* () {
    const stored = JSON.parse(readFileSync(grantFile, "utf8")) as {
      jwt: string;
      expiresAt: number;
      granted: { maxSessions: 1; maxSessionSeconds: number };
    };
    rmSync(grantFile, { force: true });
    const grant = { ...stored, jwt: Redacted.make(stored.jwt) };
    return yield* Effect.scoped(
      Effect.gen(function* () {
        let allocated: Reactor.Session | undefined;
        let deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
        const opened = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          onAllocated: ({ session, allocation }) =>
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;
              allocated = session;
              deadline = now + workSeconds * 1000;
              const record: OwnerRecord = {
                sessionId: session.id,
                jwt: stored.jwt,
                expiresAt: grant.expiresAt,
                allocatedAt: now,
                allocation,
              };
              writeFileSync(recordFile, JSON.stringify(record), { mode: 0o600 });
            }),
        });
        const provider = opened.source.provider;
        yield* provider.setAutoplay(true);
        // The longest clip, so it still plays when the attacher arrives. H3
        // keeps no history, so the attacher knows a playing clip only by its
        // id; the clip queued behind it is the one whose metadata it can read.
        const playing = yield* provider.prepare({
          prompt,
          seconds: 15,
          metadata: `${marker}:playing`,
        });
        const first = yield* playing.submit;
        const queued = yield* provider.prepare({
          prompt,
          seconds: 5,
          metadata: `${marker}:queued`,
        });
        const second = yield* queued.submit;
        const operation = yield* provider.operation(playing);
        yield* operation.reached("started").pipe(Effect.timeout(yield* until(deadline)));
        const media = yield* Native.media(allocated!);
        yield* media
          .video(tracks.video)
          .pipe(Stream.take(24), Stream.runDrain, Effect.timeout(yield* until(deadline)));
        yield* Console.log(
          `owner-streaming ${allocated!.id} ${first.clip.clip_id} ${second.clip.clip_id}`,
        );
        return yield* Effect.never;
      }).pipe(Effect.provide(clientLayer(target))),
    );
  });

/**
 * Kill the owner while it streams and take the session over from this
 * process. The `takeover` check attaches with the raw session API and ends
 * the session through the durable owner record; the `resume` check resumes it
 * with `Orchestration.resumeH3`, which adopts it, and ends it by closing the
 * resumed source, so the library's own close report is the termination.
 */
const takeover = (target: Target, run: Run, budget: Budget, check: "takeover" | "resume") =>
  Effect.gen(function* () {
    const { grant, rate } = yield* admitted(target, run, budget);
    const marker = `hosted-qualification:${run.evidence.runId}`;
    const directory = mkdtempSync(join(tmpdir(), "reactor-takeover-"));
    const grantFile = join(directory, "grant.json");
    const recordFile = join(directory, "owner.json");
    writeFileSync(grantFile, JSON.stringify({ ...grant, jwt: Redacted.value(grant.jwt) }), {
      mode: 0o600,
    });
    const readRecord = (): OwnerRecord | undefined =>
      existsSync(recordFile)
        ? (JSON.parse(readFileSync(recordFile, "utf8")) as OwnerRecord)
        : undefined;
    /**
     * The durable owner record terminates the session, with the persisted
     * grant as the credential: on the normal path after the takeover, and on
     * any failure after the owner allocated, once the owner is dead.
     */
    const terminateByRecord = Effect.gen(function* () {
      const record = readRecord();
      if (record === undefined || run.evidence.termination !== undefined) return;
      run.evidence.session ??= {
        id: record.sessionId,
        allocatedMs: record.allocatedAt - run.origin,
        tracks: [],
      };
      const coordinator = yield* Reactor.Coordinator.make({
        apiUrl: target.apiUrl,
        credential: Effect.succeed(Redacted.make(record.jwt)),
      });
      const requestedMs = since(run.origin);
      const termination = yield* coordinator.terminate(record.sessionId);
      run.evidence.termination = {
        requestedMs,
        reportedMs: since(run.origin),
        confirmed: termination.confirmed,
        remote: termination,
        trail: [],
      };
      yield* mark(run, "terminated", termination.confirmed ? "confirmed" : "unconfirmed");
    });
    const body = Effect.scoped(
      Effect.gen(function* () {
        // The owner needs the grant, never the API key.
        const env = { ...process.env };
        delete env.REACTOR_API_KEY;
        const child = spawn(
          process.execPath,
          [script, "owner", grantFile, recordFile, marker, ...target.ownerArgs],
          { stdio: ["ignore", "pipe", "inherit"], env },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => child.kill("SIGKILL")));
        // The owner reports once it streams. Its own steps end at its work
        // deadline, so this bounds only how long it takes to start.
        const streaming = yield* Effect.callback<
          { sessionId: string; playing: string; queued: string },
          Refused
        >((resume) => {
          const lines = createInterface({ input: child.stdout });
          lines.on("line", (line) => {
            const [tag, sessionId, playing, queued] = line.split(" ");
            if (
              tag === "owner-streaming" &&
              sessionId !== undefined &&
              playing !== undefined &&
              queued !== undefined
            )
              resume(Effect.succeed({ sessionId, playing, queued }));
          });
          child.once("exit", () =>
            resume(Effect.fail(new Refused({ message: "the owner exited before streaming" }))),
          );
        }).pipe(Effect.timeout(`${workSeconds + 15} seconds`));
        const record = readRecord()!;
        const deadline = record.allocatedAt + workSeconds * 1000;
        run.evidence.session = {
          id: record.sessionId,
          allocatedMs: record.allocatedAt - run.origin,
          tracks: [],
        };
        const ownerStreamingMs = since(run.origin);
        child.kill("SIGKILL");
        const killedMs = since(run.origin);
        run.evidence.takeover = { ownerStreamingMs, killedMs };
        yield* mark(run, "owner killed", `clip ${streaming.playing}`);
        const video = new VideoReader();
        /** The attached or resumed session's provider, its video, and how it ends. */
        const attach = Effect.gen(function* () {
          if (check === "takeover") {
            const client = yield* Reactor.Client;
            const session = yield* client.attachConnected({
              sessionId: record.sessionId,
              jwt: Redacted.make(record.jwt),
            });
            const media = yield* Native.media(session);
            return {
              provider: yield* H3.make(session),
              tracks: media.tracks,
              video: media.video(tracks.video),
              close: Effect.void,
            };
          }
          const opened = yield* Orchestration.resumeH3({
            allocation: record.allocation,
            jwt: Redacted.make(record.jwt),
          });
          const media = yield* opened.source.media;
          return {
            provider: opened.source.provider,
            tracks: [] as readonly MediaGeneration["tracks"][number][],
            video: media.video,
            // The adopted session is owned: closing its source terminates it.
            close: Effect.gen(function* () {
              const requestedMs = since(run.origin);
              const report = yield* opened.source.close;
              run.evidence.termination = {
                requestedMs,
                reportedMs: since(run.origin),
                confirmed: report.lease.remote.confirmed,
                close: report.lease,
                trail: [],
              };
              yield* mark(
                run,
                "closed",
                `${report.lease.ownership ?? "?"}, ${report.lease.remote.confirmed ? "confirmed" : "unconfirmed"}`,
              );
            }),
          };
        });
        const taken = yield* Effect.scoped(
          Effect.gen(function* () {
            const attached = yield* attach;
            const attachMs = since(run.origin) - killedMs;
            yield* mark(run, check === "takeover" ? "attached" : "resumed");
            const provider = attached.provider;
            // The first coherent state and queue the attacher reads: the playing
            // clip shows only as `playing_clip_id`, the queued one with its metadata.
            const facts = yield* Effect.gen(function* () {
              for (;;) {
                const snapshot = yield* provider.current;
                if (snapshot._tag === "Ready") return snapshot;
                yield* Effect.sleep("50 millis");
              }
            }).pipe(Effect.timeout(Duration.min(Duration.seconds(5), yield* until(deadline))));
            const queued = [...facts.queue.generation, ...facts.queue.playout].find(
              (clip) => clip.clip_id === streaming.queued,
            );
            run.evidence.session = {
              ...run.evidence.session!,
              tracks: attached.tracks.map(({ name, kind, direction }) => ({
                name,
                kind,
                direction,
              })),
            };
            const attachedAt = since(run.origin);
            yield* readInto(
              Reactor.recorder(attached.video).pipe(Stream.take(48)),
              video,
              run.origin,
            ).pipe(
              Effect.timeout(
                Duration.min(Duration.millis(target.windowMs), yield* until(deadline)),
              ),
              Effect.ignore,
            );
            yield* attached.close;
            return {
              attachMs,
              playingClipId: facts.state.playing_clip_id,
              clipIdentified: facts.state.playing_clip_id === streaming.playing,
              queuedListed: queued !== undefined,
              metadataPreserved: queued?.metadata.includes(`${marker}:queued`) === true,
              firstFreshFrameMs: video.firstAfter(attachedAt),
            };
          }).pipe(Effect.provide(clientLayer(target))),
        );
        // This process never enqueues; the library must not replay the owner's.
        const enqueues = run.spans
          .records()
          .filter(
            (span) =>
              span.name === "reactor.h3.enqueue" ||
              (span.name === "reactor.session.command" &&
                span.attributes["reactor.operation"] === "enqueue"),
          ).length;
        // What this process asked of the session: a resume only reads it.
        const commands = run.spans
          .records()
          .filter((span) => span.name === "reactor.session.command")
          .map((span) => String(span.attributes["reactor.operation"]));
        run.evidence.takeover = {
          ownerStreamingMs,
          killedMs,
          attachMs: taken.attachMs,
          clipIdentified: taken.clipIdentified,
          metadataPreserved: taken.metadataPreserved,
          enqueuesAfterAttach: enqueues,
          ...(taken.firstFreshFrameMs === undefined
            ? {}
            : { firstFreshFrameMs: taken.firstFreshFrameMs }),
          video: video.summary(),
        };
        yield* mark(run, "observed");
        if (check === "resume") {
          const reads = commands.filter((name) => name !== "get_state" && name !== "get_queue");
          judge(
            run,
            "only reads on resume",
            reads.length === 0 ? undefined : `the resume sent ${reads.join(", ")}`,
          );
          const close = run.evidence.termination?.close;
          judge(
            run,
            "adopted close terminates",
            close === undefined
              ? "the resumed source was never closed"
              : close.ownership !== "owned"
                ? `the resumed session's close reported it ${close.ownership ?? "without ownership"}`
                : close.remote.attempted
                  ? undefined
                  : "closing the adopted session attempted no termination",
          );
        }
        yield* terminateByRecord;
        judge(
          run,
          "attach within 5 s",
          taken.attachMs <= 5_000 ? undefined : `attaching took ${Math.round(taken.attachMs)} ms`,
        );
        judge(
          run,
          "clip identified",
          taken.clipIdentified
            ? undefined
            : `the attached state named ${taken.playingClipId ?? "no clip"} playing, not the owner's`,
        );
        judge(
          run,
          "metadata preserved",
          taken.metadataPreserved
            ? undefined
            : taken.queuedListed
              ? "the attached queue lost the queued clip's metadata"
              : "the owner's queued clip was not in the attached queue",
        );
        judge(
          run,
          "no enqueue on attach",
          enqueues === 0 ? undefined : `${enqueues} enqueue(s) after attaching`,
        );
        const fresh = video.summary();
        judge(
          run,
          "fresh frames",
          fresh.frames === 0 ? "no frame arrived after attaching" : liveVideo(fresh),
        );
      }),
    );
    return yield* body.pipe(
      // The owner is dead once the body's scope closed; whatever failed, the
      // session it allocated is terminated through its record.
      Effect.ensuring(terminateByRecord.pipe(Effect.ignore)),
      Effect.ensuring(afterSession(target, run, grant.jwt, rate)),
      Effect.ensuring(Effect.sync(() => rmSync(directory, { recursive: true, force: true }))),
    );
  });

const stamp = (at: number) => new Date(at).toISOString().replaceAll(/[-:]|\.\d+/g, "");

/** Run one check against `target`, saving its evidence in `ledger`; the exit code. */
const execute = async (
  target: Target,
  check: Check,
  budget: Budget,
  ledger: string,
): Promise<number> => {
  const origin = Date.now();
  const runId = randomUUID();
  const file = join(ledger, `${stamp(origin)}-${check}-${target.mode}-${runId.slice(0, 8)}.json`);
  const secrets = [Redacted.value(target.apiKey)];
  const run: Run = {
    origin,
    evidence: {
      format,
      runId,
      check,
      mode: target.mode,
      startedAt: new Date(origin).toISOString(),
      environment: environment(target),
      budget: {
        checkUsd: budget.checkUsd,
        totalUsd: budget.totalUsd,
        reservedBeforeUsd: round(budget.reservedUsd),
        sessionSeconds,
      },
      milestones: [],
      spans: [],
      outcomes: [],
      criteria: [],
      missing: [],
      reasons: [],
    },
    spans: spanRecorder(origin),
    secrets,
    writer: new Writer(file, () => secrets),
  };
  save(run);
  const program = (
    check === "scheduler"
      ? scheduler(target, run, budget)
      : check === "takeover" || check === "resume"
        ? takeover(target, run, budget, check)
        : vertical(target, run, budget, check)
  ).pipe(
    Effect.provideService(Tracer.Tracer, run.spans.tracer),
    Effect.provide(Reactor.FetchHttp.layer),
  );
  const fiber = Effect.runFork(program);
  // Ctrl-C interrupts the check, so its finalizers still close the session.
  const interrupt = () => void Effect.runFork(Fiber.interrupt(fiber));
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
  const exit = await Effect.runPromise(Fiber.await(fiber));
  process.off("SIGINT", interrupt);
  process.off("SIGTERM", interrupt);
  run.evidence.finishedAt = new Date().toISOString();
  conclude(run.evidence, exit._tag === "Failure" ? failureText(exit.cause) : undefined);
  const session = run.evidence.session;
  if (session !== undefined && run.evidence.termination?.confirmed !== true)
    run.evidence.cleanup = `Session ${session.id} was not confirmed ended. Its token capped it at ${sessionSeconds} s, so the server ends it by ${new Date(origin + session.allocatedMs + sessionSeconds * 1000).toISOString()}; confirm in the Reactor dashboard that it ended, and what it cost.`;
  const replacement = run.evidence.scheduler?.replacement;
  if (replacement?.session !== undefined && replacement.termination?.confirmed !== true)
    run.evidence.cleanup = `${run.evidence.cleanup === undefined ? "" : `${run.evidence.cleanup}\n`}Replacement session ${replacement.session.id} was not confirmed ended. Its token capped it at ${sessionSeconds} s; confirm its termination and cost in the Reactor dashboard.`;
  save(run);
  console.log(`hosted-qualification-${run.evidence.verdict} ${check} ${file}`);
  for (const reason of run.evidence.reasons) console.log(`  - ${reason}`);
  if (run.evidence.cleanup !== undefined) console.error(run.evidence.cleanup);
  return run.evidence.verdict === "pass" ? 0 : 1;
};

const refused = (cause: unknown): number => {
  console.error(`hosted-qualification-refused ${refusal(cause).message}`);
  return 2;
};

const apiKey = (): Redacted.Redacted<string> => {
  const value = process.env.REACTOR_API_KEY;
  if (value === undefined || value.length === 0)
    throw new Refused({ message: "REACTOR_API_KEY is not set" });
  return Redacted.make(value);
};

const paidTarget = (network: string, key: Redacted.Redacted<string> = apiKey()): Target => ({
  mode: "paid",
  apiUrl: process.env.REACTOR_API_URL ?? "https://api.reactor.inc",
  apiKey: key,
  peers: Native.layer(),
  network,
  ownerArgs: [],
  windowMs: 6_000,
});

const paidRuns = (ledger: readonly LedgerEntry[]) =>
  ledger.filter((entry) => entry.evidence.mode === "paid");

const paid = async (args: readonly string[]): Promise<number> => {
  let unlock: (() => void) | undefined;
  try {
    const auth = authorize(args);
    mkdirSync(auth.ledger, { recursive: true });
    unlock = lockLedger(auth.ledger);
    const earlier = paidRuns(readLedger(auth.ledger));
    if (auth.check === "turn")
      admitRelayCheck(
        earlier.flatMap(({ evidence }) =>
          evidence.network?.pair === undefined
            ? []
            : [[evidence.network.pair.local ?? "", evidence.network.pair.remote ?? ""] as const],
        ),
      );
    const target = paidTarget(auth.network);
    return await execute(
      target,
      auth.check,
      {
        checkUsd: auth.budgetUsd,
        totalUsd: auth.totalBudgetUsd,
        reservedUsd: earlier.reduce((sum, { evidence }) => sum + reservedUsd(evidence), 0),
      },
      auth.ledger,
    );
  } catch (cause) {
    return refused(cause);
  } finally {
    unlock?.();
  }
};

/** A free run of `check` against a local twin; faults exercise the failure paths. */
const rehearse = async (args: readonly string[]): Promise<number> => {
  const check = args[0] as Check;
  if (!checks.includes(check)) return refused("name the check to rehearse");
  let given: ReadonlyMap<string, string>;
  try {
    given = options(args.slice(1), ["faults", "ledger"]);
  } catch (cause) {
    return refused(cause);
  }
  const faults = (given.get("faults") ?? "").split(",").filter((fault) => fault.length > 0);
  const ledger = given.get("ledger") ?? mkdtempSync(join(tmpdir(), "reactor-rehearsal-"));
  mkdirSync(ledger, { recursive: true });
  const Twin = await import("./twin/index.js");
  const twin = await Twin.startTwin({
    relay: check === "turn",
    faults: Object.fromEntries(faults.map((fault) => [fault, true])),
  });
  try {
    return await execute(
      {
        mode: "rehearsal",
        apiUrl: twin.url,
        apiKey: Redacted.make(twin.apiKey),
        peers: Twin.twinPeers(twin.url),
        network: "loopback twin",
        ownerArgs: [`--twin=${twin.url}`],
        windowMs: 1_500,
      },
      check,
      {
        checkUsd: check === "scheduler" ? maxSchedulerUsd : maxCheckUsd,
        totalUsd: maxTotalUsd,
        reservedUsd: 0,
      },
      ledger,
    );
  } finally {
    await twin.close();
  }
};

/** The takeover's owner process; `--twin=<url>` points it at a rehearsal twin. */
const ownerProcess = async (args: readonly string[]): Promise<number> => {
  const [grantFile, recordFile, marker, twinArg] = args;
  const twinUrl = twinArg?.startsWith("--twin=") === true ? twinArg.slice(7) : undefined;
  const target: Target =
    twinUrl === undefined
      ? {
          mode: "paid",
          apiUrl: process.env.REACTOR_API_URL ?? "https://api.reactor.inc",
          apiKey: Redacted.make(""),
          peers: Native.layer(),
          network: "",
          ownerArgs: [],
          windowMs: 0,
        }
      : {
          mode: "rehearsal",
          apiUrl: twinUrl,
          apiKey: Redacted.make(""),
          peers: (await import("./twin/index.js")).twinPeers(twinUrl),
          network: "",
          ownerArgs: [],
          windowMs: 0,
        };
  await Effect.runPromise(
    owner(target, grantFile!, recordFile!, marker!).pipe(Effect.provide(Reactor.FetchHttp.layer)),
  );
  return 0;
};

/**
 * Free checks before paying: the ledger, the published rate against the
 * budget, a token's granted limits (minting allocates nothing), and loading
 * the native library. It prints what the remaining budget buys, in order.
 */
const preflight = async (args: readonly string[]): Promise<number> => {
  try {
    const given = options(args, ["total-budget-usd", "budget-usd", "ledger"], ["--no-mint"]);
    const total = Number(given.get("total-budget-usd"));
    const perCheck = Number(given.get("budget-usd") ?? maxCheckUsd);
    const ledgerDirectory = given.get("ledger");
    if (!(total > 0 && total <= maxTotalUsd) || !(perCheck > 0 && perCheck <= maxCheckUsd))
      throw new Refused({
        message: `budgets must be positive, at most $${maxCheckUsd} a check and $${maxTotalUsd} in total`,
      });
    if (ledgerDirectory === undefined)
      throw new Refused({ message: "--ledger=<directory> is required" });
    const ledger = readLedger(ledgerDirectory);
    const earlier = paidRuns(ledger);
    const reserved = earlier.reduce((sum, { evidence }) => sum + reservedUsd(evidence), 0);
    for (const { file, evidence } of earlier)
      console.log(
        `ledger ${file}: ${evidence.check} ${evidence.verdict ?? "unfinished"}, reserved $${reservedUsd(evidence).toFixed(4)}`,
      );
    // Pricing needs no credential; only minting does.
    const target = paidTarget("preflight", given.has("--no-mint") ? Redacted.make("") : apiKey());
    const report = await Effect.runPromise(
      Effect.gen(function* () {
        const coordinator = yield* Reactor.Coordinator.make({ apiUrl: target.apiUrl });
        const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
        const worst = worstCaseUsd(rate);
        const granted = given.has("--no-mint")
          ? undefined
          : yield* coordinator
              .mintToken({
                apiKey: target.apiKey,
                modelName: H3.modelName,
                maxSessionDuration: `${sessionSeconds} seconds`,
                expiresAfter: `${tokenSeconds} seconds`,
              })
              .pipe(Effect.map((grant) => grant.granted));
        yield* Layer.build(target.peers).pipe(Effect.scoped);
        return { rate, worst, granted };
      }).pipe(Effect.provide(Reactor.FetchHttp.layer)),
    );
    console.log(
      `rate ${report.rate.creditsPerSecond} credits/s at ${report.rate.creditsPerDollar} credits/$: a ${sessionSeconds} s session, billed by the minute, costs up to $${report.worst.toFixed(4)}`,
    );
    admit(report.rate, perCheck);
    const sessions = Math.floor((total - reserved + 1e-9) / report.worst);
    console.log(
      `reserved $${reserved.toFixed(4)} of $${total}: the rest buys ${sessions} more capped session(s)`,
    );
    if (report.granted !== undefined) {
      acceptGrant(report.granted);
      console.log(
        `a token grants ${report.granted.maxSessions} session of at most ${report.granted.maxSessionSeconds} s`,
      );
    }
    const native = environment(target).native;
    console.log(`native library ${native === undefined ? "loaded" : JSON.stringify(native)}`);
    const passed = new Set(
      earlier
        .filter(({ evidence }) => evidence.verdict === "pass")
        .map(({ evidence }) => evidence.check),
    );
    const order = (["vertical", "takeover"] as const).filter((check) => !passed.has(check));
    console.log(
      `next: ${order.length === 0 ? "nothing required" : order.join(", then ")}; turn only if no run selected a relay pair`,
    );
    return sessions >= order.length ? 0 : 2;
  } catch (cause) {
    return refused(cause);
  }
};

const summarizeFiles = (args: readonly string[]): number => {
  const files = args.flatMap((path) =>
    statSync(path).isDirectory()
      ? readdirSync(path)
          .filter((name) => name.endsWith(".json"))
          .sort()
          .map((name) => join(path, name))
      : [path],
  );
  const directories = [...new Set(files.map((file) => dirname(file)))];
  const entries = directories.flatMap((directory) =>
    readLedger(directory).filter((entry) => files.includes(entry.file)),
  );
  console.log(summarize(entries.map((entry) => entry.evidence)));
  return 0;
};

const main = async (args: readonly string[]): Promise<number> => {
  switch (args[0]) {
    case "owner":
      return ownerProcess(args.slice(1));
    case "preflight":
      return preflight(args.slice(1));
    case "rehearse":
      return rehearse(args.slice(1));
    case "summarize":
      return summarizeFiles(args.slice(1));
    default:
      return paid(args);
  }
};

if (import.meta.main) process.exitCode = await main(process.argv.slice(2));
