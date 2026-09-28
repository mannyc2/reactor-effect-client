/**
 * The hosted qualification: paid checks against hosted Reactor, run only by a
 * maintainer who authorizes the spend, and a free rehearsal of the same checks
 * against a local twin. README.md says what each check costs and gathers, why,
 * and how the evidence is shared.
 *
 *   bun integration/hosted/qualify.ts preflight --total-budget-usd=1.50 --ledger=<dir>
 *   bun integration/hosted/qualify.ts rehearse <check> [--faults=<a,b>] [--ledger=<dir>]
 *     [--constructor=legacy|continuous]   (scheduler-renewal only; continuous by default)
 *   bun integration/hosted/qualify.ts <vertical|takeover|turn|audio|resume|scheduler> \
 *     --budget-usd=0.75 --total-budget-usd=1.50 --ledger=<dir> --network="<where>" \
 *     --i-authorize-paid-sessions
 *   bun integration/hosted/qualify.ts scheduler-renewal --budget-usd=1.50 \
 *     --total-budget-usd=3.75 --ledger=<dir> --network="<where>" --i-authorize-paid-sessions
 *   bun integration/hosted/qualify.ts summarize <evidence file or ledger>...
 *
 * `REACTOR_API_KEY` mints one token per session, each capped at 50 s
 * server-side; `scheduler-renewal` uses two, and a paid `scheduler-renewal`
 * always runs the continuous constructor.
 * `REACTOR_API_URL` overrides the coordinator. A paid check
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
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
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
import type { AudioFrame, VideoFrame, MediaGeneration } from "reactor-effect-client/host";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Testing from "reactor-effect-client/testing";
import * as Native from "reactor-effect-native";
import {
  AudioReader,
  FrameAttribution,
  SeamRecorder,
  png,
  elapsedClock,
  ContractTally,
  VideoReader,
  readInto,
  sampleStats,
  since,
  spanRecorder,
  tallyReply,
  terminationTrail,
} from "./collect.js";
import type {
  ItemSeam,
  Pressure,
  RunItem,
  StatsSample,
  SchedulerBoundary,
  SchedulerRenewal,
} from "./evidence.js";
import {
  Writer,
  cleanupInstructions,
  recordedSessions,
  conclude,
  format,
  lockLedger,
  readLedger,
  rejudged,
  reservedUsd,
  renewalJudgments,
  confirmedOwnedCleanup,
  schedulerBoundaries,
} from "./evidence.js";
import type { Draft, LedgerEntry } from "./evidence.js";
import {
  Refused,
  acceptGrant,
  acceptRenewalGrant,
  ceilingFor,
  admit,
  admitRelayCheck,
  admitTotal,
  authorize,
  billedUsd,
  checks,
  liveClipVideo,
  liveVideo,
  maxCheckUsd,
  maxTotalUsd,
  options,
  reservationUsd,
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
const grayPng = (width: number, height: number): Uint8Array =>
  png(width, height, new Uint8Array(width * height * 3).fill(128));

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
  /** Harness fault injection is reachable only through the loopback entrypoint. */
  readonly faults?: readonly string[];
  /** Only rehearsal may select legacy; hosted qualification always uses continuous. */
  readonly renewalConstructor?: "legacy" | "continuous";
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
    run.evidence.budget.worstCaseUsd = worst;
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
        billedUsd(rate, (endedMs - session.allocatedMs) / 1000),
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

/** Every scheduler-check clip asks for this long: a short build, and a boundary every few seconds. */
const clipSeconds = 5;
/** Clips submitted after the position-zero pair, so each edit finds two clips waiting. */
const queuedClips = 9;
/** How long before an edit is sent it reads the queue, so the edit itself leaves on its aim. */
const queueLeadMs = 600;
/** How far to either side of a boundary its seam is looked for. */
const seamWindowMs = 1500;
/** An edit sent at least this long before the playing clip ends must decide the next clip. */
const judgedAimMs = 1000;

/**
 * One bounded H3 session with autoplay on. Before each clip boundary in
 * `schedulerBoundaries`, an edit moves a waiting clip to position zero or pops
 * the next one, at a set distance from the playing clip's expected end. The
 * check records which clip starts next and how the seam looks in the
 * provider's messages and the decoded frames.
 */
const scheduler = (target: Target, run: Run, budget: Budget) =>
  Effect.gen(function* () {
    const { grant, rate } = yield* admitted(target, run, budget, sessionsFor("scheduler"));
    run.evidence.scheduler = {
      builds: [],
      boundaries: [],
      metadata: { observed: {}, mismatched: {} },
    };
    const marker = `hosted-qualification:${run.evidence.runId}:scheduler`;
    const observed: {
      readonly type: string;
      readonly clipId: string;
      readonly seconds: number;
      readonly metadata: string;
      readonly atMs: number;
    }[] = [];
    const video = new VideoReader();
    const audio = new AudioReader();
    const saveMedia = () => {
      run.evidence.scheduler = {
        ...run.evidence.scheduler!,
        media: { video: video.summary(), audio: audio.summary() },
      };
    };
    let deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
    const body = Effect.scoped(
      Effect.gen(function* () {
        let session: Reactor.Session | undefined;
        yield* Effect.addFinalizer(() => Effect.sync(saveMedia));
        const opened = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          onAllocated: ({ session: allocated }) =>
            Effect.gen(function* () {
              session = allocated;
              deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
              run.evidence.session = {
                id: allocated.id,
                allocatedMs: since(run.origin),
                tracks: [],
              };
              yield* mark(run, "allocated");
            }),
        });
        yield* Effect.addFinalizer(() => closeSession(run, session!).pipe(Effect.ignore));
        const provider = opened.source.provider;
        yield* provider.events({ capacity: 4096 }).pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              if (event._tag !== "Message" || event.disposition !== "applied") return;
              const message = event.message;
              if (message.type === "unknown" || !("clip" in message.data)) return;
              if (observed.length < 1024)
                observed.push({
                  type: message.type,
                  clipId: message.data.clip.clip_id,
                  seconds: message.data.clip.seconds,
                  metadata: message.data.clip.metadata,
                  atMs: since(run.origin),
                });
            }),
          ),
          Effect.forkScoped,
        );
        const media = yield* Native.media(session!);
        run.evidence.session = {
          ...run.evidence.session!,
          tracks: media.tracks.map(({ name, kind, direction }) => ({ name, kind, direction })),
        };
        yield* readInto(Reactor.recorder(media.video(tracks.video)), video, run.origin).pipe(
          Effect.ignore,
          Effect.forkScoped,
        );
        if (media.tracks.some((track) => track.kind === "audio"))
          yield* readInto(Reactor.recorder(media.audio(tracks.audio)), audio, run.origin).pipe(
            Effect.ignore,
            Effect.forkScoped,
          );

        /** The first observation `find` returns, waited for until the check's deadline. */
        const seen = <A>(find: () => A | undefined) =>
          Effect.gen(function* () {
            const left = yield* until(deadline);
            return yield* Effect.gen(function* () {
              for (;;) {
                const found = find();
                if (found !== undefined) return found;
                yield* Effect.sleep("10 millis");
              }
            }).pipe(Effect.timeout(left));
          });
        /** Waits until `atMs` after the run's start, never past the check's deadline. */
        const sleepUntil = (atMs: number) =>
          Effect.gen(function* () {
            const left = Duration.toMillis(yield* until(deadline));
            yield* Effect.sleep(
              Duration.millis(Math.min(left, Math.max(0, atMs - since(run.origin)))),
            );
          });
        const started = () => observed.filter((event) => event.type === "clip_started");
        const submit = (name: string, position?: number) =>
          Effect.gen(function* () {
            const submission = yield* provider.prepare({
              prompt,
              seconds: clipSeconds,
              metadata: `${marker}:${name}`,
              ...(position === undefined ? {} : { position }),
            });
            const submittedMs = since(run.origin);
            const acceptance = yield* recorded(run, submission.submit);
            run.evidence.outcomes.push("replied");
            return { clipId: acceptance.clip.clip_id, submittedMs };
          });
        const queue = Effect.gen(function* () {
          const reply = yield* recorded(run, provider.getQueue);
          run.evidence.outcomes.push("replied");
          return reply.value;
        });
        /** An edit's outcome, `true` when the provider refused it. Its reason is provider text, never kept. */
        const edit = (command: Effect.Effect<unknown, Reactor.CommandFailure>) =>
          recorded(run, command).pipe(
            Effect.tap(() => Effect.sync(() => run.evidence.outcomes.push("replied"))),
            Effect.as(false),
            Effect.catchIf(
              (error) => error.reason._tag === "Remote" && error.context.outcome === "replied",
              () => Effect.succeed(true),
            ),
          );

        yield* recorded(run, provider.setAutoplay(false));
        // A build popped while it runs must never reach playout.
        const popped = yield* submit("popped");
        const wasGeneration = (yield* queue).generation[0]?.clip_id === popped.clipId;
        yield* recorded(run, provider.pop(popped.clipId));
        run.evidence.outcomes.push("replied");
        const poppedMs = since(run.origin);
        // H3 documents position zero as next, behind the build that is running.
        const first = yield* submit("first");
        const zero = yield* submit("position-zero", 0);
        const generationOrder = (yield* queue).generation.map((clip) => clip.clip_id);
        run.evidence.scheduler = {
          ...run.evidence.scheduler!,
          positionZero: {
            buildingClipId: first.clipId,
            requestedClipId: zero.clipId,
            generationOrder,
          },
        };
        judge(
          run,
          "position zero while building",
          generationOrder[0] === first.clipId && generationOrder[1] === zero.clipId
            ? undefined
            : "the building clip and position-zero request were not observed in that order",
        );
        const builds = [first, zero];
        for (let index = 1; index <= queuedClips; index++)
          builds.push(yield* submit(`clip-${index}`));
        yield* mark(run, "clips submitted");
        yield* seen(() =>
          observed.find(
            (event) => event.type === "clip_generated" && event.clipId === first.clipId,
          ),
        );
        yield* recorded(run, provider.setAutoplay(true));
        yield* mark(run, "autoplay on");

        const boundaries: Mutable<SchedulerBoundary>[] = [];
        const recordBoundaries = () => {
          run.evidence.scheduler = {
            ...run.evidence.scheduler!,
            boundaries: boundaries.map((boundary) => ({ ...boundary })),
          };
        };
        const acceptedPops: string[] = [];
        for (const [index, planned] of schedulerBoundaries.entries()) {
          const ending = yield* seen(() => started()[index]);
          const endsAtMs = ending.atMs + ending.seconds * 1000;
          const boundary: Mutable<SchedulerBoundary> = {
            edit: planned.edit,
            aimMs: planned.aimMs,
            ending: { clipId: ending.clipId, seconds: ending.seconds, startedMs: ending.atMs },
          };
          boundaries.push(boundary);
          recordBoundaries();
          if (planned.edit !== "none") {
            yield* sleepUntil(endsAtMs - planned.aimMs - queueLeadMs);
            const played = new Set(started().map((event) => event.clipId));
            const waiting = (yield* queue).playout
              .map((clip) => clip.clip_id)
              .filter((clipId) => !played.has(clipId));
            // Without two clips waiting the edit cannot show anything, so it is not staged.
            if (waiting.length >= 2) {
              const edited = planned.edit === "move" ? waiting[1]! : waiting[0]!;
              yield* sleepUntil(endsAtMs - planned.aimMs);
              const sentMs = since(run.origin);
              const refused = yield* edit(
                planned.edit === "move" ? provider.move(edited, 0) : provider.pop(edited),
              );
              boundary.editedClipId = edited;
              boundary.expectedClipId = waiting[1]!;
              boundary.command = { sentMs, replyMs: since(run.origin), refused };
              if (planned.edit === "pop" && !refused) acceptedPops.push(edited);
            }
            recordBoundaries();
            yield* mark(
              run,
              `boundary ${index + 1}: ${planned.edit} ${planned.aimMs} ms before the end`,
              boundary.command === undefined
                ? "not staged"
                : boundary.command.refused
                  ? "refused"
                  : undefined,
            );
          }
          const finished = yield* seen(() =>
            observed.find(
              (event) =>
                (event.type === "clip_finished" || event.type === "clip_stopped") &&
                event.clipId === ending.clipId,
            ),
          );
          const next = yield* seen(() => started()[index + 1]);
          boundary.ending = { ...boundary.ending, finishedMs: finished.atMs };
          boundary.next = { clipId: next.clipId, startedMs: next.atMs };
          recordBoundaries();
        }
        // The last seam's frames arrive after its clip starts.
        yield* sleepUntil((boundaries.at(-1)?.next?.startedMs ?? 0) + seamWindowMs);
        for (const boundary of boundaries) {
          if (boundary.ending.finishedMs === undefined || boundary.next === undefined) continue;
          const pause = video.pause(
            boundary.ending.finishedMs - seamWindowMs,
            boundary.next.startedMs + seamWindowMs,
          );
          if (pause !== undefined) boundary.pause = pause;
        }
        recordBoundaries();
        for (const [index, boundary] of boundaries.entries()) {
          if (boundary.edit === "none" || boundary.aimMs < judgedAimMs) continue;
          judge(
            run,
            `${boundary.edit} ${boundary.aimMs} ms before boundary ${index + 1}`,
            boundary.command === undefined
              ? "the edit was not staged: fewer than two clips were waiting"
              : boundary.command.refused
                ? "the provider refused the edit"
                : boundary.next?.clipId !== boundary.expectedClipId
                  ? "a different clip started next"
                  : undefined,
          );
        }
        const startedIds = new Set(started().map((event) => event.clipId));
        judge(
          run,
          "popped clips never start",
          acceptedPops.some((clipId) => startedIds.has(clipId))
            ? "a clip started after its pop was accepted"
            : undefined,
        );

        const afterPop = observed.filter(
          (event) => event.clipId === popped.clipId && event.atMs > poppedMs,
        );
        const generatedAfterPop = afterPop.some((event) => event.type === "clip_generated");
        const startedAfterPop = afterPop.some((event) => event.type === "clip_started");
        run.evidence.scheduler = {
          ...run.evidence.scheduler,
          poppedBuild: {
            clipId: popped.clipId,
            wasGeneration,
            poppedMs,
            observedUntilMs: since(run.origin),
            generatedAfterPop,
            startedAfterPop,
          },
        };
        judge(
          run,
          "pop the build in flight",
          !wasGeneration
            ? "the clip was not at the head of the generation queue before pop"
            : generatedAfterPop || startedAfterPop
              ? "the popped build generated or started after the pop reply"
              : undefined,
        );

        const recordedBuilds = builds.map(({ clipId, submittedMs }) => {
          const generated = observed.find(
            (event) => event.type === "clip_generated" && event.clipId === clipId,
          );
          return {
            clipId,
            requestedSeconds: clipSeconds,
            submittedMs,
            ...(generated === undefined
              ? {}
              : {
                  readyMs: generated.atMs,
                  readySeconds: generated.seconds,
                  submitToReadyMs: generated.atMs - submittedMs,
                }),
          };
        });
        run.evidence.scheduler = { ...run.evidence.scheduler, builds: recordedBuilds };

        const watched = new Set([popped.clipId, ...builds.map((build) => build.clipId)]);
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
        saveMedia();
        yield* mark(run, "scheduler observed");
      }),
    ).pipe(Effect.provide(clientLayer(target)));
    return yield* body.pipe(Effect.ensuring(afterSession(target, run, grant.jwt, rate)));
  });

/** A scheduled item's request: the scheduler check's prompt and length. */
const itemRequest = (seconds = clipSeconds) =>
  new Orchestration.ClipRequest({ prompt, references: [], durationSeconds: seconds, metadata: {} });

/**
 * Where a boundary's frames are looked for: from before the ending clip's end
 * to well after the next one starts, since decoded frames trail H3's messages
 * by up to about 1.2 s on hosted Reactor.
 */
const seamBefore = seamWindowMs;
const seamAfter = 2 * seamWindowMs;

/** What a scenario gets from `ownedSession`. */
interface OwnedSession {
  readonly owner: Orchestration.HandleShape;
  readonly provider: Orchestration.H3Source["provider"];
  readonly video: VideoReader;
  readonly seams: SeamRecorder;
  /** Where boundary frames are written for review; never inside the ledger. */
  readonly directory: string;
  /** The first value `find` returns, waited for until the check's deadline. */
  readonly seen: <A>(find: () => A | undefined) => Effect.Effect<A, Cause.TimeoutError>;
  /** Waits until `atMs` after the run's start, never past the check's deadline. */
  readonly sleepUntil: (atMs: number) => Effect.Effect<void>;
  /** Milliseconds since the run's start. */
  readonly now: () => number;
}

/**
 * One capped H3 session behind the public renewal owner, which can never open
 * a second: its one source lives as long as the grant lets it, and another open
 * refuses before it could allocate. The owner's media is read to the end; the
 * owner's close ends the session and is the check's termination record.
 */
const ownedSession = <A, E>(
  target: Target,
  run: Run,
  budget: Budget,
  check: Check,
  scenario: (session: OwnedSession) => Effect.Effect<A, E, Scope.Scope>,
) =>
  Effect.gen(function* () {
    const { grant, rate } = yield* admitted(target, run, budget, sessionsFor(check));
    const video = new VideoReader();
    const audio = new AudioReader();
    const seams = new SeamRecorder();
    const directory = join(tmpdir(), `reactor-seams-${run.evidence.runId}`);
    let deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
    const body = Effect.scoped(
      Effect.gen(function* () {
        let opens = 0;
        let provider: Orchestration.H3Source["provider"] | undefined;
        const open = Effect.gen(function* () {
          if (opens++ > 0)
            return yield* Reactor.ReactorError.fromCode(
              "InvalidState",
              `the ${check} check opens one session`,
            );
          const opened = yield* Orchestration.openH3({
            mint: Effect.succeed(grant),
            // H3 flushes to black at every clip boundary unless told to hold the last frame,
            // as a show does: seams are measured as it airs them.
            source: { holdLastFrame: true },
            onAllocated: ({ session }) =>
              Effect.gen(function* () {
                deadline = (yield* Clock.currentTimeMillis) + workSeconds * 1000;
                run.evidence.session = {
                  id: session.id,
                  allocatedMs: since(run.origin),
                  tracks: [],
                };
                yield* mark(run, "allocated");
              }),
          });
          provider = opened.source.provider;
          // The grant caps the session; the owner must never plan a replacement for it.
          return { source: opened.source, lifetime: "Infinity" as const };
        });
        const owner = yield* Orchestration.make({ open });
        yield* mark(run, "opened");
        yield* readInto(
          Reactor.recorder(owner.media.video),
          {
            add: (element: Reactor.Recorded<VideoFrame>, atMs: number) => {
              video.add(element, atMs);
              seams.add(element, atMs);
            },
          },
          run.origin,
        ).pipe(Effect.ignore, Effect.forkScoped);
        yield* readInto(Reactor.recorder(owner.media.audio), audio, run.origin).pipe(
          Effect.ignore,
          Effect.forkScoped,
        );
        const seen = <A>(find: () => A | undefined) =>
          Effect.gen(function* () {
            const left = yield* until(deadline);
            return yield* Effect.gen(function* () {
              for (;;) {
                const found = find();
                if (found !== undefined) return found;
                yield* Effect.sleep("10 millis");
              }
            }).pipe(Effect.timeout(left));
          });
        const sleepUntil = (atMs: number) =>
          Effect.gen(function* () {
            const left = Duration.toMillis(yield* until(deadline));
            yield* Effect.sleep(
              Duration.millis(Math.min(left, Math.max(0, atMs - since(run.origin)))),
            );
          });
        const result = yield* scenario({
          owner,
          provider: provider!,
          video,
          seams,
          directory,
          seen,
          sleepUntil,
          now: () => since(run.origin),
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              const requestedMs = since(run.origin);
              const report = yield* owner.close;
              const lease = report.sessions[0]?.lease;
              if (lease !== undefined)
                run.evidence.termination = {
                  requestedMs,
                  reportedMs: since(run.origin),
                  confirmed: lease.remote.confirmed,
                  close: lease,
                  trail: [],
                };
              yield* mark(
                run,
                "closed",
                lease?.remote.confirmed === true ? "confirmed" : "unconfirmed",
              );
            }).pipe(Effect.ignore),
          ),
        );
        return { result, media: { video: video.summary(), audio: audio.summary() } };
      }),
    ).pipe(Effect.provide(clientLayer(target)));
    return yield* body.pipe(Effect.ensuring(afterSession(target, run, grant.jwt, rate)));
  });

/** The run-relative record of every item a scenario schedules, kept from as-run. */
const itemLog = (
  scheduler: Orchestration.SchedulerShape,
  run: Run,
  onStarted: (key: string, startedMs: number, seconds: number) => void = () => undefined,
) =>
  Effect.gen(function* () {
    const items = new Map<string, Mutable<RunItem>>();
    const starts: string[] = [];
    yield* scheduler.asRun.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          const item = items.get(event.key);
          if (item === undefined) return;
          const atMs = event.at - run.origin;
          const status = event.status;
          item.last = status._tag;
          switch (status._tag) {
            case "Ready":
              item.readyMs ??= atMs;
              break;
            case "Started":
              item.startedMs = atMs;
              item.seconds = status.durationSeconds;
              starts.push(event.key);
              onStarted(event.key, atMs, status.durationSeconds);
              break;
            case "Ended":
              item.endedMs = atMs;
              item.airedSeconds = round(status.airedSeconds, 3);
              item.termination = status.termination;
              break;
            case "Dropped":
              item.dropped = status.reason;
              break;
            default:
              break;
          }
        }),
      ),
      Effect.forkScoped,
    );
    yield* Effect.yieldNow;
    return {
      items,
      starts,
      track: (key: string) => {
        items.set(key, { key, submittedMs: since(run.origin), last: "Accepted" });
        return Orchestration.ItemKey.make(key);
      },
      snapshot: () => [...items.values()].map((item) => ({ ...item })),
    };
  });

/**
 * A boundary between two items as the decoded picture shows it: the longest
 * pause, the largest change and how it compares with the ending clip's own,
 * and its two sides written out for review.
 */
const measureSeam = (
  session: OwnedSession,
  window: number | undefined,
  ending: RunItem,
  next: RunItem,
  continued: boolean,
  label: string,
): ItemSeam => {
  const endedMs = ending.endedMs ?? next.startedMs;
  const startedMs = next.startedMs;
  if (endedMs === undefined || startedMs === undefined) {
    if (window !== undefined) session.seams.release(window);
    return { ending: ending.key, next: next.key, continued };
  }
  const fromMs = endedMs - seamBefore;
  const toMs = startedMs + seamAfter;
  const pause = session.video.pause(fromMs, toMs);
  const darkFrames = session.video.dark(fromMs, toMs);
  const jump = session.seams.jump(fromMs, toMs);
  const frames =
    window === undefined || jump === undefined
      ? []
      : session.seams.save(window, jump.atMs, session.directory, label);
  if (window !== undefined && frames.length === 0) session.seams.release(window);
  return {
    ending: ending.key,
    next: next.key,
    continued,
    endedMs,
    startedMs,
    ...(pause === undefined ? {} : { pause }),
    darkFrames,
    ...(jump === undefined ? {} : { jump }),
    ...(frames.length === 0 ? {} : { frames }),
  };
};

/** The inserts and batch of `scheduler-edits`, in the order they should air. */
const plannedEdits = ["p1", "xc", "p2", "xn", "p3", "y"] as const;

/**
 * 0.7.0's edit API on hosted H3, through the public scheduler: a line of three
 * beats, a clip inserted before the second continuing from the first, one
 * inserted before the third built on its own, and an edit batch that withdraws
 * a queued clip and inserts one after the playing clip, timed to take effect a
 * second before that clip ends. Every boundary's pause and join are measured,
 * and its two sides kept for review outside the evidence.
 */
const schedulerEdits = (target: Target, run: Run, budget: Budget) =>
  ownedSession(target, run, budget, "scheduler-edits", (session) =>
    Effect.gen(function* () {
      const evidence: Mutable<NonNullable<Draft["schedulerEdits"]>> = {
        items: [],
        plannedOrder: [...plannedEdits],
        startOrder: [],
        seams: [],
      };
      run.evidence.schedulerEdits = evidence;
      const scheduler = yield* Orchestration.makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "0 seconds", target: "1 second" },
          clip: () => itemRequest(),
        },
      }).pipe(Effect.provideService(Orchestration.Engine, session.owner.engine));
      // A window opens around each clip's expected end, so its boundary's frames are kept.
      const windows = new Map<string, number>();
      const log = yield* itemLog(scheduler, run, (key, startedMs, seconds) => {
        const endMs = startedMs + seconds * 1000;
        windows.set(key, session.seams.watch(endMs - seamBefore, endMs + seamAfter));
      });
      const record = () => {
        evidence.items = log.snapshot();
        evidence.startOrder = [...log.starts];
        save(run);
      };
      // Each boundary is measured, and its frames freed, once its window has passed.
      const measured = new Set<number>();
      const measure = (final: boolean) =>
        Effect.sync(() => {
          for (let index = 0; index + 1 < log.starts.length; index++) {
            if (measured.has(index)) continue;
            const ending = log.items.get(log.starts[index]!)!;
            const next = log.items.get(log.starts[index + 1]!)!;
            if (!final && (next.startedMs ?? Infinity) + seamAfter > session.now()) continue;
            measured.add(index);
            evidence.seams = [
              ...evidence.seams,
              measureSeam(
                session,
                windows.get(ending.key),
                ending,
                next,
                next.key === "xc",
                `${index + 1}-${ending.key}-${next.key}`,
              ),
            ];
          }
          record();
        });
      yield* measure(false).pipe(
        Effect.andThen(Effect.sleep("250 millis")),
        Effect.forever,
        Effect.forkScoped,
      );

      yield* recorded(run, session.owner.engine.setAutoplay(true));
      yield* recorded(
        run,
        scheduler.submitGroup({
          key: log.track("line"),
          lane: "line",
          parts: [
            { key: log.track("p1"), request: itemRequest() },
            { key: log.track("p2"), request: itemRequest() },
            { key: log.track("p3"), request: itemRequest() },
          ],
        }),
      );
      log.items.delete("line");
      yield* recorded(
        run,
        scheduler.submit({ key: log.track("w1"), lane: "line", request: itemRequest() }),
      );
      yield* mark(run, "line submitted");
      yield* session.seen(() => log.items.get("p1")?.startedMs);
      yield* recorded(
        run,
        scheduler.insert({
          key: log.track("xc"),
          request: itemRequest(),
          before: Orchestration.ItemKey.make("p2"),
          continuity: "previous",
        }),
      );
      yield* recorded(
        run,
        scheduler.insert({
          key: log.track("xn"),
          request: itemRequest(),
          before: Orchestration.ItemKey.make("p3"),
        }),
      );
      yield* mark(run, "inserted");
      record();

      // The batch should take effect about a second before p3 ends: its insert is
      // sent one measured build before that.
      const p3Started = yield* session.seen(() => log.items.get("p3")?.startedMs);
      const p3 = log.items.get("p3")!;
      const perSecond = (yield* scheduler.state).estimates.build?.median ?? 0.42;
      const endsMs = p3Started + (p3.seconds ?? clipSeconds) * 1000;
      yield* session.sleepUntil(endsMs - 1000 - perSecond * clipSeconds * 1000);
      const submittedMs = session.now();
      const batch = yield* recorded(
        run,
        scheduler.edit([
          { _tag: "Withdraw", key: Orchestration.ItemKey.make("w1") },
          {
            _tag: "Insert",
            insert: {
              key: log.track("y"),
              request: itemRequest(),
              after: Orchestration.ItemKey.make("p3"),
            },
          },
        ]),
      );
      evidence.batch = {
        submittedMs,
        withdrawn: ["w1"],
        inserted: "y",
        withdrawnStarted: false,
      };
      yield* batch.committed.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            evidence.batch = { ...evidence.batch!, committedMs: session.now() };
          }),
        ),
        Effect.ignore,
        Effect.forkScoped,
      );
      yield* mark(run, "batch submitted");
      const yStarted = yield* session.seen(() => log.items.get("y")?.startedMs);
      yield* session.sleepUntil(yStarted + seamAfter + 250);
      yield* measure(true);
      const state = yield* scheduler.state;
      evidence.estimates = {
        ...(state.estimates.build === undefined
          ? {}
          : {
              buildMedian: round(state.estimates.build.median, 3),
              buildP95: round(state.estimates.build.p95, 3),
            }),
        length: round(state.estimates.length, 4),
      };
      const w1 = log.items.get("w1")!;
      evidence.batch = {
        ...evidence.batch,
        ...(p3.endedMs === undefined ? {} : { boundaryMs: p3.endedMs }),
        withdrawnStarted: w1.startedMs !== undefined,
      };
      record();

      judge(
        run,
        "inserts and the batch air in their planned places",
        evidence.startOrder.join(",") === plannedEdits.join(",")
          ? undefined
          : `started in the order ${evidence.startOrder.join(", ")}`,
      );
      judge(
        run,
        "a batch takes effect before its boundary",
        evidence.batch.committedMs === undefined
          ? "the batch never took effect"
          : p3.endedMs !== undefined && evidence.batch.committedMs >= p3.endedMs
            ? "the batch took effect after the playing clip ended"
            : undefined,
      );
      judge(
        run,
        "a batch's withdrawn clip never starts",
        w1.startedMs !== undefined
          ? "the withdrawn clip started"
          : w1.dropped !== "withdrawn"
            ? `the withdrawn clip ended ${w1.last}`
            : undefined,
      );
      judge(
        run,
        "every seam measured",
        evidence.seams.length < plannedEdits.length - 1 ||
          evidence.seams.some((seam) => seam.pause === undefined || seam.jump === undefined)
          ? "a boundary has no pause or join measurement"
          : undefined,
      );
      yield* mark(run, "edits observed", `seam frames in ${session.directory}`);
    }),
  ).pipe(
    Effect.map(({ media }) => {
      run.evidence.schedulerEdits = { ...run.evidence.schedulerEdits!, media };
      save(run);
    }),
  );

/** How many enqueue-then-read probes `scheduler-cut` sends. */
const orderingProbes = 3;

/**
 * Raw H3 first, with autoplay off: position zero behind a build that is
 * running, how long a popped build holds the build slot, and whether a queue
 * read sent right behind an enqueue, before its reply, already lists the new
 * clip. Then the public scheduler with a cut lane: a 15 s clip plays and a
 * cut-lane clip stops it once Ready; the cut's seam is measured and kept.
 */
const schedulerCut = (target: Target, run: Run, budget: Budget) =>
  ownedSession(target, run, budget, "scheduler-cut", (session) =>
    Effect.gen(function* () {
      const evidence: Mutable<NonNullable<Draft["schedulerCut"]>> = { ordering: [], items: [] };
      run.evidence.schedulerCut = evidence;
      const { provider } = session;
      const marker = `hosted-qualification:${run.evidence.runId}:scheduler-cut`;
      const generated = new Map<string, number>();
      yield* provider.events({ capacity: 4096 }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event._tag !== "Message" || event.disposition !== "applied") return;
            const message = event.message;
            if (message.type === "clip_generated" && "clip" in message.data)
              generated.set(message.data.clip.clip_id, since(run.origin));
          }),
        ),
        Effect.forkScoped,
      );
      // `sending` completes as the enqueue commits, just before it is written to the channel.
      const submit = (name: string, position?: number, sending?: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          const submission = yield* provider.prepare(
            {
              prompt,
              seconds: clipSeconds,
              metadata: `${marker}:${name}`,
              ...(position === undefined ? {} : { position }),
            },
            sending === undefined
              ? {}
              : { commit: () => Deferred.succeed(sending, undefined).pipe(Effect.asVoid) },
          );
          const submittedMs = since(run.origin);
          const acceptance = yield* recorded(run, submission.submit);
          run.evidence.outcomes.push("replied");
          return {
            clipId: acceptance.clip.clip_id,
            submittedMs,
            ...(acceptance.evidence.kind === "correlated"
              ? { replySequence: acceptance.evidence.source.sequence }
              : {}),
          };
        });
      const read = Effect.gen(function* () {
        const reply = yield* recorded(run, provider.getQueue);
        run.evidence.outcomes.push("replied");
        return reply;
      });
      const queue = Effect.map(read, (reply) => reply.value);
      const pop = (clipId: string) =>
        recorded(run, provider.pop(clipId)).pipe(
          Effect.tap(() => Effect.sync(() => run.evidence.outcomes.push("replied"))),
          Effect.ignore,
        );

      yield* recorded(run, provider.setAutoplay(false));
      // A lone build, for comparison with one queued behind a popped build.
      const lone = yield* submit("lone");
      const loneReady = yield* session.seen(() => generated.get(lone.clipId));
      yield* pop(lone.clipId);

      // H3 documents position zero as next, behind the build that is running.
      const first = yield* submit("first");
      const zero = yield* submit("position-zero", 0);
      const tail = yield* submit("tail");
      const generationOrder = (yield* queue).generation.map((clip) => clip.clip_id);
      evidence.positionZero = {
        buildingClipId: first.clipId,
        requestedClipId: zero.clipId,
        tailClipId: tail.clipId,
        generationOrder,
      };
      // The first clip may have finished building before the read; if it is listed, it leads.
      const place = (clipId: string) => generationOrder.indexOf(clipId);
      judge(
        run,
        "position zero goes next, behind the build that is running",
        place(zero.clipId) < 0 || place(tail.clipId) < place(zero.clipId)
          ? "the position-zero clip was not ahead of the clip queued after it"
          : place(first.clipId) > place(zero.clipId)
            ? "the position-zero clip went ahead of the build that was running"
            : undefined,
      );
      yield* pop(zero.clipId);
      yield* pop(tail.clipId);
      const firstReady = yield* session.seen(() => generated.get(first.clipId));
      // H3 documents the running build as unaffected: it takes one build, as the lone one did.
      evidence.positionZero = {
        ...evidence.positionZero,
        buildingBuildMs: firstReady - first.submittedMs,
      };
      yield* pop(first.clipId);
      save(run);

      // A build popped while it runs: how long the clip queued behind it waits.
      const popMe = yield* submit("popped-build");
      const behind = yield* submit("behind-popped");
      yield* pop(popMe.clipId);
      const poppedMs = since(run.origin);
      const behindReady = yield* session.seen(() => generated.get(behind.clipId));
      evidence.poppedBuild = {
        poppedMs,
        nextSubmittedMs: behind.submittedMs,
        nextReadyMs: behindReady,
        nextBuildMs: behindReady - behind.submittedMs,
        loneBuildMs: loneReady - lone.submittedMs,
      };
      yield* pop(behind.clipId);
      save(run);

      // An enqueue, and a queue read sent as soon as the enqueue is on its way, long before
      // its reply can arrive.
      const probes: string[] = [];
      for (let index = 1; index <= orderingProbes; index++) {
        const name = `ordering-${index}`;
        const sending = yield* Deferred.make<void>();
        const enqueue = yield* Effect.forkChild(Effect.result(submit(name, undefined, sending)));
        yield* Deferred.await(sending);
        yield* Effect.sleep("1 millis");
        const sentMs = since(run.origin);
        const reply = yield* read;
        const listed = [...reply.value.generation, ...reply.value.playout].some((clip) =>
          clip.metadata.includes(`${marker}:${name}`),
        );
        const accepted = yield* Fiber.join(enqueue);
        if (Result.isSuccess(accepted)) probes.push(accepted.success.clipId);
        const enqueueSequence = Result.isSuccess(accepted)
          ? accepted.success.replySequence
          : undefined;
        evidence.ordering = [
          ...evidence.ordering,
          {
            sentMs,
            listed,
            clipKnown: Result.isSuccess(accepted),
            ...(enqueueSequence === undefined
              ? {}
              : { repliedFirst: reply.source.sequence < enqueueSequence }),
          },
        ];
      }
      for (const clipId of probes) yield* pop(clipId);
      judge(
        run,
        "a queue read sent right behind an enqueue lists the new clip",
        evidence.ordering.every((probe) => probe.listed)
          ? undefined
          : `${evidence.ordering.filter((probe) => !probe.listed).length} of ${evidence.ordering.length} reads did not list it`,
      );
      // Nothing raw may air once autoplay is on.
      const left = yield* queue;
      for (const clip of [...left.generation, ...left.playout]) yield* pop(clip.clip_id);
      yield* mark(run, "raw probes done");
      save(run);

      const scheduler = yield* Orchestration.makeScheduler({
        lanes: [{ name: "urgent", cut: true }, { name: "line" }],
        filler: {
          runway: { floor: "0 seconds", target: "1 second" },
          clip: () => itemRequest(),
        },
      }).pipe(Effect.provideService(Orchestration.Engine, session.owner.engine));
      const log = yield* itemLog(scheduler, run);
      yield* recorded(run, session.owner.engine.setAutoplay(true));
      yield* recorded(
        run,
        scheduler.submit({ key: log.track("long"), lane: "line", request: itemRequest(15) }),
      );
      const longStarted = yield* session.seen(() => log.items.get("long")?.startedMs);
      yield* session.sleepUntil(longStarted + 2_500);
      const cutterSubmittedMs = session.now();
      const window = session.seams.watch(cutterSubmittedMs, cutterSubmittedMs + 5_000);
      yield* recorded(
        run,
        scheduler.submit({ key: log.track("cutter"), lane: "urgent", request: itemRequest() }),
      );
      yield* mark(run, "cutter submitted");
      const cutterStarted = yield* session.seen(() => log.items.get("cutter")?.startedMs);
      yield* session.sleepUntil(cutterStarted + seamAfter + 250);
      const long = log.items.get("long")!;
      const cutter = log.items.get("cutter")!;
      evidence.items = log.snapshot();
      evidence.cut = {
        longKey: "long",
        cutterKey: "cutter",
        cutterSubmittedMs,
        seam: measureSeam(session, window, long, cutter, false, "cut-long-cutter"),
      };
      // H3's stop is not scoped to a clip: a second one stops whatever plays by then.
      const stops = run.spans
        .records()
        .filter(
          (span) =>
            span.name === "reactor.session.command" &&
            span.attributes["reactor.operation"] === "stop",
        ).length;
      judge(
        run,
        "a cut lane's clip stops a lower lane's playing clip",
        long.termination !== "stopped"
          ? `the long clip ended ${long.termination ?? long.last}`
          : log.starts[log.starts.indexOf("long") + 1] !== "cutter"
            ? "the cut-lane clip did not start next"
            : cutter.termination === "stopped"
              ? `the cut-lane clip was itself stopped after ${cutter.airedSeconds ?? "?"} s`
              : stops !== 1
                ? `${stops} stops were sent for one cut`
                : undefined,
      );
      save(run);
      yield* mark(run, "cut observed", `seam frames in ${session.directory}`);
    }),
  ).pipe(
    Effect.map(({ media }) => {
      run.evidence.schedulerCut = { ...run.evidence.schedulerCut!, media };
      save(run);
    }),
  );

/** The public renewal owner controls every activation; the harness only submits and observes. */
const pressureEvidence = (value: {
  readonly deliveredVideo: bigint;
  readonly deliveredAudio: bigint;
  readonly droppedVideo: bigint;
  readonly droppedAudio: bigint;
  readonly readerOverflows: bigint;
}): Pressure => ({
  deliveredVideo: String(value.deliveredVideo),
  deliveredAudio: String(value.deliveredAudio),
  droppedVideo: String(value.droppedVideo),
  droppedAudio: String(value.droppedAudio),
  readerOverflows: String(value.readerOverflows),
});
const schedulerRenewal = (target: Target, run: Run, budget: Budget) =>
  Effect.scoped(
    Effect.gen(function* () {
      const constructor =
        target.mode === "rehearsal" ? (target.renewalConstructor ?? "continuous") : "continuous";
      const time = yield* elapsedClock;
      const clock = yield* Clock.Clock;
      const attribution = new FrameAttribution();
      const video = new VideoReader(),
        audio = new AudioReader();
      const sources: {
        sessionId: string;
        generation: string;
        video: VideoReader;
        audio: AudioReader;
      }[] = [];
      let attributed = true;
      let videoLoss: Extract<Reactor.Recorded<VideoFrame>, { readonly _tag: "Lost" }> | undefined;
      let audioLoss: Extract<Reactor.Recorded<AudioFrame>, { readonly _tag: "Lost" }> | undefined;
      let evidence: SchedulerRenewal = {
        version: 1,
        clock: "effect-monotonic",
        scenario: "two-source-accepted-drain",
        configuration: {
          constructor,
          ...(constructor === "continuous"
            ? ({ retainedSuccessfulCleanups: 1, maxUnresolvedCleanups: 2 } as const)
            : {}),
          setupLimitMs: 20000,
          workLimitMs: 40000,
          cleanupLimitMs: 20000,
          leadMs: 40000,
          graceMs: 250,
          clipSeconds: 5,
          maxSessions: 2,
          maxOpenAttempts: 2,
        },
        openAttempts: 0,
        allocations: [],
        items: [],
        switches: [],
        fillerRequests: 0,
        fillerEvents: [],
        media: {
          video: video.summary(),
          audio: audio.summary(),
          sources: [],
          attributionComplete: true,
          audioCompleteness: "unverified",
        },
      };
      const failure = yield* Deferred.make<never, Reactor.ReactorFailure>();
      // The first failed write stops the scenario; every checkpoint is otherwise
      // best effort, so a lost write never skips a close. Cleanup re-raises it.
      let checkpointFailure: { readonly cause: unknown } | undefined;
      const snapshot = () => {
        const summaries = sources.map((source) => ({
          ...source,
          video: source.video.summary(),
          audio: source.audio.summary(),
        }));
        const oldId = evidence.allocations[0]?.sessionId,
          nextId = evidence.allocations[1]?.sessionId;
        const oldFrames = summaries
          .filter((source) => source.sessionId === oldId)
          .flatMap((source) => source.video.arrivalsMs ?? []);
        const nextFrames = summaries
          .filter((source) => source.sessionId === nextId)
          .flatMap((source) => source.video.arrivalsMs ?? []);
        const boundary =
          oldId !== undefined &&
          nextId !== undefined &&
          oldFrames.length > 0 &&
          nextFrames.length > 0
            ? {
                retiringSessionId: oldId,
                replacementSessionId: nextId,
                lastRetiringFrameMs: Math.max(...oldFrames),
                firstReplacementFrameMs: Math.min(...nextFrames),
                gapMs: Math.min(...nextFrames) - Math.max(...oldFrames),
              }
            : undefined;
        evidence = {
          ...evidence,
          media: {
            ...evidence.media,
            video: video.summary(),
            audio: audio.summary(),
            sources: summaries,
            attributionComplete: attributed,
            ...(boundary === undefined ? {} : { decodedBoundary: boundary }),
          },
        };
        run.evidence.schedulerRenewal = evidence;
        try {
          save(run);
        } catch (cause) {
          if (checkpointFailure !== undefined) return;
          checkpointFailure = { cause };
          Deferred.doneUnsafe(failure, Effect.die(cause));
        }
      };
      const allocation = (
        slot: 1 | 2,
        fields: Partial<SchedulerRenewal["allocations"][number]>,
      ) => {
        evidence = {
          ...evidence,
          allocations: evidence.allocations.map((entry) =>
            entry.slot === slot ? { ...entry, ...fields } : entry,
          ),
        };
        snapshot();
      };
      // Checked after each checkpoint that precedes a spend or a command. Once a
      // write has failed, nothing more is minted, opened or submitted: the SDK's
      // renewal fiber, which the failure race does not interrupt, opens sources too.
      const recordable = Effect.suspend(() =>
        checkpointFailure === undefined
          ? Effect.void
          : Reactor.ReactorError.fromCode(
              "InvalidState",
              "an evidence checkpoint failed, so nothing more is opened or submitted",
              { outcome: "not-submitted" },
            ),
      );
      // Every intermediate file remains failed until conclude validates complete evidence.
      run.evidence.verdict = "fail";
      run.evidence.reasons = ["scheduler renewal is incomplete"];
      snapshot();
      const observationScope = yield* Effect.scope;
      // A child scope: an owner the cleanup below never reaches still closes,
      // with its sources, when the scenario's own scope does.
      const ownerScope = yield* Scope.fork(observationScope);
      let handle:
        | { readonly _tag: "Legacy"; readonly owner: Orchestration.HandleShape }
        | { readonly _tag: "Continuous"; readonly owner: Orchestration.ContinuousHandleShape }
        | undefined;
      let firstAllocation: number | undefined;
      let prepared = false;
      let rate: { creditsPerSecond: number; creditsPerDollar: number } | undefined;
      const sourceReader = (frame: VideoFrame | AudioFrame) => {
        const tag = attribution.get(frame);
        if (tag === undefined) {
          attributed = false;
          return undefined;
        }
        let row = sources.find(
          (source) =>
            source.sessionId === tag.sessionId && source.generation === String(tag.generation),
        );
        if (row === undefined) {
          if (sources.length >= 16) {
            attributed = false;
            return undefined;
          }
          row = {
            sessionId: tag.sessionId,
            generation: String(tag.generation),
            video: new VideoReader(),
            audio: new AudioReader(),
          };
          sources.push(row);
        }
        return row;
      };
      const watch = <E extends Reactor.ReactorFailure>(effect: Effect.Effect<void, E>) =>
        effect.pipe(
          Effect.catchCause((cause) => Deferred.failCause(failure, cause)),
          Effect.forkScoped,
        );
      const waitFor = (predicate: () => boolean) =>
        Effect.gen(function* () {
          while (!predicate()) yield* Effect.sleep("10 millis");
        });
      const workDeadline = Effect.gen(function* () {
        for (;;) {
          const deadline = firstAllocation === undefined ? 20000 : firstAllocation + 40000;
          const left = deadline - time.now();
          if (left <= 0)
            return yield* Reactor.ReactorError.fromCode(
              "Timeout",
              "scheduler renewal exceeded its shared deadline",
            );
          yield* Effect.sleep(left);
        }
      });
      const body = Effect.gen(function* () {
        yield* recordable;
        const admittedRun = yield* admitted(target, run, budget, sessionsFor("scheduler-renewal"));
        rate = admittedRun.rate;
        yield* gate(() => acceptRenewalGrant(admittedRun.grant.granted));
        evidence = {
          ...evidence,
          allocations: [
            {
              slot: 1,
              grant: { ...admittedRun.grant.granted, expiresAt: admittedRun.grant.expiresAt },
            },
          ],
        };
        snapshot();
        yield* recordable;
        const coordinator = yield* Reactor.Coordinator.make({ apiUrl: target.apiUrl });
        const second = yield* coordinator.mintToken({
          apiKey: target.apiKey,
          modelName: H3.modelName,
          maxSessionDuration: `${sessionSeconds} seconds`,
          expiresAfter: `${tokenSeconds} seconds`,
        });
        run.secrets.push(Redacted.value(second.jwt));
        yield* gate(() => acceptRenewalGrant(second.granted));
        evidence = {
          ...evidence,
          allocations: [
            ...evidence.allocations,
            { slot: 2, grant: { ...second.granted, expiresAt: second.expiresAt } },
          ],
        };
        snapshot();
        const grants = [admittedRun.grant, second] as const;
        const open = Effect.gen(function* () {
          const index = evidence.openAttempts;
          evidence = { ...evidence, openAttempts: index + 1 };
          snapshot();
          yield* recordable;
          if (index >= grants.length) {
            const refused = Reactor.ReactorError.fromCode(
              "InvalidState",
              "scheduler renewal refused a third open attempt",
            );
            yield* Deferred.fail(failure, refused);
            return yield* refused;
          }
          const slot = index === 0 ? 1 : 2;
          let allocatedSession: Reactor.Session | undefined;
          const opened = yield* Orchestration.openH3({
            mint: Effect.succeed(grants[index]!),
            onAllocated: ({ session, allocation: record }) =>
              Effect.suspend(() => {
                allocatedSession = session;
                const allocatedMs = time.now();
                firstAllocation ??= allocatedMs;
                const wall = clock.currentTimeMillisUnsafe();
                allocation(slot, {
                  sessionId: session.id,
                  allocatedMs,
                  allocatedAt: DateTime.formatIso(DateTime.makeUnsafe(wall)),
                  capEndsAt: DateTime.formatIso(DateTime.makeUnsafe(record.endsAt! * 1000)),
                });
                // Keep the identity durable before rejecting a late first allocation;
                // the acquisition owner still needs to close that actual lease.
                return slot === 1 && allocatedMs > evidence.configuration.setupLimitMs
                  ? Reactor.ReactorError.fromCode("Timeout", "renewal setup deadline elapsed")
                  : Effect.void;
              }),
          }).pipe(
            Effect.onExit((exit) =>
              Effect.gen(function* () {
                if (Exit.isSuccess(exit)) return;
                // A failed open's own report is canonical whether or not it names a
                // session; an interrupted one is read back from its session, if any.
                const error = Cause.findErrorOption(exit.cause);
                const report =
                  Option.isSome(error) && Reactor.AcquisitionFailure.is(error.value)
                    ? error.value.cleanup
                    : allocatedSession === undefined
                      ? undefined
                      : (yield* allocatedSession.current).close;
                // With no session named and no report ruling allocation out, one may
                // exist under this grant: the stop rules and the operator must see it,
                // and the run stops before the SDK can try another allocation.
                const unknown = allocatedSession === undefined && report?.allocation !== "none";
                if (unknown) run.evidence.outcomes.push("unknown");
                if (report !== undefined || unknown)
                  allocation(slot, {
                    ...(report === undefined ? {} : { leaseCleanup: report, closedMs: time.now() }),
                    ...(unknown ? { allocation: "unknown" as const } : {}),
                  });
                if (unknown)
                  yield* Deferred.fail(
                    failure,
                    Reactor.ReactorError.fromCode(
                      "InvalidState",
                      "a source allocation's outcome is unknown",
                      { outcome: "unknown" },
                    ),
                  );
              }),
            ),
          );
          const decorated = attribution.source(opened.source);
          const source: Orchestration.Source = {
            ...decorated,
            prepareRouted: (plan, hooks = {}) =>
              decorated.prepareRouted(plan, {
                ...hooks,
                result: (id, result) =>
                  Effect.gen(function* () {
                    // The physical result hook names the source even when no provider
                    // clip could be correlated. It forwards the original outcome unchanged.
                    evidence = {
                      ...evidence,
                      items: evidence.items.map((item) =>
                        item.key === plan.request.metadata.qualificationKey
                          ? {
                              ...item,
                              sessionId: opened.source.id,
                              ...(Result.isSuccess(result) ? { clipId: result.success } : {}),
                            }
                          : item,
                      ),
                    };
                    if (Result.isFailure(result) && result.failure.context.outcome !== undefined)
                      run.evidence.outcomes.push(result.failure.context.outcome);
                    snapshot();
                    yield* hooks.result?.(id, result) ?? Effect.void;
                  }),
              }),
            close: Effect.gen(function* () {
              allocation(slot, { closeRequestedMs: time.now() });
              if (target.mode === "rehearsal" && target.faults?.includes("stallClose") === true)
                return yield* Effect.never;
              const cleanup = yield* opened.source.close;
              allocation(slot, { closedMs: time.now(), cleanup });
              if (!confirmedOwnedCleanup(cleanup, opened.source.id))
                yield* Deferred.fail(
                  failure,
                  Reactor.ReactorError.fromCode("Shutdown", "source cleanup was not confirmed"),
                );
              return cleanup;
            }),
          };
          return { source, lifetime: opened.lifetime };
        });
        const ownerOptions = {
          open,
          lead: "40 seconds",
          handoffGrace: "250 millis",
          maxSessions: 2,
        } as const;
        handle =
          constructor === "continuous"
            ? {
                _tag: "Continuous",
                owner: yield* Orchestration.makeContinuous({
                  ...ownerOptions,
                  retainedSuccessfulCleanups: 1,
                  maxUnresolvedCleanups: 2,
                }).pipe(Scope.provide(ownerScope)),
              }
            : {
                _tag: "Legacy",
                owner: yield* Orchestration.make(ownerOptions).pipe(Scope.provide(ownerScope)),
              };
        const owner = handle.owner;
        yield* watch(
          readInto(
            attribution.recorded(owner.media.video),
            {
              add: (element, atMs) => {
                video.add(element, atMs);
                if (element._tag === "Lost") videoLoss = element;
                else {
                  const source = sourceReader(element.frame);
                  if (videoLoss !== undefined) source?.video.add(videoLoss, atMs);
                  source?.video.add(element, atMs);
                  videoLoss = undefined;
                }
              },
            },
            time.elapsed,
          ),
        );
        yield* watch(
          readInto(
            attribution.recorded(owner.media.audio),
            {
              add: (element, atMs) => {
                audio.add(element, atMs);
                if (element._tag === "Lost") audioLoss = element;
                else {
                  const source = sourceReader(element.frame);
                  if (audioLoss !== undefined) source?.audio.add(audioLoss, atMs);
                  source?.audio.add(element, atMs);
                  audioLoss = undefined;
                }
              },
            },
            time.elapsed,
          ),
        );
        const observeState = (state: Orchestration.EngineState) => {
          const records = [
            ...state.queued,
            ...state.ready,
            ...Option.toArray(Option.map(state.building, (building) => building.record)),
            ...Option.toArray(Option.flatMap(state.playing, (playing) => playing.record)),
          ];
          evidence = {
            ...evidence,
            items: evidence.items.map((item) => {
              const record = records.find(
                (clip) => clip.request?.metadata.qualificationKey === item.key,
              );
              return record === undefined
                ? item
                : { ...item, clipId: record.clipId, sessionId: record.sessionId };
            }),
          };
          const preferred = Option.getOrUndefined(state.preferredSessionId);
          if (
            prepared &&
            preferred !== undefined &&
            preferred === evidence.allocations[1]?.sessionId &&
            evidence.prepared === undefined
          )
            evidence = {
              ...evidence,
              prepared: { atMs: time.now(), preferredSessionId: preferred },
            };
        };
        const observation = yield* owner.observe({ capacity: 4096 });
        observeState(observation.initial.engine);
        yield* watch(
          observation.events.pipe(
            Stream.runForEach((event) =>
              Effect.gen(function* () {
                if (event._tag === "Renewal") {
                  if (event.event._tag === "Prepared") prepared = true;
                  if (event.event._tag === "Switched") {
                    const renewal = event.event;
                    evidence = {
                      ...evidence,
                      switches: [
                        ...evidence.switches,
                        {
                          atMs: time.now(),
                          retiringSessionId: renewal.sessionId,
                          ...(renewal.handoff === undefined ? {} : { handoff: renewal.handoff }),
                          tail: {
                            ...renewal.tail,
                            sourceDrops: {
                              video:
                                renewal.tail.sourceDrops.video === null
                                  ? null
                                  : String(renewal.tail.sourceDrops.video),
                              audio:
                                renewal.tail.sourceDrops.audio === null
                                  ? null
                                  : String(renewal.tail.sourceDrops.audio),
                            },
                          },
                        },
                      ],
                    };
                  }
                }
                observeState(yield* owner.engine.state);
                snapshot();
              }),
            ),
          ),
        );
        const request = (key: string) =>
          new Orchestration.ClipRequest({
            prompt,
            references: [],
            durationSeconds: 5,
            metadata: { qualificationKey: key },
          });
        const scheduler = yield* Orchestration.makeScheduler(
          Orchestration.lineup({
            runway: { floor: 0, target: "5 seconds" },
            clip: () => {
              evidence = { ...evidence, fillerRequests: evidence.fillerRequests + 1 };
              return request("filler");
            },
          }),
        ).pipe(
          Effect.provideService(Orchestration.Engine, owner.engine),
          Scope.provide(ownerScope),
        );
        yield* scheduler.asRun.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              const status: SchedulerRenewal["items"][number]["statuses"][number] = {
                _tag: event.status._tag,
                atMs: time.now(),
                ...("sessionId" in event.status ? { sessionId: event.status.sessionId } : {}),
                ...("terminal" in event.status ? { terminal: event.status.terminal } : {}),
                ...(event.status._tag === "Ended" ? { termination: event.status.termination } : {}),
                ...(event.status._tag === "Failed"
                  ? { failureKind: event.status.reason._tag }
                  : {}),
              };
              evidence = {
                ...evidence,
                items: evidence.items.map((item) =>
                  item.key === event.key ? { ...item, statuses: [...item.statuses, status] } : item,
                ),
              };
              if (!evidence.items.some((item) => item.key === event.key))
                evidence = {
                  ...evidence,
                  fillerEvents: [...evidence.fillerEvents, { key: event.key, status }],
                };
              observeState(yield* owner.engine.state);
              snapshot();
              if (event.status._tag === "Unknown" || event.status._tag === "Failed") {
                if (event.status._tag === "Unknown") run.evidence.outcomes.push("unknown");
                yield* Deferred.fail(
                  failure,
                  Reactor.ReactorError.fromCode(
                    "InvalidState",
                    "scheduler item has no qualified outcome",
                  ),
                );
              }
            }),
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        const submit = (key: "qualification-A" | "qualification-B") =>
          Effect.gen(function* () {
            evidence = {
              ...evidence,
              items: [...evidence.items, { key, requestedSeconds: 5, statuses: [] }],
            };
            snapshot();
            yield* recordable;
            return yield* recorded(
              run,
              scheduler.submit({
                key: Orchestration.ItemKey.make(key),
                lane: "line",
                request: request(key),
              }),
            );
          });
        const a = yield* submit("qualification-A");
        const aStarted = yield* a.started;
        if (aStarted._tag !== "Started")
          return yield* Reactor.ReactorError.fromCode("InvalidState", "A did not start");
        const aEnded = yield* a.outcome;
        if (aEnded._tag !== "Ended")
          return yield* Reactor.ReactorError.fromCode("InvalidState", "A did not end");
        // Waiting for A's recorded Ended, not only its handle, puts that record
        // before the retiring close B's Ready unblocks, as the judgment requires.
        yield* waitFor(
          () =>
            evidence.prepared !== undefined &&
            evidence.items.some(
              (item) =>
                item.key === "qualification-A" &&
                item.statuses.some((status) => status._tag === "Ended"),
            ),
        );
        const b = yield* submit("qualification-B");
        yield* waitFor(
          () =>
            evidence.items[1]?.sessionId === evidence.allocations[1]?.sessionId &&
            evidence.items[1]!.statuses.some(
              (status) => status._tag === "Building" || status._tag === "Ready",
            ),
        );
        evidence = {
          ...evidence,
          drain: {
            requestedMs: time.now(),
            acceptedKeys: ["qualification-A", "qualification-B"],
            outcome: "pending",
            allocationsWhenRequested: evidence.allocations.filter(
              (slot) => slot.sessionId !== undefined,
            ).length,
          },
        };
        snapshot();
        yield* recordable;
        yield* recorded(run, scheduler.drain({ finish: "accepted" })).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              evidence = { ...evidence, drain: { ...evidence.drain!, outcome: "failed" } };
              snapshot();
            }),
          ),
        );
        evidence = {
          ...evidence,
          drain: {
            ...evidence.drain!,
            completedMs: time.now(),
            outcome: "completed",
            allocationsWhenCompleted: evidence.allocations.filter(
              (slot) => slot.sessionId !== undefined,
            ).length,
          },
        };
        if ((yield* b.outcome)._tag !== "Ended")
          return yield* Reactor.ReactorError.fromCode("InvalidState", "B did not end");
        yield* waitFor(
          () =>
            evidence.switches.length === 1 &&
            evidence.items[1]!.statuses.some((status) => status._tag === "Ended"),
        );
        evidence = {
          ...evidence,
          media: { ...evidence.media, pressure: pressureEvidence(yield* owner.media.pressure) },
        };
        snapshot();
      });
      yield* body.pipe(
        Effect.raceFirst(Deferred.await(failure)),
        Effect.raceFirst(workDeadline),
        Effect.onExit(() =>
          Effect.gen(function* () {
            evidence = {
              ...evidence,
              cleanup: {
                _tag: constructor === "continuous" ? "Continuous" : "Legacy",
                requestedMs: time.now(),
                incomplete: ["canonical close is pending"],
              },
            };
            snapshot();
            // A failed checkpoint is written, best effort, before SDK finalizers. An
            // uninterruptible source close still needs the separately documented
            // external emergency bound.
            let overdue = false;
            const closing = yield* Effect.gen(function* () {
              if (handle !== undefined) {
                const result =
                  handle._tag === "Continuous"
                    ? { _tag: "Continuous" as const, summary: yield* handle.owner.close }
                    : { _tag: "Legacy" as const, report: yield* handle.owner.close };
                evidence = {
                  ...evidence,
                  cleanup: {
                    ...result,
                    requestedMs: evidence.cleanup!.requestedMs,
                    completedMs: time.now(),
                    incomplete: overdue ? ["cleanup observation deadline elapsed"] : [],
                  },
                };
                snapshot();
              }
              yield* Scope.close(ownerScope, Exit.void);
            }).pipe(Effect.forkIn(observationScope));
            const closed = yield* Fiber.await(closing).pipe(
              Effect.asSome,
              Effect.interruptible,
              Effect.timeoutOrElse({
                duration: 20000,
                orElse: () =>
                  Effect.sync(() => {
                    overdue = true;
                    evidence = {
                      ...evidence,
                      cleanup: {
                        ...evidence.cleanup!,
                        incomplete: ["cleanup observation deadline elapsed"],
                      },
                    };
                    snapshot();
                    return Option.none();
                  }),
              }),
            );
            if (Option.isNone(closed))
              return yield* Reactor.ReactorError.fromCode(
                "Timeout",
                "canonical cleanup exceeded its observation budget",
              );
            if (Exit.isFailure(closed.value)) {
              evidence = {
                ...evidence,
                cleanup: { ...evidence.cleanup!, incomplete: ["canonical close failed"] },
              };
              snapshot();
              return yield* Effect.failCause(closed.value.cause);
            }
            if (
              rate !== undefined &&
              evidence.allocations.every(
                (slot) => slot.closedMs !== undefined && slot.allocatedMs !== undefined,
              )
            )
              run.evidence.budget.estimatedUsd = round(
                evidence.allocations.reduce(
                  (sum, slot) =>
                    sum + billedUsd(rate!, (slot.closedMs! - slot.allocatedMs!) / 1000),
                  0,
                ),
              );
            snapshot();
          }).pipe(
            // Only once every close above has run does a failed checkpoint fail the run.
            Effect.ensuring(
              Effect.suspend(() =>
                checkpointFailure === undefined ? Effect.void : Effect.die(checkpointFailure.cause),
              ),
            ),
          ),
        ),
      );
      run.evidence.criteria.push(...renewalJudgments(run.evidence));
      snapshot();
    }),
  ).pipe(Effect.provide(clientLayer(target)));

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

/** Rehearsal only: from the first save `from` matches on, every save fails, as on a full disk. */
class WriteFault extends Writer {
  private failing = false;
  constructor(
    path: string,
    secrets: () => readonly string[],
    private readonly from: (evidence: Draft) => boolean,
  ) {
    super(path, secrets);
  }

  override save(evidence: Draft): void {
    this.failing ||= this.from(evidence);
    if (this.failing) throw new Error("fixture: the evidence file could not be written");
    super.save(evidence);
  }
}

/** The rehearsal faults that fail evidence writes, by the checkpoint they start at. */
const writeFaults = new Map<string, (evidence: Draft) => boolean>([
  ["failCleanupWrites", (evidence) => evidence.schedulerRenewal?.cleanup !== undefined],
  ["failSecondOpenWrites", (evidence) => (evidence.schedulerRenewal?.openAttempts ?? 0) >= 2],
]);

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
  const failsFrom = target.faults
    ?.map((fault) => writeFaults.get(fault))
    .find((from) => from !== undefined);
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
    writer:
      failsFrom === undefined
        ? new Writer(file, () => secrets)
        : new WriteFault(file, () => secrets, failsFrom),
  };
  save(run);
  try {
    return await runClaimed(target, check, budget, run, file);
  } catch (cause) {
    // The file exists and may hold this run's reservation, so whatever broke,
    // the run failed: only a refusal before the file existed exits 2.
    const reason = `the run could not be concluded: ${describe(cause)}`;
    try {
      run.evidence.verdict = "fail";
      run.evidence.reasons = [...run.evidence.reasons, reason];
      save(run);
    } catch {
      // The file keeps its last checkpoint; the reason is printed below.
    }
    console.log(`hosted-qualification-fail ${check} ${file}`);
    console.log(`  - ${reason}`);
    // Sessions may exist although the run could not be concluded; the operator
    // still needs their identities and the dashboard instructions.
    try {
      console.log(`  - ${sessionsLine(run.evidence)}`);
      const instructions = cleanupInstructions(run.evidence);
      if (instructions !== undefined) console.error(instructions);
    } catch {
      // The reason above still fails the run.
    }
    return 1;
  }
};

/** Without a complete durable record, these are what billing is reconciled against. */
const sessionsLine = (evidence: Run["evidence"]): string => {
  const sessions = recordedSessions(evidence);
  return `sessions this run recorded: ${sessions.length === 0 ? "none" : sessions.join(", ")}`;
};

/** A thrown value as bounded text, even one that cannot print itself. */
const describe = (cause: unknown): string => {
  try {
    return String(cause).slice(0, 300);
  } catch {
    return "a value that could not be printed";
  }
};

/** Runs the check whose file `execute` claimed and concludes its evidence; the exit code. */
const runClaimed = async (
  target: Target,
  check: Check,
  budget: Budget,
  run: Run,
  file: string,
): Promise<number> => {
  type CheckProgram =
    | ReturnType<typeof schedulerRenewal>
    | ReturnType<typeof scheduler>
    | ReturnType<typeof schedulerEdits>;
  const selected: Effect.Effect<
    void,
    Effect.Error<CheckProgram>,
    Effect.Services<CheckProgram>
  > = check === "scheduler-renewal"
    ? schedulerRenewal(target, run, budget)
    : check === "scheduler"
      ? scheduler(target, run, budget)
      : check === "scheduler-edits"
        ? schedulerEdits(target, run, budget)
        : check === "scheduler-cut"
          ? schedulerCut(target, run, budget)
          : check === "takeover" || check === "resume"
            ? takeover(target, run, budget, check)
            : vertical(target, run, budget, check);
  const program = selected.pipe(
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
  const instructions = cleanupInstructions(run.evidence);
  if (instructions !== undefined) run.evidence.cleanup = instructions;
  try {
    save(run);
  } catch (cause) {
    // The check ran, so its file already holds whatever it reserved. A record
    // that could not be completed fails the run; only a refusal exits 2.
    run.evidence.verdict = "fail";
    run.evidence.reasons = [
      ...run.evidence.reasons,
      `the final evidence was not saved: ${describe(cause)}`,
      sessionsLine(run.evidence),
    ];
  }
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
    given = options(args.slice(1), ["faults", "ledger", "constructor"]);
    if (
      given.has("constructor") &&
      (check !== "scheduler-renewal" ||
        !["legacy", "continuous"].includes(given.get("constructor")!))
    )
      throw new Refused({
        message: "--constructor=legacy|continuous is only for scheduler-renewal rehearsal",
      });
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
        faults,
        renewalConstructor: given.get("constructor") === "legacy" ? "legacy" : "continuous",
      },
      check,
      {
        checkUsd: ceilingFor(check),
        totalUsd: maxTotalUsd,
        reservedUsd: 0,
      },
      ledger,
    );
  } finally {
    // What the stand-in coordinator allocated: evidence that could not be written cannot say.
    const open = [...twin.sessions.values()].filter((session) => session.state !== "CLOSED");
    console.log(`rehearsal twin: ${twin.sessionsCreated} created, ${open.length} not closed`);
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
        `ledger ${file}: ${evidence.check} ${rejudged(evidence).verdict ?? "unfinished"}, reserved $${reservedUsd(evidence).toFixed(4)}`,
      );
    // Pricing needs no credential; only minting does.
    const target = paidTarget("preflight", given.has("--no-mint") ? Redacted.make("") : apiKey());
    const report = await Effect.runPromise(
      Effect.gen(function* () {
        const coordinator = yield* Reactor.Coordinator.make({ apiUrl: target.apiUrl });
        const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
        // Each session counts at what admission would reserve for it, rounded up.
        const worst = reservationUsd(worstCaseUsd(rate));
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
        .filter(({ evidence }) => rejudged(evidence).verdict === "pass")
        .map(({ evidence }) => evidence.check),
    );
    const order = (["vertical", "takeover"] as const).filter((check) => !passed.has(check));
    // Informational only: each runs in its own release's ledger, so neither sets the exit code.
    const later = (["scheduler", "scheduler-renewal"] as const)
      .filter((check) => !passed.has(check))
      .map(
        (check) =>
          `${check} reserves ${sessionsFor(check)} capped session${sessionsFor(check) === 1 ? "" : "s"}`,
      );
    console.log(
      `next: ${order.length === 0 ? "nothing required" : order.join(", then ")}; turn only if no run selected a relay pair${later.length === 0 ? "" : `; ${later.join(" and ")}, in their release's ledger`}`,
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
