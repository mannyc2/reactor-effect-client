/**
 * The hosted qualification: paid checks against hosted Reactor, run only by a
 * maintainer who authorizes the spend. Nothing in CI or `verify` runs it.
 *
 *   bun integration/hosted/qualify.ts <vertical|takeover|turn> \
 *     --budget-usd=0.50 --evidence=<new file> --i-authorize-paid-sessions
 *
 * `REACTOR_API_KEY` mints one token per check, for one session capped at 60 s
 * server-side; `REACTOR_API_URL` overrides the coordinator. Each check refuses
 * before allocating unless the published rate fits its budget and the
 * returned token grants no more than it asked for. It stops at the first
 * unknown outcome or unconfirmed termination, writes its evidence (never a
 * token) to a file that must not exist yet, and never repeats by itself.
 *
 * - vertical: allocate, register the owner, connect, enqueue a clip, and
 *   require correlated acceptance, lifecycle progression, changing non-black
 *   BGRA frames, audio when the session offers it, the selected ICE pair and
 *   confirmed termination.
 * - takeover: an owner process allocates and streams, is killed, and this
 *   process attaches to the same session within 5 s with the persisted grant,
 *   identifies the accepted clip without enqueueing, sees fresh frames, and
 *   terminates the session as its durable owner.
 * - turn: the vertical check, which passes only on a relay pair; run it only
 *   when neither earlier check selected one (pass their evidence as
 *   `--previous=<file>`), from a network that blocks direct paths.
 */
import { spawn } from "node:child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Native from "reactor-effect-native";
import {
  Refused,
  acceptGrant,
  admit,
  authorize,
  liveVideo,
  sessionSeconds,
  stopFor,
  tokenSeconds,
} from "./gates.js";
import type { Authorization, Outcome } from "./gates.js";

const prompt = "A slow camera move across a sunlit table with a glass of water.";

interface Evidence {
  check: string;
  startedAt: string;
  worstCaseUsd?: number;
  grant?: { maxSessions: number; maxSessionSeconds: number; expiresAt: number };
  sessionId?: string;
  acceptance?: { clipId: string; evidence: string; generation: string };
  lifecycle?: Record<string, { message: string; transportGeneration: string }>;
  media?: { frames: number; format: string[]; audioBlocks: number; audioOffered: boolean };
  icePair?: { local: string | null; remote: string | null };
  takeover?: { attachMs: number; clipIdentified: boolean; freshFrames: number };
  termination?: { attempted: boolean; confirmed: boolean; evidence: string | null };
  outcomes: Outcome[];
  verdict?: "pass" | "fail";
  reason?: string;
}

const outcomeOf = (error: unknown): Outcome | undefined =>
  Reactor.isReactorFailure(error) ? error.context.outcome : undefined;

const refusal = (cause: unknown): Refused =>
  cause instanceof Refused ? cause : new Refused({ message: String(cause) });

const settings = Effect.gen(function* () {
  const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
  const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
    Config.withDefault("https://api.reactor.inc"),
  );
  return { apiKey, apiUrl };
});

/** The rate and grant gates: nothing is allocated unless both pass. */
const grantFor = (auth: Authorization, evidence: Evidence) =>
  Effect.gen(function* () {
    const { apiKey, apiUrl } = yield* settings;
    const coordinator = yield* Reactor.Coordinator.make({ apiUrl });
    const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
    evidence.worstCaseUsd = yield* Effect.try({
      try: () => admit(rate, auth.budgetUsd),
      catch: refusal,
    });
    const grant = yield* coordinator.mintToken({
      apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
      expiresAfter: `${tokenSeconds} seconds`,
    });
    yield* Effect.try({ try: () => acceptGrant(grant.granted), catch: refusal });
    evidence.grant = { ...grant.granted, expiresAt: grant.expiresAt };
    return { grant, apiUrl };
  });

const clientLayer = (apiUrl: string) =>
  Reactor.layer({ apiUrl }).pipe(
    Layer.provideMerge(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
  );

/** One paid session through the public opener, observed end to end. */
const vertical = (auth: Authorization, evidence: Evidence, requireRelay: boolean) =>
  Effect.gen(function* () {
    const { grant, apiUrl } = yield* grantFor(auth, evidence);
    const closed = yield* Effect.scoped(
      Effect.gen(function* () {
        let owned: Reactor.Session | undefined;
        const opened = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          onAllocated: ({ session }) =>
            Effect.sync(() => {
              owned = session;
              evidence.sessionId = session.id;
            }),
        });
        const session = owned!;
        const provider = opened.source.provider;
        const submission = yield* provider.prepare({ prompt, seconds: 5 });
        const acceptance = yield* submission.submit.pipe(
          Effect.tapError((error) =>
            Effect.sync(() => {
              const outcome = outcomeOf(error);
              if (outcome !== undefined) evidence.outcomes.push(outcome);
            }),
          ),
        );
        evidence.outcomes.push("replied");
        evidence.acceptance = {
          clipId: acceptance.clip.clip_id,
          evidence: acceptance.evidence.kind,
          generation: String(acceptance.evidence.source.generation),
        };
        const op = yield* provider.operation(submission);
        const started = yield* op.reached("started").pipe(Effect.timeout("45 seconds"));
        const facts = yield* op.facts;
        evidence.lifecycle = Object.fromEntries(
          (["generated", "started"] as const).flatMap((phase) => {
            const fact = phase === "generated" ? facts.generated : started;
            return fact === undefined
              ? []
              : [
                  [
                    phase,
                    {
                      message: fact.message,
                      transportGeneration: String(fact.transportGeneration),
                    },
                  ],
                ];
          }),
        );
        const media = yield* Native.media(session);
        const frames = yield* media
          .video("main_video")
          .pipe(Stream.take(24), Stream.runCollect, Effect.timeout("20 seconds"));
        const audioOffered = media.tracks.some(
          (track) => track.kind === "audio" && track.direction === "recvonly",
        );
        const audio = audioOffered
          ? yield* media
              .audio("main_audio")
              .pipe(Stream.take(50), Stream.runCollect, Effect.timeout("10 seconds"))
          : [];
        evidence.media = {
          frames: frames.length,
          format: [...new Set(frames.map((frame) => frame.format))],
          audioBlocks: audio.length,
          audioOffered,
        };
        const stats = yield* session.stats;
        evidence.icePair = {
          local: stats.pair?.localCandidateType ?? null,
          remote: stats.pair?.remoteCandidateType ?? null,
        };
        const video = liveVideo(frames);
        if (video !== undefined) return yield* new Refused({ message: video });
        if (audioOffered && audio.length === 0)
          return yield* new Refused({ message: "the session offered audio and none arrived" });
        if (
          requireRelay &&
          evidence.icePair.local !== "relay" &&
          evidence.icePair.remote !== "relay"
        )
          return yield* new Refused({ message: "the selected pair was not a relay pair" });
        return yield* session.close;
      }).pipe(Effect.provide(clientLayer(apiUrl))),
    );
    evidence.termination = {
      attempted: closed.remote.attempted,
      confirmed: closed.remote.confirmed,
      evidence: closed.remote.evidence,
    };
  });

/** The owner of the takeover: allocate, stream, report, then wait to be killed. */
const owner = (grantFile: string, recordFile: string) =>
  Effect.gen(function* () {
    const { apiUrl } = yield* settings;
    const stored = JSON.parse(readFileSync(grantFile, "utf8")) as {
      jwt: string;
      expiresAt: number;
      granted: { maxSessions: 1; maxSessionSeconds: number };
    };
    const grant = { ...stored, jwt: Redacted.make(stored.jwt) };
    return yield* Effect.scoped(
      Effect.gen(function* () {
        let owned: Reactor.Session | undefined;
        const opened = yield* Orchestration.openH3({
          mint: Effect.succeed(grant),
          // The durable owner record: what a later process needs to take over.
          onAllocated: ({ session }) =>
            Effect.sync(() => {
              owned = session;
              writeFileSync(
                recordFile,
                JSON.stringify({
                  sessionId: session.id,
                  jwt: stored.jwt,
                  expiresAt: grant.expiresAt,
                }),
                { mode: 0o600 },
              );
            }),
        });
        const submission = yield* opened.source.provider.prepare({ prompt, seconds: 5 });
        const acceptance = yield* submission.submit;
        const op = yield* opened.source.provider.operation(submission);
        yield* op.reached("started").pipe(Effect.timeout("45 seconds"));
        const media = yield* Native.media(owned!);
        yield* media.video("main_video").pipe(Stream.take(24), Stream.runDrain);
        yield* Console.log(`owner-streaming ${owned!.id} ${acceptance.clip.clip_id}`);
        return yield* Effect.never;
      }).pipe(Effect.provide(clientLayer(apiUrl))),
    );
  });

/** Kill the owner while it streams and take the session over from here. */
const takeover = (auth: Authorization, evidence: Evidence) =>
  Effect.gen(function* () {
    const { grant, apiUrl } = yield* grantFor(auth, evidence);
    const directory = join(tmpdir(), `reactor-takeover-${process.pid}`);
    const grantFile = `${directory}.grant.json`,
      recordFile = `${directory}.owner.json`;
    writeFileSync(grantFile, JSON.stringify({ ...grant, jwt: Redacted.value(grant.jwt) }), {
      mode: 0o600,
    });
    try {
      const child = spawn(process.execPath, [process.argv[1]!, "owner", grantFile, recordFile], {
        stdio: ["ignore", "pipe", "inherit"],
        env: process.env,
      });
      const streaming = yield* Effect.callback<{ sessionId: string; clipId: string }, Refused>(
        (resume) => {
          const lines = createInterface({ input: child.stdout });
          lines.on("line", (line) => {
            const [tag, sessionId, clipId] = line.split(" ");
            if (tag === "owner-streaming" && sessionId !== undefined && clipId !== undefined)
              resume(Effect.succeed({ sessionId, clipId }));
          });
          child.once("exit", () =>
            resume(Effect.fail(new Refused({ message: "the owner exited early" }))),
          );
        },
      ).pipe(Effect.timeout("90 seconds"));
      evidence.sessionId = streaming.sessionId;
      const record = JSON.parse(readFileSync(recordFile, "utf8")) as {
        sessionId: string;
        jwt: string;
      };
      child.kill("SIGKILL");
      const killedAt = performance.now();
      const taken = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Reactor.Client;
          const session = yield* client.attachConnected({
            sessionId: record.sessionId,
            jwt: Redacted.make(record.jwt),
          });
          const attachMs = performance.now() - killedAt;
          const provider = yield* H3.make(session);
          const snapshot = yield* provider.current;
          const media = yield* Native.media(session);
          const fresh = yield* media
            .video("main_video")
            .pipe(Stream.take(12), Stream.runCollect, Effect.timeout("20 seconds"));
          return {
            attachMs,
            clipIdentified: snapshot.clips.some((clip) => clip.clip.clip_id === streaming.clipId),
            freshFrames: fresh.length,
            live: liveVideo(fresh),
          };
        }).pipe(Effect.provide(clientLayer(apiUrl))),
      );
      evidence.takeover = {
        attachMs: Math.round(taken.attachMs),
        clipIdentified: taken.clipIdentified,
        freshFrames: taken.freshFrames,
      };
      // The attached process never owned the session: its durable owner record
      // terminates it, with the persisted grant as the credential.
      const coordinator = yield* Reactor.Coordinator.make({
        apiUrl,
        credential: Effect.succeed(Redacted.make(record.jwt)),
      });
      const termination = yield* coordinator.terminate(record.sessionId);
      evidence.termination = {
        attempted: termination.attempted,
        confirmed: termination.confirmed,
        evidence: termination.evidence,
      };
      if (taken.attachMs > 5000)
        return yield* new Refused({ message: "attaching took longer than 5 s after the kill" });
      if (!taken.clipIdentified)
        return yield* new Refused({ message: "the accepted clip was not identified after attach" });
      if (taken.live !== undefined) return yield* new Refused({ message: taken.live });
    } finally {
      rmSync(grantFile, { force: true });
      rmSync(recordFile, { force: true });
    }
  }).pipe(Effect.provide(Reactor.FetchHttp.layer));

const main = async (args: readonly string[]): Promise<number> => {
  if (args[0] === "owner") {
    await Effect.runPromise(
      owner(args[1]!, args[2]!).pipe(Effect.provide(Reactor.FetchHttp.layer)),
    );
    return 0;
  }
  let auth: Authorization;
  try {
    auth = authorize(args);
  } catch (cause) {
    console.error(`hosted-qualification-refused ${(cause as Error).message}`);
    return 2;
  }
  if (existsSync(auth.evidence)) {
    console.error(
      "hosted-qualification-refused the evidence file exists; a check never repeats itself",
    );
    return 2;
  }
  if (auth.check === "turn")
    for (const previous of args.filter((arg) => arg.startsWith("--previous=")))
      if (
        JSON.stringify(JSON.parse(readFileSync(previous.slice(11), "utf8"))).includes('"relay"')
      ) {
        console.error(
          "hosted-qualification-refused an earlier check already selected a relay pair",
        );
        return 2;
      }
  const evidence: Evidence = {
    check: auth.check,
    startedAt: new Date().toISOString(),
    outcomes: [],
  };
  const run =
    auth.check === "takeover"
      ? takeover(auth, evidence)
      : vertical(auth, evidence, auth.check === "turn").pipe(
          Effect.provide(Reactor.FetchHttp.layer),
        );
  const exit = await Effect.runPromiseExit(run);
  const stopped = stopFor({
    outcomes: evidence.outcomes,
    terminationConfirmed: evidence.termination?.confirmed,
  });
  evidence.verdict = exit._tag === "Success" && stopped === undefined ? "pass" : "fail";
  if (evidence.verdict === "fail")
    evidence.reason =
      stopped ??
      (exit._tag === "Failure" ? String(exit.cause).slice(0, 2000) : "the check did not complete");
  writeFileSync(auth.evidence, `${JSON.stringify(evidence, null, 2)}\n`, { flag: "wx" });
  console.log(`hosted-qualification-${evidence.verdict} ${auth.check} ${auth.evidence}`);
  return evidence.verdict === "pass" ? 0 : 1;
};

if (import.meta.main) process.exitCode = await main(process.argv.slice(2));
