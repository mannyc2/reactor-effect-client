/**
 * One run of a check, gates first: the ledger's lock, the published rate
 * against the run's budget and what the ledger has left, then an evidence file
 * claimed with the run's worst case before any token exists. From that point a
 * failure is a failed run, never a refusal, and the session is still closed.
 */
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as H3 from "reactor-effect-client/H3";
import { isReactorFailure } from "reactor-effect-client/ReactorError";
import { checks } from "./Checks.js";
import type { Evidence } from "./Evidence.js";
import { format, judged } from "./Evidence.js";
import * as Ledger from "./Ledger.js";
import * as Run from "./Run.js";
import type { Authorization } from "./Spend.js";
import { admit, admitRelay, Refused } from "./Spend.js";
import { Target } from "./Target.js";

/** A failure as the evidence states it: the library's own message, never provider text. */
export const describe = (cause: Cause.Cause<unknown>): string => {
  if (Cause.hasInterruptsOnly(cause)) return "the run was interrupted";
  const error = Cause.squash(cause);
  if (Schema.is(Refused)(error)) return error.message;
  if (Cause.isTimeoutError(error)) return "a step ran past its deadline";
  if (isReactorFailure(error))
    return `${error.reason._tag}: ${error.message}${error.context.outcome === undefined ? "" : ` (outcome ${error.context.outcome})`}`;
  if (Schema.is(Ledger.SaveFailed)(error)) return error.message;
  return `unexpected: ${String(error).slice(0, 300)}`;
};

const Manifest = Schema.fromJsonString(
  Schema.Struct({ name: Schema.String, version: Schema.String }),
);

/** Where the run happens: runtime, commit and the packages as they resolve from here. */
const environment = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const target = yield* Target;
  const here = path.dirname(yield* path.fromFileUrl(new URL(import.meta.url)));
  const git = (...args: ReadonlyArray<string>) =>
    spawner.string(ChildProcess.make("git", args, { cwd: here })).pipe(Effect.option);
  const version = (name: string) =>
    Effect.gen(function* () {
      let directory = path.dirname(yield* path.fromFileUrl(new URL(import.meta.resolve(name))));
      for (;;) {
        const manifest = yield* fs
          .readFileString(path.join(directory, "package.json"))
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Manifest)), Effect.option);
        if (Option.isSome(manifest) && manifest.value.name === name) return manifest.value.version;
        const parent = path.dirname(directory);
        if (parent === directory) return yield* Effect.fail("not found");
        directory = parent;
      }
    }).pipe(Effect.option);
  const packages: Record<string, string> = {};
  for (const name of [
    "reactor-effect-client",
    "reactor-effect-native",
    "effect",
    "@effect/platform-node",
  ]) {
    const found = yield* version(name);
    if (Option.isSome(found)) packages[name] = found.value;
  }
  const commit = yield* git("rev-parse", "HEAD");
  const status = yield* git("status", "--porcelain", "--untracked-files=no");
  return {
    runtime:
      process.versions.bun === undefined
        ? `node ${process.version}`
        : `bun ${process.versions.bun}`,
    os: `${process.platform} ${process.arch}`,
    ...(Option.isSome(commit) ? { commit: commit.value.trim() } : {}),
    ...(Option.isSome(status) ? { dirty: status.value.trim().length > 0 } : {}),
    packages,
    network: target.network,
    apiOrigin: new URL(target.apiUrl).origin,
  } satisfies Evidence["environment"];
});

/**
 * Runs `authorization.check` into `ledger` and returns its judged evidence.
 * It fails with `Refused` only before the evidence file is claimed, and so
 * before anything was spent.
 */
export const execute = (input: {
  readonly authorization: Authorization;
  readonly ledger: string;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { authorization, ledger } = input;
      const target = yield* Target;
      const path = yield* Path.Path;
      yield* Ledger.lock(ledger);
      const earlier = yield* Ledger.entries(ledger);
      const reservedUsd = earlier.reduce((total, run) => total + Ledger.reserved(run), 0);
      if (authorization.check === "turn" && target.mode === "paid")
        yield* admitRelay(
          earlier.flatMap((run) =>
            run.mode === "paid" && run.network?.pair !== undefined ? [run.network.pair] : [],
          ),
        );
      const coordinator = yield* Coordinator.Coordinator;
      const rate = yield* coordinator.pricing.pipe(
        Effect.flatMap((pricing) => Coordinator.modelRate(pricing, H3.modelName)),
        Effect.mapError((error) => Refused.make({ message: `pricing: ${error.message}` })),
      );
      const worstCaseUsd = yield* admit({ rate, authorization, reservedUsd });
      const runId = (yield* Random.nextIntBetween(0, 0xffffffff)).toString(16).padStart(8, "0");
      const startedAt = DateTime.formatIso(yield* DateTime.now);
      const stamp = startedAt.replaceAll(/[-:]|\.\d+/g, "");
      const initial: Evidence = {
        format,
        runId,
        check: authorization.check,
        mode: target.mode,
        startedAt,
        environment: yield* environment,
        budget: {
          checkUsd: authorization.budgetUsd,
          totalUsd: authorization.totalUsd,
          reservedBeforeUsd: reservedUsd,
          rate,
          worstCaseUsd,
        },
        grants: [],
        sessions: [],
        milestones: [],
        outcomes: [],
        criteria: [],
        spans: [],
        reasons: [],
        missing: [],
      };
      const run = yield* Run.make(
        initial,
        path.join(ledger, `${stamp}-${authorization.check}-${target.mode}-${runId}.json`),
      );
      yield* run.secret(target.apiKey);
      // The first save claims the file and records the reservation; failing it spent nothing.
      yield* run
        .mark("admitted", `worst case $${worstCaseUsd.toFixed(4)}`)
        .pipe(Effect.mapError((error) => Refused.make({ message: error.message })));
      const exit = yield* checks[authorization.check].pipe(
        Effect.provideService(Run.Run, run),
        Effect.withTracer(run.tracer),
        Effect.exit,
      );
      const finishedAt = DateTime.formatIso(yield* DateTime.now);
      const evidence = judged({
        evidence: { ...(yield* run.evidence), finishedAt },
        failure: Exit.isFailure(exit) ? describe(exit.cause) : undefined,
      });
      yield* run.update(() => evidence);
      yield* run.save.pipe(
        Effect.catch((error) =>
          Effect.logError(
            `${error.message}; the run held sessions ${evidence.sessions.map((session) => session.id).join(", ") || "none"}`,
          ),
        ),
      );
      return evidence;
    }),
  );
