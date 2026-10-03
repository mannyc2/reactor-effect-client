/**
 * One run of a check, gates first: the ledger's lock, the published rate
 * against the run's budget and what the ledger has left, then an evidence file
 * claimed with the run's worst case before any token exists. From that point a
 * failure is a failed run, never a refusal, and the session is still closed.
 */
import { createRequire } from "node:module";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import { checks } from "./Checks.js";
import type { Evidence } from "./Evidence.js";
import { format, judged } from "./Evidence.js";
import * as Ledger from "./Ledger.js";
import * as Run from "./Run.js";
import type { Authorization } from "./Spend.js";
import { admit, admitRelay, plans, Refused } from "./Spend.js";
import { Target } from "./Target.js";

const Manifest = Schema.fromJsonString(
  Schema.Struct({ name: Schema.String, version: Schema.String }),
);

/** What the native addon's platform package says it was built from; staging writes it. */
const NativeIdentity = Schema.fromJsonString(
  Schema.Struct({
    platform: Schema.String,
    file: Schema.String,
    sha256: Schema.String,
    build: Schema.Struct({
      sourceSha256: Schema.String,
      target: Schema.String,
      webrtcPrebuilt: Schema.String,
    }),
  }),
);

/** The platform packages the native peer loads, by Node's platform and architecture. */
const addonPlatforms: Readonly<Record<string, string>> = {
  "darwin-arm64": "darwin-arm64",
  "linux-x64": "linux-x64-gnu",
};

/**
 * The identity of the addon the native peer loads here: its platform package's
 * `native-identity.json`, found as the binding finds the package. None when no
 * platform package is installed.
 */
const nativeIdentity = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = addonPlatforms[`${process.platform}-${process.arch}`];
  if (platform === undefined) return yield* Effect.fail("no addon for this host");
  const binding = yield* path.fromFileUrl(new URL(import.meta.resolve("reactor-effect-native")));
  const manifest = yield* Effect.try(() =>
    createRequire(binding).resolve(`reactor-effect-native-${platform}/package.json`),
  );
  const identity = yield* fs
    .readFileString(path.join(path.dirname(manifest), "native-identity.json"))
    .pipe(Effect.flatMap(Schema.decodeUnknownEffect(NativeIdentity)));
  return {
    platform: identity.platform,
    file: identity.file,
    sha256: identity.sha256,
    sourceSha256: identity.build.sourceSha256,
    target: identity.build.target,
    webrtcPrebuilt: identity.build.webrtcPrebuilt,
  };
}).pipe(Effect.option);

/** Package `name` as it resolves from here: its directory and version. */
const resolved = (name: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    let directory = path.dirname(yield* path.fromFileUrl(new URL(import.meta.resolve(name))));
    for (;;) {
      const manifest = yield* fs
        .readFileString(path.join(directory, "package.json"))
        .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Manifest)), Effect.option);
      if (Option.isSome(manifest) && manifest.value.name === name)
        return { directory, version: manifest.value.version };
      const parent = path.dirname(directory);
      if (parent === directory) return yield* Effect.fail("not found");
      directory = parent;
    }
  }).pipe(Effect.option);

/**
 * Whether the package at `directory` was built before its sources last
 * changed. Evidence names the commit checked out, so a run on an older build
 * would credit that commit with code it never ran. A package without
 * sources, as one installed from npm, counts as built.
 */
export const staleBuild = Effect.fnUntraced(function* (directory: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const modified = (folder: string, extension: string) =>
    Effect.gen(function* () {
      const root = path.join(directory, folder);
      const files = (yield* fs.readDirectory(root, { recursive: true })).filter((file) =>
        file.endsWith(extension),
      );
      return yield* Effect.forEach(files, (file) =>
        Effect.map(fs.stat(path.join(root, file)), (info) =>
          Option.match(info.mtime, { onNone: () => 0, onSome: (date) => date.getTime() }),
        ),
      );
    }).pipe(Effect.orElseSucceed((): ReadonlyArray<number> => []));
  const sources = yield* modified("src", ".ts");
  if (sources.length === 0) return false;
  const built = yield* modified("dist", ".js");
  return built.length === 0 || Math.max(...sources) > Math.min(...built);
});

/** Where the run happens: runtime, commit and the packages as they resolve from here. */
const environment = Effect.gen(function* () {
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const target = yield* Target;
  const here = path.dirname(yield* path.fromFileUrl(new URL(import.meta.url)));
  const git = (...args: ReadonlyArray<string>) =>
    spawner.string(ChildProcess.make("git", args, { cwd: here })).pipe(Effect.option);
  const packages: Record<string, string> = {};
  for (const name of [
    "reactor-effect-client",
    "reactor-effect-native",
    "effect",
    "@effect/platform-node",
  ]) {
    const found = yield* resolved(name);
    if (Option.isSome(found)) packages[name] = found.value.version;
  }
  // Outside a checkout git prints no commit, only an error, and there is no tree to call clean.
  const commit = Option.filter(
    Option.map(yield* git("rev-parse", "HEAD"), (out) => out.trim()),
    (hash) => hash.length > 0,
  );
  const status = Option.isSome(commit)
    ? yield* git("status", "--porcelain", "--untracked-files=no")
    : Option.none();
  const native = yield* nativeIdentity;
  return {
    // Bun's types declare its version on every runtime; only Bun's process has one.
    runtime: "bun" in process.versions ? `bun ${process.versions.bun}` : `node ${process.version}`,
    os: `${process.platform} ${process.arch}`,
    ...(Option.isSome(commit) ? { commit: commit.value } : {}),
    ...(Option.isSome(status) ? { dirty: status.value.trim().length > 0 } : {}),
    packages,
    ...(Option.isSome(native) ? { native: native.value } : {}),
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
      for (const name of ["reactor-effect-client", "reactor-effect-native"]) {
        const found = yield* resolved(name);
        if (Option.isSome(found) && (yield* staleBuild(found.value.directory)))
          return yield* Refused.make({
            message: `${name} was built before its sources last changed: run \`bun run build\``,
          });
      }
      yield* Ledger.lock(ledger);
      const earlier = yield* Ledger.entries(ledger);
      const reservedUsd = earlier.reduce((total, run) => total + Ledger.reserved(run), 0);
      if (authorization.check === "turn" && target.mode === "paid")
        yield* admitRelay(
          earlier.flatMap((run) =>
            run.mode === "paid" && run.network?.pair !== undefined ? [run.network.pair] : [],
          ),
        );
      // The showreel is paid for its footage: without an ffmpeg that can record it, it buys
      // nothing. Asked here, before the run's clock starts, so the check finds the answer.
      if (authorization.check === "showreel") {
        const unable = yield* target.cannotRecord;
        if (unable !== undefined && target.mode === "paid")
          return yield* Refused.make({ message: `showreel records with ffmpeg, and ${unable}` });
      }
      // A Vidu call starts from a photo of a person: without one, a paid run buys nothing.
      if (
        (authorization.check === "avatar" || authorization.check === "character") &&
        target.mode === "paid" &&
        target.photo === undefined
      )
        return yield* Refused.make({
          message: `${authorization.check} makes its avatar from a photo: give one with --avatar-image <file>`,
        });
      const coordinator = yield* CoordinatorClient.CoordinatorClient;
      const rate = yield* coordinator.pricing.pipe(
        Effect.flatMap((pricing) =>
          CoordinatorClient.modelRate(pricing, plans[authorization.check].model.name),
        ),
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
      yield* run.flush.pipe(Effect.mapError((error) => Refused.make({ message: error.message })));
      yield* run
        .mark("admitted", `worst case $${worstCaseUsd.toFixed(4)}`)
        .pipe(Effect.mapError((error) => Refused.make({ message: error.message })));
      const exit = yield* Effect.gen(function* () {
        yield* Run.liveness.pipe(Effect.forkScoped);
        yield* checks[authorization.check];
      }).pipe(
        Effect.scoped,
        Effect.provideService(Run.Run, run),
        Effect.withTracer(run.tracer),
        Effect.exit,
      );
      const finishedAt = DateTime.formatIso(yield* DateTime.now);
      const evidence = judged({
        evidence: { ...(yield* run.evidence), finishedAt },
        failure: Exit.isFailure(exit) ? Run.describe(exit.cause) : undefined,
      });
      yield* run.update(() => evidence);
      yield* run.flush.pipe(
        Effect.catch((error) =>
          Effect.logError(
            `${error.message}; the run held sessions ${evidence.sessions.map((session) => session.id).join(", ") || "none"}`,
          ),
        ),
      );
      return evidence;
    }),
  );
