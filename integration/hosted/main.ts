/**
 * The hosted qualification. README.md says what each check costs and
 * gathers, and why.
 *
 *   bun integration/hosted/main.ts rehearse <check> [--faults '<json>'] [--ledger <dir>] \
 *     [--moderation-prompt-file <path>] [--avatar-image <path>]
 *   bun integration/hosted/main.ts preflight [--check <check>] --total-budget-usd 3.50 \
 *     --ledger <dir>
 *   bun integration/hosted/main.ts run <check> --budget-usd 1.75 --total-budget-usd 3.50 \
 *     --ledger <dir> --network "<where, without addresses>" --i-authorize-paid-sessions \
 *     [--moderation-prompt-file <path>] [--avatar-image <path>]
 *   bun integration/hosted/main.ts summarize <file or ledger>...
 *
 * A run exits 0 when it passes, 1 when it fails, and 2 when it refused before
 * claiming its evidence file, and so before spending anything.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as Argument from "effect/cli/Argument";
import * as Command from "effect/cli/Command";
import * as Flag from "effect/cli/Flag";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import * as ReactorTest from "reactor-effect-client/ReactorTest";
import * as NativePeer from "reactor-effect-native/NativePeer";
import * as Avatar from "./checks/Avatar.js";
import type { Evidence } from "./Evidence.js";
import { EvidenceJson, Probe } from "./Evidence.js";
import * as Ledger from "./Ledger.js";
import * as Probes from "./Probes.js";
import { execute } from "./Qualify.js";
import * as Spend from "./Spend.js";
import { summarize } from "./Summary.js";
import * as Target from "./Target.js";

/** The run failed after it claimed its evidence: exit 1. */
class Failed extends Schema.TaggedError<Failed>("reactor-effect-integration/hosted/main/Failed")(
  "Failed",
  { message: Schema.String },
) {}

const check = Argument.Literals("check", Spend.checks);
const ledger = Flag.String("ledger").pipe(
  Flag.withDescription("the evidence directory: the ledger"),
);
const moderationPromptFile = Flag.String("moderation-prompt-file").pipe(
  Flag.withDescription(
    "a file holding a prompt meant to be flagged by content moderation; `cut` ends with it, and `show` loses its second session to it",
  ),
  Flag.optional,
);

/** The moderation prompt, read from the operator's file: redacted, and never in a message. */
const moderationPrompt = (file: Option.Option<string>) =>
  Effect.gen(function* () {
    if (Option.isNone(file)) return undefined;
    const text = yield* (yield* FileSystem.FileSystem)
      .readFileString(file.value)
      .pipe(
        Effect.mapError(() =>
          Spend.Refused.make({ message: "the moderation prompt file cannot be read" }),
        ),
      );
    const prompt = text.trim();
    if (prompt.length === 0)
      return yield* Spend.Refused.make({ message: "the moderation prompt file is empty" });
    return Redacted.make(prompt);
  });

const avatarImage = Flag.String("avatar-image").pipe(
  Flag.withDescription(
    "a photo of one person, a PNG, JPEG or WebP under 20 MB, that `avatar` and `character` make their avatar from",
  ),
  Flag.optional,
);

/**
 * The photo `avatar` and `character` make their avatar from, read from the operator's file: refused unless it
 * is one Vidu S2-Avatar takes, by its size and its first bytes. Neither it nor its path is
 * ever in a message.
 */
const photo = (file: Option.Option<string>) =>
  Effect.gen(function* () {
    if (Option.isNone(file)) return undefined;
    const fs = yield* FileSystem.FileSystem;
    const unreadable = () => Spend.Refused.make({ message: "the avatar image cannot be read" });
    const info = yield* fs.stat(file.value).pipe(Effect.mapError(unreadable));
    if (info.type !== "File") return yield* unreadable();
    const size = Number(info.size);
    if (size >= Avatar.maxPhotoBytes)
      return yield* Spend.Refused.make({
        message: `the avatar image is ${size} bytes; Vidu S2-Avatar takes one under ${Avatar.maxPhotoBytes}`,
      });
    const bytes = yield* fs.readFile(file.value).pipe(Effect.mapError(unreadable));
    const type = Avatar.photoType(bytes);
    if (type === undefined)
      return yield* Spend.Refused.make({
        message: "the avatar image is not a PNG, JPEG or WebP by its first bytes",
      });
    return { bytes, type } satisfies Target.Photo;
  });

const report = Effect.fnUntraced(function* (evidence: Evidence) {
  yield* Console.log(summarize([evidence]));
  if (evidence.verdict !== "pass")
    return yield* Failed.make({ message: evidence.reasons.join("; ") });
});

const apiUrl = Config.String("REACTOR_API_URL").pipe(
  Config.withDefault(CoordinatorClient.defaultApiUrl),
);
const apiKey = Config.Redacted("REACTOR_API_KEY").pipe(
  Effect.mapError(() => Spend.Refused.make({ message: "REACTOR_API_KEY is required" })),
);

const rehearse = Command.make(
  "rehearse",
  {
    check,
    ledger: ledger.pipe(Flag.optional),
    faults: Flag.String("faults").pipe(
      Flag.withSchema(ReactorTest.Fault.pipe(Schema.Array, Schema.fromJsonString)),
      Flag.withDescription('ReactorTest faults as JSON, e.g. [{"_tag":"NoAudio"}]'),
      Flag.optional,
    ),
    moderationPromptFile,
    avatarImage,
  },
  Effect.fnUntraced(function* (input) {
    const directory = Option.isSome(input.ledger)
      ? input.ledger.value
      : yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
          prefix: "reactor-rehearsal-",
        });
    const evidence = yield* execute({
      authorization: {
        check: input.check,
        budgetUsd: Spend.ceilingFor(input.check),
        totalUsd: Spend.maxTotalUsd,
      },
      ledger: directory,
    });
    return yield* report(evidence);
  }, Effect.scoped),
).pipe(
  Command.provide((input) =>
    Layer.unwrap(
      Effect.gen(function* () {
        const prompt = yield* moderationPrompt(input.moderationPromptFile);
        const faults = Option.getOrElse(input.faults, () => []);
        return Target.rehearsal({
          // Unless the faults say otherwise, the simulated moderation flags the prompt given.
          faults:
            prompt === undefined || faults.some((fault) => fault._tag === "Moderate")
              ? faults
              : [...faults, { _tag: "Moderate", prompt: Redacted.value(prompt) }],
          candidate: input.check === "turn" ? "relay" : "host",
          moderationPrompt: prompt,
          photo: yield* photo(input.avatarImage),
        }).pipe(Layer.provideMerge(Target.movingClock));
      }),
    ),
  ),
  Command.withDescription("Run a check against ReactorTest, for free"),
);

const paid = Command.make(
  "run",
  {
    check,
    ledger,
    budget: Flag.Finite("budget-usd"),
    total: Flag.Finite("total-budget-usd"),
    network: Flag.String("network").pipe(
      Flag.withSchema(Schema.Trim.check(Schema.isNonEmpty())),
      Flag.withDescription("where the check runs from, without addresses"),
    ),
    authorized: Flag.Boolean("i-authorize-paid-sessions").pipe(Flag.withDefault(false)),
    moderationPromptFile,
    avatarImage,
  },
  Effect.fnUntraced(function* (input) {
    const authorization = yield* Spend.authorize({
      check: input.check,
      budgetUsd: input.budget,
      totalUsd: input.total,
    });
    return yield* report(yield* execute({ authorization, ledger: input.ledger }));
  }),
).pipe(
  Command.provide((input) =>
    Layer.unwrap(
      Effect.gen(function* () {
        // Refused before any layer that could reach the network is built.
        if (!input.authorized)
          return yield* Spend.Refused.make({
            message: "paid use needs --i-authorize-paid-sessions from the authorizing maintainer",
          });
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        return Target.paid({
          apiKey: yield* apiKey,
          apiUrl: yield* apiUrl,
          network: input.network,
          // Seam frames are for a person to look at, beside the run and never in the ledger.
          seams: yield* fs.makeTempDirectory({ prefix: "reactor-seams-" }),
          script: yield* path.fromFileUrl(new URL(import.meta.url)),
          moderationPrompt: yield* moderationPrompt(input.moderationPromptFile),
          photo: yield* photo(input.avatarImage),
        });
      }),
    ),
  ),
  Command.withDescription("Run a check against hosted Reactor, for money"),
);

const preflight = Command.make(
  "preflight",
  {
    check: Flag.Literals("check", Spend.checks).pipe(
      Flag.withDescription("the check whose model's rate and token are read"),
      Flag.withDefault("vertical"),
    ),
    ledger,
    total: Flag.Finite("total-budget-usd"),
  },
  Effect.fnUntraced(function* (input) {
    const { check } = input;
    const model = Spend.plans[check].model.name;
    const earlier = yield* Ledger.entries(input.ledger);
    const reservedUsd = earlier.reduce((total, run) => total + Ledger.reserved(run), 0);
    const coordinator = yield* CoordinatorClient.CoordinatorClient;
    const rate = yield* CoordinatorClient.modelRate(yield* coordinator.pricing, model);
    const worst = yield* Spend.admit({
      rate,
      authorization: { check, budgetUsd: Spend.ceilingFor(check), totalUsd: input.total },
      reservedUsd,
    });
    yield* Console.log(
      `${model}: rate ${rate.creditsPerSecond} credits/s, billed per ${rate.per}, at ${rate.creditsPerDollar} credits/$: ${check} bills up to $${worst.toFixed(4)}; the ledger holds $${reservedUsd.toFixed(4)}`,
    );
    // Minting allocates nothing; a token that grants more than asked refuses here.
    const grant = yield* coordinator.mintToken({
      modelName: model,
      maxSessionDuration:
        Spend.plans[check].seconds === "unlimited"
          ? "unlimited"
          : `${Spend.plans[check].seconds} seconds`,
      expiresAfter: `${Spend.tokenSecondsFor(check)} seconds`,
    });
    const granted = yield* Spend.provenGrant({
      jwt: Redacted.value(grant.jwt),
      granted: grant.granted,
    });
    yield* Spend.acceptGrant({ check, granted });
    yield* Console.log(`a token grants one session of ${granted.maxSessionSeconds} s`);
    // Building the native peer loads and verifies the library.
    yield* Layer.build(NativePeer.layer()).pipe(Effect.scoped);
    yield* Console.log("the native library loads");
    // adoption's paid owner runs under Node on the isolated native peer.
    const path = yield* Path.Path;
    const owner = yield* Target.probeOwner(yield* path.fromFileUrl(new URL(import.meta.url)));
    yield* Console.log(
      `node ${owner.node} runs adoption's owner: it started on ${owner.host} and exited ${owner.exitCode} without a grant`,
    );
    // Free questions about tokens and the key: each allocates nothing.
    for (const probe of yield* Probes.run({ apiUrl: coordinator.apiUrl, apiKey: yield* apiKey }))
      yield* Console.log(
        `probe: ${yield* Schema.encodeEffect(Schema.fromJsonString(Probe))(probe)}`,
      );
  }),
).pipe(
  Command.provide(
    Layer.unwrap(
      Effect.gen(function* () {
        return CoordinatorClient.layer({ apiUrl: yield* apiUrl, apiKey: yield* apiKey }).pipe(
          Layer.provideMerge(FetchHttpClient.layer),
        );
      }),
    ),
  ),
  Command.withDescription(
    "Check the rate, the ledger, a token, the native library and adoption's owner, for free",
  ),
);

const summarizeRuns = Command.make(
  "summarize",
  { paths: Argument.String("path").pipe(Argument.variadic()) },
  Effect.fnUntraced(function* (input) {
    const fs = yield* FileSystem.FileSystem;
    const runs = yield* Effect.forEach(input.paths, (path) =>
      Effect.gen(function* () {
        const info = yield* fs.stat(path);
        if (info.type === "Directory") return yield* Ledger.entries(path);
        const text = yield* fs.readFileString(path);
        return [yield* Schema.decodeEffect(EvidenceJson)(text)];
      }),
    );
    yield* Console.log(summarize(runs.flat()));
  }),
).pipe(Command.withDescription("Print runs as Markdown"));

/** The takeover's owner process: its grant arrives on stdin. Never run by hand. */
const owner = Command.make(
  "owner",
  {
    isolated: Flag.Boolean("isolated").pipe(
      Flag.withDescription("give each connection's native peer a child process; needs Node"),
      Flag.withDefault(false),
    ),
    idle: Flag.Boolean("idle").pipe(
      Flag.withDescription("connect and set up the session, playing nothing"),
      Flag.withDefault(false),
    ),
    queuedSeconds: Flag.Int("queued-seconds").pipe(
      Flag.withDescription("how long the clip queued behind the 15 s one asks for"),
      Flag.optional,
    ),
  },
  Effect.fnUntraced(function* (input) {
    const stdio = yield* Stdio.Stdio;
    // Bun's types declare its version on every runtime; only Bun's process has one.
    const runtime =
      "bun" in process.versions ? `bun ${process.versions.bun}` : `node ${process.version}`;
    return yield* Target.ownerProcess({
      lines: stdio.stdin.pipe(Stream.decodeText(), Stream.splitLines),
      host: `${runtime}, ${input.isolated ? "the isolated native peer" : "the native peer in process"}`,
      queuedSeconds: Option.getOrUndefined(input.queuedSeconds),
      idle: input.idle,
    });
  }),
).pipe(
  Command.provide((input) =>
    Layer.unwrap(
      Effect.map(apiUrl, (url) =>
        Reactor.layer().pipe(
          Layer.provide(CoordinatorClient.layer({ apiUrl: url })),
          Layer.provide(input.isolated ? NativePeer.layerIsolated() : NativePeer.layer()),
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
    ),
  ),
);

const qualify = Command.make("qualify").pipe(
  Command.withSubcommands([rehearse, paid, preflight, summarizeRuns, owner]),
  Command.withDescription(`Hosted qualification: ${Spend.checks.join(", ")}`),
);

qualify.pipe(
  Command.run({ version: "0.10.0" }),
  // The application's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain({
    teardown: (exit, onExit) => {
      if (Exit.isSuccess(exit)) return onExit(0);
      onExit(Schema.is(Spend.Refused)(exit.cause.pipe(Cause.squash)) ? 2 : 1);
    },
  }),
);
