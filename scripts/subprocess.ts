/**
 * How the checks run a child: on the selected Node or Bun, with credential-like variables removed
 * from its environment, and either with this process's terminal, so its output streams live, or
 * beside other children, with its output printed whole once it ends.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

/** The Node and Bun the checks run on: `NODE_BINARY` and `BUN_BINARY`, or `node` and this Bun. */
export const runtimes = Config.all({
  node: Config.String("NODE_BINARY").pipe(Config.withDefault("node")),
  bun: Config.String("BUN_BINARY").pipe(Config.withDefault(process.execPath)),
});

const credential = /REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/;

/**
 * This process's environment without its credential-like variables. Config reads variables by
 * name and can't list them, so the environment is read whole here.
 */
export const environment = Effect.sync(() => {
  const kept: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env))
    if (value !== undefined && !credential.test(name)) kept[name] = value;
  return kept;
});

/**
 * A child that exited unsuccessfully. It has reported its own failure, so the check only exits
 * with its status.
 */
export class ChildFailed extends Schema.TaggedError<ChildFailed>(
  "reactor-effect/scripts/subprocess/ChildFailed",
)("ChildFailed", { command: Schema.String, status: Schema.Int }) {
  override readonly [Runtime.errorExitCode] = this.status;
  override readonly [Runtime.errorReported] = false;

  override get message(): string {
    return `${this.command} exited ${this.status}`;
  }
}

/**
 * Runs `command` with this process's standard streams and waits for it. It stays in this process
 * group, so an interrupt typed at the terminal reaches it as it reaches this process.
 */
export const runInherited = Effect.fnUntraced(function* (
  command: string,
  args: ReadonlyArray<string>,
  options: { readonly cwd: string; readonly env: Readonly<Record<string, string>> },
) {
  const handle = yield* ChildProcess.make(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
    detached: false,
  });
  const status = yield* handle.exitCode;
  if (status !== ChildProcessSpawner.ExitCode(0))
    return yield* ChildFailed.make({ command: [command, ...args].join(" "), status });
}, Effect.scoped);

/** A child that ran past its check's limit. What it printed until then is printed with it. */
export class ChildTimedOut extends Schema.TaggedError<ChildTimedOut>(
  "reactor-effect/scripts/subprocess/ChildTimedOut",
)("ChildTimedOut", { command: Schema.String, seconds: Schema.Finite }) {
  override get message(): string {
    return `${this.command} did not finish within ${this.seconds} seconds`;
  }
}

/** A child that runs beside others, and the label its output is printed under. */
export interface Step {
  readonly label: string;
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: Readonly<Record<string, string>>;
  /** How long it may run, if it has a limit. */
  readonly limit?: Duration.Input;
}

const monotonicSeconds = Clock.clockWith((clock) => clock.monotonicTimeNanos).pipe(
  Effect.map((nanos) => Number(nanos / 1_000_000n) / 1000),
);

/**
 * Runs `step` with its stdout and stderr collected rather than streamed, and prints them whole
 * under its label and time once it ends, so steps running side by side don't interleave. A step
 * ended early, because another failed, prints only that it stopped, and its child is sent SIGTERM,
 * then SIGKILL 5 s later. Like `runInherited`'s, the child stays in this process group, so an
 * interrupt typed at the terminal reaches it and everything it started.
 */
export const runStep = Effect.fnUntraced(function* (step: Step) {
  const command = [step.command, ...step.args].join(" ");
  const started = yield* monotonicSeconds;
  const printed = yield* Ref.make("");
  const report = (
    exit: Exit.Exit<ChildProcessSpawner.ExitCode, PlatformError.PlatformError | ChildTimedOut>,
  ) =>
    Effect.gen(function* () {
      const seconds = ((yield* monotonicSeconds) - started).toFixed(1);
      if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
        return yield* Console.log(`stopped ${step.label} after ${seconds} s`);
      const passed = Exit.isSuccess(exit) && exit.value === ChildProcessSpawner.ExitCode(0);
      const output = yield* Ref.get(printed);
      yield* Console.log(
        `${passed ? "passed" : "FAILED"} ${step.label} in ${seconds} s\n${output}`,
      );
    });
  const run = Effect.gen(function* () {
    const handle = yield* ChildProcess.make(step.command, step.args, {
      cwd: step.cwd,
      env: step.env,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      detached: false,
      forceKillAfter: "5 seconds",
    });
    const [, status] = yield* Effect.all(
      [
        handle.all.pipe(
          Stream.decodeText(),
          Stream.runForEach((text) => Ref.update(printed, (before) => before + text)),
        ),
        handle.exitCode,
      ],
      { concurrency: 2 },
    );
    return status;
  }).pipe(Effect.scoped);
  const limit = step.limit;
  const limited: Effect.Effect<
    ChildProcessSpawner.ExitCode,
    PlatformError.PlatformError | ChildTimedOut,
    ChildProcessSpawner.ChildProcessSpawner
  > =
    limit === undefined
      ? run
      : run.pipe(
          Effect.timeoutOrElse({
            duration: limit,
            orElse: () =>
              Effect.fail(ChildTimedOut.make({ command, seconds: Duration.toSeconds(limit) })),
          }),
        );
  const status = yield* limited.pipe(Effect.onExit(report));
  if (status !== ChildProcessSpawner.ExitCode(0))
    return yield* ChildFailed.make({ command, status });
});

/**
 * Runs `steps` side by side, each as `runStep` does. The first to fail stops the rest and fails
 * the whole.
 */
export const runSteps = (steps: ReadonlyArray<Step>) =>
  Effect.forEach(steps, runStep, { concurrency: "unbounded", discard: true });
