/**
 * How the checks run a child: on the selected Node or Bun, with credential-like variables removed
 * from its environment, and with this process's terminal, so its output streams live.
 */
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

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
