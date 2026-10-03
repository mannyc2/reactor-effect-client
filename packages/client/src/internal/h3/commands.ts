/**
 * H3's commands: each one's arguments and the message it replies with, and
 * the check that a deployment's OpenAPI document offers them.
 */
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import { ReactorError } from "../../ReactorError.js";
import type { MessageType } from "./messages.js";
import type { Clip } from "./messages.js";
import type { Family, RequestFields } from "./family.js";
import { documentedVersion, modelName, source } from "./profile.js";

const Upload = Schema.Struct({
  upload_id: Schema.String,
  name: Schema.String,
  mime_type: Schema.String,
  size: Schema.Int,
});
const NoArguments = Schema.Record(Schema.String, Schema.Never);

/**
 * One command-to-reply authority for the client, the simulated model and the
 * deployment check. Required keys are what this adapter always sends;
 * nullable parameters follow the documented deployment shape.
 */
export const Commands = {
  enqueue: {
    args: Schema.Struct({
      prompt: Schema.String,
      reference_images: Upload.pipe(Schema.Array, Schema.NullOr),
      reference_audios: Upload.pipe(Schema.Array, Schema.NullOr, Schema.optionalKey),
      seconds: Schema.Finite.pipe(Schema.NullOr, Schema.optionalKey),
      seed: Schema.Int.pipe(Schema.NullOr, Schema.optionalKey),
      position: Schema.Int.pipe(Schema.NullOr, Schema.optionalKey),
      metadata: Schema.String,
      continue_from_clip_id: Schema.optionalKey(Schema.String),
    }),
    reply: "clip_queued",
  },
  move: {
    args: Schema.Struct({ clip_id: Schema.String, position: Schema.Int }),
    reply: "clip_moved",
  },
  pop: { args: Schema.Struct({ clip_id: Schema.String }), reply: "clip_popped" },
  play: { args: Schema.Struct({ clip_id: Schema.String }), reply: null },
  stop: { args: NoArguments, reply: null },
  set_seed: { args: Schema.Struct({ seed: Schema.Int }), reply: "seed_accepted" },
  set_clip_seconds: {
    args: Schema.Struct({ seconds: Schema.Finite }),
    reply: "clip_length_accepted",
  },
  set_canvas: { args: Schema.Struct({ aspect: Schema.String }), reply: "canvas_accepted" },
  set_autoplay: { args: Schema.Struct({ enabled: Schema.Boolean }), reply: "autoplay_accepted" },
  set_flush_on_clip_end: {
    args: Schema.Struct({ enabled: Schema.Boolean }),
    reply: "flush_accepted",
  },
  get_queue: { args: NoArguments, reply: "queue_update" },
  get_state: { args: NoArguments, reply: "state_update" },
  reset: { args: NoArguments, reply: "session_reset" },
} as const satisfies Readonly<
  Record<string, { readonly args: Schema.Top; readonly reply: MessageType | null }>
>;

export type CommandName = keyof typeof Commands;
export type CommandArgs<K extends CommandName> = {
  readonly [F in keyof (typeof Commands)[K]["args"]["Type"]]: Exclude<
    (typeof Commands)[K]["args"]["Type"][F],
    null
  >;
};
export type ReplyCommand = {
  [K in CommandName]: (typeof Commands)[K]["reply"] extends MessageType ? K : never;
}[CommandName];
export type ReplyType<K extends ReplyCommand> = (typeof Commands)[K]["reply"];
export type ControlCommand = Exclude<CommandName, ReplyCommand>;

/** What a provider needs to start: it enqueues, and reads the state and queue in full. */
const required: ReadonlyArray<CommandName> = ["enqueue", "get_state", "get_queue"];

/** What this adapter supports, and what the deployment it checked declares. */
export interface Contract<
  Name extends string = typeof modelName,
  Version extends string = typeof documentedVersion,
> {
  readonly modelName: Name;
  readonly documentedVersion: Version;
  readonly source: string;
  /**
   * Whether the deployment's `enqueue` declares `reference_audios`. When it
   * does not, a request with audio is refused as `UnsupportedCapability`
   * before anything is uploaded or sent.
   */
  readonly referenceAudio: boolean;
  /**
   * The commands the deployment offers. One it lacks fails its operation as
   * `UnsupportedCapability` before anything is sent.
   */
  readonly commands: ReadonlySet<CommandName>;
  readonly deployment: { readonly title: string | null; readonly version: string | null };
}

const Operation = Schema.Struct({
  post: Schema.Struct({
    requestBody: Schema.optionalKey(
      Schema.Struct({
        content: Schema.Struct({
          "application/json": Schema.Struct({ schema: Schema.Json }),
        }),
      }),
    ),
  }),
});
const Document = Schema.Struct({
  openapi: Schema.String.check(Schema.isPattern(/^3\.[01]\./)),
  info: Schema.optionalKey(
    Schema.Struct({
      title: Schema.optionalKey(Schema.String),
      version: Schema.optionalKey(Schema.String),
    }),
  ),
  paths: Schema.Record(Schema.String, Schema.Json),
  components: Schema.optionalKey(
    Schema.Struct({ schemas: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)) }),
  ),
});
const Properties = Schema.Struct({ properties: Schema.Record(Schema.String, Schema.Json) });
const Reference = Schema.Struct({ $ref: Schema.String });

const incompatible =
  (version: string) =>
  (reason: string): ReactorError =>
    ReactorError.fromCode(
      "UnsupportedCapability",
      `Deployment does not offer H3 ${version}: ${reason}`,
      { operation: "H3 schema", outcome: "not-submitted" },
    );

/**
 * Admits a deployment whose OpenAPI document offers the commands a provider
 * needs to start, and records which others it offers. Payloads are checked
 * where they arrive: a message that does not match its Schema fails the
 * provider as `Protocol`.
 */
export const deploymentContractFor =
  <Name extends string, Version extends string>(
    family: Pick<
      Family<never, RequestFields, Clip, Name, Version>,
      "modelName" | "documentedVersion" | "source" | "refuses"
    >,
  ) =>
  (openapi: Schema.Json | undefined): Result.Result<Contract<Name, Version>, ReactorError> =>
    Result.gen(function* () {
      const refuse = incompatible(family.documentedVersion);
      const document = yield* Result.mapError(Schema.decodeUnknownResult(Document)(openapi), () =>
        refuse("not an OpenAPI 3.0 or 3.1 document"),
      );
      const components = document.components?.schemas ?? {};
      const resolve = (schema: Schema.Json): Schema.Json => {
        const reference = Schema.decodeUnknownResult(Reference)(schema);
        if (Result.isFailure(reference)) return schema;
        const name = reference.success.$ref.replace("#/components/schemas/", "");
        return components[name] ?? schema;
      };
      let referenceAudio = false;
      const commands = new Set<CommandName>();
      for (const name of Struct.keys(Commands)) {
        const operation = Schema.decodeUnknownResult(Operation)(document.paths[`/events/${name}`]);
        if (Result.isFailure(operation)) {
          if (required.includes(name)) return yield* Result.fail(refuse(`command ${name}`));
          continue;
        }
        commands.add(name);
        if (name !== "enqueue") continue;
        const body = operation.success.post.requestBody?.content["application/json"].schema;
        const declared =
          body === undefined ? undefined : Schema.decodeUnknownResult(Properties)(resolve(body));
        if (
          family.refuses !== undefined &&
          declared !== undefined &&
          Result.isSuccess(declared) &&
          Object.hasOwn(declared.success.properties, family.refuses)
        )
          return yield* Result.fail(refuse(`enqueue declares ${family.refuses}`));
        referenceAudio =
          declared !== undefined &&
          Result.isSuccess(declared) &&
          Object.hasOwn(declared.success.properties, "reference_audios");
      }
      return {
        modelName: family.modelName,
        documentedVersion: family.documentedVersion,
        source: family.source,
        referenceAudio,
        commands,
        deployment: {
          title: document.info?.title ?? null,
          version: document.info?.version ?? null,
        },
      };
    });

export const deploymentContract = deploymentContractFor({ modelName, documentedVersion, source });
