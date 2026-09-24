import { randomUUID } from "node:crypto";
import { Deferred, Effect } from "effect";
import { ReactorError } from "../../src/errors.js";
import type { MessageCode } from "../../src/errors.js";
import { jsonObject, structFromObject } from "../../src/json.js";
import type { JsonObject } from "../../src/json.js";
import { Observations } from "../../src/observation.js";
import { CommandFailure } from "../../src/session/commands.js";
import type {
  CommandReply,
  Session,
  SessionEvent,
  Snapshot,
  ReadyState,
  Uploaded,
  UploadTimeoutOptions,
} from "../../src/session/index.js";
import { providerSchema } from "./ProviderSchema.js";

export interface FixtureClip {
  readonly clip_id: string;
  readonly prompt: string;
  readonly metadata: string;
  readonly frames: number;
  readonly seconds: number;
  readonly seed: number;
  readonly ready: boolean;
  readonly has_reference_image: boolean;
  readonly reference_image_count: number;
  readonly has_reference_audio: boolean;
  readonly reference_audio_count: number;
}
export const fixtureClip = (fields: Partial<FixtureClip> = {}): FixtureClip => ({
  clip_id: randomUUID(),
  prompt: "An independent fixture clip",
  metadata: "foreign metadata",
  frames: 175,
  seconds: 175 / 24,
  seed: 7,
  ready: false,
  has_reference_image: false,
  reference_image_count: 0,
  has_reference_audio: false,
  reference_audio_count: 0,
  ...fields,
});
export interface WireMessage {
  readonly type: string;
  readonly data: JsonObject;
}
export interface Call {
  readonly command: string;
  readonly args: JsonObject;
  readonly requestId: string;
  readonly generation: bigint;
}
/** A command argument the wire carries as text; anything else is an adapter bug. */
export const textArg = (value: unknown): string => {
  if (typeof value !== "string")
    throw new TypeError(`expected a text argument, got ${typeof value}`);
  return value;
};
/** The fields the adapter encodes into a clip's metadata text. */
export const metadataOf = (metadata: string): Readonly<Record<string, unknown>> =>
  JSON.parse(metadata) as Record<string, unknown>;

export interface ReplyContext {
  readonly fake: Fixture;
  readonly call: Call;
  readonly defaults: Effect.Effect<WireMessage | undefined, CommandFailure>;
  /**
   * Send now the broadcasts `defaults` queued to follow the reply, for a
   * script whose reply is lost after the model acted on the command.
   */
  readonly announced: Effect.Effect<void>;
  readonly fail: (
    outcome: "unknown" | "replied" | "not-submitted",
    code?: MessageCode,
  ) => CommandFailure;
}
export interface Script {
  readonly schema?: JsonObject;
  readonly command?: Readonly<
    Record<
      string,
      (context: ReplyContext) => Effect.Effect<WireMessage | undefined, CommandFailure>
    >
  >;
  readonly upload?: (
    name: string,
    mimeType: string,
    bytes: Uint8Array,
  ) => Effect.Effect<Uploaded, ReactorError>;
  /** Deliberately violate same-object observation to verify the provider refuses fabricated results. */
  readonly omitObservation?: boolean;
  readonly initialGeneration?: readonly FixtureClip[];
  readonly initialPlayout?: readonly FixtureClip[];
  readonly initialHistory?: readonly FixtureClip[];
}
export interface Emission {
  readonly requestId?: string;
  readonly generation?: bigint;
  readonly correlation?: CommandReply["correlation"];
}
export interface Fixture {
  readonly session: Session;
  readonly calls: Call[];
  readonly reads: string[];
  readonly uploaded: {
    readonly name: string;
    readonly mimeType: string;
    readonly bytes: Uint8Array;
    readonly options: UploadTimeoutOptions | undefined;
  }[];
  readonly accepted: FixtureClip[];
  readonly returns: CommandReply[];
  readonly lifecycleCalls: { connect: number; reconnect: number; close: number };
  readonly emit: (
    type: string,
    data: JsonObject,
    options?: Emission,
  ) => Effect.Effect<CommandReply>;
  readonly replay: (source: SessionEvent) => Effect.Effect<void>;
  readonly status: (status: Snapshot["status"], generation?: bigint) => Effect.Effect<void>;
  readonly failObservation: (error: ReactorError) => Effect.Effect<void>;
  readonly queue: () => JsonObject;
  readonly state: (changes?: JsonObject) => JsonObject;
  readonly subscribers: () => number;
}

/** Public Session fixture; it imports no H3 implementation schema or reducer. */
export const fixture = (script: Script = {}): Effect.Effect<Fixture> =>
  Effect.sync(() => {
    const observers = new Observations<SessionEvent>();
    const id = "h3-offline-session";
    let revision = 0n,
      generation = 1n,
      requestIndex = 0,
      status: Snapshot["status"] = "ready";
    let generationQueue = [...(script.initialGeneration ?? [])],
      playout = [...(script.initialPlayout ?? [])],
      history = [...(script.initialHistory ?? [])];
    let playing: FixtureClip | undefined,
      autoplay = false,
      flush = true,
      seed = 1000,
      duration = 15,
      aspect = "16:9";
    const calls: Call[] = [],
      accepted: FixtureClip[] = [],
      reads: string[] = [],
      returns: CommandReply[] = [];
    const uploaded: {
      name: string;
      mimeType: string;
      bytes: Uint8Array;
      options: UploadTimeoutOptions | undefined;
    }[] = [];
    const lifecycleCalls = { connect: 0, reconnect: 0, close: 0 };
    const ready = (): ReadyState => ({
      status: "ready",
      generation,
      remote: {
        ownership: "attached",
        sessionId: id,
        connectionId: 1,
        descriptor: {
          session_id: id,
          state: "ACTIVE",
          capabilities: { protocol_version: "1.0", tracks: [] },
          selected_transport: { protocol: "webrtc", version: "1.0" },
          raw: { session_id: id, state: "ACTIVE" },
        },
      },
    });
    const current = (): Snapshot => ({
      ...ready(),
      status,
      generation,
      pending: { data: 0, control: 0 },
      pausedLocally: [],
      claimedTracks: [],
      unresolvedPublications: [],
      receivedTracks: [],
      observationOverflows: observers.overflowCount,
      subscribers: observers.size,
    });
    const queue = (): JsonObject => ({
      generation: generationQueue.map((clip) => ({ ...clip })),
      playout: playout.map((clip) => ({ ...clip })),
      history: history.map((clip) => ({ ...clip })),
    });
    const state = (changes: JsonObject = {}): JsonObject => ({
      clip_seconds: duration,
      clip_seconds_min: 5,
      clip_seconds_max: 15.084,
      seed,
      autoplay,
      flush_on_clip_end: flush,
      aspect,
      width: aspect === "16:9" ? 1344 : 768,
      height: aspect === "9:16" ? 1344 : 768,
      playing: playing !== undefined,
      playing_clip_id: playing?.clip_id ?? null,
      generation_queued: generationQueue.length,
      generation_capacity: 20,
      playout_queued: playout.length,
      playout_capacity: 10,
      clips_played: 0,
      seconds_sent: 0,
      valid_commands: ["enqueue", "play", "stop", "move", "pop", "get_queue", "get_state", "reset"],
      ...changes,
    });
    const payload = (
      type: string,
      data: JsonObject,
      requestId = "",
      correlation: CommandReply["correlation"] = "unsolicited",
      remoteGeneration = generation,
    ): CommandReply => ({
      _tag: "Model",
      kind: "message",
      outcome: "replied",
      type,
      data,
      requestId,
      sequence: ++revision,
      generation: remoteGeneration,
      correlation,
      raw: {
        request_id: requestId,
        kind: 0,
        payload: { case: "message", value: { type, data: structFromObject(data) } },
      },
    });
    const sendEvent = (event: SessionEvent) => observers.emit(event, 2048);
    const broadcast = (type: string, data: JsonObject) => sendEvent(payload(type, data));
    /**
     * The model's own behavior. As H3 documents it ("Replies `clip_queued` and
     * broadcasts `queue_update` and `state_update`"), a command's reply comes
     * first and the snapshots it changed follow: `announce` queues them in
     * `later`, which the command sends after its reply.
     */
    const defaults = (
      call: Call,
      later: (() => void)[],
    ): Effect.Effect<WireMessage | undefined, CommandFailure> =>
      Effect.sync(() => {
        const announce = (type: string, data: JsonObject) =>
          later.push(() => broadcast(type, data));
        const { command, args } = call;
        const known = (clipId: unknown) =>
          [...generationQueue, ...playout, ...history, ...accepted].find(
            (clip) => clip.clip_id === clipId,
          );
        const denied = (): WireMessage => ({
          type: "command_error",
          data: { command, reason: "Fixture refused the requested operation" },
        });
        switch (command) {
          case "get_state":
            return { type: "state_update", data: state() };
          case "get_queue":
            return { type: "queue_update", data: queue() };
          case "enqueue": {
            const requested = typeof args.seconds === "number" ? args.seconds : duration;
            // Fixed documented fixture expectations; do not call the adapter's frame helper.
            const frames = Math.min(
              362,
              124 + Math.ceil(Math.max(0, Math.ceil(requested * 24 - 1e-6) - 124) / 17) * 17,
            );
            const count = Array.isArray(args.reference_images) ? args.reference_images.length : 0;
            const clip = fixtureClip({
              prompt: textArg(args.prompt),
              metadata: textArg(args.metadata),
              frames,
              seconds: frames / 24,
              seed: typeof args.seed === "number" ? args.seed : seed++,
              has_reference_image: count > 0,
              reference_image_count: count,
            });
            accepted.push(clip);
            generationQueue.splice(
              typeof args.position === "number" ? args.position : generationQueue.length,
              0,
              clip,
            );
            announce("queue_update", queue());
            announce("state_update", state());
            return { type: "clip_queued", data: { clip: { ...clip } } };
          }
          case "move": {
            const target = generationQueue.some((clip) => clip.clip_id === args.clip_id)
              ? generationQueue
              : playout;
            const index = target.findIndex((clip) => clip.clip_id === args.clip_id);
            if (index < 0) return denied();
            const clip = target.splice(index, 1)[0]!;
            const position = Math.min(Number(args.position), target.length);
            target.splice(position, 0, clip);
            announce("queue_update", queue());
            return {
              type: "clip_moved",
              data: {
                clip: { ...clip },
                queue: target === generationQueue ? "generation" : "playout",
                position,
              },
            };
          }
          case "pop": {
            const clip = known(args.clip_id);
            if (clip === undefined) return denied();
            generationQueue = generationQueue.filter((value) => value.clip_id !== clip.clip_id);
            playout = playout.filter((value) => value.clip_id !== clip.clip_id);
            announce("queue_update", queue());
            announce("state_update", state());
            return { type: "clip_popped", data: { clip: { ...clip } } };
          }
          case "play": {
            const clip =
              args.clip_id === ""
                ? playout[0]
                : playout.find((clip) => clip.clip_id === args.clip_id);
            if (clip === undefined || playing !== undefined) return denied();
            playing = clip;
            playout = playout.filter((value) => value.clip_id !== clip.clip_id);
            announce("clip_started", { clip: { ...clip } });
            announce("queue_update", queue());
            announce("state_update", state());
            return undefined;
          }
          case "stop": {
            if (playing === undefined) return denied();
            const clip = playing;
            playing = undefined;
            announce("clip_stopped", { clip: { ...clip }, seconds_sent: 2.5 });
            announce("state_update", state());
            return undefined;
          }
          case "set_seed":
            seed = Number(args.seed);
            announce("state_update", state());
            return { type: "seed_accepted", data: { seed } };
          case "set_clip_seconds": {
            const frames = Math.min(
              362,
              124 +
                Math.ceil(Math.max(0, Math.ceil(Number(args.seconds) * 24 - 1e-6) - 124) / 17) * 17,
            );
            duration = frames / 24;
            announce("state_update", state());
            return { type: "clip_length_accepted", data: { frames, clip_seconds: duration } };
          }
          case "set_canvas":
            aspect = textArg(args.aspect);
            announce("state_update", state());
            return {
              type: "canvas_accepted",
              data: { aspect, width: state().width!, height: state().height! },
            };
          case "set_autoplay":
            autoplay = args.enabled === true;
            announce("state_update", state());
            return { type: "autoplay_accepted", data: { enabled: autoplay } };
          case "set_flush_on_clip_end":
            flush = args.enabled === true;
            announce("state_update", state());
            return { type: "flush_accepted", data: { enabled: flush } };
          case "reset": {
            const cleared = generationQueue.length + playout.length,
              wasPlaying = playing !== undefined;
            generationQueue = [];
            playout = [];
            history = [];
            playing = undefined;
            autoplay = false;
            flush = true;
            seed = 1000;
            duration = 15;
            aspect = "16:9";
            announce("queue_update", queue());
            announce("state_update", state());
            return {
              type: "session_reset",
              data: { cleared_clips: cleared, was_playing: wasPlaying },
            };
          }
          default:
            return denied();
        }
      });
    const session: Session = {
      id,
      ownership: "attached",
      connect: Effect.sync(() => {
        lifecycleCalls.connect++;
      }),
      reconnect: Effect.sync(() => {
        lifecycleCalls.reconnect++;
      }),
      ready: Effect.suspend(() =>
        status === "ready"
          ? Effect.succeed(ready())
          : Effect.fail(ReactorError.fromCode("InvalidState", "Fixture session is not ready")),
      ),
      current: Effect.sync(current),
      events: (bounds) => observers.stream(bounds),
      observe: (bounds) =>
        Effect.gen(function* () {
          const events = yield* observers.subscribe(bounds);
          return { initial: current(), revision, events };
        }),
      schema: Effect.sync(() => {
        reads.push("schema");
        return { openapi: script.schema ?? providerSchema() };
      }),
      command: (command, input, options = {}) =>
        Effect.gen(function* () {
          if (status !== "ready")
            return yield* CommandFailure.from(
              ReactorError.fromCode("InvalidState", "Fixture session is not ready"),
              {
                operation: command,
                outcome: "not-submitted",
              },
            );
          const call: Call = {
            command,
            args: jsonObject(input),
            requestId: `fixture-command-${++requestIndex}`,
            generation,
          };
          calls.push(call);
          if (options.uploads !== undefined)
            throw new Error(
              "H3 prompt/image fixture uses JSON reference arrays, not legacy attachment commands",
            );
          const fail = (
            outcome: "unknown" | "replied" | "not-submitted",
            code: MessageCode = "Timeout",
          ) =>
            CommandFailure.from(ReactorError.fromCode(code, "Fixture command failure"), {
              operation: command,
              outcome,
              requestId: call.requestId,
              generation: call.generation,
            });
          const later: (() => void)[] = [];
          const announced = Effect.sync(() => {
            for (const announce of later.splice(0)) announce();
          });
          const commandEffect =
            script.command?.[command]?.({
              fake,
              call,
              defaults: defaults(call, later),
              announced,
              fail,
            }) ?? defaults(call, later);
          const message = yield* commandEffect.pipe(
            Effect.timeoutOrElse({
              duration: options.replyTimeout ?? 1000,
              orElse: () => Effect.fail(fail("unknown")),
            }),
          );
          const result: CommandReply =
            message === undefined
              ? {
                  _tag: "Model",
                  kind: "ack",
                  outcome: "replied",
                  requestId: call.requestId,
                  generation: call.generation,
                  sequence: ++revision,
                  correlation: "matched",
                  raw: { request_id: call.requestId, kind: 0 },
                }
              : payload(message.type, message.data, call.requestId, "matched", call.generation);
          returns.push(result);
          if (!script.omitObservation) sendEvent(result);
          yield* announced;
          return result;
        }),
      upload: (name, mimeType, bytes, options) =>
        Effect.gen(function* () {
          uploaded.push({ name, mimeType, bytes: new Uint8Array(bytes), options });
          return yield* (
            script.upload?.(name, mimeType, bytes) ??
              Effect.succeed({
                file: {
                  upload_id: randomUUID(),
                  name,
                  mime_type: mimeType,
                  size: BigInt(bytes.length),
                },
                transfer: "confirmed" as const,
                notification: "submitted" as const,
              })
          );
        }),
      requestRecordingClip: () =>
        Effect.fail(
          ReactorError.fromCode("UnsupportedCapability", "Not a fixture recording operation"),
        ),
      recording: Effect.fail(
        ReactorError.fromCode("UnsupportedCapability", "Not a fixture recording operation"),
      ),
      stats: Effect.succeed({ sampledAtMs: 0, generation, warnings: [] }),
      close: Effect.sync(() => {
        lifecycleCalls.close++;
        return {
          localClosed: true,
          allocation: "known" as const,
          ownership: "attached" as const,
          sessionId: id,
          remote: {
            attempted: false,
            responseReceived: false,
            confirmed: false,
            evidence: null,
            deleteStatus: null,
            state: null,
          },
          unpublishSubmitted: [],
          unresolvedPublications: [],
          localErrors: [],
        };
      }),
    };
    const fake: Fixture = {
      session,
      calls,
      accepted,
      reads,
      uploaded,
      returns,
      lifecycleCalls,
      queue,
      state,
      emit: (type, data, options = {}) =>
        Effect.sync(() => {
          const source = payload(
            type,
            data,
            options.requestId,
            options.correlation,
            options.generation,
          );
          sendEvent(source);
          return source;
        }),
      replay: (source) => Effect.sync(() => sendEvent(source)),
      status: (next, nextGeneration = generation) =>
        Effect.sync(() => {
          status = next;
          generation = nextGeneration;
          sendEvent({ _tag: "Status", status, generation, sequence: ++revision });
        }),
      failObservation: (error) => Effect.sync(() => observers.fail(error)),
      subscribers: () => observers.size,
    };
    return fake;
  });

export const gate = Effect.gen(function* () {
  const signal = yield* Deferred.make<void>();
  return { wait: Deferred.await(signal), release: Deferred.succeed(signal, undefined) };
});
