/**
 * One simulated H3 session at work: its model takes one input at a time, what
 * it sends goes over the session's open connection, and its timers and media
 * run in the session's scope until the session ends.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as FiberSet from "effect/FiberSet";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { DescMessage, MessageInitShape } from "@bufbuild/protobuf";
import type { Clip, Message } from "../h3/messages.js";
import { h3ReferenceTurboRealtime as profile } from "../h3/profile.js";
import type { Entry, Options } from "../../ReactorTest.js";
import * as Wire from "../wire.js";
import type { Faults } from "./faults.js";
import * as H3 from "./h3.js";
import * as Media from "./media.js";
import type { Link } from "./peer.js";
import type { Sampler } from "./timing.js";

export const monotonic = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1e6);

export const until = (at: number): Effect.Effect<void> =>
  Effect.flatMap(monotonic, (now) =>
    at > now ? Effect.sleep(Duration.millis(at - now)) : Effect.void,
  );

export interface Environment {
  readonly options: Options;
  readonly faults: Faults;
  /** Draws every delay, seeded once for the whole simulation so a run repeats. */
  readonly timing: Sampler;
  readonly openapi: typeof Wire.StructJson.Type;
  readonly log: (entry: Omit<Entry, "at">) => Effect.Effect<void>;
}

const range = (count: number) => Array.from({ length: Math.max(0, count) }, (_, index) => index);

export const make = Effect.fnUntraced(function* (sessionId: string, environment: Environment) {
  const { options, faults, log } = environment;
  const model = yield* Ref.make(H3.initial);
  const lock = yield* Semaphore.make(1);
  const timers = yield* FiberSet.make<void>();
  /** Each playing clip's media, by its start token: a stop ends one, a boundary none. */
  const players = yield* FiberMap.make<number, void>();
  /**
   * The open connection. Its media arrives at one latency, drawn when it
   * opens, so frames keep their order across a boundary as a track does.
   */
  const connection = yield* Ref.make<
    | { readonly link: Link; readonly paused: ReadonlySet<string>; readonly latency: number }
    | undefined
  >(undefined);

  /** The simulation encodes only messages it built, so a failure is a defect. */
  const send = <Desc extends DescMessage>(
    channel: "control" | "data",
    schema: Desc,
    message: MessageInitShape<Desc>,
  ) =>
    Effect.flatMap(Ref.get(connection), (open) =>
      open === undefined
        ? Effect.void
        : Wire.encode(schema, message).pipe(
            Effect.orDie,
            Effect.flatMap((bytes) => open.link.deliver(channel, bytes)),
          ),
    );
  const data = (requestId: string, kind: Wire.MessageKind, message: Message) =>
    Effect.gen(function* () {
      if (message.type !== "queue_update" && message.type !== "state_update")
        yield* log({
          sessionId,
          kind: "message",
          name: message.type,
          ...("clip" in message.data ? { clipId: message.data.clip.clip_id } : {}),
        });
      const json = yield* Effect.orDie(Schema.decodeUnknownEffect(Wire.StructJson)(message.data));
      yield* send("data", Wire.DataServerMessageSchema, {
        requestId,
        kind,
        payload: { case: "message", value: { type: message.type, data: json } },
      });
    });
  const respond = (
    requestId: string,
    payload?: MessageInitShape<typeof Wire.DataServerMessageSchema>["payload"],
  ) =>
    send("data", Wire.DataServerMessageSchema, {
      requestId,
      kind: Wire.MessageKind.RESPONSE,
      ...(payload && { payload }),
    });
  const later = (ms: number, input: H3.Input) =>
    FiberSet.run(timers, Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(apply(input))));
  /** Media reaches the open connection on a track it has resumed. */
  const media = (track: string, deliver: (link: Link) => Effect.Effect<void>) =>
    Effect.flatMap(Ref.get(connection), (open) =>
      open === undefined || open.paused.has(track) ? Effect.void : deliver(open.link),
    );
  const latency = Effect.map(Ref.get(connection), (open) => open?.latency ?? 0);
  /** One black frame, sent `at` a boundary: the flush H3 documents for `flush_on_clip_end`. */
  const flush = (clip: Clip, at: number) =>
    until(at).pipe(
      Effect.andThen(
        media(profile.tracks.video, (link) =>
          link.video({
            data: Media.render({
              clipId: clip.clip_id,
              index: clip.frames,
              picture: "black",
              width: options.width,
              height: options.height,
            }),
            frameId: BigInt(clip.frames + 1),
            timestampMicros: BigInt(Math.round(at * 1000)),
          }),
        ),
      ),
    );
  /** One clip's frames and audio, each at its own time, until the clip ends or stops. */
  const play = (clip: Clip, startedAt: number) =>
    Effect.gen(function* () {
      const video = yield* faults.standing((fault) => fault._tag === "Video");
      const silent = yield* faults.standing((fault) => fault._tag === "NoAudio");
      const picture = video?._tag === "Video" ? video.video : "live";
      const delay = yield* latency;
      const frameMs = 1000 / profile.fps;
      const frame = (index: number) =>
        until(startedAt + delay + index * frameMs).pipe(
          Effect.andThen(
            media(profile.tracks.video, (link) =>
              link.video({
                data: Media.render({
                  clipId: clip.clip_id,
                  index,
                  picture,
                  width: options.width,
                  height: options.height,
                }),
                frameId: BigInt(index + 1),
                timestampMicros: BigInt(Math.round((startedAt + index * frameMs) * 1000)),
              }),
            ),
          ),
        );
      const block = (index: number) =>
        until(startedAt + delay + index * Media.audioBlockMs).pipe(
          Effect.andThen(
            media(profile.tracks.audio, (link) =>
              link.audio(
                Media.tone({ clipId: clip.clip_id, first: index * Media.samplesPerBlock }),
              ),
            ),
          ),
        );
      const blocks = Math.floor((clip.seconds * 1000) / Media.audioBlockMs);
      yield* Effect.all(
        [
          Effect.forEach(range(picture === "absent" ? 0 : clip.frames), frame, { discard: true }),
          Effect.forEach(range(silent === undefined ? blocks : 0), block, { discard: true }),
        ],
        { concurrency: 2, discard: true },
      );
    });

  const perform = (output: H3.Output, withheld: string | undefined) =>
    Effect.gen(function* () {
      if ("requestId" in output && output.requestId === withheld) return;
      switch (output._tag) {
        case "Reply":
          return yield* data(output.requestId, Wire.MessageKind.RESPONSE, output.message);
        case "Broadcast":
          return yield* data("", Wire.MessageKind.NOTIFICATION, output.message);
        case "Ack":
          return yield* respond(output.requestId);
        case "Unknown":
          return yield* respond(output.requestId, {
            case: "error",
            value: { code: "unknown_command", message: output.command },
          });
        case "Build": {
          const fault = yield* faults.trip(
            (candidate) => candidate._tag === "StallBuild" || candidate._tag === "FailBuild",
          );
          if (fault?._tag === "StallBuild") return;
          const speed = output.continued
            ? environment.timing.continuedBuildSpeed
            : environment.timing.buildSpeed;
          const ms = (output.seconds / (yield* speed)) * 1000;
          return yield* later(
            ms,
            fault?._tag === "FailBuild"
              ? { _tag: "BuildFailed", token: output.token, reason: fault.reason ?? "build failed" }
              : { _tag: "Built", token: output.token },
          );
        }
        case "Arm": {
          const seam = yield* environment.timing.delay("seam");
          return yield* later(seam, { _tag: "Start", token: output.token });
        }
        case "Land": {
          const lag = yield* environment.timing.delay("stop");
          return yield* later(lag, { _tag: "Landed", token: output.token });
        }
        case "Play":
          yield* later(output.clip.seconds * 1000, { _tag: "Finish", token: output.token });
          return yield* FiberMap.run(players, output.token, play(output.clip, output.startedAt));
        case "Halt":
          return yield* FiberMap.remove(players, output.token);
        case "Flush": {
          const at = (yield* monotonic) + (yield* latency);
          return yield* FiberSet.run(timers, flush(output.clip, at));
        }
        case "Continuation":
          return yield* log({
            sessionId,
            kind: "build",
            name: output.applied ? "continued" : "independent",
            clipId: output.clipId,
            continuedFrom: output.from,
          });
      }
    }).pipe(Effect.asVoid);

  function apply(input: H3.Input, withheld?: string): Effect.Effect<void> {
    return lock.withPermit(
      Effect.gen(function* () {
        const [next, outputs] = H3.step(yield* Ref.get(model), input, {
          now: yield* monotonic,
          generationCapacity: options.generationCapacity,
          playoutCapacity: options.playoutCapacity,
        });
        yield* Ref.set(model, next);
        for (const output of outputs) yield* perform(output, withheld);
      }),
    );
  }

  const control = (message: Wire.ControlClientMessage) => {
    const answer = (
      payload: NonNullable<MessageInitShape<typeof Wire.ControlServerMessageSchema>["payload"]>,
    ) =>
      send("control", Wire.ControlServerMessageSchema, {
        requestId: message.requestId,
        kind: Wire.MessageKind.RESPONSE,
        payload,
      });
    const pause = (name: string, paused: boolean) =>
      Ref.update(connection, (open) => {
        if (open === undefined) return open;
        const next = new Set(open.paused);
        if (paused) next.add(name);
        else next.delete(name);
        return { ...open, paused: next };
      });
    switch (message.payload.case) {
      case "requestSchema":
        return answer({ case: "modelSchema", value: { openapi: environment.openapi } });
      case "pauseTrack":
        return pause(message.payload.value.name, true);
      case "resumeTrack":
        return pause(message.payload.value.name, false);
      case "publishTrack":
        return answer({ case: "error", value: { code: "unknown_track", message: "no input" } });
      case "requestClip":
      case "requestRecording":
        return answer({ case: "clipFailed", value: { reason: "recorder disabled" } });
      default:
        // Pings and upload or unpublish notifications need no answer.
        return Effect.void;
    }
  };

  const command = (message: Wire.DataClientMessage) =>
    Effect.gen(function* () {
      if (message.payload.case !== "command") return;
      const { type: name, data: args } = message.payload.value;
      const requestId = message.requestId;
      const fault = yield* faults.trip(
        (candidate) => candidate._tag === "DropReply" && candidate.command === name,
      );
      const applied = fault?._tag === "DropReply" && fault.applied === true;
      const dropped = fault && (applied ? "reply" : "command");
      yield* log({ sessionId, kind: "command", name, ...(dropped && { dropped }) });
      if (dropped === "command") return;
      const input = args ?? {};
      const images = input.reference_images;
      const invalid =
        name === "enqueue" && Array.isArray(images) && images.length > 0
          ? yield* faults.trip((candidate) => candidate._tag === "InvalidImage")
          : undefined;
      yield* apply(
        {
          _tag: "Command",
          requestId,
          name,
          args: input,
          ...(invalid === undefined ? {} : { refuse: "a reference image is invalid" }),
        },
        applied ? requestId : undefined,
      );
    });

  return {
    /** The connection's channels opened: hosted Reactor holds its media until it resumes the tracks. */
    connect: (link: Link) =>
      Effect.flatMap(environment.timing.delay("channel"), (latency) =>
        Ref.set(connection, {
          link,
          paused: new Set([profile.tracks.video, profile.tracks.audio]),
          latency,
        }),
      ).pipe(Effect.andThen(apply({ _tag: "Connected" }))),
    disconnect: (link: Link) =>
      Ref.update(connection, (open) => (open?.link === link ? undefined : open)),
    receive: (link: Link, channel: "control" | "data", bytes: Uint8Array) =>
      Effect.gen(function* () {
        if ((yield* Ref.get(connection))?.link !== link) return;
        if (channel === "control")
          yield* control(yield* Wire.decode(Wire.ControlClientMessageSchema, bytes));
        else yield* command(yield* Wire.decode(Wire.DataClientMessageSchema, bytes));
      }).pipe(Effect.ignore),
  };
});

export type Playout = Effect.Success<ReturnType<typeof make>>;
