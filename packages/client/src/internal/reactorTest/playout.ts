/**
 * One simulated H3 session at work: its model takes one input at a time, what
 * it sends goes over the session's open connection, and its timers and media
 * run in the session's scope until the session ends.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberHandle from "effect/FiberHandle";
import * as FiberSet from "effect/FiberSet";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import type { Clip, Message } from "../../h3/messages.js";
import { h3ReferenceTurboRealtime as profile } from "../../h3/profile.js";
import { objectFromStruct, structFromObject } from "../../json.js";
import type { Entry, Options } from "../../ReactorTest.js";
import {
  ControlClientMessage,
  ControlServerMessage,
  DataClientMessage,
  DataServerMessage,
  MessageKind,
} from "../../wire.generated.js";
import type { Google_Struct } from "../../wire.generated.js";
import type { Faults } from "./faults.js";
import * as H3 from "./h3.js";
import * as Media from "./media.js";
import type { Link } from "./peer.js";

export const monotonic = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1e6);

export const until = (at: number): Effect.Effect<void> =>
  Effect.flatMap(monotonic, (now) =>
    at > now ? Effect.sleep(Duration.millis(at - now)) : Effect.void,
  );

export interface Environment {
  readonly options: Options;
  readonly faults: Faults;
  /** Seeded once for the whole simulation, so seams repeat run to run. */
  readonly random: Random.Random;
  readonly openapi: Google_Struct;
  readonly log: (entry: Omit<Entry, "at">) => Effect.Effect<void>;
}

const range = (count: number) => Array.from({ length: Math.max(0, count) }, (_, index) => index);

export const make = Effect.fnUntraced(function* (sessionId: string, environment: Environment) {
  const { options, faults, log } = environment;
  const model = yield* Ref.make(H3.initial);
  const lock = yield* Semaphore.make(1);
  const timers = yield* FiberSet.make<void>();
  const player = yield* FiberHandle.make<void>();
  const connection = yield* Ref.make<
    { readonly link: Link; readonly paused: ReadonlySet<string> } | undefined
  >(undefined);

  const send = (channel: "control" | "data", bytes: Uint8Array<ArrayBuffer>) =>
    Effect.flatMap(Ref.get(connection), (open) =>
      open === undefined ? Effect.void : open.link.deliver(channel, bytes),
    );
  const data = (requestId: string, kind: number, message: Message) =>
    Effect.gen(function* () {
      if (message.type !== "queue_update" && message.type !== "state_update")
        yield* log({
          sessionId,
          kind: "message",
          name: message.type,
          ...("clip" in message.data ? { clipId: message.data.clip.clip_id } : {}),
        });
      const value = { type: message.type, data: structFromObject(message.data) };
      yield* send(
        "data",
        DataServerMessage.encode({
          request_id: requestId,
          kind,
          payload: { case: "message", value },
        }),
      );
    });
  const respond = (requestId: string, payload?: DataServerMessage["payload"]) =>
    send(
      "data",
      DataServerMessage.encode({
        request_id: requestId,
        kind: MessageKind.MESSAGE_KIND_RESPONSE,
        ...(payload && { payload }),
      }),
    );
  const later = (ms: number, input: H3.Input) =>
    FiberSet.run(timers, Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(apply(input))));
  /** Media reaches the open connection on a track it has resumed. */
  const media = (track: string, deliver: (link: Link) => Effect.Effect<void>) =>
    Effect.flatMap(Ref.get(connection), (open) =>
      open === undefined || open.paused.has(track) ? Effect.void : deliver(open.link),
    );
  /** One clip's frames and audio, each at its own time, until the clip ends or stops. */
  const play = (clip: Clip, startedAt: number) =>
    Effect.gen(function* () {
      const video = yield* faults.standing((fault) => fault._tag === "Video");
      const silent = yield* faults.standing((fault) => fault._tag === "NoAudio");
      const picture = video?._tag === "Video" ? video.video : "live";
      const latency = Duration.toMillis(options.channelLatency);
      const frameMs = 1000 / profile.fps;
      const frame = (index: number) =>
        until(startedAt + latency + index * frameMs).pipe(
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
        until(startedAt + latency + index * Media.audioBlockMs).pipe(
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
          return yield* data(output.requestId, MessageKind.MESSAGE_KIND_RESPONSE, output.message);
        case "Broadcast":
          return yield* data("", MessageKind.MESSAGE_KIND_NOTIFICATION, output.message);
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
          const ms = (output.seconds / options.buildSpeed) * 1000;
          return yield* later(
            ms,
            fault?._tag === "FailBuild"
              ? { _tag: "BuildFailed", token: output.token, reason: fault.reason ?? "build failed" }
              : { _tag: "Built", token: output.token },
          );
        }
        case "Arm": {
          const seam = yield* Random.nextBetween(
            Duration.toMillis(options.seamMin),
            Duration.toMillis(options.seamMax),
          ).pipe(Effect.provideService(Random.Random, environment.random));
          return yield* later(seam, { _tag: "Start", token: output.token });
        }
        case "Play":
          yield* later(output.clip.seconds * 1000, { _tag: "Finish", token: output.token });
          return yield* FiberHandle.run(player, play(output.clip, output.startedAt));
        case "Halt":
          return yield* FiberHandle.clear(player);
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

  const control = (message: ControlClientMessage) => {
    const answer = (payload: NonNullable<ControlServerMessage["payload"]>) =>
      send(
        "control",
        ControlServerMessage.encode({
          request_id: message.request_id,
          kind: MessageKind.MESSAGE_KIND_RESPONSE,
          payload,
        }),
      );
    const pause = (name: string, paused: boolean) =>
      Ref.update(connection, (open) => {
        if (open === undefined) return open;
        const next = new Set(open.paused);
        if (paused) next.add(name);
        else next.delete(name);
        return { ...open, paused: next };
      });
    switch (message.payload?.case) {
      case "request_schema":
        return answer({ case: "model_schema", value: { openapi: environment.openapi } });
      case "pause_track":
        return pause(message.payload.value.name, true);
      case "resume_track":
        return pause(message.payload.value.name, false);
      case "publish_track":
        return answer({ case: "error", value: { code: "unknown_track", message: "no input" } });
      case "request_clip":
      case "request_recording":
        return answer({ case: "clip_failed", value: { reason: "recorder disabled" } });
      default:
        // Pings and upload or unpublish notifications need no answer.
        return Effect.void;
    }
  };

  const command = (message: DataClientMessage) =>
    Effect.gen(function* () {
      if (message.payload?.case !== "command") return;
      const { type: name, data: args } = message.payload.value;
      const requestId = message.request_id;
      const fault = yield* faults.trip(
        (candidate) => candidate._tag === "DropReply" && candidate.command === name,
      );
      const applied = fault?._tag === "DropReply" && fault.applied === true;
      const dropped = fault && (applied ? "reply" : "command");
      yield* log({ sessionId, kind: "command", name, ...(dropped && { dropped }) });
      if (dropped === "command") return;
      const input = yield* Effect.try(() => (args === undefined ? {} : objectFromStruct(args)));
      yield* apply(
        { _tag: "Command", requestId, name, args: input },
        applied ? requestId : undefined,
      );
    });

  return {
    /** The connection's channels opened: hosted Reactor holds its media until it resumes the tracks. */
    connect: (link: Link) =>
      Ref.set(connection, {
        link,
        paused: new Set([profile.tracks.video, profile.tracks.audio]),
      }).pipe(Effect.andThen(apply({ _tag: "Connected" }))),
    disconnect: (link: Link) =>
      Ref.update(connection, (open) => (open?.link === link ? undefined : open)),
    receive: (link: Link, channel: "control" | "data", bytes: Uint8Array) =>
      Effect.gen(function* () {
        if ((yield* Ref.get(connection))?.link !== link) return;
        if (channel === "control")
          yield* control(yield* Effect.try(() => ControlClientMessage.decode(bytes)));
        else yield* command(yield* Effect.try(() => DataClientMessage.decode(bytes)));
      }).pipe(Effect.ignore),
  };
});

export type Playout = Effect.Success<ReturnType<typeof make>>;
