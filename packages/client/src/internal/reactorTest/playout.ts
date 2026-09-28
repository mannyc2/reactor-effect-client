/**
 * One simulated H3 session at work: its model takes one input at a time, what
 * it sends goes over the session's open connections, and its timers and media
 * run in the session's scope until the session ends.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as FiberSet from "effect/FiberSet";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import type { DescMessage, MessageInitShape } from "@bufbuild/protobuf";
import type { Entry, Fault, Options } from "../../ReactorTest.js";
import * as Wire from "../wire.js";
import type { Faults } from "./faults.js";
import * as H3 from "./h3.js";
import type { Clip, Message } from "./h3.js";
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
  /** Ends the session, as a moderation verdict does. */
  readonly terminate: Effect.Effect<void>;
  /** The recorder's clip of the session's last `seconds`, or its recording so far. */
  readonly record: (
    kind: "snap" | "recording",
    seconds: number,
  ) => Effect.Effect<MessageInitShape<typeof Wire.ClipReadySchema>>;
}

/**
 * An open connection. Its media arrives at one latency, drawn when it opens,
 * so frames keep their order across a boundary as a track does, and it
 * receives only the tracks it has resumed.
 */
interface Connection {
  readonly link: Link;
  readonly paused: ReadonlySet<string>;
  readonly latency: number;
  /** The clips playing to it, by their start tokens: a stop ends one, a boundary none. */
  readonly players: FiberMap.FiberMap<number, void>;
  /** Closes with the connection, and its media with it. */
  readonly scope: Scope.Closeable;
}

const range = (count: number) => Array.from({ length: Math.max(0, count) }, (_, index) => index);
const tracks = [H3.documented.tracks.video, H3.documented.tracks.audio];

export const make = Effect.fnUntraced(function* (sessionId: string, environment: Environment) {
  const { options, faults, log } = environment;
  const model = yield* Ref.make(H3.initial);
  const lock = yield* Semaphore.make(1);
  const scope = yield* Effect.scope;
  const timers = yield* FiberSet.make<void>();
  /** The open connections, by the id of the slot each was registered as. */
  const connections = yield* Ref.make<ReadonlyMap<number, Connection>>(new Map());
  /**
   * Each command not yet answered: the slot it came from, which its answer
   * goes back to whenever it is sent, and whether a fault withholds or delays it.
   */
  const requests = yield* Ref.make<
    ReadonlyMap<
      string,
      { readonly from: number; readonly withheld: boolean; readonly lateMs: number }
    >
  >(new Map());
  /**
   * Sends a command's answer back to the slot it came from, at once or as late
   * as a fault says, and forgets the command; a withheld answer is never sent.
   */
  const answer = (requestId: string, reply: (to: number) => Effect.Effect<void>) =>
    Effect.gen(function* () {
      const request = yield* Ref.modify(requests, (all) => {
        const next = new Map(all);
        next.delete(requestId);
        return [all.get(requestId), next] as const;
      });
      if (request === undefined || request.withheld) return;
      if (request.lateMs === 0) return yield* reply(request.from);
      yield* FiberSet.run(
        timers,
        Effect.sleep(Duration.millis(request.lateMs)).pipe(Effect.andThen(reply(request.from))),
      );
    });

  /**
   * Sends to one connection, or to every open one. The simulation encodes only
   * messages it built, so a failure is a defect.
   */
  const send = <Desc extends DescMessage>(
    to: number | "all",
    channel: "control" | "data",
    schema: Desc,
    message: MessageInitShape<Desc>,
  ) =>
    Effect.gen(function* () {
      const open = yield* Ref.get(connections);
      const targets = to === "all" ? [...open.values()] : [open.get(to)];
      const bytes = yield* Effect.orDie(Wire.encode(schema, message));
      for (const target of targets)
        if (target !== undefined) yield* target.link.deliver(channel, bytes);
    });
  const data = (to: number | "all", requestId: string, kind: Wire.MessageKind, message: Message) =>
    Effect.gen(function* () {
      if (message.type !== "queue_update" && message.type !== "state_update")
        yield* log({
          sessionId,
          kind: "message",
          name: message.type,
          ...("clip" in message.data ? { clipId: message.data.clip.clip_id } : {}),
        });
      const json = yield* Effect.orDie(Schema.decodeUnknownEffect(Wire.StructJson)(message.data));
      yield* send(to, "data", Wire.DataServerMessageSchema, {
        requestId,
        kind,
        payload: { case: "message", value: { type: message.type, data: json } },
      });
    });
  const respond = (
    to: number,
    requestId: string,
    payload?: MessageInitShape<typeof Wire.DataServerMessageSchema>["payload"],
  ) =>
    send(to, "data", Wire.DataServerMessageSchema, {
      requestId,
      kind: Wire.MessageKind.RESPONSE,
      ...(payload && { payload }),
    });
  const later = (ms: number, input: H3.Input) =>
    FiberSet.run(timers, Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(apply(input))));
  /** Media reaches a connection, while it is open, on a track it has resumed. */
  const media = (
    to: number,
    link: Link,
    track: string,
    deliver: (link: Link) => Effect.Effect<void>,
  ) =>
    Effect.flatMap(Ref.get(connections), (open) => {
      const connection = open.get(to);
      return connection?.link !== link || connection.paused.has(track)
        ? Effect.void
        : deliver(link);
    });
  /** One black frame, sent `at` a boundary: the flush H3 documents for `flush_on_clip_end`. */
  const flush = (to: number, link: Link, clip: Clip, at: number) =>
    until(at).pipe(
      Effect.andThen(
        media(to, link, H3.documented.tracks.video, () =>
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
  /**
   * One clip's frames and audio to one connection, each at its own time, until
   * it ends or stops. A connection that opens while it plays joins it there.
   */
  const play = (to: number, connection: Connection, clip: Clip, startedAt: number) =>
    Effect.gen(function* () {
      const video = yield* faults.standing((fault) => fault._tag === "Video");
      const silent = yield* faults.standing((fault) => fault._tag === "NoAudio");
      const picture = video?._tag === "Video" ? video.video : "live";
      const { link, latency: delay } = connection;
      const frameMs = 1000 / H3.documented.fps;
      const past = (yield* monotonic) - startedAt - delay;
      const from = (step: number) => Math.max(0, Math.ceil(past / step));
      const frame = (index: number) =>
        until(startedAt + delay + index * frameMs).pipe(
          Effect.andThen(
            media(to, link, H3.documented.tracks.video, () =>
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
            media(to, link, H3.documented.tracks.audio, () =>
              link.audio(
                Media.tone({ clipId: clip.clip_id, first: index * Media.samplesPerBlock }),
              ),
            ),
          ),
        );
      const blocks = Math.floor((clip.seconds * 1000) / Media.audioBlockMs);
      const frames = picture === "absent" ? 0 : clip.frames;
      yield* Effect.all(
        [
          Effect.forEach(range(frames).slice(from(frameMs)), frame, { discard: true }),
          Effect.forEach(
            range(silent === undefined ? blocks : 0).slice(from(Media.audioBlockMs)),
            block,
            { discard: true },
          ),
        ],
        { concurrency: 2, discard: true },
      );
    });

  /** `greeting` is a connection that just opened, which alone hears the state and queue. */
  const perform = (output: H3.Output, greeting: number | undefined) =>
    Effect.gen(function* () {
      switch (output._tag) {
        case "Reply":
          return yield* answer(output.requestId, (to) =>
            data(to, output.requestId, Wire.MessageKind.RESPONSE, output.message),
          );
        case "Broadcast":
          return yield* data(greeting ?? "all", "", Wire.MessageKind.NOTIFICATION, output.message);
        case "Ack":
          return yield* answer(output.requestId, (to) => respond(to, output.requestId));
        case "Unknown":
          return yield* answer(output.requestId, (to) =>
            respond(to, output.requestId, {
              case: "error",
              value: { code: "unknown_command", message: output.command },
            }),
          );
        case "Build": {
          const fault = yield* faults.trip(
            (candidate) => candidate._tag === "StallBuild" || candidate._tag === "FailBuild",
          );
          if (fault?._tag === "StallBuild") return;
          const speed = output.continued
            ? environment.timing.continuedBuildSpeed
            : environment.timing.buildSpeed;
          return yield* later(
            (output.seconds / (yield* speed)) * 1000,
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
          for (const [to, connection] of yield* Ref.get(connections))
            yield* FiberMap.run(
              connection.players,
              output.token,
              play(to, connection, output.clip, output.startedAt),
            );
          return;
        case "Halt":
          for (const connection of (yield* Ref.get(connections)).values())
            yield* FiberMap.remove(connection.players, output.token);
          return;
        case "Flush": {
          const now = yield* monotonic;
          for (const [to, connection] of yield* Ref.get(connections))
            yield* FiberSet.run(
              timers,
              flush(to, connection.link, output.clip, now + connection.latency),
            );
          return;
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

  /**
   * Content moderation screens an enqueue while the model takes it: the
   * verdict follows the reply by the moderation delay, naming no category,
   * input, command or request, as hosted H3's did; on `terminate` the session
   * then ends. A fault may withhold the verdict and end the session all the same.
   */
  const moderate = (fault: Extract<Fault, { readonly _tag: "Moderate" }>) =>
    Effect.gen(function* () {
      const action = fault.action ?? "terminate";
      yield* log({ sessionId, kind: "session", name: `moderation ${action}` });
      if (fault.verdict !== false)
        yield* send("all", "control", Wire.ControlServerMessageSchema, {
          requestId: "",
          kind: Wire.MessageKind.NOTIFICATION,
          payload: { case: "moderation", value: { action, categories: [] } },
        });
      if (action === "terminate") yield* environment.terminate;
    });

  function apply(input: H3.Input, greeting?: number): Effect.Effect<void> {
    return lock.withPermit(
      Effect.gen(function* () {
        const [next, outputs] = H3.step(yield* Ref.get(model), input, {
          now: yield* monotonic,
          generationCapacity: options.generationCapacity,
          playoutCapacity: options.playoutCapacity,
        });
        yield* Ref.set(model, next);
        for (const output of outputs) yield* perform(output, greeting);
      }),
    );
  }

  const control = (from: number, message: Wire.ControlClientMessage) => {
    const answer = (
      payload: NonNullable<MessageInitShape<typeof Wire.ControlServerMessageSchema>["payload"]>,
    ) =>
      send(from, "control", Wire.ControlServerMessageSchema, {
        requestId: message.requestId,
        kind: Wire.MessageKind.RESPONSE,
        payload,
      });
    const pause = (name: string, paused: boolean) =>
      Ref.update(connections, (open) => {
        const connection = open.get(from);
        if (connection === undefined) return open;
        const next = new Set(connection.paused);
        if (paused) next.add(name);
        else next.delete(name);
        return new Map(open).set(from, { ...connection, paused: next });
      });
    switch (message.payload.case) {
      case "requestSchema":
        return answer({ case: "modelSchema", value: { openapi: environment.openapi } });
      case "pauseTrack":
        return pause(message.payload.value.name, true).pipe(
          Effect.andThen(
            log({ sessionId, kind: "track", name: `pause ${message.payload.value.name}` }),
          ),
        );
      case "resumeTrack":
        return pause(message.payload.value.name, false).pipe(
          Effect.andThen(
            log({ sessionId, kind: "track", name: `resume ${message.payload.value.name}` }),
          ),
        );
      case "publishTrack":
        return answer({ case: "error", value: { code: "unknown_track", message: "no input" } });
      case "requestClip":
      case "requestRecording": {
        if (!options.recorder)
          return answer({ case: "clipFailed", value: { reason: "recorder disabled" } });
        const clip =
          message.payload.case === "requestClip"
            ? environment.record("snap", message.payload.value.durationSeconds)
            : environment.record("recording", 0);
        return Effect.flatMap(clip, (value) => answer({ case: "clipReady", value }));
      }
      default:
        // Pings and upload or unpublish notifications need no answer.
        return Effect.void;
    }
  };

  const command = (from: number, message: Wire.DataClientMessage) =>
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
      const late = yield* faults.trip(
        (candidate) => candidate._tag === "LateReply" && candidate.command === name,
      );
      const lateMs = late?._tag === "LateReply" ? Duration.toMillis(late.after) : 0;
      yield* Ref.update(requests, (all) =>
        new Map(all).set(requestId, { from, withheld: applied, lateMs }),
      );
      const flagged =
        name === "enqueue"
          ? yield* faults.trip(
              (candidate) =>
                candidate._tag === "Moderate" &&
                (candidate.prompt === undefined || candidate.prompt === input.prompt),
            )
          : undefined;
      yield* apply({
        _tag: "Command",
        requestId,
        name,
        args: input,
        ...(invalid === undefined ? {} : { refuse: "a reference image is invalid" }),
      });
      if (flagged?._tag === "Moderate")
        yield* FiberSet.run(
          timers,
          Effect.flatMap(environment.timing.delay("moderation"), (delay) =>
            Effect.sleep(Duration.millis(delay)).pipe(Effect.andThen(moderate(flagged))),
          ),
        );
    });

  return {
    /**
     * A connection's channels opened: hosted Reactor holds its media until it
     * resumes the tracks, and a clip already playing plays on to it.
     */
    connect: (to: number, link: Link) =>
      Effect.gen(function* () {
        const own = yield* Scope.fork(scope);
        const connection: Connection = {
          link,
          paused: new Set(tracks),
          latency: yield* environment.timing.delay("channel"),
          players: yield* FiberMap.make<number, void>().pipe(Scope.provide(own)),
          scope: own,
        };
        yield* Ref.update(connections, (open) => new Map(open).set(to, connection));
        const playing = (yield* Ref.get(model)).playing;
        if (playing !== undefined)
          yield* FiberMap.run(
            connection.players,
            playing.token,
            play(to, connection, playing.clip, playing.startedAt),
          );
        yield* apply({ _tag: "Connected" }, to);
      }),
    disconnect: (from: number) =>
      Effect.gen(function* () {
        const closed = yield* Ref.modify(connections, (open) => {
          const next = new Map(open);
          next.delete(from);
          return [open.get(from), next] as const;
        });
        if (closed !== undefined) yield* Scope.close(closed.scope, Exit.void);
      }),
    /** A message the client sent. One the simulation cannot decode is the client's bug. */
    receive: (from: number, link: Link, channel: "control" | "data", bytes: Uint8Array) =>
      Effect.gen(function* () {
        if ((yield* Ref.get(connections)).get(from)?.link !== link) return;
        if (channel === "control")
          yield* control(from, yield* Wire.decode(Wire.ControlClientMessageSchema, bytes));
        else yield* command(from, yield* Wire.decode(Wire.DataClientMessageSchema, bytes));
      }).pipe(Effect.orDie),
  };
});

export type Playout = Effect.Success<ReturnType<typeof make>>;
