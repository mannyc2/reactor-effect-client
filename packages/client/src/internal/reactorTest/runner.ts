/**
 * One simulated session at work, whatever its model: what the model sends goes
 * over the session's open connections, a command's answer goes back to the
 * connection it came from, and timers and media run in the session's scope
 * until the session ends. Each simulated model brings its own state machine.
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
import type { DescMessage, MessageInitShape } from "@bufbuild/protobuf";
import type { Entry, Options } from "../../ReactorTest.js";
import * as Wire from "../wire.js";
import type { Faults } from "./faults.js";
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
  readonly log: (entry: Omit<Entry, "at">) => Effect.Effect<void>;
  /** Ends the session, as a moderation verdict does. */
  readonly terminate: Effect.Effect<void>;
  /** The recorder's clip of the session's last `seconds`, or its recording so far. */
  readonly record: (
    kind: "snap" | "recording",
    seconds: number,
  ) => Effect.Effect<MessageInitShape<typeof Wire.ClipReadySchema>>;
}

/** A message a model sends on the data channel. */
export interface Message {
  readonly type: string;
  readonly data: object;
}

/**
 * An open connection. Its media arrives at one latency, drawn when it opens,
 * so frames keep their order across a boundary as a track does, and it
 * receives only the tracks it has resumed.
 */
export interface Connection {
  readonly link: Link;
  readonly paused: ReadonlySet<string>;
  readonly latency: number;
  /** The model's media playing to it, by the token the model started each with. */
  readonly players: FiberMap.FiberMap<number, void>;
  /** Closes with the connection, and its media with it. */
  readonly scope: Scope.Closeable;
}

/** What a running session offers the model it simulates. */
export interface Runner<M extends Message> {
  readonly sessionId: string;
  readonly environment: Environment;
  readonly connections: Effect.Effect<ReadonlyMap<number, Connection>>;
  /** Runs `effect` in the session's scope, without waiting for it. */
  readonly fork: (effect: Effect.Effect<void>) => Effect.Effect<void>;
  /** Runs `effect` after `ms`, in the session's scope. */
  readonly later: (ms: number, effect: Effect.Effect<void>) => Effect.Effect<void>;
  /** The command's correlated reply, to the connection it came from, as late as a fault says. */
  readonly reply: (requestId: string, message: M) => Effect.Effect<void>;
  /** The bodyless acknowledgement of a command whose handler sent no reply. */
  readonly ack: (requestId: string) => Effect.Effect<void>;
  /** A correlated error frame, as the runtime answers a command it could not run. */
  readonly reject: (requestId: string, code: string, message: string) => Effect.Effect<void>;
  /** A message to every open connection, or only to `to`. */
  readonly broadcast: (message: M, to?: number) => Effect.Effect<void>;
  /** Reactor stops sending `track` to every open connection until that connection resumes it. */
  readonly unsubscribe: (track: string) => Effect.Effect<void>;
  /** Media reaches a connection while it is open, on a track it has resumed. */
  readonly media: (
    to: number,
    link: Link,
    track: string,
    deliver: (link: Link) => Effect.Effect<void>,
  ) => Effect.Effect<void>;
  /**
   * Content moderation's verdict, to every connection, naming no category,
   * input, command or request, as hosted Reactor's did; on `terminate` the
   * session then ends. A verdict withheld ends the session all the same.
   */
  readonly moderate: (input: {
    readonly action: "terminate" | "warn";
    readonly verdict: boolean;
  }) => Effect.Effect<void>;
}

/** A simulated model: how the session log names what it sends, and the model itself. */
export interface Simulation<M extends Message> {
  /** The log entry for a message the model sends, when it is sent; undefined leaves it out. */
  readonly describe: (
    message: M,
  ) => { readonly name: string; readonly clipId?: string } | undefined;
  readonly start: (runner: Runner<M>) => Effect.Effect<Model, never, Scope.Scope>;
}

/** What a simulated model gives the runner. */
export interface Model {
  /** The deployment document `request_schema` answers with. */
  readonly openapi: typeof Wire.StructJson.Type;
  /** The receive-only tracks; a connection opens with each paused. */
  readonly outputs: ReadonlyArray<string>;
  /** A connection's channels opened. */
  readonly connected: (to: number, connection: Connection) => Effect.Effect<void>;
  /** A command arrived; its answer goes through the runner's `reply`, `ack` or `reject`. */
  readonly command: (command: {
    readonly requestId: string;
    readonly name: string;
    readonly args: Schema.JsonObject;
  }) => Effect.Effect<void>;
}

export const make = Effect.fnUntraced(function* <M extends Message>(
  sessionId: string,
  environment: Environment,
  simulation: Simulation<M>,
) {
  const { log } = environment;
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
  const fork = (effect: Effect.Effect<void>) => Effect.asVoid(FiberSet.run(timers, effect));
  const later = (ms: number, effect: Effect.Effect<void>) =>
    Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(effect), fork);
  /**
   * Sends a command's answer back to the slot it came from, at once or as late
   * as a fault says, and forgets the command; a withheld answer is never sent.
   */
  const answer = (requestId: string, send: (to: number) => Effect.Effect<void>) =>
    Effect.gen(function* () {
      const request = yield* Ref.modify(requests, (all) => {
        const next = new Map(all);
        next.delete(requestId);
        return [all.get(requestId), next] as const;
      });
      if (request === undefined || request.withheld) return;
      if (request.lateMs === 0) return yield* send(request.from);
      yield* later(request.lateMs, send(request.from));
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
      const bytes = yield* Effect.orDie(Wire.encode(schema)(message));
      for (const target of targets)
        if (target !== undefined) yield* target.link.deliver(channel, bytes);
    });
  const data = (to: number | "all", requestId: string, kind: Wire.MessageKind, message: M) =>
    Effect.gen(function* () {
      const described = simulation.describe(message);
      if (described !== undefined) yield* log({ sessionId, kind: "message", ...described });
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

  const runner: Runner<M> = {
    sessionId,
    environment,
    connections: Ref.get(connections),
    fork,
    later,
    reply: (requestId, message) =>
      answer(requestId, (to) => data(to, requestId, Wire.MessageKind.RESPONSE, message)),
    ack: (requestId) => answer(requestId, (to) => respond(to, requestId)),
    reject: (requestId, code, message) =>
      answer(requestId, (to) =>
        respond(to, requestId, { case: "error", value: { code, message } }),
      ),
    broadcast: (message, to) => data(to ?? "all", "", Wire.MessageKind.NOTIFICATION, message),
    unsubscribe: (track) =>
      Ref.update(
        connections,
        (open) =>
          new Map(
            Array.from(open, ([to, connection]) => [
              to,
              { ...connection, paused: new Set(connection.paused).add(track) },
            ]),
          ),
      ),
    media: (to, link, track, deliver) =>
      Effect.flatMap(Ref.get(connections), (open) => {
        const connection = open.get(to);
        return connection?.link !== link || connection.paused.has(track)
          ? Effect.void
          : deliver(link);
      }),
    moderate: ({ action, verdict }) =>
      Effect.gen(function* () {
        yield* log({ sessionId, kind: "session", name: `moderation ${action}` });
        if (verdict)
          yield* send("all", "control", Wire.ControlServerMessageSchema, {
            requestId: "",
            kind: Wire.MessageKind.NOTIFICATION,
            payload: { case: "moderation", value: { action, categories: [] } },
          });
        if (action === "terminate") yield* environment.terminate;
      }),
  };
  const model = yield* simulation.start(runner);

  const control = (from: number, message: Wire.ControlClientMessage) => {
    const reply = (
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
        return reply({ case: "modelSchema", value: { openapi: model.openapi } });
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
      // ReactorTest's peers decode media and send none, so no track is ever published.
      case "publishTrack":
        return reply({ case: "error", value: { code: "unknown_track", message: "no input" } });
      case "requestClip":
      case "requestRecording": {
        if (!environment.options.recorder)
          return reply({ case: "clipFailed", value: { reason: "recorder disabled" } });
        const clip =
          message.payload.case === "requestClip"
            ? environment.record("snap", message.payload.value.durationSeconds)
            : environment.record("recording", 0);
        return Effect.flatMap(clip, (value) => reply({ case: "clipReady", value }));
      }
      default:
        // Pings and upload or unpublish notifications need no answer.
        return Effect.void;
    }
  };

  const command = (from: number, message: Wire.DataClientMessage) =>
    Effect.gen(function* () {
      if (message.payload.case !== "command") return;
      const { type: name, data, uploads } = message.payload.value;
      const requestId = message.requestId;
      // The runtime injects each uploaded file into the argument its key names.
      const args: Schema.JsonObject = {
        ...data,
        ...Object.fromEntries(
          Object.entries(uploads).map(([key, file]) => [
            key,
            {
              upload_id: file.uploadId,
              name: file.name,
              mime_type: file.mimeType,
              size: Number(file.size),
            },
          ]),
        ),
      };
      const { faults } = environment;
      const fault = yield* faults.trip(
        (candidate) => candidate._tag === "DropReply" && candidate.command === name,
      );
      const applied = fault?._tag === "DropReply" && fault.applied === true;
      const dropped = fault && (applied ? "reply" : "command");
      yield* log({ sessionId, kind: "command", name, ...(dropped && { dropped }) });
      if (dropped === "command") return;
      const late = yield* faults.trip(
        (candidate) => candidate._tag === "LateReply" && candidate.command === name,
      );
      const lateMs = late?._tag === "LateReply" ? Duration.toMillis(late.after) : 0;
      yield* Ref.update(requests, (all) =>
        new Map(all).set(requestId, { from, withheld: applied, lateMs }),
      );
      yield* model.command({ requestId, name, args });
    });

  return {
    /**
     * A connection's channels opened: hosted Reactor holds its media until it
     * resumes the tracks, and the model then greets it.
     */
    connect: (to: number, link: Link) =>
      Effect.gen(function* () {
        const own = yield* Scope.fork(scope);
        const connection: Connection = {
          link,
          paused: new Set(model.outputs),
          latency: yield* environment.timing.delay("channel"),
          players: yield* FiberMap.make<number, void>().pipe(Scope.provide(own)),
          scope: own,
        };
        yield* Ref.update(connections, (open) => new Map(open).set(to, connection));
        yield* model.connected(to, connection);
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
          yield* control(from, yield* Wire.decode(Wire.ControlClientMessageSchema)(bytes));
        else yield* command(from, yield* Wire.decode(Wire.DataClientMessageSchema)(bytes));
      }).pipe(Effect.orDie),
  };
});

export type Session = Effect.Success<ReturnType<typeof make>>;
