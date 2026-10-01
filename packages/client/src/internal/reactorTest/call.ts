/**
 * One simulated Vidu S2-Avatar session at work: its model takes one input at a
 * time, and the character's picture and speech run on the session's runner.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Wire from "../wire.js";
import { clipId } from "./h3.js";
import * as Media from "./media.js";
import { monotonic, until } from "./runner.js";
import type { Connection, Model, Runner, Simulation } from "./runner.js";
import * as Vidu from "./vidu.js";

/** The avatars Reactor keeps across sessions, by id. */
export interface Avatars {
  readonly find: (id: string) => Effect.Effect<Vidu.Avatar | undefined>;
  readonly save: (avatar: Vidu.Avatar) => Effect.Effect<void>;
  readonly nextId: Effect.Effect<string>;
}

const range = (count: number) => Array.from({ length: Math.max(0, count) }, (_, index) => index);
const frameMs = 1000 / Vidu.documented.fps;
const { video, audio } = Vidu.documented.tracks;

/** What plays now, so a connection that opens joins it where it is. */
interface Playing {
  readonly picture:
    | { readonly token: number; readonly call: number; readonly at: number }
    | undefined;
  readonly speech:
    | { readonly token: number; readonly turn: number; readonly at: number; readonly ms: number }
    | undefined;
}

const start = (avatars: Avatars) =>
  Effect.fnUntraced(function* (runner: Runner<Vidu.Message>) {
    const { options, faults, timing } = runner.environment;
    const model = yield* Ref.make(Vidu.initial);
    const playing = yield* Ref.make<Playing>({ picture: undefined, speech: undefined });
    const lock = yield* Semaphore.make(1);
    const openapi = yield* Schema.decodeUnknownEffect(Wire.StructJson)(Vidu.deployment()).pipe(
      Effect.orDie,
    );

    const later = (ms: number, input: Vidu.Input) => runner.later(ms, apply(input));
    /**
     * The character's picture to one connection, a frame each 40 ms from `at`
     * until the call stops it. Its frames name the call, counted from 1.
     */
    const show = (to: number, connection: Connection, call: number, at: number) =>
      Effect.gen(function* () {
        const fault = yield* faults.standing((candidate) => candidate._tag === "Video");
        const picture = fault?._tag === "Video" ? fault.video : "live";
        if (picture === "absent") return;
        const { link, latency } = connection;
        const past = (yield* monotonic) - at - latency;
        for (let index = Math.max(0, Math.ceil(past / frameMs)); ; index++) {
          yield* until(at + latency + index * frameMs);
          yield* runner.media(to, link, video, () =>
            link.video({
              data: Media.render({
                clipId: clipId(call),
                index,
                picture,
                width: options.width,
                height: options.height,
              }),
              frameId: BigInt(index + 1),
              timestampMicros: BigInt(Math.round((at + index * frameMs) * 1000)),
            }),
          );
        }
      });
    /** One answer's speech to one connection: a tone named for the answer, from `at` for `ms`. */
    const speak = (to: number, connection: Connection, speech: NonNullable<Playing["speech"]>) =>
      Effect.gen(function* () {
        if ((yield* faults.standing((fault) => fault._tag === "NoAudio")) !== undefined) return;
        const { link, latency } = connection;
        const past = (yield* monotonic) - speech.at - latency;
        const blocks = Math.floor(speech.ms / Media.audioBlockMs);
        const from = Math.max(0, Math.ceil(past / Media.audioBlockMs));
        yield* Effect.forEach(
          range(blocks).slice(from),
          (index) =>
            until(speech.at + latency + index * Media.audioBlockMs).pipe(
              Effect.andThen(
                runner.media(to, link, audio, () =>
                  link.audio(
                    Media.tone({
                      clipId: clipId(speech.turn),
                      first: index * Media.samplesPerBlock,
                    }),
                  ),
                ),
              ),
            ),
          { discard: true },
        );
      });
    const everyone = (run: (to: number, connection: Connection) => Effect.Effect<void>) =>
      Effect.flatMap(runner.connections, (open) =>
        Effect.forEach([...open], ([to, connection]) => run(to, connection), { discard: true }),
      );

    /** `greeting` is a connection that just opened, which alone hears the snapshot. */
    const perform = (output: Vidu.Output, greeting: number | undefined) =>
      Effect.gen(function* () {
        switch (output._tag) {
          case "Reply":
            return yield* runner.reply(output.requestId, output.message);
          case "Ack":
            return yield* runner.ack(output.requestId);
          case "Reject":
            return yield* runner.reject(output.requestId, output.code, output.message);
          case "Broadcast":
            return yield* runner.broadcast(output.message, greeting);
          case "Prepare":
            return yield* later(yield* timing.delay("avatar"), {
              _tag: "AvatarReady",
              token: output.token,
            });
          case "Warm": {
            const ms = yield* timing.delay("call");
            yield* later(ms / 2, { _tag: "WarmingUp", token: output.token });
            return yield* later(ms, { _tag: "Live", token: output.token });
          }
          case "Think":
            return yield* later(yield* timing.delay("answer"), {
              _tag: "Answer",
              token: output.token,
            });
          case "Speak": {
            const speech = {
              token: output.token,
              turn: output.turn,
              at: yield* monotonic,
              ms: yield* timing.delay("speech"),
            };
            yield* Ref.update(playing, (now) => ({ ...now, speech }));
            yield* everyone((to, connection) =>
              Effect.asVoid(
                FiberMap.run(connection.players, speech.token, speak(to, connection, speech)),
              ),
            );
            return yield* later(speech.ms, { _tag: "Spoken", token: output.token });
          }
          case "Hush":
            yield* Ref.update(playing, (now) =>
              now.speech?.token === output.token ? { ...now, speech: undefined } : now,
            );
            return yield* everyone((_, connection) =>
              FiberMap.remove(connection.players, output.token),
            );
          case "Hangup":
            return yield* later(yield* timing.delay("hangup"), {
              _tag: "Released",
              token: output.token,
              requestId: output.requestId,
            });
          case "Picture": {
            const picture = { token: output.token, call: output.call, at: output.at };
            yield* Ref.update(playing, (now) => ({ ...now, picture }));
            return yield* everyone((to, connection) =>
              Effect.asVoid(
                FiberMap.run(
                  connection.players,
                  picture.token,
                  show(to, connection, picture.call, picture.at),
                ),
              ),
            );
          }
          case "Dark":
            yield* Ref.set(playing, { picture: undefined, speech: undefined });
            return yield* everyone((_, connection) =>
              FiberMap.remove(connection.players, output.token),
            );
          case "Save":
            return yield* avatars.save(output.avatar);
        }
      }).pipe(Effect.asVoid);

    function apply(input: Vidu.Input, greeting?: number): Effect.Effect<void> {
      return lock.withPermit(
        Effect.gen(function* () {
          const [next, outputs] = Vidu.step({
            model: yield* Ref.get(model),
            input,
            env: { now: yield* monotonic, unix: yield* Clock.currentTimeMillis },
          });
          yield* Ref.set(model, next);
          for (const output of outputs) yield* perform(output, greeting);
        }),
      );
    }

    return {
      openapi,
      outputs: [video, audio],
      /** The character's picture and speech play on to a connection that opens, which alone hears the snapshot. */
      connected: (to, connection) =>
        Effect.gen(function* () {
          const { picture, speech } = yield* Ref.get(playing);
          if (picture !== undefined)
            yield* FiberMap.run(
              connection.players,
              picture.token,
              show(to, connection, picture.call, picture.at),
            );
          if (speech !== undefined)
            yield* FiberMap.run(connection.players, speech.token, speak(to, connection, speech));
          yield* apply({ _tag: "Connected" }, to);
        }),
      command: ({ requestId, name, args }) =>
        Effect.gen(function* () {
          const id = args.avatar_id;
          const saved =
            name === "attach_avatar" && typeof id === "string"
              ? yield* avatars.find(id)
              : undefined;
          const newAvatarId = name === "create_avatar" ? yield* avatars.nextId : undefined;
          yield* apply({
            _tag: "Command",
            requestId,
            name,
            args,
            saved,
            ...(newAvatarId === undefined ? {} : { newAvatarId }),
          });
        }),
    } satisfies Model;
  });

/** Vidu S2-Avatar as the runner simulates it, its avatars kept by the simulated Reactor. */
export const simulation = (avatars: Avatars): Simulation<Vidu.Message> => ({
  describe: (message) => (message.type === "session_state" ? undefined : { name: message.type }),
  start: start(avatars),
});
