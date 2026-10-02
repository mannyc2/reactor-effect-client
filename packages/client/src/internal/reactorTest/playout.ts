/**
 * One simulated H3 session at work: its model takes one input at a time, and
 * its builds, boundaries and clips' media run on the session's runner.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Wire from "../wire.js";
import * as H3 from "./h3.js";
import type { Clip, Message } from "./h3.js";
import * as Media from "./media.js";
import type { Link } from "./peer.js";
import { monotonic, until } from "./runner.js";
import type { Connection, Model, Runner, Simulation } from "./runner.js";

const range = (count: number) => Array.from({ length: Math.max(0, count) }, (_, index) => index);

const start = Effect.fnUntraced(function* (runner: Runner<Message>) {
  const { options, faults, timing } = runner.environment;
  const model = yield* Ref.make(H3.initial);
  const lock = yield* Semaphore.make(1);
  const openapi = yield* Schema.decodeUnknownEffect(Wire.StructJson)(
    H3.deployment(options.referenceAudio),
  ).pipe(Effect.orDie);

  const later = (ms: number, input: H3.Input) => runner.later(ms, apply(input));
  /** One black frame, sent `at` a boundary: the flush H3 documents for `flush_on_clip_end`. */
  const flush = (to: number, link: Link, clip: Clip, at: number) =>
    until(at).pipe(
      Effect.andThen(
        runner.media(to, link, H3.documented.tracks.video, () =>
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
            runner.media(to, link, H3.documented.tracks.video, () =>
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
            runner.media(to, link, H3.documented.tracks.audio, () =>
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
          return yield* runner.reply(output.requestId, output.message);
        case "Broadcast":
          return yield* runner.broadcast(output.message, greeting);
        case "Ack":
          return yield* runner.ack(output.requestId);
        case "Unknown":
          return yield* runner.reject(output.requestId, "unknown_command", output.command);
        case "Build": {
          const fault = yield* faults.trip(
            (candidate) => candidate._tag === "StallBuild" || candidate._tag === "FailBuild",
          );
          if (fault?._tag === "StallBuild") return;
          const speed = output.continued ? timing.continuedBuildSpeed : timing.buildSpeed;
          return yield* later(
            (output.seconds / (yield* speed)) * 1000,
            fault?._tag === "FailBuild"
              ? { _tag: "BuildFailed", token: output.token, reason: fault.reason ?? "build failed" }
              : { _tag: "Built", token: output.token },
          );
        }
        case "Arm": {
          const seam = yield* timing.delay("seam");
          return yield* later(seam, { _tag: "Start", token: output.token });
        }
        case "Land": {
          const lag = yield* timing.delay("stop");
          return yield* later(lag, { _tag: "Landed", token: output.token });
        }
        case "Play":
          yield* later(output.clip.seconds * 1000, { _tag: "Finish", token: output.token });
          for (const [to, connection] of yield* runner.connections)
            yield* FiberMap.run(
              connection.players,
              output.token,
              play(to, connection, output.clip, output.startedAt),
            );
          return;
        case "Halt":
          for (const connection of (yield* runner.connections).values())
            yield* FiberMap.remove(connection.players, output.token);
          return;
        case "Flush": {
          const now = yield* monotonic;
          for (const [to, connection] of yield* runner.connections)
            yield* runner.fork(flush(to, connection.link, output.clip, now + connection.latency));
          return;
        }
        case "Continuation":
          return yield* runner.environment.log({
            sessionId: runner.sessionId,
            kind: "build",
            name: output.applied ? "continued" : "independent",
            clipId: output.clipId,
            continuedFrom: output.from,
          });
      }
    }).pipe(Effect.asVoid);

  function apply(input: H3.Input, greeting?: number): Effect.Effect<void> {
    return lock.withPermit(
      Effect.gen(function* () {
        const [next, outputs] = H3.step({
          model: yield* Ref.get(model),
          input,
          env: {
            now: yield* monotonic,
            generationCapacity: options.generationCapacity,
            playoutCapacity: options.playoutCapacity,
          },
        });
        yield* Ref.set(model, next);
        for (const output of outputs) yield* perform(output, greeting);
      }),
    );
  }

  return {
    openapi,
    outputs: [H3.documented.tracks.video, H3.documented.tracks.audio],
    /** A clip already playing plays on to a connection that opens, which alone hears the state. */
    connected: (to, connection) =>
      Effect.gen(function* () {
        const playing = (yield* Ref.get(model)).playing;
        if (playing !== undefined)
          yield* FiberMap.run(
            connection.players,
            playing.token,
            play(to, connection, playing.clip, playing.startedAt),
          );
        yield* apply({ _tag: "Connected" }, to);
      }),
    /**
     * Content moderation screens an enqueue while the model takes it: the
     * verdict follows the reply by the moderation delay.
     */
    command: ({ requestId, name, args }) =>
      Effect.gen(function* () {
        const images = args.reference_images;
        const invalid =
          name === "enqueue" && Array.isArray(images) && images.length > 0
            ? yield* faults.trip((candidate) => candidate._tag === "InvalidImage")
            : undefined;
        const flagged =
          name === "enqueue"
            ? yield* faults.trip(
                (candidate) =>
                  candidate._tag === "Moderate" &&
                  (candidate.prompt === undefined || candidate.prompt === args.prompt),
              )
            : undefined;
        yield* apply({
          _tag: "Command",
          requestId,
          name,
          args,
          ...(invalid === undefined ? {} : { refuse: "a reference image is invalid" }),
        });
        if (flagged?._tag === "Moderate")
          yield* runner.fork(
            Effect.flatMap(timing.delay("moderation"), (delay) =>
              Effect.sleep(Duration.millis(delay)).pipe(
                Effect.andThen(
                  runner.moderate({
                    action: flagged.action ?? "terminate",
                    verdict: flagged.verdict !== false,
                  }),
                ),
              ),
            ),
          );
      }),
  } satisfies Model;
});

/** H3 as the runner simulates it. */
export const simulation: Simulation<Message> = {
  describe: (message) =>
    message.type === "queue_update" || message.type === "state_update"
      ? undefined
      : {
          name: message.type,
          ...("clip" in message.data ? { clipId: message.data.clip.clip_id } : {}),
        },
  start,
};
