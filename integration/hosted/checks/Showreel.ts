/**
 * `showreel`: footage of hosted H3 for the README and the documentation,
 * recorded on one session capped at 70 s. A playout over `H3Source` airs five
 * scenes of 8 s back to back in one lane, holding the last frame at each seam
 * as a show does. From the first scene's start to the last one's end, the
 * playout's decoded picture and sound go to an MP4 through ffmpeg as they air,
 * seams included. Once the session has closed, ffmpeg makes a poster from the
 * middle scene and a README-sized loop across the seam into it. The files go
 * beside the run's evidence, in a directory named as its evidence file is;
 * the evidence keeps only their names and sizes.
 *
 * The planned timeline, from the allocation A, at the timing paid runs
 * measured (connected about 3 s after allocation, a 5 s clip built in about
 * 2.2 s, so an 8 s one in about 3.5 s, one build at a time, and seams of 46
 * to 169 ms):
 *
 *   A+3    the playout opens and the five scenes go in
 *   A+7    the first scene airs and the recording starts; each later scene is
 *          built while the one before it airs
 *   A+47   the last scene ends and the recording stops; the drain completes
 *          and the session closes about A+48, 12 s before the work deadline
 *          and 22 s before the cap
 *   then   ffmpeg finishes the reel and makes the poster and the loop
 */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { Air, Pieces } from "../Checks.js";
import type * as Evidence from "../Evidence.js";
import * as Media from "../Media.js";
import { Run } from "../Run.js";
import { Target } from "../Target.js";

/** Each scene's length: built in about 3.5 s, one is Ready long before the one before it ends. */
const sceneSeconds = 8;
/** What every scene shares, so that five clips built on their own read as one reel. */
const look =
  "Cinematic 35 mm film, anamorphic lens, a slow and steady camera move, warm golden-hour sunlight, soft lens flares, rich contrast.";
/** The scenes, in the order they air. */
const scenes = [
  {
    key: "salt-flat",
    scene:
      "Dawn on a vast salt flat under a thin sheet of water: a red vintage convertible glides across it, the sky and the car mirrored in the surface.",
  },
  {
    key: "chrome",
    scene:
      "Close on the red vintage convertible's chrome headlight and grille as it drives, the low sun flaring across the metal and dust glowing in the air.",
  },
  {
    key: "canyon",
    scene:
      "The red vintage convertible winds along a road between towering red sandstone canyon walls, heat shimmering above the asphalt.",
  },
  {
    key: "coast",
    scene:
      "The red vintage convertible follows a cliffside road above the ocean, where waves break on the rocks below and burst into golden spray.",
  },
  {
    key: "horizon",
    scene:
      "An aerial view rising over the coastline as the sun meets the horizon, the red vintage convertible small on the clifftop road below.",
  },
] as const;
const keys: ReadonlyArray<string> = scenes.map(({ key }) => key);
const [opening] = scenes;
const closing = scenes[scenes.length - 1] ?? opening;
/** The scene the poster comes from, and whose seam the loop crosses. */
const centre = scenes[Math.floor(scenes.length / 2)] ?? opening;
/**
 * The longest a scene's end may go unfollowed on air, as `show` judges it:
 * hosted clips followed each other within 40 ms.
 */
const gapMs = 1_500;
/** How long the loop lasts, centred on the seam into the centre scene. */
const loopSeconds = 8;
const files = { reel: "reel.mp4", poster: "poster.png", loop: "loop.gif" } as const;

/** Adds `fields` to the showreel's evidence. */
const recordReel = (fields: Partial<Evidence.ShowreelRecord>) =>
  Effect.flatMap(Run, (run) =>
    run.update((evidence) => ({
      ...evidence,
      showreel: {
        scenes: [],
        gaps: [],
        readerOverflows: [],
        files: [],
        ...evidence.showreel,
        ...fields,
      },
    })),
  );

/** When a scene reached the moment waited for, from the run's start, or why it never will. */
type Reached = { readonly atMs: number } | { readonly missed: string };

/**
 * Waits for scene `key` to start or end (`moment`), or for it to settle
 * without doing so: it failed or was dropped, the playout stopped, or the
 * work deadline passed once the session was allocated. Before the allocation
 * the library's own deadlines bound opening the session, and an open that
 * fails stops the playout.
 */
const reach = (pieces: Pieces, air: Air, key: string, moment: "startedMs" | "endedMs") => {
  const late: Reached = { missed: `${key} did not by the work deadline` };
  return Effect.raceAll([
    air
      .when((all): Reached | undefined => {
        const item = all.get(key);
        const atMs = item?.[moment];
        if (atMs !== undefined) return { atMs };
        if (item?.failed !== undefined) return { missed: `${key} failed: ${item.failed.reason}` };
        if (item?.dropped !== undefined) return { missed: `${key} was dropped: ${item.dropped}` };
        return undefined;
      })
      .pipe(Effect.orElseSucceed(() => late)),
    air.allocated.pipe(Effect.flatMap(pieces.until), Effect.flatMap(Effect.sleep), Effect.as(late)),
    Effect.map(air.playout.failure, (failure): Reached => ({
      missed: `the playout stopped: ${failure._tag}`,
    })),
  ]);
};

/** `value`, kept from `low` to `high`. */
const clamp = (value: number, low: number, high: number) => Math.min(Math.max(value, low), high);

export const showreel = (pieces: Pieces) =>
  Effect.scoped(
    Effect.gen(function* () {
      const run = yield* Run;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      // The recording belongs to this scope, which outlives the session's, so that its file is
      // finished, and its poster and loop made, only once the session has closed.
      const scope = yield* Scope.Scope;
      const directory = path.join(path.dirname(run.file), path.basename(run.file, ".json"));
      const at = (name: string) => path.join(directory, name);
      const unable = yield* (yield* Target).cannotRecord;
      yield* recordReel({
        scenes: scenes.map(({ key }) => ({ key, seconds: sceneSeconds })),
        ...(unable === undefined ? {} : { notRecorded: unable }),
      });
      // Nothing is opened, and so nothing spent, without a place for the footage.
      if (
        unable === undefined &&
        Option.isNone(yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.option))
      )
        return yield* pieces.judge("the reel was recorded", [
          false,
          "the directory beside the evidence cannot be made",
        ]);
      const recorder = yield* Ref.make<Media.Recorder | undefined>(undefined);
      /** The centre scene's start on air, from the run's start. */
      const centreStart = yield* Ref.make<number | undefined>(undefined);
      const ended = yield* Deferred.make<void>();
      const aired = yield* Effect.exit(
        pieces.onAir("showreel", { lanes: [{ name: "reel" }], sessions: 1 }, (air) =>
          Effect.gen(function* () {
            const overflows: Array<{ readonly track: string; readonly atMs: number }> = [];
            yield* air.playout.events.pipe(
              Stream.runForEach((event) =>
                event._tag === "ReaderOverflow"
                  ? Effect.sync(() => {
                      overflows.push({
                        track: event.track,
                        atMs: pieces.round(event.at - run.origin),
                      });
                    })
                  : Effect.void,
              ),
              Effect.forkScoped,
            );
            for (const { key, scene } of scenes)
              yield* air.submit(key, "reel", { prompt: `${scene} ${look}`, seconds: sceneSeconds });
            yield* run.mark("scenes submitted");
            const started = yield* reach(pieces, air, opening.key, "startedMs");
            if ("missed" in started)
              return yield* pieces.judge("every scene accepted, built, started and ended", [
                false,
                `the first scene never started: ${started.missed}`,
              ]);
            const fromMs = started.atMs;
            if (unable === undefined) {
              const recording = yield* Media.record({
                file: at(files.reel),
                video: air.playout.video,
                audio: air.playout.audio,
                fromMs: run.origin + fromMs,
                until: Deferred.await(ended),
              }).pipe(Scope.provide(scope));
              yield* Ref.set(recorder, recording);
              yield* run.mark("recording");
            }
            const last = yield* reach(pieces, air, closing.key, "endedMs");
            yield* Deferred.succeed(ended, undefined);
            const toMs = "atMs" in last ? last.atMs : undefined;
            yield* run.mark("reel ended");
            yield* air.playout
              .drain({ finish: "accepted" })
              .pipe(Effect.timeout(yield* pieces.until(air.deadline())));
            const all = yield* SubscriptionRef.get(air.items);
            yield* Ref.set(centreStart, all.get(centre.key)?.startedMs);
            const seams: Array<Evidence.Seam> = [];
            for (const [index, ending] of keys.entries()) {
              const next = keys[index + 1];
              if (next !== undefined) seams.push(yield* air.seam(ending, next, false));
            }
            yield* pieces.recordPlayout({ seams });
            const gaps = seams.flatMap((seam) =>
              seam.endedMs === undefined || seam.startedMs === undefined
                ? []
                : [
                    {
                      ending: seam.ending,
                      next: seam.next,
                      fromMs: seam.endedMs,
                      toMs: seam.startedMs,
                    },
                  ],
            );
            yield* recordReel({
              gaps,
              readerOverflows: [...overflows],
              reel: { fromMs, ...(toMs === undefined ? {} : { toMs }) },
            });
            const order = air.starts();
            const unfinished = keys.filter((key) => {
              const scene = all.get(key);
              return scene?.readyMs === undefined || scene.termination !== "finished";
            });
            yield* pieces.judge(
              "every scene accepted, built, started and ended",
              [
                unfinished.length === 0,
                `${unfinished.join(", ")} did not build, air and finish${"missed" in last ? `: ${last.missed}` : ""}`,
              ],
              [order.join(",") === keys.join(","), `started in the order ${order.join(", ")}`],
            );
            yield* pieces.judge("every seam measured", [
              gaps.length === keys.length - 1 &&
                seams.every((seam) => seam.pause !== undefined && seam.darkFrames !== undefined),
              "a seam has no gap, pause or dark-frame measurement",
            ]);
            const long = gaps.filter((gap) => gap.toMs - gap.fromMs > gapMs);
            yield* pieces.judge("no gap on air between scenes", [
              long.length === 0,
              long
                .map(
                  (gap) =>
                    `${gap.ending} to ${gap.next} left ${Math.round(gap.toMs - gap.fromMs)} ms`,
                )
                .join(", "),
            ]);
            yield* pieces.judge("the reel's readers keep up", [
              overflows.length === 0,
              overflows
                .map(
                  (overflow) => `the ${overflow.track} reader fell behind at ${overflow.atMs} ms`,
                )
                .join(", "),
            ]);
            yield* run.mark("showreel observed");
          }).pipe(Effect.ensuring(Deferred.succeed(ended, undefined))),
        ),
      );
      // The session has closed, and the reel's tracks ended with it: ffmpeg finishes the file.
      const recording = yield* Ref.get(recorder);
      if (recording !== undefined) {
        const written = yield* recording.finished;
        yield* recordReel({ recording: written });
        yield* run.save;
        const span = (yield* run.evidence).showreel?.reel;
        const reelSeconds = written.frames / 24;
        const startedAt = yield* Ref.get(centreStart);
        const seamSeconds =
          startedAt === undefined || span === undefined
            ? undefined
            : (startedAt - span.fromMs) / 1000;
        const posterAt = clamp(
          seamSeconds === undefined ? reelSeconds / 2 : seamSeconds + sceneSeconds / 2,
          0,
          Math.max(0, reelSeconds - 0.1),
        );
        const loopFrom = clamp(
          (seamSeconds ?? 0) - loopSeconds / 2,
          0,
          Math.max(0, reelSeconds - loopSeconds),
        );
        const loopFor = Math.min(loopSeconds, reelSeconds);
        const posterExit =
          written.exitCode === 0
            ? yield* Media.poster({
                reel: at(files.reel),
                file: at(files.poster),
                atSeconds: posterAt,
              })
            : undefined;
        const loopExit =
          written.exitCode === 0
            ? yield* Media.loop({
                reel: at(files.reel),
                file: at(files.loop),
                fromSeconds: loopFrom,
                seconds: loopFor,
              })
            : undefined;
        const made = (yield* Effect.forEach(Object.values(files), (name) =>
          fs.stat(at(name)).pipe(
            Effect.map((info) => ({ name, bytes: Number(info.size) })),
            Effect.option,
            Effect.map(Option.toArray),
          ),
        )).flat();
        yield* recordReel({
          files: made,
          ...(posterExit === undefined ? {} : { poster: { atSeconds: pieces.round(posterAt, 3) } }),
          ...(loopExit === undefined
            ? {}
            : {
                loop: { fromSeconds: pieces.round(loopFrom, 3), seconds: pieces.round(loopFor, 3) },
              }),
        });
        const bytesOf = (name: string) => made.find((file) => file.name === name)?.bytes ?? 0;
        const spanMs = span?.toMs === undefined ? undefined : span.toMs - span.fromMs;
        const soundOffered = ((yield* run.evidence).playout?.audio?.blocks ?? 0) > 0;
        yield* pieces.judge(
          "the reel was recorded",
          [written.failure === undefined, written.failure ?? ""],
          [written.exitCode === 0, `ffmpeg exited ${String(written.exitCode)}`],
          [bytesOf(files.reel) > 0, `${files.reel} is empty`],
          [
            spanMs === undefined || reelSeconds * 1000 >= spanMs - 500,
            `its ${written.frames} frames last ${pieces.round(reelSeconds, 2)} s of the scenes' ${pieces.round((spanMs ?? 0) / 1000, 2)} s on air`,
          ],
          [
            !soundOffered || (written.audio?.blocks ?? 0) > 0,
            "the session offered sound and none was recorded",
          ],
        );
        yield* pieces.judge(
          "the poster and the loop were made after the session closed",
          [posterExit === 0 && bytesOf(files.poster) > 0, `${files.poster} was not made`],
          [loopExit === 0 && bytesOf(files.loop) > 0, `${files.loop} was not made`],
        );
      } else if (unable === undefined)
        yield* pieces.judge("the reel was recorded", [false, "the first scene never started"]);
      if (Exit.isFailure(aired)) return yield* Effect.failCause(aired.cause);
    }),
  );
