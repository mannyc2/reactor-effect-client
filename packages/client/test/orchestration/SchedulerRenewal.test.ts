import { expect, test } from "vitest";
import { Effect, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { failure, runClock, videoFrame } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, millis - elapsed));
  });

test("renewal drains old filler and preserves submission order on the replacement", () =>
  runClock(
    Effect.gen(function* () {
      const presented: {
        readonly session: number;
        readonly prompt: string;
        readonly at: number;
      }[] = [];
      let opened = 0;
      const handle = yield* Renewal.make({
        lead: "7 seconds",
        open: Effect.gen(function* () {
          const session = opened++;
          let sequence = 0;
          const source = yield* Simulation.source({
            buildRatio: 0,
            present: (record, at, sink) =>
              Effect.gen(function* () {
                presented.push({ session, prompt: record.request.prompt, at });
                for (let n = 0; n < Math.round(record.durationSeconds * 24); n++) {
                  yield* sink.video({ ...videoFrame(n), sequence: BigInt(sequence++) });
                  yield* Effect.sleep(1000 / 24);
                }
              }),
          });
          return { source, lifetime: session === 0 ? "10 seconds" : "Infinity" };
        }),
      });
      yield* Stream.runDrain(handle.media.video).pipe(Effect.forkScoped);
      const moves: string[] = [];
      const engine: EngineShape = {
        ...handle.engine,
        move: (id, position, queue) =>
          Effect.sync(() => {
            moves.push(`${id}:${position}:${queue}`);
          }).pipe(Effect.andThen(handle.engine.move(id, position, queue))),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "15 seconds" },
          clip: ({ index }) => clip(`filler-${index}`),
        },
      }).pipe(Effect.provideService(Engine, engine));

      yield* advance(3_500);
      const overlap = yield* engine.state;
      expect(opened).toBe(2);
      expect(Option.isSome(overlap.retiringSessionId)).toBe(true);
      const replacement = Option.getOrUndefined(overlap.preferredSessionId);
      expect(replacement).toBeDefined();
      const keys = ["first", "second", "third"].map((value) => ItemKey.make(value));
      for (const [index, key] of keys.entries())
        yield* scheduler.submit({ key, lane: "line", request: clip(`line-${index}`) });

      yield* advance(20_000);
      const lines = presented.filter((entry) => entry.prompt.startsWith("line-"));
      expect(lines.map((entry) => entry.prompt)).toEqual(["line-0", "line-1", "line-2"]);
      expect(lines.every((entry) => entry.session === 1)).toBe(true);
      expect(lines[0]!.at).toBeLessThanOrEqual(6_000);
      expect(new Set(moves).size).toBe(moves.length);
      expect((yield* scheduler.state).starved).toBeLessThanOrEqual(1);
    }),
  ));

test("uncertain filler on the retiring source does not hold replacement runway", () =>
  runClock(
    Effect.gen(function* () {
      const sourceIds: string[] = [];
      const handle = yield* Renewal.make({
        lead: "7 seconds",
        open: Effect.gen(function* () {
          const source = yield* Simulation.source({ fixedBuildTime: 0, buildRatio: 0 });
          sourceIds.push(source.id);
          return { source, lifetime: sourceIds.length === 1 ? "10 seconds" : "Infinity" };
        }),
      });
      yield* Stream.runDrain(handle.media.video).pipe(Effect.forkScoped);
      const attempts: { readonly source: string; readonly prompt: string }[] = [];
      const engine: EngineShape = {
        ...handle.engine,
        enqueue: (request) =>
          Effect.gen(function* () {
            const preferred = Option.getOrUndefined(
              (yield* handle.engine.state).preferredSessionId,
            );
            attempts.push({ source: preferred ?? "absent", prompt: request.prompt });
            if (preferred === sourceIds[0]) return yield* failure("unknown");
            return yield* handle.engine.enqueue(request);
          }),
      };
      yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "5 seconds" },
          clip: ({ index }) => clip(`filler-${index}`),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(3_500);
      expect(sourceIds).toHaveLength(2);
      expect(attempts).toContainEqual({ source: sourceIds[0], prompt: "filler-0" });
      expect(attempts).toContainEqual({ source: sourceIds[1], prompt: "filler-1" });
    }),
  ));
