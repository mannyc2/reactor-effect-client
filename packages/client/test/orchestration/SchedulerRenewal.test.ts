import { expect, test } from "vitest";
import { Effect, Fiber, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape, Renewal as RenewalEvent } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { failure, gate, refusal, runClock, sourceFixture, videoFrame } from "./SourceFixture.js";
import type { SourceFixture } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, millis - elapsed));
  });

test("accepted drain finishes queued lines and prevents an allocation at the renewal lead", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.make({
        lead: "40 seconds",
        open: Effect.gen(function* () {
          opened++;
          return {
            source: yield* Simulation.source({ fixedBuildTime: "300 millis", buildRatio: 0 }),
            lifetime: "60 seconds",
          };
        }),
      });
      let dispatches = 0;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: (request, source) =>
          Effect.gen(function* () {
            if (dispatches++ === 0) yield* Effect.sleep("5 seconds");
            return yield* handle.engine.enqueueOnSource!(request, source);
          }),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: { runway: { floor: 0, target: "5 seconds" }, clip: () => clip("unused") },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(18_000);
      const first = yield* scheduler.submit({
        key: ItemKey.make("drain-A"),
        lane: "line",
        request: clip("A"),
      });
      const second = yield* scheduler.submit({
        key: ItemKey.make("drain-B"),
        lane: "line",
        request: clip("B"),
      });
      const draining = yield* scheduler.drain({ finish: "accepted" }).pipe(Effect.forkScoped);
      yield* advance(20_000);
      expect(opened).toBe(1);
      expect(draining.pollUnsafe()?._tag).toBe("Success");
      expect((yield* first.outcome)._tag).toBe("Ended");
      expect((yield* second.outcome)._tag).toBe("Ended");
      expect((yield* scheduler.state).accepting).toBe(false);
    }),
  ));

// PR #34, c1171c9: deterministic loss and build ordering cannot be forced by
// hosted qualification. Exercise the actual renewal, scheduler and simulation.
for (const fault of ["earlier-frame-loss", "old-line-building"] as const)
  test(`40-second renewal overlap preserves output with ${fault}`, () =>
    runClock(
      Effect.gen(function* () {
        const presented: {
          prompt: string;
          at: number;
          session: number;
          durationSeconds: number;
        }[] = [];
        const renewals: RenewalEvent[] = [];
        let opened = 0;
        const handle = yield* Renewal.make({
          lead: "40 seconds",
          onRenewal: (event) =>
            Effect.sync(() => {
              renewals.push(event);
            }),
          open: Effect.gen(function* () {
            const session = opened++;
            let sequence = 0;
            const source = yield* Simulation.source({
              build: (record) =>
                Effect.sleep(record.request.prompt === "line-A" ? "8 seconds" : "300 millis").pipe(
                  Effect.as(record.durationSeconds),
                ),
              present: (record, at, sink) =>
                Effect.gen(function* () {
                  presented.push({
                    prompt: record.request.prompt,
                    at,
                    session,
                    durationSeconds: record.durationSeconds,
                  });
                  for (let frame = 0; frame < record.durationSeconds * 24; frame++) {
                    if (!(fault === "earlier-frame-loss" && session === 0 && sequence === 0))
                      yield* sink.video({ ...videoFrame(frame), sequence: BigInt(sequence) });
                    sequence++;
                    yield* Effect.sleep(1000 / 24);
                  }
                }),
            });
            return { source, lifetime: session === 0 ? "60 seconds" : "Infinity" };
          }),
        });
        yield* Stream.runDrain(handle.media.video).pipe(Effect.forkScoped);
        const scheduler = yield* makeScheduler({
          lanes: [{ name: "line" }],
          filler: {
            runway: { floor: "10 seconds", target: "15 seconds" },
            clip: ({ index }) => clip(`filler-${index}`),
          },
        }).pipe(Effect.provideService(Engine, handle.engine));
        yield* advance(19_000);
        if (fault === "old-line-building")
          yield* scheduler.submit({
            key: ItemKey.make("line-A"),
            lane: "line",
            request: clip("line-A"),
          });
        yield* advance(1_500);
        expect(opened).toBe(2);
        const line = yield* scheduler.submit({
          key: ItemKey.make("line-B"),
          lane: "line",
          request: clip("line-B"),
        });
        yield* advance(19_000);
        const switched = renewals.find((event) => event._tag === "Switched");
        expect(switched).toBeDefined();
        const starts = presented.filter((entry) => entry.prompt.startsWith("line-"));
        expect(starts.map((entry) => entry.prompt)).toEqual(
          fault === "old-line-building" ? ["line-A", "line-B"] : ["line-B"],
        );
        expect(starts.at(-1)?.session).toBe(1);
        const gaps = presented
          .slice(1)
          .map(
            (entry, index) =>
              entry.at - presented[index]!.at - presented[index]!.durationSeconds * 1000,
          );
        expect(Math.max(...gaps)).toBeLessThanOrEqual(150);
        if (fault === "earlier-frame-loss" && switched?._tag === "Switched") {
          expect(switched.ageSeconds).toBeLessThan(30);
          expect(switched.tail.video.status).toBe("incomplete");
        }
        expect((yield* line.started)._tag).toBe("Started");
        expect((yield* scheduler.state).starved).toBe(0);
      }),
    ));

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
      const enqueue = (request: ClipRequest) =>
        Effect.gen(function* () {
          const preferred = Option.getOrUndefined((yield* handle.engine.state).preferredSessionId);
          attempts.push({ source: preferred ?? "absent", prompt: request.prompt });
          if (preferred === sourceIds[0]) return yield* failure("unknown");
          return yield* handle.engine.enqueue(request);
        });
      const engine: EngineShape = {
        ...handle.engine,
        enqueue,
        enqueueOnSource: enqueue,
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

test("a source-fenced enqueue refuses a route switch before dispatch", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate;
      const release = yield* gate;
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.make({
        lead: "7 seconds",
        open: Effect.gen(function* () {
          const index = sources.length;
          const source = yield* sourceFixture(`fenced-${index + 1}`, {
            ...(index === 0
              ? { prework: () => entered.release.pipe(Effect.andThen(release.wait)) }
              : {}),
          });
          sources.push(source);
          return { source: source.source, lifetime: index === 0 ? "10 seconds" : "Infinity" };
        }),
      });
      yield* Stream.runDrain(handle.media.video).pipe(Effect.forkScoped);
      const enqueueOnSource = handle.engine.enqueueOnSource;
      expect(enqueueOnSource).toBeDefined();
      const pending = yield* enqueueOnSource!(clip("fenced line"), "fenced-1").pipe(
        Effect.forkScoped,
      );
      yield* entered.wait;
      yield* advance(3_500);
      expect(Option.getOrUndefined((yield* handle.engine.state).preferredSessionId)).toBe(
        "fenced-2",
      );
      yield* release.release;
      const denied = yield* Effect.flip(Fiber.join(pending));
      expect(refusal(denied)).toBe("RouteChanged");
      expect(denied.context.outcome).toBe("not-submitted");
      expect(sources.flatMap((source) => source.sends)).toEqual([]);
    }),
  ));
