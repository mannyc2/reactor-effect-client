import { expect, test } from "vitest";
import { Clock, Effect, Fiber, Option, Stream } from "effect";
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

// PR #34, 886091b: an accepted drain withdrew filler and stopped building it
// while an accepted line was still building, freezing the host for 4.22 s.
// Builds run one at a time, and the runway covers each case's builds, so any
// gap before the last line is the drain's doing.
const acceptedDrains = [
  { name: "a building line", builds: { "line-A": 8_000 }, aired: ["line-A"] },
  {
    // line-A is Ready at 7 s and airs from 10.6 s to 15.8 s; line-B is not
    // Ready until 19 s, so filler held past line-A's Ready must cover the gap.
    name: "a Ready line ahead of one still building",
    builds: { "line-A": 4_000, "line-B": 12_000 },
    aired: ["line-A", "line-B"],
  },
  { name: "a line whose build fails", builds: { "line-A": -2_000 }, aired: [] },
] as const;
for (const { name, builds, aired: expected } of acceptedDrains)
  test(`accepted drain keeps filler playing past ${name}`, () =>
    runClock(
      Effect.gen(function* () {
        const presented: { prompt: string; at: number; durationSeconds: number }[] = [];
        const buildMs = (prompt: string): number | undefined =>
          (builds as Readonly<Record<string, number>>)[prompt];
        const handle = yield* Renewal.make({
          open: Effect.gen(function* () {
            const source = yield* Simulation.source({
              build: (record) => {
                const ms = buildMs(record.request.prompt) ?? 300;
                // A negative time fails the build after that long: the
                // simulation fails a build that returns no duration.
                return Effect.sleep(Math.abs(ms)).pipe(
                  Effect.as(ms < 0 ? 0 : record.durationSeconds),
                );
              },
              present: (record, at) =>
                Effect.sync(() => {
                  presented.push({
                    prompt: record.request.prompt,
                    at,
                    durationSeconds: record.durationSeconds,
                  });
                }).pipe(Effect.andThen(Effect.sleep(record.durationSeconds * 1000))),
            });
            return { source, lifetime: "Infinity" };
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
        yield* advance(3_000);
        const lines = [];
        for (const prompt of Object.keys(builds))
          lines.push(
            yield* scheduler.submit({
              key: ItemKey.make(prompt),
              lane: "line",
              request: clip(prompt),
            }),
          );
        const drainedAt = yield* Clock.currentTimeMillis;
        const draining = yield* scheduler.drain({ finish: "accepted" }).pipe(Effect.forkScoped);
        yield* advance(30_000);
        expect(draining.pollUnsafe()?._tag).toBe("Success");
        for (const line of lines)
          expect((yield* line.outcome)._tag).toBe(expected.length === 0 ? "Failed" : "Ended");
        const prompts = presented.map((entry) => entry.prompt);
        expect(prompts.filter((prompt) => prompt.startsWith("line-"))).toEqual(expected);
        if (expected.length === 2)
          expect(prompts.slice(prompts.indexOf("line-A") + 1, prompts.indexOf("line-B"))).toEqual([
            "filler-2",
          ]);
        const last = expected.length === 0 ? -1 : prompts.lastIndexOf(expected.at(-1)!);
        if (last >= 0) {
          const aired = presented.slice(0, last + 1);
          const gaps = aired
            .slice(1)
            .map(
              (entry, index) => entry.at - aired[index]!.at - aired[index]!.durationSeconds * 1000,
            );
          expect(Math.max(...gaps)).toBeLessThanOrEqual(150);
          // Filler stops once every line is Ready: nothing airs after the last.
          expect(prompts.slice(last + 1)).toEqual([]);
        } else {
          // Once the only line has failed, the drain owes no more air: nothing
          // new starts after the failure, whatever was playing then finishes.
          const failedAt = drainedAt + 2_000;
          expect(presented.filter((entry) => entry.at > failedAt + 150)).toEqual([]);
        }
        // The only starvation is the drained host's, after its last line; a
        // drain left with nothing to finish pauses autoplay before running dry.
        expect((yield* scheduler.state).starved).toBe(expected.length === 0 ? 0 : 1);
      }),
    ));

// A Ready line held for a future At anchor still needs runway up to that
// anchor. Releasing filler at its Ready exposed the anchor, and deferring it
// rebuilt the line and its filler until the anchor came.
test("accepted drain keeps filler under a Ready line held for an At anchor", () =>
  runClock(
    Effect.gen(function* () {
      const presented: { prompt: string; at: number; durationSeconds: number }[] = [];
      const builds = new Map<string, number>();
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const source = yield* Simulation.source({
            build: (record) =>
              Effect.sync(() => {
                builds.set(record.request.prompt, (builds.get(record.request.prompt) ?? 0) + 1);
              }).pipe(
                Effect.andThen(Effect.sleep("300 millis")),
                Effect.as(record.durationSeconds),
              ),
            present: (record, at) =>
              Effect.sync(() => {
                presented.push({
                  prompt: record.request.prompt,
                  at,
                  durationSeconds: record.durationSeconds,
                });
              }).pipe(Effect.andThen(Effect.sleep(record.durationSeconds * 1000))),
          });
          return { source, lifetime: "Infinity" };
        }),
      });
      yield* Stream.runDrain(handle.media.video).pipe(Effect.forkScoped);
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index }) => clip(`filler-${index}`),
        },
      }).pipe(Effect.provideService(Engine, handle.engine));
      yield* advance(1_000);
      const anchor = (yield* Clock.currentTimeMillis) + 8_000;
      const line = yield* scheduler.submit({
        key: ItemKey.make("anchored"),
        lane: "line",
        request: clip("anchored"),
        start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
      });
      const draining = yield* scheduler.drain({ finish: "accepted" }).pipe(Effect.forkScoped);
      yield* advance(35_000);
      expect(draining.pollUnsafe()?._tag).toBe("Success");
      expect((yield* line.outcome)._tag).toBe("Ended");
      // The same run without a drain builds the line twice: an At line is
      // admitted early and deferred once. The drain must not add to that;
      // releasing filler at Ready built the line 8 times and 9 filler clips.
      expect(builds.get("anchored")).toBeLessThanOrEqual(2);
      expect(
        [...builds].filter(([prompt]) => prompt.startsWith("filler-")).length,
      ).toBeLessThanOrEqual(4);
      const prompts = presented.map((entry) => entry.prompt);
      const lineAt = prompts.indexOf("anchored");
      expect(lineAt).toBeGreaterThan(0);
      expect(prompts.slice(lineAt + 1)).toEqual([]);
      const started = presented[lineAt]!.at;
      expect(started).toBeGreaterThanOrEqual(anchor);
      const aired = presented.slice(0, lineAt + 1);
      const gaps = aired
        .slice(1)
        .map((entry, index) => entry.at - aired[index]!.at - aired[index]!.durationSeconds * 1000);
      expect(Math.max(...gaps)).toBeLessThanOrEqual(150);
    }),
  ));

// PR #34, c1171c9 and 886091b: deterministic loss and build ordering cannot be
// forced by hosted qualification. Exercise the actual renewal, scheduler and
// simulation. "final-clip-frame-loss" drops a frame from every retiring clip, so
// the last one is short too: a frame the provider never sent cannot arrive
// later, and waiting for it idled the old session until expiry.
for (const fault of [
  "earlier-frame-loss",
  "old-line-building",
  "final-clip-frame-loss",
  "final-clip-frame-loss-no-line",
] as const)
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
        let switchedAt: number | undefined;
        let opened = 0;
        const finalLoss =
          fault === "final-clip-frame-loss" || fault === "final-clip-frame-loss-no-line";
        const handle = yield* Renewal.make({
          lead: "40 seconds",
          onRenewal: (event) =>
            Effect.gen(function* () {
              renewals.push(event);
              if (event._tag === "Switched") switchedAt = yield* Clock.currentTimeMillis;
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
                    const lost =
                      session === 0 &&
                      ((fault === "earlier-frame-loss" && sequence === 0) ||
                        (finalLoss && frame === 0));
                    if (!lost)
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
        const line =
          fault === "final-clip-frame-loss-no-line"
            ? undefined
            : yield* scheduler.submit({
                key: ItemKey.make("line-B"),
                lane: "line",
                request: clip("line-B"),
              });
        yield* advance(19_000);
        const switched = renewals.find((event) => event._tag === "Switched");
        expect(switched).toBeDefined();
        const starts = presented.filter((entry) => entry.prompt.startsWith("line-"));
        expect(starts.map((entry) => entry.prompt)).toEqual(
          fault === "old-line-building"
            ? ["line-A", "line-B"]
            : fault === "final-clip-frame-loss-no-line"
              ? []
              : ["line-B"],
        );
        if (line !== undefined) expect(starts.at(-1)?.session).toBe(1);
        const gaps = presented
          .slice(1)
          .map(
            (entry, index) =>
              entry.at - presented[index]!.at - presented[index]!.durationSeconds * 1000,
          );
        if ((fault === "earlier-frame-loss" || finalLoss) && switched?._tag === "Switched") {
          // Without a line, the retiring source first plays out the filler it
          // built before Prepared; either way it switches long before expiry at 60.
          expect(switched.ageSeconds).toBeLessThan(
            fault === "final-clip-frame-loss-no-line" ? 40 : 30,
          );
          expect(switched.tail.video.status).toBe("incomplete");
          expect(switched.tail.video.receivedFrames).toBeLessThan(
            switched.tail.video.expectedFrames,
          );
        }
        if (finalLoss) {
          // The switch waits the default 250 ms grace past the short clip's
          // Ended, plus at most one 100 ms renewal tick and one test step.
          const retiring = presented.filter((entry) => entry.session === 0).at(-1)!;
          const endedAt = retiring.at + retiring.durationSeconds * 1000;
          expect(switchedAt).toBeDefined();
          expect(switchedAt! - endedAt).toBeGreaterThanOrEqual(250);
          expect(switchedAt! - endedAt).toBeLessThanOrEqual(400);
          expect(Math.max(...gaps)).toBeLessThanOrEqual(450);
        } else expect(Math.max(...gaps)).toBeLessThanOrEqual(150);
        if (line !== undefined) expect((yield* line.started)._tag).toBe("Started");
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
