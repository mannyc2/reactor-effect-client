/** Resuming a recorded H3 session after its owner died: adopt, observe, and end it. */
import { expect, test } from "vitest";
import { Clock, Duration, Effect, Layer, Redacted, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import { AcquisitionFailure } from "../../src/errors.js";
import * as H3 from "../../src/h3/index.js";
import { bindSession } from "../../src/orchestration/h3-source.js";
import { openH3With, resumeH3With } from "../../src/orchestration/open-h3.js";
import type { Allocation } from "../../src/orchestration/open-h3.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { Client } from "../../src/session/index.js";
import type { AttachOptions, CloseReport, Factory, Session } from "../../src/session/index.js";
import type { MediaGeneration } from "../../src/session/media.js";
import { fixture, fixtureClip } from "../h3/ProviderSession.js";
import type { Fixture } from "../h3/ProviderSession.js";
import { signals } from "./Signals.js";
import { cleanPressure, run, runClock, until } from "./SourceFixture.js";

const media: MediaGeneration = {
  generation: 1n,
  tracks: [],
  retired: Effect.never,
  video: () => Stream.never,
  audio: () => Stream.never,
  snapshot: Effect.succeed(cleanPressure),
};
const bind = bindSession(() => Effect.succeed(media));
const resumeH3 = resumeH3With(bind);
const openH3 = openH3With(bind);
const jwt = Redacted.make("SECRET-RESUMED-JWT");
const provider = { replyTimeout: 100, setupTimeout: 1000 };

/** What an adopted session's close reports: it is owned, so it terminated and confirmed. */
const terminated = (report: CloseReport): CloseReport => ({
  ...report,
  ownership: "owned",
  remote: {
    attempted: true,
    responseReceived: true,
    confirmed: true,
    evidence: "absent",
    deleteStatus: 204,
    state: null,
  },
});
/** The fixture session as `attachConnected({ adopt: true })` returns it. */
const adopted = (fake: Fixture): Session => ({
  ...fake.session,
  ownership: "owned",
  close: fake.session.close.pipe(Effect.map(terminated)),
});
const allocation = (fields: Partial<Allocation> = {}): Allocation => ({
  sessionId: "h3-offline-session",
  ownership: "owned",
  model: H3.modelName,
  expiresAt: 4_102_444_800,
  endsAt: 120,
  ...fields,
});

/** A Client whose attach returns `resumed` and whose create returns `created`. */
const clientOf = (
  attaches: AttachOptions[],
  resumed: Session,
  created: Session = resumed,
  creates: { count: number } = { count: 0 },
) =>
  Layer.succeed(Client, {
    create: () =>
      Effect.sync(() => {
        creates.count++;
        return created;
      }),
    attach: () => Effect.die("attach is not used"),
    createConnected: () => Effect.die("createConnected is not used"),
    attachConnected: (options) =>
      Effect.sync(() => {
        attaches.push(options);
        return resumed;
      }),
  } satisfies Factory);

test("a resume adopts the recorded session with its token and takes its lifetime from endsAt", () =>
  runClock(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const attaches: AttachOptions[] = [];
      yield* TestClock.setTime(100_000);
      const opened = yield* resumeH3({
        allocation: allocation({ endsAt: 130 }),
        jwt,
        source: { provider },
      }).pipe(Effect.provide(clientOf(attaches, adopted(fake))));
      expect(attaches).toEqual([{ sessionId: "h3-offline-session", jwt, adopt: true }]);
      expect(opened.source.provider.sessionId).toBe("h3-offline-session");
      expect(Duration.toMillis(Duration.fromInputUnsafe(opened.lifetime))).toBe(30_000);
      const report = yield* opened.source.close;
      expect(report.lease.ownership).toBe("owned");
      expect(report.lease.remote.confirmed).toBe(true);
    }),
  ));

test("resuming a busy session only reads it: no canvas, reset or playback command is sent", () =>
  run(
    Effect.gen(function* () {
      const playing = fixtureClip({ ready: true });
      const queued = fixtureClip({ metadata: "the dead owner's queued clip" });
      // The dead owner's clip still plays: every state read says so.
      const fake = yield* fixture({
        initialGeneration: [queued],
        command: {
          get_state: ({ fake }) =>
            Effect.succeed({
              type: "state_update",
              data: fake.state({ playing: true, playing_clip_id: playing.clip_id, autoplay: true }),
            }),
        },
      });
      const opened = yield* resumeH3({
        allocation: allocation({ endsAt: (yield* Clock.currentTimeMillis) / 1000 + 60 }),
        jwt,
        source: { provider, holdLastFrame: true },
      }).pipe(Effect.provide(clientOf([], adopted(fake))));
      // Reads, and the one setting H3 accepts while it plays.
      expect(fake.calls.map((call) => call.command)).toEqual([
        "get_state",
        "get_queue",
        "set_flush_on_clip_end",
        "get_state",
        "get_queue",
      ]);
      const state = yield* opened.source.state;
      expect(state.playing._tag).toBe("Some");
      expect(state.queued.map((clip) => clip.provider.metadata)).toEqual([
        "the dead owner's queued clip",
      ]);
    }),
  ));

test("a record without endsAt, for another model, past its end, or with a canvas is refused before any attach", () =>
  runClock(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const attaches: AttachOptions[] = [];
      yield* TestClock.setTime(100_000);
      const { endsAt: _, ...legacy } = allocation();
      for (const input of [
        { allocation: legacy, jwt },
        { allocation: allocation({ model: "reactor/fast-h3" }), jwt },
        { allocation: allocation({ endsAt: 100 }), jwt },
        { allocation: allocation({ endsAt: 200 }), jwt, source: { canvas: "1:1" } as never },
      ]) {
        const result = yield* Effect.result(
          resumeH3(input).pipe(Effect.provide(clientOf(attaches, adopted(fake)))),
        );
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) {
          expect(AcquisitionFailure.is(result.failure)).toBe(true);
          expect(result.failure.reason._tag).toBe("InvalidInput");
          expect(result.failure.cleanup.allocation).toBe("none");
        }
      }
      expect(attaches).toEqual([]);
      expect(fake.calls).toEqual([]);
    }),
  ));

test("a resume that fails after attaching terminates the adopted session and reports it", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture({ schema: { openapi: "2.0" } });
      const result = yield* Effect.result(
        resumeH3({
          allocation: allocation({ endsAt: (yield* Clock.currentTimeMillis) / 1000 + 60 }),
          jwt,
        }).pipe(Effect.provide(clientOf([], adopted(fake)))),
      );
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(AcquisitionFailure.is(result.failure)).toBe(true);
        expect(result.failure.reason._tag).toBe("UnsupportedCapability");
        expect(result.failure.cleanup.remote.confirmed).toBe(true);
      }
      expect(fake.lifecycleCalls.close).toBe(1);
    }),
  ));

test("renewal retires a resumed source by terminating its adopted session", () =>
  runClock(
    Effect.gen(function* () {
      const resumedFake = yield* fixture();
      const freshFake = yield* fixture();
      const fresh: Session = { ...freshFake.session, id: "fresh-session", ownership: "owned" };
      const creates = { count: 0 };
      const recorded = signals<Renewal.Renewal>();
      let opens = 0;
      const client = clientOf([], adopted(resumedFake), fresh, creates);
      const handle = yield* Renewal.make({
        lead: "500 millis",
        open: Effect.suspend(() =>
          opens++ === 0
            ? resumeH3({ allocation: allocation({ endsAt: 2 }), jwt, source: { provider } })
            : openH3({
                mint: Effect.succeed({
                  jwt,
                  expiresAt: 4_102_444_800,
                  granted: { maxSessions: 1 as const, maxSessionSeconds: 120 },
                }),
                source: { provider },
              }),
        ).pipe(Effect.provide(client)),
        onRenewal: (event) =>
          Effect.sync(() => {
            recorded.record(event);
          }),
      });
      expect(yield* handle.sessionId).toMatchObject({ value: "h3-offline-session" });
      yield* until(
        () => recorded.values.some((event) => event._tag === "Replaced"),
        TestClock.adjust(100),
        `the renewal never replaced the resumed session: ${recorded.values.map((event) => event._tag).join(", ")}`,
      );
      expect(creates.count).toBe(1);
      expect(resumedFake.lifecycleCalls.close).toBe(1);
      expect(yield* handle.sessionId).toMatchObject({ value: "fresh-session" });
      const cleanup = yield* handle.close;
      const retired = cleanup.sessions.find(
        (entry) => entry.lease.sessionId === "h3-offline-session",
      );
      expect(retired?.lease.ownership).toBe("owned");
      expect(retired?.lease.remote.confirmed).toBe(true);
    }),
  ));
