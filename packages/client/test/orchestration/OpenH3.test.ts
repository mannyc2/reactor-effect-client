/** A paid H3 opener: mint, allocate, register the owner, connect, then derive the source. */
import { expect, test } from "vitest";
import { Context, Duration, Effect, Exit, Layer, Redacted, Result, Stream } from "effect";
import { AcquisitionFailure, ReactorError } from "../../src/errors.js";
import * as H3 from "../../src/h3/index.js";
import { bindSession } from "../../src/orchestration/h3-source.js";
import { openH3With } from "../../src/orchestration/open-h3.js";
import type { Allocated } from "../../src/orchestration/open-h3.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { Engine, Handle, Media } from "../../src/orchestration/types.js";
import { Client } from "../../src/session/index.js";
import type { CreateOptions, Factory, Session } from "../../src/session/index.js";
import type { MediaGeneration } from "../../src/session/media.js";
import { fixture } from "../h3/ProviderSession.js";
import { TestClock } from "effect/testing";
import { cleanPressure, run, runClock } from "./SourceFixture.js";

const media: MediaGeneration = {
  generation: 1n,
  tracks: [],
  retired: Effect.never,
  video: () => Stream.never,
  audio: () => Stream.never,
  snapshot: Effect.succeed(cleanPressure),
};
const openH3 = openH3With(bindSession(() => Effect.succeed(media)));
const grant = {
  jwt: Redacted.make("SECRET-SESSION-JWT"),
  expiresAt: 4_102_444_800,
  granted: { maxSessions: 1 as const, maxSessionSeconds: 120 },
};

/** A Client whose create returns the fixture session and records its request. */
const clientOf = (session: Session, created: CreateOptions[]) =>
  Layer.succeed(Client, {
    create: (options) =>
      Effect.sync(() => {
        created.push(options);
        return session;
      }),
    attach: () => Effect.die("attach is not used"),
    createConnected: () => Effect.die("createConnected is not used"),
    attachConnected: () => Effect.die("attachConnected is not used"),
  } satisfies Factory);

test("the owner is registered after allocation, with the grant, before connect", () =>
  runClock(
    Effect.gen(function* () {
      const fake = yield* fixture();
      // The create request goes out at 1,000 s; the granted 120 s end no later than 1,120 s.
      yield* TestClock.setTime(1_000_000);
      const created: CreateOptions[] = [];
      const registered: { allocated: Allocated; connects: number }[] = [];
      const opened = yield* openH3({
        mint: Effect.succeed(grant),
        onAllocated: (allocated) =>
          Effect.sync(() => {
            registered.push({ allocated, connects: fake.lifecycleCalls.connect });
          }),
        source: { provider: { replyTimeout: 100, setupTimeout: 1000 } },
      }).pipe(Effect.provide(clientOf(fake.session, created)));
      expect(created).toEqual([{ model: H3.modelName, jwt: grant.jwt }]);
      expect(registered).toHaveLength(1);
      expect(registered[0]!.connects).toBe(0);
      expect(registered[0]!.allocated.session).toBe(fake.session);
      expect(Redacted.isRedacted(registered[0]!.allocated.grant.jwt)).toBe(true);
      // The owner record to persist names the session and never carries the token.
      expect(registered[0]!.allocated.allocation).toEqual({
        sessionId: fake.session.id,
        ownership: fake.session.ownership,
        model: H3.modelName,
        expiresAt: grant.expiresAt,
        endsAt: 1_120,
      });
      expect(fake.lifecycleCalls.connect).toBe(1);
      expect(opened.source.provider.sessionId).toBe(fake.session.id);
      // The granted seconds become a lifetime with a unit, never bare milliseconds.
      expect(Duration.toSeconds(Duration.fromInputUnsafe(opened.lifetime))).toBe(120);
    }),
  ));

test("a failed registration closes the allocated session before failing, and never connects", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const result = yield* Effect.result(
        openH3({
          mint: Effect.succeed(grant),
          onAllocated: () => Effect.fail(ReactorError.fromCode("InvalidState", "registry down")),
        }).pipe(Effect.provide(clientOf(fake.session, []))),
      );
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(AcquisitionFailure.is(result.failure)).toBe(true);
        if (AcquisitionFailure.is(result.failure))
          expect(result.failure.cleanup.localClosed).toBe(true);
      }
      expect(fake.lifecycleCalls).toMatchObject({ connect: 0, close: 1 });
    }),
  ));

test("a failed mint allocates nothing", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const created: CreateOptions[] = [];
      const exit = yield* Effect.exit(
        openH3({ mint: Effect.fail(ReactorError.fromCode("Http", "token refused")) }).pipe(
          Effect.provide(clientOf(fake.session, created)),
        ),
      );
      expect(Exit.isFailure(exit)).toBe(true);
      expect(created).toEqual([]);
    }),
  ));

test("the opener is a renewal open", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const handle = yield* Renewal.make({
        open: openH3({
          mint: Effect.succeed(grant),
          source: { provider: { replyTimeout: 100, setupTimeout: 1000 } },
        }),
      }).pipe(Effect.provide(clientOf(fake.session, [])));
      expect(yield* handle.sessionId).toMatchObject({ value: fake.session.id });
      yield* handle.close;
      expect(fake.lifecycleCalls.close).toBe(1);
    }),
  ));

test("the orchestration layer provides one handle's three services", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const context = yield* Layer.build(
        Renewal.layer({ open: openH3({ mint: Effect.succeed(grant) }) }).pipe(
          Layer.provide(clientOf(fake.session, [])),
        ),
      );
      const handle = Context.get(context, Handle);
      expect(Context.get(context, Engine)).toBe(handle.engine);
      expect(Context.get(context, Media)).toBe(handle.media);
    }),
  ));
