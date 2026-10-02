/** The canonical session over the in-process native host, driven through the scripted fake addon. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import { PeerFactory } from "reactor-effect-client/Peer";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { Session } from "reactor-effect-client/Session";
import { expect } from "vitest";
import { coordinator, fakeAddon, nativeClient, nativePeer } from "./support.js";
import type { FakeAddon } from "./support.js";

const sessionId = "sess_native_fixture";
const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
] as const;
const settings = { apiUrl: "https://coordinator.fixture" } as const;
const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };
const fixture = coordinator({ sessionId, tracks });

layer(NodeServices.layer, { excludeTestServices: true })(
  "native canonical session boundary",
  (it) => {
    it.effect(
      "dispatches owned media and preserves bounded submitted requests across caller cancellation",
      () =>
        Effect.gen(function* () {
          const addon = yield* fakeAddon;
          const remote = yield* fixture;
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* nativeClient({
                settings: { ...settings, maxPending: 32 },
                options: { addon: addon.path },
              });
              const client = yield* factory.create(create);
              const ready = yield* client.ready;
              expect(ready.remote.ownership).toBe("owned");

              const reference = {
                uploadId: "fixture-upload",
                name: "image.png",
                mimeType: "image/png",
                size: -1n,
              };
              const invalid = yield* Effect.result(
                client.command("echo", {}, { uploads: new Map([["picture", reference]]) }),
              );
              expect(invalid).toMatchObject({
                _tag: "Failure",
                failure: {
                  reason: { _tag: "InvalidInput" },
                  context: { outcome: "not-submitted" },
                },
              });
              expect((yield* client.snapshot).pending.data).toBe(0);

              // The fake addon's snapshot is a test gate: both generation readers
              // subscribe before it releases media.
              const media = yield* client.decoded;
              expect(media.generation).toBe(ready.generation);
              const videoReader = yield* Effect.forkChild(
                media.video("main_video").pipe(Stream.runHead),
              );
              const audioReader = yield* Effect.forkChild(
                media.audio("main_audio").pipe(Stream.runHead),
              );
              yield* Effect.yieldNow;
              yield* media.pressure;
              const video = Option.getOrThrow(yield* Fiber.join(videoReader));
              const audio = Option.getOrThrow(yield* Fiber.join(audioReader));

              for (let index = 0; index < 32; index++) {
                const fiber = yield* Effect.forkChild(client.command("echo", { index }));
                yield* Effect.sleep(5);
                yield* Fiber.interrupt(fiber);
              }

              const pressure = yield* client.snapshot;
              const overflow = yield* Effect.result(client.command("echo", { index: 32 }));
              const closeReport = yield* client.close;
              const afterClose = yield* Effect.result(client.command("echo", {}));
              const closed = yield* client.snapshot;
              return { video, audio, pressure, overflow, afterClose, closed, closeReport };
            }),
          ).pipe(Effect.provideService(HttpClient.HttpClient, remote.client));

          expect(result.video).toMatchObject({
            _tag: "VideoFrame",
            format: "BGRA",
            track: "main_video",
            width: 1,
            height: 1,
            frameId: 18446744073709551615n,
            timestampMicros: 9007199254740993n,
            // The admission sequence crosses the binding as a full 64-bit value.
            sequence: 9007199254740993n,
          });
          expect([...result.video.data]).toEqual([1, 2, 3, 4]);
          expect([...result.video.metadata]).toEqual([9, 8, 7]);
          expect(result.audio).toMatchObject({
            _tag: "AudioFrame",
            track: "main_audio",
            sampleRate: 48_000,
            channels: 2,
            sequence: 3n,
          });
          expect([...result.audio.samples]).toEqual([1, -2, 300, -400]);

          expect(result.pressure.pending.data).toBe(32);
          expect(result.overflow).toMatchObject({
            _tag: "Failure",
            failure: { reason: { _tag: "Overflow" }, context: { outcome: "not-submitted" } },
          });
          expect(result.afterClose).toMatchObject({
            _tag: "Failure",
            failure: { reason: { _tag: "Closed" }, context: { outcome: "not-submitted" } },
          });
          expect(result.closed.status).toBe("closed");
          expect(result.closeReport.localClosed).toBe(true);
          expect(result.closeReport.localErrors).toEqual([]);
        }),
      15_000,
    );

    it.effect(
      "bounds a native owner join that never completes, retains its peer and still terminates the remote session",
      () =>
        Effect.gen(function* () {
          const addon = yield* fakeAddon;
          yield* Effect.addFinalizer(() => addon.hold("shutdown", false));
          const remote = yield* fixture;
          const options = { addon: addon.path, shutdownTimeout: "250 millis" } as const;
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* nativeClient({ settings, options });
              const client = yield* factory.create(create);
              yield* addon.hold("shutdown", true);
              const started = performance.now();
              const report = yield* client.close;
              const closeMs = performance.now() - started;
              // The wedged join keeps its peer, and the process admits no new
              // owner, so nothing is allocated for one.
              const degraded = yield* Effect.result(factory.create(create));
              const allocatedWhileDegraded = (yield* remote.allocated).length;
              yield* addon.hold("shutdown", false);
              // Once the join completes, the process admits peers again.
              const recovered = yield* factory
                .create(create)
                .pipe(Effect.retry({ schedule: Schedule.spaced("10 millis"), times: 200 }));
              const recoveredReport = yield* recovered.close;
              return { report, closeMs, degraded, allocatedWhileDegraded, recoveredReport };
            }),
          ).pipe(Effect.provideService(HttpClient.HttpClient, remote.client));

          expect(result.closeMs).toBeGreaterThanOrEqual(200);
          expect(result.closeMs).toBeLessThan(2_000);
          expect(result.report.localClosed).toBe(false);
          expect(result.report.localErrors).toHaveLength(1);
          const [shutdown] = result.report.localErrors;
          expect(shutdown?.reason).toBe("Shutdown");
          // The connection finalizer dies with the typed deadline failure, which
          // cleanup records before it goes on to terminate the owned session.
          expect(shutdown?.message).toBe(
            "native owner join exceeded its deadline; handle retained",
          );
          expect(result.report.remote).toMatchObject({
            attempted: true,
            confirmed: true,
            evidence: "absent",
          });
          expect((yield* remote.deleted).has(sessionId)).toBe(true);

          expect(result.degraded).toMatchObject({
            _tag: "Failure",
            failure: { reason: { _tag: "Native" }, context: { outcome: "not-submitted" } },
          });
          expect(result.allocatedWhileDegraded).toBe(1);
          expect(result.recoveredReport.localClosed).toBe(true);
          expect(result.recoveredReport.localErrors).toEqual([]);
          expect(yield* remote.allocated).toHaveLength(2);
        }),
      15_000,
    );

    it.effect(
      "rejects a shutdown deadline that is not a positive duration when the layer is built",
      () =>
        Effect.gen(function* () {
          const addon = yield* fakeAddon;
          const remote = yield* fixture;
          for (const shutdownTimeout of [0, -1, Number.NaN]) {
            // No Client can exist, so nothing can allocate a remote session.
            const result = yield* nativeClient({
              settings,
              options: { addon: addon.path, shutdownTimeout },
            }).pipe(
              Effect.result,
              Effect.scoped,
              Effect.provideService(HttpClient.HttpClient, remote.client),
            );
            expect(result).toMatchObject({
              _tag: "Failure",
              failure: { reason: { _tag: "InvalidInput" }, context: { outcome: "not-submitted" } },
            });
          }
          expect(yield* remote.allocated).toEqual([]);
        }),
    );

    it.effect("fails to build the layer, before any allocation, when the addon cannot load", () =>
      Effect.gen(function* () {
        const remote = yield* fixture;
        const result = yield* nativeClient({
          settings,
          options: { addon: "/nonexistent/reactor-effect-native.node" },
        }).pipe(
          Effect.result,
          Effect.scoped,
          Effect.provideService(HttpClient.HttpClient, remote.client),
        );
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "Native" }, context: { outcome: "not-submitted" } },
        });
        expect(yield* remote.allocated).toEqual([]);
      }),
    );
  },
);

/**
 * The canonical client over in-process peers on the fake's in-process module,
 * whose controls a test drives. These sessions do not reconnect themselves, so
 * a failed connection stays failed for the test to read.
 */
const clientOver = (addon: FakeAddon) =>
  Effect.gen(function* () {
    const services = yield* Layer.build(CoordinatorClient.layer(settings));
    return yield* Reactor.make({ reconnect: false }).pipe(
      Effect.provide(services),
      Effect.provideService(
        PeerFactory,
        PeerFactory.of({ check: Effect.void, make: nativePeer({ addon: addon.module }) }),
      ),
    );
  });

/** The first snapshot at which the session has lost its connection. */
const disconnected = (client: Session) =>
  client.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "disconnected"),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );

layer(NodeServices.layer)("native canonical session on the fiber's Clock", (it) => {
  it.effect("classifies a failed native connection from its statistics", () =>
    Effect.gen(function* () {
      const addon = yield* fakeAddon;
      const remote = yield* fixture;
      const { ready, lost } = yield* Effect.gen(function* () {
        const factory = yield* clientOver(addon);
        const client = yield* factory.create(create);
        const ready = yield* client.ready;
        const [made] = addon.module.controls.peers;
        assert(made !== undefined, "the session opened an addon peer");
        made.fail();
        return { ready, lost: yield* disconnected(client) };
      }).pipe(Effect.scoped, Effect.provideService(HttpClient.HttpClient, remote.client));
      // The fake's statistics list no candidate pair: nothing ICE could use.
      expect(lost.lastError).toMatchObject({
        reason: { _tag: "IceFailed", pairs: 0, candidateTypes: [] },
        context: { generation: ready.generation },
      });
    }),
  );

  it.effect(
    "reports Disconnected when the failed connection's statistics never return, on the fiber's Clock",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const remote = yield* fixture;
        const { held, lost } = yield* Effect.gen(function* () {
          const factory = yield* clientOver(addon);
          const client = yield* factory.create(create);
          yield* addon.hold("stats", true);
          // Release the held read before the session's close joins its peer.
          yield* Effect.addFinalizer(() => addon.hold("stats", false));
          const [made] = addon.module.controls.peers;
          assert(made !== undefined, "the session opened an addon peer");
          made.fail();
          yield* TestClock.withLive(addon.reached("stats"));
          // The read never answers, and only the fiber's Clock ends the wait.
          const held = yield* client.snapshot;
          yield* TestClock.adjust("2 seconds");
          return { held, lost: yield* disconnected(client) };
        }).pipe(Effect.scoped, Effect.provideService(HttpClient.HttpClient, remote.client));
        expect(held.status).toBe("ready");
        const error = lost.lastError;
        assert(error !== undefined, "the failed connection was reported");
        expect(error).toMatchObject({
          reason: { _tag: "Disconnected" },
          message: "peer state failed",
        });
        const detail = error.context.detail && Redacted.value(error.context.detail);
        assert(ReactorError.is(detail), "the failure carries why classification ended");
        expect(detail.reason._tag).toBe("Timeout");
      }),
  );
});
