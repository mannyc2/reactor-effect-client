import { expect, test } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as Http from "effect/unstable/http/HttpClient";
import { CoordinatorClient } from "../../src/coordinator/_internal/client.js";
import { ReactorError } from "../../src/errors.js";
import { cleanupSession } from "../../src/session/_internal/cleanup.js";
import { SessionLifecycle } from "../../src/session/_internal/lifecycle.js";
import { RemoteSession } from "../../src/session/_internal/remote.js";
import type { CloseReport, SessionOptions } from "../../src/SessionTypes.js";
import * as W from "../../src/wire.generated.js";
import { FakeTrack, HttpFixture, makeSession, MockPeer, withFixture } from "../fixtures.js";

const options: SessionOptions = {
  apiUrl: "https://lifecycle.fixture",
  intent: { _tag: "Create", model: { name: "fixture/model" } },
};
const coordinator = () =>
  new CoordinatorClient(
    options,
    Http.make(() => Effect.die("unexpected fixture HTTP request")),
  );

test("cleanup phases: independent publication, retirement, scope and remote failures all survive", async () => {
  const lifecycle = new SessionLifecycle(() => {});
  const remote = new RemoteSession();
  const http = coordinator();
  const order: string[] = [];
  const submissionError = new ReactorError({
    code: "Disconnected",
    message: "release submission failed",
    context: { outcome: "unknown" },
  });
  const retirementError = new ReactorError({
    code: "Shutdown",
    message: "synchronous peer close failed",
  });
  const peer = new MockPeer(new HttpFixture());
  http.create = () =>
    Effect.succeed({
      sessionId: "cleanup-fixture",
      reply: { session_id: "cleanup-fixture", state: "ACTIVE" },
    });
  http.terminate = () =>
    Effect.sync(() => {
      order.push("terminate");
    }).pipe(Effect.andThen(Effect.die("remote defect")));
  try {
    await Effect.runPromise(remote.allocate(options, http, lifecycle));
    const connection = lifecycle.begin(false, true, () => peer);
    lifecycle.transition("connecting");
    lifecycle.transition("closing");
    for (const name of ["typed", "defect", "released"]) connection.claimed.add(name);
    connection.pendingClaims.set("pending-request", "unresolved-track");
    peer.send = (_channel, bytes) =>
      Effect.suspend(() => {
        const payload = W.ControlClientMessage.decode(bytes).payload;
        if (payload?.case !== "unpublish_track") return Effect.die("unexpected cleanup command");
        order.push(`unpublish:${payload.value.name}`);
        return payload.value.name === "typed"
          ? Effect.fail(submissionError)
          : payload.value.name === "defect"
            ? Effect.die("publication defect")
            : Effect.void;
      });
    await Effect.runPromise(
      Scope.addFinalizer(
        connection.scope,
        Effect.sync(() => {
          order.push("child finalizer");
        }).pipe(Effect.andThen(Effect.die("child defect"))),
      ),
    );
    await Effect.runPromise(
      Scope.addFinalizer(
        lifecycle.scope,
        Effect.sync(() => {
          order.push("outer finalizer");
        }).pipe(Effect.andThen(Effect.die("outer defect"))),
      ),
    );
    const report = await Effect.runPromise(
      cleanupSession({
        connection,
        scope: lifecycle.scope,
        remote,
        http,
        commandTimeout: 50,
        retire: () => {
          order.push("retire");
          throw retirementError;
        },
      }).pipe(Effect.uninterruptible),
    );
    expect(order).toEqual([
      "unpublish:typed",
      "unpublish:defect",
      "unpublish:released",
      "retire",
      "outer finalizer",
      "child finalizer",
      "terminate",
    ]);
    expect(report.localClosed).toBe(false);
    expect(report.localErrors).toHaveLength(4);
    expect(report.localErrors[0]).toBe(submissionError);
    expect(report.localErrors[1]).toMatchObject({
      code: "Shutdown",
      message: "publication cleanup failed",
    });
    expect(report.localErrors[2]).toBe(retirementError);
    expect(report.localErrors[3]).toMatchObject({
      code: "Shutdown",
      message: "local cleanup did not complete cleanly",
    });
    const scopeCause = report.localErrors[3]?.context.detail;
    if (!Cause.isCause(scopeCause)) throw new Error("scope failure lost its full Cause");
    expect(Cause.pretty(scopeCause)).toContain("child defect");
    expect(Cause.pretty(scopeCause)).toContain("outer defect");
    expect(report.remote).toMatchObject({
      attempted: true,
      confirmed: false,
      responseReceived: false,
      error: { code: "Shutdown", context: { outcome: "unknown", sessionId: "cleanup-fixture" } },
    });
    expect(report.unpublishSubmitted).toEqual(["released"]);
    expect(report.unresolvedPublications).toEqual(["unresolved-track"]);
    connection.pendingClaims.clear();
    expect(report.unresolvedPublications).toEqual(["unresolved-track"]);
    expect(Object.isFrozen(report)).toBe(true);
    expect(Object.isFrozen(report.localErrors)).toBe(true);
    expect(Object.isFrozen(report.unpublishSubmitted)).toBe(true);
    expect(Object.isFrozen(report.unresolvedPublications)).toBe(true);
  } finally {
    await Effect.runPromiseExit(Scope.close(lifecycle.scope, Exit.void));
  }
});

for (const commandTimeout of [25, 5_000]) {
  test(`cleanup phases: publication deadline remains bounded under the close mask (${commandTimeout} ms command budget)`, async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const lifecycle = new SessionLifecycle(() => {});
        const remote = new RemoteSession();
        const http = coordinator();
        const order: string[] = [];
        const peer = new MockPeer(new HttpFixture());
        yield* remote.allocate(
          { ...options, intent: { _tag: "Attach", sessionId: "borrowed" } },
          http,
          lifecycle,
        );
        const connection = lifecycle.begin(false, true, () => peer);
        lifecycle.transition("connecting");
        lifecycle.transition("closing");
        connection.claimed.add("stalled");
        peer.send = () =>
          Effect.sync(() => {
            order.push("unpublish");
          }).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                order.push("publication interrupted");
              }),
            ),
          );
        yield* Scope.addFinalizer(
          lifecycle.scope,
          Effect.sync(() => {
            order.push("shutdown");
          }),
        );
        let completed = false;
        const closing = yield* cleanupSession({
          connection,
          scope: lifecycle.scope,
          remote,
          http,
          commandTimeout,
          retire: () => {
            order.push("retire");
            peer.close();
          },
        }).pipe(
          Effect.uninterruptible,
          Effect.tap(() =>
            Effect.sync(() => {
              completed = true;
            }),
          ),
          Effect.forkChild,
        );
        yield* TestClock.adjust(Math.min(1_000, commandTimeout) - 1);
        expect(completed).toBe(false);
        expect(order).toEqual(["unpublish"]);
        yield* TestClock.adjust(1);
        const report = yield* Fiber.join(closing);
        expect(completed).toBe(true);
        expect(order).toEqual(["unpublish", "publication interrupted", "retire", "shutdown"]);
        expect(report.localErrors).toHaveLength(1);
        expect(report.localErrors[0]).toMatchObject({
          code: "Timeout",
          message: "close unpublish: deadline",
        });
        expect(report.unpublishSubmitted).toEqual([]);
        expect(report.ownership).toBe("attached");
        expect(report.remote.attempted).toBe(false);
      }).pipe(Effect.provide(TestClock.layer()), Effect.timeout(2_000)),
    );
  });
}

test("session lifecycle: interrupted concurrent close joins shutdown and publishes one report before callbacks", () =>
  withFixture(async (fixture) => {
    const shutdownEntered = Deferred.makeUnsafe<void>();
    const releaseShutdown = Deferred.makeUnsafe<void>();
    const order: string[] = [];
    let callbacks = 0;
    let callbackStatus: string | undefined;
    let callbackReport: CloseReport | undefined;
    const { session, peers } = makeSession(
      fixture,
      {
        onClose: () => {
          callbacks++;
          callbackStatus = session.snapshot.status;
          // A completed close is synchronously reentrant from the callback.
          callbackReport = Effect.runSync(session.close());
          throw new Error("consumer callback defect");
        },
      },
      (peer) => {
        const close = peer.close.bind(peer);
        peer.close = () => {
          order.push("peer.close");
          close();
        };
        Object.assign(peer, {
          shutdown: Effect.gen(function* () {
            order.push("shutdown started");
            yield* Deferred.succeed(shutdownEntered, undefined);
            yield* Deferred.await(releaseShutdown);
            order.push("shutdown joined");
          }),
        });
      },
    );
    const source = new FakeTrack("audio");
    await Effect.runPromise(session.start());
    await Effect.runPromise(session.publish("input_audio", source));
    const peer = peers[0]!;
    peer.sendHook = (channel, bytes) => {
      if (
        channel === "control" &&
        W.ControlClientMessage.decode(bytes).payload?.case === "unpublish_track"
      )
        order.push("unpublish");
    };
    const terminate = session.http.terminate.bind(session.http);
    session.http.terminate = (id) =>
      Effect.sync(() => {
        order.push("terminate");
      }).pipe(Effect.andThen(terminate(id)));
    const owner = Effect.runFork(session.close());
    try {
      await Effect.runPromise(Deferred.await(shutdownEntered));
      const second = Effect.runPromise(session.close());
      const third = Effect.runPromise(session.close());
      const interrupted = Effect.runPromise(Fiber.interrupt(owner));
      expect(session.snapshot.status).toBe("closing");
      expect(order).toEqual(["unpublish", "peer.close", "shutdown started"]);
      expect(source.readyState).toBe("live");
      expect(source.clones[0]?.readyState).toBe("ended");
      expect(callbacks).toBe(0);
      const sent = peer.sent.length;
      const refused = await Effect.runPromise(Effect.flip(session.command("closed-command", {})));
      expect(refused).toMatchObject({ code: "Closed", context: { outcome: "not-submitted" } });
      peer.emit({ type: "track", name: "late-track", mid: "late" });
      peer.emit({ type: "state", state: "failed" });
      expect(session.snapshot.receivedTracks).toEqual([]);
      expect(peer.sent.length).toBe(sent);
      expect(order).toEqual(["unpublish", "peer.close", "shutdown started"]);

      Deferred.doneUnsafe(releaseShutdown, Effect.void);
      const [report, other] = await Promise.all([second, third]);
      await interrupted;
      expect(report).toBe(other);
      expect(report).toBe(await Effect.runPromise(session.close()));
      expect(session.snapshot.close).toBe(report);
      expect(callbackReport).toBe(report);
      expect(report.localClosed).toBe(true);
      expect(report.remote.confirmed).toBe(true);
      expect(report.unpublishSubmitted).toEqual(["input_audio"]);
      expect(callbackStatus).toBe("closed");
      expect(callbacks).toBe(1);
      expect(order).toEqual([
        "unpublish",
        "peer.close",
        "shutdown started",
        "shutdown joined",
        "terminate",
      ]);
      expect(fixture.calls.filter((call) => call.method === "DELETE")).toHaveLength(1);
    } finally {
      Deferred.doneUnsafe(releaseShutdown, Effect.void);
      await Effect.runPromise(Fiber.interrupt(owner));
      await Effect.runPromise(session.close());
    }
  }));

test("session lifecycle: phase events retain their generation and order through reconnect and close", () =>
  withFixture(async (fixture) => {
    const { session } = makeSession(fixture);
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const observation = yield* session.observe();
            expect(observation.initial.status).toBe("idle");
            const events = yield* Stream.runCollect(observation.events).pipe(Effect.forkScoped);
            yield* session.start();
            yield* session.reconnect();
            yield* session.close();
            const observed = yield* Fiber.join(events);
            expect(
              observed
                .filter((event) => event._tag === "Status")
                .map((event) => [event.status, event.generation]),
            ).toEqual([
              ["connecting", 1n],
              ["waiting", 1n],
              ["ready", 1n],
              ["connecting", 2n],
              ["waiting", 2n],
              ["ready", 2n],
              ["closing", 2n],
              ["closed", 2n],
            ]);
            expect(observed.map((event) => event.sequence)).toEqual([
              1n,
              2n,
              3n,
              4n,
              5n,
              6n,
              7n,
              8n,
            ]);
          }),
        ),
      );
    } finally {
      await Effect.runPromise(session.close());
    }
  }));

test("session lifecycle: a retired generation's shutdown defect reaches reconnect without reviving its callbacks", () =>
  withFixture(async (fixture) => {
    const shutdownError = new ReactorError({
      code: "Shutdown",
      message: "retired native shutdown failed",
    });
    const shutdowns: number[] = [];
    let generations = 0;
    const { session, peers } = makeSession(fixture, {}, (peer) => {
      const generation = ++generations;
      Object.assign(peer, {
        shutdown: Effect.suspend(() => {
          shutdowns.push(generation);
          return generation === 1 ? Effect.fail(shutdownError) : Effect.void;
        }),
      });
    });
    try {
      await Effect.runPromise(session.start());
      const exit = await Effect.runPromiseExit(session.reconnect());
      if (!Exit.isFailure(exit)) throw new Error("reconnect swallowed the shutdown defect");
      const defect = Cause.findDefect(exit.cause);
      if (defect._tag !== "Success")
        throw new Error("shutdown failure was not preserved as a defect");
      expect(defect.success).toBe(shutdownError);
      expect(session.snapshot.status).toBe("connecting");
      expect(session.snapshot.generation).toBe(2n);
      expect(shutdowns).toEqual([1]);
      expect(peers[1]?.prepares).toBe(0);
      peers[0]!.emit({ type: "state", state: "failed" });
      peers[0]!.emit({ type: "track", name: "retired-track", mid: "retired" });
      expect(session.snapshot.status).toBe("connecting");
      expect(session.snapshot.lastError).toBeUndefined();
      expect(session.snapshot.receivedTracks).toEqual([]);
      const report = await Effect.runPromise(session.close());
      expect(shutdowns).toEqual([1, 2]);
      expect(report.localClosed).toBe(true);
      expect(report.remote.confirmed).toBe(true);
    } finally {
      await Effect.runPromise(session.close());
    }
  }));

test("session lifecycle: close preserves an unresolved publication and fences its late claim response", () =>
  withFixture(async (fixture) => {
    const entered = Deferred.makeUnsafe<string>();
    const { session, peers } = makeSession(fixture, { commandTimeoutMs: 1_000 });
    await Effect.runPromise(session.start());
    const peer = peers[0]!;
    peer.autoReply = false;
    peer.sendHook = (channel, bytes) => {
      if (channel !== "control") return;
      const request = W.ControlClientMessage.decode(bytes);
      if (request.payload?.case === "publish_track")
        Deferred.doneUnsafe(entered, Effect.succeed(request.request_id));
    };
    const source = new FakeTrack("audio");
    const publication = Effect.runFork(session.publish("input_audio", source));
    try {
      const requestId = await Effect.runPromise(Deferred.await(entered));
      const report = await Effect.runPromise(session.close());
      const exit = await Effect.runPromiseExit(Fiber.join(publication));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(report.unresolvedPublications).toEqual(["input_audio"]);
      expect(report.unpublishSubmitted).toEqual([]);
      expect(report.remote.confirmed).toBe(true);
      expect(source.clones).toHaveLength(0);
      expect(source.readyState).toBe("live");
      peer.replyControl({
        request_id: requestId,
        kind: 2,
        payload: { case: "publish_track", value: { name: "input_audio" } },
      });
      expect(session.snapshot.status).toBe("closed");
      expect(session.snapshot.claimedTracks).toEqual([]);
      expect(session.snapshot.pending.control).toBe(0);
      expect(await Effect.runPromise(session.close())).toBe(report);
      expect(report.unresolvedPublications).toEqual(["input_audio"]);
    } finally {
      await Effect.runPromise(Fiber.interrupt(publication));
      await Effect.runPromise(session.close());
    }
  }));
