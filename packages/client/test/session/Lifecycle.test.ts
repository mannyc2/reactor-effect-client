import { expect, test } from "vitest";
import { Cause, Deferred, Effect, Exit, Scope } from "effect";
import { ReactorError } from "../../src/errors.js";
import { Connection, SessionLifecycle } from "../../src/session/_internal/lifecycle.js";
import type { ReadyState, Status } from "../../src/SessionTypes.js";
import { HttpFixture, MockPeer } from "../fixtures.js";

const negotiated: ReadyState["remote"] = {
  ownership: "owned",
  sessionId: "lifecycle-fixture",
  connectionId: 17,
  descriptor: {
    session_id: "lifecycle-fixture",
    state: "ACTIVE",
    capabilities: { protocol_version: "1.0", tracks: [] },
    selected_transport: { protocol: "webrtc", version: "1.0" },
    raw: {},
  },
};

test("lifecycle: replacing a generation fences callbacks before retiring the old peer", async () => {
  const statuses: Array<{ status: Status; generation: bigint }> = [];
  const lifecycle = new SessionLifecycle((status) => {
    statuses.push({ status, generation: lifecycle.generation });
  });
  let peers = 0;
  const makePeer = () => {
    peers++;
    return new MockPeer(new HttpFixture());
  };
  try {
    expect(() => lifecycle.transition("ready")).toThrow("illegal transition idle -> ready");
    expect(lifecycle.generation).toBe(0n);
    const first = lifecycle.begin(false, false, makePeer);
    lifecycle.transition("connecting");
    lifecycle.transition("waiting");
    first.negotiated = negotiated;
    lifecycle.transition("ready");
    lifecycle.transition("ready");
    expect(first).toBe(lifecycle.currentReady());
    expect(() => lifecycle.begin(false, true, makePeer)).toThrow("connect while ready");
    expect(() => lifecycle.begin(true, false, makePeer)).toThrow("without a known session");
    expect(peers).toBe(1);
    expect(lifecycle.generation).toBe(1n);

    const second = lifecycle.begin(true, true, makePeer);
    lifecycle.transition("connecting");
    expect(second.generation).toBe(2n);
    expect(lifecycle.accepts(first)).toBe(false);
    expect(lifecycle.accepts(second)).toBe(true);
    expect(() => lifecycle.assertCurrent(first)).toThrow("retired connection generation");
    const oldFailure = ReactorError.fromCode("Disconnected", "old generation failure");
    first.failure = oldFailure;
    const retired = Effect.runSyncExit(Effect.sync(() => lifecycle.assertCurrent(first)));
    if (!Exit.isFailure(retired)) throw new Error("retired generation was accepted");
    const defect = Cause.findDefect(retired.cause);
    expect(defect._tag).toBe("Success");
    if (defect._tag === "Success") expect(defect.success).toBe(oldFailure);
    expect(lifecycle.disconnect(first, oldFailure)).toBe(false);
    expect(lifecycle.status).toBe("connecting");
    expect(lifecycle.lastError).toBeUndefined();

    const currentFailure = ReactorError.fromCode("Disconnected", "current generation failure");
    expect(lifecycle.disconnect(second, currentFailure)).toBe(true);
    expect(lifecycle.lastError).toBe(currentFailure);
    expect(lifecycle.disconnect(first, oldFailure)).toBe(false);
    expect(lifecycle.lastError).toBe(currentFailure);
    expect(statuses).toEqual([
      { status: "connecting", generation: 1n },
      { status: "waiting", generation: 1n },
      { status: "ready", generation: 1n },
      { status: "connecting", generation: 2n },
      { status: "disconnected", generation: 2n },
    ]);
  } finally {
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});

test("lifecycle: readiness requires all three signals in any order and cannot revive a failed peer", async () => {
  const scope = Scope.makeUnsafe();
  const signals = ["peerConnected", "controlOpen", "dataOpen"] as const;
  try {
    for (const first of signals)
      for (const second of signals.filter((signal) => signal !== first)) {
        const last = signals.find((signal) => signal !== first && signal !== second)!;
        const connection = new Connection(1n, scope, new MockPeer(new HttpFixture()));
        for (const signal of [first, second]) {
          connection[signal] = true;
          connection.readyGate();
          expect(Deferred.isDoneUnsafe(connection.ready)).toBe(false);
        }
        connection[last] = true;
        connection.readyGate();
        expect(Deferred.isDoneUnsafe(connection.ready)).toBe(true);
        expect(Effect.runSyncExit(Deferred.await(connection.ready))).toEqual(Exit.void);
      }
    const failed = new Connection(2n, scope, new MockPeer(new HttpFixture()));
    failed.failure = ReactorError.fromCode("Disconnected", "failed before readiness");
    for (const signal of signals) failed[signal] = true;
    failed.readyGate();
    expect(Deferred.isDoneUnsafe(failed.ready)).toBe(false);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("lifecycle: missing negotiated state stays a defect, while close refuses new work", async () => {
  const lifecycle = new SessionLifecycle(() => {});
  try {
    const connection = lifecycle.begin(false, true, () => new MockPeer(new HttpFixture()));
    lifecycle.transition("connecting");
    lifecycle.transition("waiting");
    lifecycle.transition("ready");
    const invariant = Effect.runSyncExit(Effect.sync(() => lifecycle.currentReady()));
    if (!Exit.isFailure(invariant)) throw new Error("missing negotiated descriptor was accepted");
    const defect = Cause.findDefect(invariant.cause);
    if (defect._tag !== "Success") throw new Error("missing descriptor was not a defect");
    expect(defect.success).toBeInstanceOf(Error);
    expect(defect.success).not.toBeInstanceOf(ReactorError);

    connection.negotiated = negotiated;
    lifecycle.transition("closing");
    expect(lifecycle.accepts(connection)).toBe(false);
    expect(() => lifecycle.assertCurrent(connection)).toThrow("retired connection generation");
    const closed = Effect.runSyncExit(Effect.sync(() => lifecycle.currentReady()));
    if (!Exit.isFailure(closed)) throw new Error("closing session accepted work");
    const refusal = Cause.findDefect(closed.cause);
    if (refusal._tag !== "Success") throw new Error("missing closed refusal");
    expect(refusal.success).toMatchObject({
      reason: { _tag: "Closed" },
      context: { outcome: "not-submitted" },
    });
    expect(lifecycle.disconnect(connection, ReactorError.fromCode("Disconnected", "late"))).toBe(
      false,
    );
    expect(lifecycle.lastError).toBeUndefined();
    lifecycle.transition("closed");
    expect(() => lifecycle.begin(true, true, () => connection.peer)).toThrow(
      "reconnect while closed",
    );
    expect(lifecycle.generation).toBe(1n);
  } finally {
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});
