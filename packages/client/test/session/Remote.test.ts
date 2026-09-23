import { expect, test } from "vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Scope } from "effect";
import * as Http from "effect/unstable/http/HttpClient";
import { CoordinatorClient } from "../../src/coordinator/_internal/client.js";
import type { Allocation } from "../../src/coordinator/_internal/client.js";
import { ReactorError } from "../../src/errors.js";
import { SessionLifecycle } from "../../src/session/_internal/lifecycle.js";
import { RemoteSession } from "../../src/session/_internal/remote.js";
import type { SessionOptions } from "../../src/SessionTypes.js";

const options: SessionOptions = {
  apiUrl: "https://lifecycle.fixture",
  intent: { _tag: "Create", model: { name: "fixture/model" } },
};
const allocation: Allocation = {
  sessionId: "owned-fixture",
  reply: { session_id: "owned-fixture", state: "ACTIVE" },
};
const coordinator = () =>
  new CoordinatorClient(
    options,
    Http.make(() => Effect.die("unexpected fixture HTTP request")),
  );

test("remote lifecycle: interrupted allocation cannot be retried or claimed by a late response", async () => {
  const remote = new RemoteSession();
  const lifecycle = new SessionLifecycle(() => {});
  const http = coordinator();
  const entered = Deferred.makeUnsafe<void>();
  const response = Deferred.makeUnsafe<Allocation>();
  let creates = 0;
  let finalized = false;
  http.create = () =>
    Effect.gen(function* () {
      creates++;
      yield* Deferred.succeed(entered, undefined);
      return yield* Deferred.await(response);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          finalized = true;
        }),
      ),
    );
  const allocate = remote.allocate(options, http, lifecycle);
  expect(remote.current).toBeUndefined();
  const pending = Effect.runFork(allocate);
  try {
    await Effect.runPromise(Deferred.await(entered));
    expect(remote.current).toEqual({ ownership: "allocating" });
    const concurrent = await Effect.runPromise(Effect.flip(allocate));
    expect(concurrent).toMatchObject({
      reason: { _tag: "InvalidState" },
      context: { outcome: "unknown" },
    });
    await Effect.runPromise(Fiber.interrupt(pending));
    expect(finalized).toBe(true);
    expect(remote.current).toEqual({ ownership: "unknown" });
    Deferred.doneUnsafe(response, Effect.succeed(allocation));
    const retried = await Effect.runPromise(Effect.flip(allocate));
    expect(retried).toMatchObject({
      reason: { _tag: "InvalidState" },
      context: { outcome: "unknown" },
    });
    expect(remote.id).toBeUndefined();
    expect(remote.isKnown).toBe(false);
    expect(creates).toBe(1);
  } finally {
    Deferred.doneUnsafe(response, Effect.succeed(allocation));
    await Effect.runPromise(Fiber.interrupt(pending));
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});

test("remote lifecycle: closing an allocation retains unknown ownership without swallowing its failure", async () => {
  const remote = new RemoteSession();
  const lifecycle = new SessionLifecycle(() => {});
  const http = coordinator();
  const entered = Deferred.makeUnsafe<void>();
  let interrupted = false;
  http.create = () =>
    Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Effect.never),
      Effect.onInterrupt(() =>
        Effect.sync(() => {
          interrupted = true;
        }),
      ),
    );
  const pending = Effect.runFork(remote.allocate(options, http, lifecycle));
  try {
    await Effect.runPromise(Deferred.await(entered));
    lifecycle.transition("closing");
    const error = ReactorError.fromCode("Closed", "session closing");
    Deferred.doneUnsafe(lifecycle.closing, Effect.fail(error));
    const exit = await Effect.runPromiseExit(Fiber.join(pending));
    if (!Exit.isFailure(exit)) throw new Error("allocation survived close");
    const failure = Cause.findError(exit.cause);
    if (failure._tag !== "Success") throw new Error("close lost its typed failure");
    expect(failure.success).toBe(error);
    expect(interrupted).toBe(true);
    expect(remote.current).toEqual({ ownership: "unknown" });
    const refused = await Effect.runPromise(Effect.flip(remote.allocate(options, http, lifecycle)));
    expect(refused).toMatchObject({
      reason: { _tag: "Closed" },
      context: { outcome: "not-submitted" },
    });
  } finally {
    await Effect.runPromise(Fiber.interrupt(pending));
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});

test("remote lifecycle: owned evidence survives reconnect facts, attached identity never allocates", async () => {
  const lifecycle = new SessionLifecycle(() => {});
  const http = coordinator();
  let creates = 0;
  http.create = () =>
    Effect.sync(() => {
      creates++;
      return allocation;
    });
  try {
    const owned = new RemoteSession();
    expect(await Effect.runPromise(owned.allocate(options, http, lifecycle))).toBe(
      allocation.sessionId,
    );
    const evidence = owned.requireKnown();
    evidence.connectionId = 42;
    owned.allocationLost();
    expect(await Effect.runPromise(owned.allocate(options, http, lifecycle))).toBe(
      allocation.sessionId,
    );
    expect(owned.requireKnown()).toBe(evidence);
    expect(evidence.connectionId).toBe(42);
    expect(evidence.descriptor).toEqual({
      session_id: "owned-fixture",
      state: "ACTIVE",
      raw: { session_id: "owned-fixture", state: "ACTIVE" },
    });
    expect(owned.isKnownTerminal()).toBe(false);
    evidence.descriptor = { session_id: "owned-fixture", state: "CLOSED", raw: {} };
    expect(owned.isKnownTerminal()).toBe(true);

    const attached = new RemoteSession();
    const attach: SessionOptions = {
      ...options,
      intent: { _tag: "Attach", sessionId: "attached-fixture", connectionId: 0 },
    };
    expect(await Effect.runPromise(attached.allocate(attach, http, lifecycle))).toBe(
      "attached-fixture",
    );
    expect(attached.current).toEqual({
      ownership: "attached",
      id: "attached-fixture",
      connectionId: 0,
    });
    attached.allocationLost();
    expect(attached.isKnown).toBe(true);
    expect(creates).toBe(1);

    lifecycle.transition("closing");
    for (const remote of [owned, attached]) {
      const refusal = await Effect.runPromise(
        Effect.flip(remote.allocate(options, http, lifecycle)),
      );
      expect(refusal).toMatchObject({
        reason: { _tag: "Closed" },
        context: { outcome: "not-submitted" },
      });
    }
    expect(creates).toBe(1);
  } finally {
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});

test("remote lifecycle: a reply that names its session but cannot describe it is still owned", async () => {
  const lifecycle = new SessionLifecycle(() => {});
  const http = coordinator();
  let creates = 0;
  http.create = () =>
    Effect.sync(() => {
      creates++;
      return {
        sessionId: "owned-fixture",
        reply: {
          session_id: "owned-fixture",
          state: "ACTIVE",
          capabilities: {
            protocol_version: "1.0",
            tracks: [{ name: "main_video", kind: "hologram", direction: "recvonly" }],
          },
        },
      };
    });
  try {
    const remote = new RemoteSession();
    const failure = await Effect.runPromise(Effect.flip(remote.allocate(options, http, lifecycle)));
    expect(failure).toMatchObject({
      reason: { _tag: "Protocol" },
      message: "unknown track kind",
      context: { operation: "create session", sessionId: "owned-fixture", outcome: "replied" },
    });
    // Ownership was recorded from the id before the rest of the reply failed.
    expect(remote.current).toEqual({ ownership: "owned", id: "owned-fixture" });
    remote.allocationLost();
    expect(remote.id).toBe("owned-fixture");
    expect(remote.isKnownTerminal()).toBe(false);
    expect(await Effect.runPromise(remote.allocate(options, http, lifecycle))).toBe(
      "owned-fixture",
    );
    expect(creates).toBe(1);
  } finally {
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});
