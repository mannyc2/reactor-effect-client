import { expect, test } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Scope } from "effect";
import * as Http from "effect/unstable/http/HttpClient";
import type { Descriptor } from "../../src/contract.js";
import { CoordinatorClient } from "../../src/coordinator/_internal/client.js";
import { ReactorError } from "../../src/errors.js";
import { SessionLifecycle } from "../../src/session/_internal/lifecycle.js";
import { RemoteSession } from "../../src/session/_internal/remote.js";
import type { SessionOptions } from "../../src/SessionTypes.js";

const options: SessionOptions = {
  apiUrl: "https://lifecycle.fixture",
  intent: { _tag: "Create", model: { name: "fixture/model" } },
};
const descriptor: Descriptor = { session_id: "owned-fixture", state: "ACTIVE", raw: {} };
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
  const response = Deferred.makeUnsafe<Descriptor>();
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
    expect(concurrent).toMatchObject({ code: "InvalidState", context: { outcome: "unknown" } });
    await Effect.runPromise(Fiber.interrupt(pending));
    expect(finalized).toBe(true);
    expect(remote.current).toEqual({ ownership: "unknown" });
    Deferred.doneUnsafe(response, Effect.succeed(descriptor));
    const retried = await Effect.runPromise(Effect.flip(allocate));
    expect(retried).toMatchObject({ code: "InvalidState", context: { outcome: "unknown" } });
    expect(remote.id).toBeUndefined();
    expect(remote.isKnown).toBe(false);
    expect(creates).toBe(1);
  } finally {
    Deferred.doneUnsafe(response, Effect.succeed(descriptor));
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
    const error = new ReactorError("Closed", "session closing");
    Deferred.doneUnsafe(lifecycle.closing, Effect.fail(error));
    const exit = await Effect.runPromiseExit(Fiber.join(pending));
    if (!Exit.isFailure(exit)) throw new Error("allocation survived close");
    const failure = Cause.findError(exit.cause);
    if (failure._tag !== "Success") throw new Error("close lost its typed failure");
    expect(failure.success).toBe(error);
    expect(interrupted).toBe(true);
    expect(remote.current).toEqual({ ownership: "unknown" });
    const refused = await Effect.runPromise(Effect.flip(remote.allocate(options, http, lifecycle)));
    expect(refused).toMatchObject({ code: "Closed", context: { outcome: "not-submitted" } });
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
      return descriptor;
    });
  try {
    const owned = new RemoteSession();
    expect(await Effect.runPromise(owned.allocate(options, http, lifecycle))).toBe(
      descriptor.session_id,
    );
    const evidence = owned.requireKnown();
    evidence.connectionId = 42;
    owned.allocationLost();
    expect(await Effect.runPromise(owned.allocate(options, http, lifecycle))).toBe(
      descriptor.session_id,
    );
    expect(owned.requireKnown()).toBe(evidence);
    expect(evidence.connectionId).toBe(42);
    expect(evidence.descriptor).toBe(descriptor);
    expect(owned.isKnownTerminal()).toBe(false);
    evidence.descriptor = { ...descriptor, state: "CLOSED" };
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
      expect(refusal).toMatchObject({ code: "Closed", context: { outcome: "not-submitted" } });
    }
    expect(creates).toBe(1);
  } finally {
    await Effect.runPromise(Scope.close(lifecycle.scope, Exit.void));
  }
});
