import { expect, test } from "vitest";
import { Effect, Exit, Layer, Redacted, Scope } from "effect";
import * as Http from "effect/unstable/http/HttpClient";
import * as Response from "effect/unstable/http/HttpClientResponse";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Client from "../src/session/index.js";
import { PeerFactory } from "../src/PeerFactory.js";
import { ReactorError } from "../src/errors.js";

const fixture = (
  options: {
    readonly refuseTermination?: boolean;
    /** Later fields of every create reply, after its valid session id. */
    readonly createFields?: Readonly<Record<string, unknown>>;
  } = {},
) => {
  const open = new Set<string>();
  const calls: string[] = [];
  let allocated = 0,
    peers = 0;
  const platform = Http.make((request, url) =>
    Effect.sync(() => {
      calls.push(`${request.method} ${url.pathname}`);
      if (url.pathname === "/sessions" && request.method === "POST") {
        const id = `session-${++allocated}`;
        open.add(id);
        return Response.fromWeb(
          request,
          globalThis.Response.json({ session_id: id, state: "WAITING", ...options.createFields }),
        );
      }
      const id = url.pathname.split("/").at(-1)!;
      if (request.method === "DELETE") {
        if (options.refuseTermination === true)
          return Response.fromWeb(request, new globalThis.Response(null, { status: 403 }));
        open.delete(id);
        return Response.fromWeb(request, new globalThis.Response(null, { status: 204 }));
      }
      return Response.fromWeb(
        request,
        open.has(id)
          ? globalThis.Response.json({ session_id: id, state: "WAITING" })
          : new globalThis.Response(null, { status: 404 }),
      );
    }),
  );
  const dependencies = Layer.mergeAll(
    Layer.succeed(Http.HttpClient, platform),
    NodeCrypto.layer,
    Layer.succeed(PeerFactory, {
      check: Effect.void,
      make: () => {
        peers++;
        throw ReactorError.fromCode("InvalidState", "unexpected connection in allocation test");
      },
    }),
  );
  return {
    open,
    calls,
    get peers() {
      return peers;
    },
    dependencies,
  };
};

test("a shared Client allocates independent sessions without opening peers", async () => {
  const fake = fixture();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const firstScope = yield* Scope.make();
        const secondScope = yield* Scope.make();
        const first = yield* client
          .create({ model: "selected/model" })
          .pipe(Scope.provide(firstScope));
        const second = yield* client
          .create({ model: "selected/model" })
          .pipe(Scope.provide(secondScope));
        expect(first.id).not.toBe(second.id);
        expect(fake.peers).toBe(0);
        expect(first.ownership).toBe("owned");
        expect(fake.open.size).toBe(2);
        yield* Scope.close(firstScope, Exit.void);
        expect(fake.open.has(first.id)).toBe(false);
        expect(fake.open.has(second.id)).toBe(true);
        yield* Scope.close(secondScope, Exit.void);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.open.size).toBe(0);
});

test("owned allocation is cleaned up when the caller fails before connect", async () => {
  const fake = fixture();
  const result = await Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        yield* client.create({ model: "selected/model", jwt: Redacted.make("fixture-token") });
        return yield* Effect.fail("supervisor registration failed");
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(Exit.isFailure(result)).toBe(true);
  expect(fake.open.size).toBe(0);
  expect(fake.calls).toEqual([
    "POST /sessions",
    "DELETE /sessions/session-1",
    "GET /sessions/session-1",
  ]);
});

const invalidDescriptions: readonly (readonly [string, Readonly<Record<string, unknown>>])[] = [
  [
    "an unknown track kind",
    {
      capabilities: {
        protocol_version: "1.0",
        tracks: [{ name: "main_video", kind: "hologram", direction: "recvonly" }],
      },
    },
  ],
  ["malformed capabilities", { capabilities: "every track" }],
];
for (const [name, createFields] of invalidDescriptions)
  test(`a create reply with a valid id and ${name} still terminates the session it names`, async () => {
    const fake = fixture({ createFields });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.make();
          const result = yield* Effect.result(client.create({ model: "selected/model" }));
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") {
            expect(result.failure).toBeInstanceOf(Client.AcquisitionFailure);
            expect(result.failure.reason._tag).toBe("Protocol");
            expect(result.failure.context).toMatchObject({
              operation: "create session",
              sessionId: "session-1",
              outcome: "replied",
            });
            const cleanup = result.failure.cleanup;
            expect(cleanup.allocation).toBe("known");
            expect(cleanup.ownership).toBe("owned");
            expect(cleanup.sessionId).toBe("session-1");
            expect(cleanup.remote).toMatchObject({
              attempted: true,
              confirmed: true,
              evidence: "absent",
            });
          }
          expect(fake.peers).toBe(0);
          expect(fake.open.size).toBe(0);
          expect(fake.calls).toEqual([
            "POST /sessions",
            "DELETE /sessions/session-1",
            "GET /sessions/session-1",
          ]);
        }),
      ).pipe(Effect.provide(fake.dependencies)),
    );
  });

test("closing an attached session does not clear or terminate the remote owner", async () => {
  const fake = fixture();
  fake.open.add("existing-session");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const attached = yield* client.attach({ sessionId: "existing-session" });
        expect(attached.ownership).toBe("attached");
        const report = yield* attached.close;
        expect(report.remote.attempted).toBe(false);
        expect(report.localClosed).toBe(true);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.open.has("existing-session")).toBe(true);
  expect(fake.calls).toEqual([]);
});

test("preflight refuses an unsupported peer before remote allocation", async () => {
  const fake = fixture();
  const result = await Effect.runPromise(
    Effect.result(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.make();
          return yield* client.create({ model: "selected/model" });
        }),
      ).pipe(
        Effect.provideService(PeerFactory, {
          check: Effect.fail(
            ReactorError.fromCode("UnsupportedHost", "fixture platform", {
              outcome: "not-submitted",
            }),
          ),
          make: () => {
            throw new Error("must not allocate");
          },
        }),
        Effect.provide(fake.dependencies),
      ),
    ),
  );
  expect(result._tag).toBe("Failure");
  if (result._tag === "Failure") expect(result.failure.reason._tag).toBe("UnsupportedHost");
  expect(fake.calls).toEqual([]);
});

test("invalid JS input fails through the typed channel before allocation", async () => {
  const fake = fixture();
  const result = await Effect.runPromise(
    Effect.result(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.make();
          return yield* client.create(null as unknown as Client.CreateOptions);
        }),
      ).pipe(Effect.provide(fake.dependencies)),
    ),
  );
  expect(result._tag).toBe("Failure");
  if (result._tag === "Failure") expect(result.failure.reason._tag).toBe("InvalidInput");
  expect(fake.calls).toEqual([]);
});

test("hybrid variables cannot change create into attach or attach into create", async () => {
  const fake = fixture();
  fake.open.add("existing-unrelated-session");
  const input = { model: "wanted-new-model", sessionId: "existing-unrelated-session" };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const created = yield* client.create(input);
        expect(created.id).not.toBe(input.sessionId);
        expect(created.ownership).toBe("owned");
        const attached = yield* client.attach(input);
        expect(attached.id).toBe(input.sessionId);
        expect(attached.ownership).toBe("attached");
        yield* attached.close;
        expect(fake.open.has(input.sessionId)).toBe(true);
        expect(fake.calls.filter((call) => call === "POST /sessions")).toHaveLength(1);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.open.has(input.sessionId)).toBe(true);
  expect(fake.calls.some((call) => call === `DELETE /sessions/${input.sessionId}`)).toBe(false);
});

test("connected acquisition failure closes its allocated session while the outer scope remains open", async () => {
  const fake = fixture();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const result = yield* Effect.result(client.createConnected({ model: "selected/model" }));
        expect(result._tag).toBe("Failure");
        expect(fake.peers).toBe(1);
        expect(fake.open.size).toBe(0);
        expect(fake.calls).toEqual([
          "POST /sessions",
          "DELETE /sessions/session-1",
          "GET /sessions/session-1",
        ]);
        // The same still-open caller scope can acquire another independent lease.
        const next = yield* client.create({ model: "selected/model" });
        expect(next.id).toBe("session-2");
        expect(fake.open.has(next.id)).toBe(true);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.open.size).toBe(0);
});

test("failed attached connection never allocates or deletes its remote owner", async () => {
  const fake = fixture();
  fake.open.add("existing-session");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const result = yield* Effect.result(
          client.attachConnected({ sessionId: "existing-session" }),
        );
        expect(result._tag).toBe("Failure");
        expect(fake.open.has("existing-session")).toBe(true);
        expect(fake.calls).toEqual([]);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
});

test("close retains failed remote termination after successful local disposal", async () => {
  const fake = fixture({ refuseTermination: true });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const session = yield* client.create({ model: "selected/model" });
        const report = yield* session.close;
        expect(report.localClosed).toBe(true);
        expect(report.allocation).toBe("known");
        expect(report.sessionId).toBe(session.id);
        expect(report.remote.confirmed).toBe(false);
        expect(report.remote.evidence).toBe(null);
        expect(report.remote.deleteStatus).toBe(403);
        expect(fake.open.has(session.id)).toBe(true);
        expect(yield* session.close).toBe(report);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.calls.filter((call) => call.startsWith("DELETE"))).toHaveLength(1);
});

test("a failed connected acquisition exposes unconfirmed cleanup without retaining its local lease", async () => {
  const fake = fixture({ refuseTermination: true });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* Client.make();
        const result = yield* Effect.result(client.createConnected({ model: "selected/model" }));
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure).toBeInstanceOf(Client.AcquisitionFailure);
          expect(result.failure.reason._tag).toBe("InvalidState");
          expect(result.failure.cleanup.localClosed).toBe(true);
          expect(result.failure.cleanup.allocation).toBe("known");
          expect(result.failure.cleanup.sessionId).toBe("session-1");
          expect(result.failure.cleanup.remote.confirmed).toBe(false);
          expect(result.failure.cleanup.remote.deleteStatus).toBe(403);
        }
        expect(fake.open.has("session-1")).toBe(true);
        expect(fake.calls).toEqual([
          "POST /sessions",
          "DELETE /sessions/session-1",
          "GET /sessions/session-1",
        ]);
      }),
    ).pipe(Effect.provide(fake.dependencies)),
  );
  expect(fake.calls.filter((call) => call.startsWith("DELETE"))).toHaveLength(1);
});
