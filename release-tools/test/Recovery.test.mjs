import assert from "node:assert/strict";
import { test } from "bun:test";
import { Effect, Result } from "effect";
import { Host, createPlan, observeRelease, runRelease } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";
import { visibility } from "../report.mjs";
import { prepared, withFixture, loadOffline, verifyOffline } from "./Fixture.mjs";

/** Exercise the real npm provider and core with only in-memory I/O capabilities.
 * No application factory, HTTP transport, Git process or credentials are opened.
 * @param {Effect.Success<ReturnType<typeof loadOffline>>} candidate
 * @param {number} responseStatus */
const offlineHost = (candidate, responseStatus) => {
  /** @type {import("@mannyc1/ts-release").JournalEvent[]} */
  const events = [];
  let sent = 0;
  let reads = 0;
  let sequence = 0;
  /** @type {"missing" | "matching" | "conflict"} */
  let registry = "missing";
  /** @type {import("@mannyc1/ts-release").JournalStore} */
  const store = {
    read: (id) =>
      Effect.sync(() => {
        assert.equal(id, candidate.plan.journalId);
        return { revision: events.length, events: [...events] };
      }),
    append: (id, revision, event) =>
      Effect.sync(() => {
        assert.equal(id, candidate.plan.journalId);
        assert.equal(event.journalId, id);
        if (revision !== events.length)
          return { _tag: "RevisionMismatch", revision: events.length };
        assert.equal(
          events.some((known) => known.eventId === event.eventId),
          false,
        );
        events.push(event);
        return { _tag: "Appended", revision: events.length };
      }),
  };
  /** @type {import("@mannyc1/ts-release/http").HttpRead} */
  const read = (request) =>
    Effect.sync(() => {
      reads++;
      assert.equal(request.method, "GET");
      assert.equal(request.url, "https://registry.npmjs.org/reactor-effect-client");
      if (registry === "missing") return { status: 404, headers: {}, body: new Uint8Array() };
      const { intent } = candidate;
      return {
        status: 200,
        headers: {},
        body: new TextEncoder().encode(
          JSON.stringify({
            name: "reactor-effect-client",
            "dist-tags": { [intent.initialTag]: intent.version },
            versions: {
              [intent.version]: {
                name: "reactor-effect-client",
                version: intent.version,
                dist: {
                  integrity: intent.integrity,
                  shasum: registry === "conflict" ? "0".repeat(40) : intent.shasum,
                },
              },
            },
          }),
        ),
      };
    });
  /** Recreate the host while preserving its journal to model a fresh invocation.
   * @returns {import("@mannyc1/ts-release").HostShape} */
  const host = () => {
    const providers = Npm.definitions({
      bundle: candidate.bundle,
      readContent: candidate.readContent,
      read,
      verifyProvenance: verifyOffline,
    });
    const provider = providers.find((entry) => entry.definitionId === "npm.publish");
    assert.ok(provider);
    return {
      providers,
      store,
      now: () => 1000,
      uniqueId: () => `offline-event-${++sequence}`,
      transport: {
        send: (request) =>
          Effect.gen(function* () {
            // Even this fake send must follow the real core's committed dispatch record.
            assert.equal(
              events.filter((event) => event.body._tag === "DispatchStarted").length,
              sent + 1,
            );
            assert.equal(request.facts.method, "PUT");
            assert.equal(request.facts.replay._tag, "None");
            sent++;
            return yield* provider.decodeResponse(request, {
              status: responseStatus,
              headers: {},
              body: new Uint8Array(),
            });
          }),
      },
    };
  };
  return {
    host,
    events,
    sent: () => sent,
    reads: () => reads,
    /** @param {"missing" | "matching" | "conflict"} next */
    registry: (next) => {
      registry = next;
    },
  };
};

test("unauthorized execution and observation can record evidence but never dispatch", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 200);
    const result = await Effect.runPromise(
      runRelease({ plan: candidate.plan, authorize: false }).pipe(
        Effect.provideService(Host, ports.host()),
      ),
    );
    assert.equal(result.operations[0]?.status, "Unattempted");
    assert.equal(result.operations[0]?.dispatches, 0);
    assert.equal(ports.sent(), 0);
    assert.equal(ports.reads(), 1);
    assert.equal(
      ports.events.some((event) => event.body._tag === "DispatchStarted"),
      false,
    );
    await Effect.runPromise(
      observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
    );
    assert.equal(ports.sent(), 0);
    assert.equal(ports.reads(), 2);
    assert.equal(ports.events.length, 2);
  }));

test("an ambiguous npm response followed by 404 cannot replay or escape through a new Plan", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 503);
    const first = await Effect.runPromise(
      runRelease({ plan: candidate.plan, authorize: true }).pipe(
        Effect.provideService(Host, ports.host()),
      ),
    );
    assert.equal(first.operations[0]?.status, "Inconclusive");
    assert.equal(ports.sent(), 1);
    const failure = ports.events.find(
      (event) =>
        event.body._tag === "ObservationRecorded" && event.body.evidenceKind === "DispatchError",
    );
    assert.ok(failure?.body._tag === "ObservationRecorded");
    assert.equal(failure.body.evidenceVersion, "npm-native-failure/1");
    assert.equal(failure.body.status, "Inconclusive");
    assert.ok(typeof failure.body.evidence === "object" && failure.body.evidence !== null);
    assert.equal(Reflect.get(failure.body.evidence, "status"), 503);
    const resumed = await Effect.runPromise(
      runRelease({ plan: candidate.plan, authorize: true }).pipe(
        Effect.provideService(Host, ports.host()),
      ),
    );
    assert.equal(resumed.operations[0]?.dispatches, 1);
    assert.equal(resumed.operations[0]?.status, "Inconclusive");
    assert.equal(ports.sent(), 1);
    assert.equal(ports.events.filter((event) => event.body._tag === "DispatchStarted").length, 1);
    const changed = await Effect.runPromise(
      createPlan("different-bundle", candidate.plan.operations, candidate.plan.journalId),
    );
    const escaped = await Effect.runPromise(
      Effect.result(
        runRelease({ plan: changed, authorize: true }).pipe(
          Effect.provideService(Host, ports.host()),
        ),
      ),
    );
    assert.ok(Result.isFailure(escaped));
    assert.equal(escaped.failure.code, "journal-envelope");
    assert.equal(ports.sent(), 1);
    ports.registry("matching");
    const observed = await Effect.runPromise(
      observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
    );
    assert.equal(observed.operations[0]?.status, "Satisfied");
    assert.equal(ports.sent(), 1);
  }));

test("a real npm receipt is satisfied while visibility waits for matching registry observations", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 201);
    const operationIds = candidate.plan.operations.map((operation) => operation.operationId);
    const accepted = await Effect.runPromise(
      runRelease({ plan: candidate.plan, authorize: true }).pipe(
        Effect.provideService(Host, ports.host()),
      ),
    );
    assert.equal(accepted.operations[0]?.status, "Satisfied");
    assert.equal(accepted.operations[0]?.receipts, 1);
    assert.equal(visibility(candidate.plan.planId, operationIds, ports.events), "Unconfirmed");
    await Effect.runPromise(
      observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
    );
    assert.equal(visibility(candidate.plan.planId, operationIds, ports.events), "Unconfirmed");
    ports.registry("matching");
    await Effect.runPromise(
      observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
    );
    assert.equal(visibility(candidate.plan.planId, operationIds, ports.events), "Satisfied");
    ports.registry("conflict");
    const conflict = await Effect.runPromise(
      observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
    );
    assert.equal(conflict.operations[0]?.status, "Conflict");
    assert.equal(visibility(candidate.plan.planId, operationIds, ports.events), "Conflict");
    await Effect.runPromise(
      runRelease({ plan: candidate.plan, authorize: true }).pipe(
        Effect.provideService(Host, ports.host()),
      ),
    );
    assert.equal(ports.sent(), 1);
  }));
