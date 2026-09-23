import assert from "node:assert/strict";
import { test } from "bun:test";
import { Effect, Result } from "effect";
import { Host, createPlan, observeRelease, runRelease } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";
import { visibility } from "../report.mjs";
import {
  browserPackage,
  clientPackage,
  hostPackages,
  loadOffline,
  nativePackage,
  operationFor,
  packageNames,
  prepared,
  reportFor,
  verifyOffline,
  withFixture,
} from "./Fixture.mjs";

/** @typedef {"missing" | "matching" | "conflict"} RegistryState */

/** Exercise the real npm provider and core with only in-memory I/O capabilities.
 * No application factory, HTTP transport, Git process or credentials are opened.
 * The registry serves each workspace package's metadata document from its admitted intent.
 * @param {import("./Fixture.mjs").Candidate} candidate
 * @param {number} initialStatus the HTTP status every fake PUT is answered with */
const offlineHost = (candidate, initialStatus) => {
  /** @type {import("@mannyc1/ts-release").JournalEvent[]} */
  const events = [];
  let sent = 0;
  let responseStatus = initialStatus;
  /** Package names in the order the core dispatched their PUTs. @type {string[]} */
  const dispatched = [];
  /** Package names in the order the core observed their registry documents. @type {string[]} */
  const reads = [];
  let sequence = 0;
  /** @type {Map<string, RegistryState>} */
  const registry = new Map();
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
      assert.equal(request.method, "GET");
      const prefix = "https://registry.npmjs.org/";
      assert.ok(request.url.startsWith(prefix), request.url);
      const name = request.url.slice(prefix.length);
      const intent = candidate.intents.get(name);
      assert.ok(intent, `registry read outside the workspace: ${request.url}`);
      assert.equal(request.url, `${prefix}${intent.name}`);
      reads.push(intent.name);
      const state = registry.get(name) ?? "missing";
      if (state === "missing") return { status: 404, headers: {}, body: new Uint8Array() };
      return {
        status: 200,
        headers: {},
        body: new TextEncoder().encode(
          JSON.stringify({
            name: intent.name,
            "dist-tags": { [intent.initialTag]: intent.version },
            versions: {
              [intent.version]: {
                name: intent.name,
                version: intent.version,
                dist: {
                  integrity: intent.integrity,
                  shasum: state === "conflict" ? "0".repeat(40) : intent.shasum,
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
            // Even this fake send must follow the real core's committed dispatch record:
            // exactly one new DispatchStarted, for this package's operation, per send.
            const started = events.filter((event) => event.body._tag === "DispatchStarted");
            assert.equal(started.length, sent + 1);
            assert.equal(request.facts.method, "PUT");
            assert.equal(request.facts.replay._tag, "None");
            const name = String(JSON.parse(request.facts.scope).intent.name);
            assert.ok(candidate.intents.has(name), `PUT outside the workspace: ${name}`);
            assert.equal(request.facts.endpoint, `https://registry.npmjs.org/${name}`);
            const latest = started.at(-1);
            assert.ok(latest?.body._tag === "DispatchStarted");
            assert.equal(latest.body.operationId, operationFor(candidate, name).operationId);
            dispatched.push(name);
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
    dispatched,
    reads,
    sent: () => sent,
    /** @param {number} status */
    respond: (status) => {
      responseStatus = status;
    },
    /** @param {RegistryState} next @param {readonly string[]} [names] */
    registry: (next, names = packageNames) => {
      for (const name of names) registry.set(name, next);
    },
  };
};

/** @param {import("./Fixture.mjs").Candidate} candidate @param {ReturnType<typeof offlineHost>} ports @param {boolean} authorize */
const run = (candidate, ports, authorize) =>
  Effect.runPromise(
    runRelease({ plan: candidate.plan, authorize }).pipe(Effect.provideService(Host, ports.host())),
  );
/** @param {import("./Fixture.mjs").Candidate} candidate @param {ReturnType<typeof offlineHost>} ports */
const observe = (candidate, ports) =>
  Effect.runPromise(
    observeRelease({ plan: candidate.plan }).pipe(Effect.provideService(Host, ports.host())),
  );
/** @param {import("./Fixture.mjs").Candidate} candidate @param {ReturnType<typeof offlineHost>} ports */
const visible = (candidate, ports) =>
  visibility(
    candidate.plan.planId,
    candidate.plan.operations.map((operation) => operation.operationId),
    ports.events,
  );
/** @param {ReturnType<typeof offlineHost>} ports */
const dispatchEvents = (ports) =>
  ports.events.filter((event) => event.body._tag === "DispatchStarted").length;

// The core (ts-release 0.4.1) gates a dependent operation on its dependency's Satisfied
// status: runRelease neither observes nor dispatches a host publication until the client's
// publication is Satisfied, while observeRelease refreshes every package regardless.

test("unauthorized execution and observation can record evidence but never dispatch", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 200);
    const result = await run(candidate, ports, false);
    for (const name of packageNames) {
      const line = reportFor(result, operationFor(candidate, name));
      assert.equal(line.status, "Unattempted");
      assert.equal(line.dispatches, 0);
      assert.equal(line.receipts, 0);
      // Only the client, whose dependencies are met, is observed by an unauthorized run.
      assert.equal(line.observations, name === clientPackage ? 1 : 0);
    }
    assert.equal(ports.sent(), 0);
    assert.deepEqual(ports.reads, [clientPackage]);
    assert.deepEqual(ports.dispatched, []);
    assert.equal(dispatchEvents(ports), 0);
    assert.equal(visible(candidate, ports), "Unconfirmed");
    const observed = await observe(candidate, ports);
    assert.equal(ports.sent(), 0);
    assert.deepEqual([...ports.reads].sort(), [clientPackage, ...packageNames].sort());
    assert.equal(ports.reads.length, 4);
    assert.equal(ports.events.length, 4);
    assert.ok(
      ports.events.every(
        (event) =>
          event.body._tag === "ObservationRecorded" &&
          event.body.evidenceKind === "Observation" &&
          event.body.status === "Absent",
      ),
    );
    for (const name of packageNames) {
      const line = reportFor(observed, operationFor(candidate, name));
      assert.equal(line.status, "Unattempted");
      assert.equal(line.observations, name === clientPackage ? 2 : 1);
    }
    assert.equal(visible(candidate, ports), "Unconfirmed");
  }));

test("an ambiguous npm response to the client publish never replays, keeps the hosts unattempted and cannot escape through a new Plan", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 503);
    const client = operationFor(candidate, clientPackage);
    const first = await run(candidate, ports, true);
    assert.equal(reportFor(first, client).status, "Inconclusive");
    assert.equal(reportFor(first, client).dispatches, 1);
    for (const name of hostPackages) {
      const line = reportFor(first, operationFor(candidate, name));
      assert.equal(line.status, "Unattempted");
      assert.equal(line.dispatches, 0);
      assert.equal(line.observations, 0);
    }
    assert.equal(ports.sent(), 1);
    assert.deepEqual(ports.dispatched, [clientPackage]);
    assert.deepEqual(ports.reads, [clientPackage]);
    const failure = ports.events.find(
      (event) =>
        event.body._tag === "ObservationRecorded" && event.body.evidenceKind === "DispatchError",
    );
    assert.ok(failure?.body._tag === "ObservationRecorded");
    assert.equal(failure.body.operationId, client.operationId);
    assert.equal(failure.body.evidenceVersion, "npm-native-failure/1");
    assert.equal(failure.body.status, "Inconclusive");
    assert.ok(typeof failure.body.evidence === "object" && failure.body.evidence !== null);
    assert.equal(Reflect.get(failure.body.evidence, "status"), 503);
    const resumed = await run(candidate, ports, true);
    assert.equal(reportFor(resumed, client).dispatches, 1);
    assert.equal(reportFor(resumed, client).status, "Inconclusive");
    for (const name of hostPackages) {
      assert.equal(reportFor(resumed, operationFor(candidate, name)).status, "Unattempted");
      assert.equal(reportFor(resumed, operationFor(candidate, name)).dispatches, 0);
    }
    assert.equal(ports.sent(), 1);
    assert.equal(dispatchEvents(ports), 1);
    assert.deepEqual(ports.reads, [clientPackage, clientPackage]);
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
    // Registry confirmation of the client alone satisfies it without any resend.
    ports.registry("matching", [clientPackage]);
    const observed = await observe(candidate, ports);
    assert.equal(reportFor(observed, client).status, "Satisfied");
    assert.equal(reportFor(observed, client).receipts, 0);
    for (const name of hostPackages)
      assert.equal(reportFor(observed, operationFor(candidate, name)).status, "Unattempted");
    assert.equal(ports.sent(), 1);
    assert.equal(visible(candidate, ports), "Unconfirmed");
    // Only then does an authorized run dispatch the host publications, once each,
    // while the client's ambiguous dispatch is never repeated.
    ports.respond(201);
    const continued = await run(candidate, ports, true);
    assert.equal(ports.sent(), 3);
    assert.equal(dispatchEvents(ports), 3);
    assert.deepEqual(ports.dispatched.slice(1).sort(), [...hostPackages].sort());
    assert.equal(reportFor(continued, client).dispatches, 1);
    assert.equal(reportFor(continued, client).status, "Satisfied");
    for (const name of hostPackages) {
      const line = reportFor(continued, operationFor(candidate, name));
      assert.equal(line.status, "Satisfied");
      assert.equal(line.dispatches, 1);
      assert.equal(line.receipts, 1);
    }
    assert.equal(visible(candidate, ports), "Unconfirmed");
    ports.registry("matching");
    await observe(candidate, ports);
    assert.equal(visible(candidate, ports), "Satisfied");
    assert.equal(ports.sent(), 3);
  }));

test("real npm receipts satisfy the client and then both hosts in one run while visibility waits for matching registry observations of every package", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const ports = offlineHost(candidate, 201);
    const accepted = await run(candidate, ports, true);
    for (const name of packageNames) {
      const line = reportFor(accepted, operationFor(candidate, name));
      assert.equal(line.status, "Satisfied");
      assert.equal(line.receipts, 1);
      assert.equal(line.dispatches, 1);
      assert.equal(line.observations, 1);
    }
    assert.equal(ports.sent(), 3);
    assert.equal(dispatchEvents(ports), 3);
    assert.equal(ports.dispatched[0], clientPackage);
    assert.deepEqual(ports.dispatched.slice(1).sort(), [...hostPackages].sort());
    assert.deepEqual(ports.reads, ports.dispatched);
    assert.equal(visible(candidate, ports), "Unconfirmed");
    await observe(candidate, ports);
    assert.equal(visible(candidate, ports), "Unconfirmed");
    ports.registry("matching", [clientPackage, browserPackage]);
    await observe(candidate, ports);
    assert.equal(visible(candidate, ports), "Unconfirmed");
    ports.registry("matching");
    const satisfied = await observe(candidate, ports);
    assert.ok(satisfied.operations.every((line) => line.status === "Satisfied"));
    assert.equal(visible(candidate, ports), "Satisfied");
    ports.registry("conflict", [nativePackage]);
    const conflict = await observe(candidate, ports);
    assert.equal(reportFor(conflict, operationFor(candidate, nativePackage)).status, "Conflict");
    assert.equal(reportFor(conflict, operationFor(candidate, clientPackage)).status, "Satisfied");
    assert.equal(reportFor(conflict, operationFor(candidate, browserPackage)).status, "Satisfied");
    assert.equal(visible(candidate, ports), "Conflict");
    const again = await run(candidate, ports, true);
    assert.equal(ports.sent(), 3);
    assert.equal(dispatchEvents(ports), 3);
    assert.ok(again.operations.every((line) => line.dispatches === 1));
  }));
