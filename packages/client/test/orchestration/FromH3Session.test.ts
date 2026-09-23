/** The public session-bound constructor over a real session and host peer. */
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { PeerFactory } from "../../src/PeerFactory.js";
import { fromH3Session } from "../../src/orchestration/index.js";
import * as Client from "../../src/session/index.js";
import { MockPeer, withFixture } from "../fixtures.js";
import type { HttpFixture } from "../fixtures.js";
import { assert, equal, run, test } from "../harness.js";

/** A host without decoded media: MockPeer implements no `rawMedia`. */
const trackOnlyHost = (fixture: HttpFixture, peers: MockPeer[]) => ({
  make: () => {
    const peer = new MockPeer(fixture);
    peers.push(peer);
    return peer;
  },
});

test("fromH3Session fails before any provider command on a host without decoded media", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const peers: MockPeer[] = [];
    const outcome = await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.createConnected({ model: "fixture/h3" });
          const peer = peers[0];
          assert(peer !== undefined);
          const sentBefore = peer.sent.length;
          const result = yield* Effect.result(fromH3Session(session));
          return { result, sent: peer.sent.length - sentBefore };
        }),
      ).pipe(Effect.provideService(PeerFactory, trackOnlyHost(fixture, peers))),
      { signal },
    );
    assert(Result.isFailure(outcome.result));
    equal(outcome.result.failure.code, "UnsupportedCapability");
    equal(outcome.sent, 0);
  }));

test("fromH3Session fails before any provider command when the session is not connected", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const peers: MockPeer[] = [];
    const result = await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.create({ model: "fixture/h3" });
          return yield* Effect.result(fromH3Session(session));
        }),
      ).pipe(Effect.provideService(PeerFactory, trackOnlyHost(fixture, peers))),
      { signal },
    );
    assert(Result.isFailure(result));
    equal(peers.length, 0);
  }));

test("fromH3Session derives its media from the session and takes no media option", () => {
  const bind = (session: Client.Session) =>
    // @ts-expect-error the media is derived from the session, never supplied
    fromH3Session(session, { media: Effect.never });
  equal(typeof bind, "function");
});
