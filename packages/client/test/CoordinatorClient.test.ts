/** Reactor's HTTP API on the simulated coordinator: what counts as proof, and who gets the token. */
import { assert, describe, it, layer } from "@effect/vitest";
import { fileURLToPath } from "node:url";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import {
  Clock,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Inspectable,
  Layer,
  Predicate,
  Redacted,
  Result,
  Schema,
  Stream,
} from "effect";
import type { Scope } from "effect";
import * as H3 from "../src/H3.js";
import { CoordinatorClient, ReactorTest } from "../src/index.js";
import { Http, ReactorError } from "../src/ReactorError.js";
import { connect, environment, tokens } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2.4 });

/** The simulated coordinator alone, with the client's CoordinatorClient over it. */
const coordinator = (faults: ReadonlyArray<ReactorTest.Fault> = []) =>
  CoordinatorClient.layer().pipe(
    Layer.provideMerge(ReactorTest.layerCoordinator({ timing, faults })),
  );

/** The requests the simulated coordinator and its storage served, as path and bearer. */
const requests = Effect.map(
  ReactorTest.ReactorTest.pipe(Effect.flatMap((test) => test.log)),
  (log) =>
    log.flatMap((entry) =>
      entry.kind === "request"
        ? [[new URL(entry.name.slice(entry.name.indexOf(" ") + 1)).pathname, entry.bearer]]
        : [],
    ),
);

/** One test on a simulated coordinator of its own, so no session or quota carries over. */
const alone = <E>(
  name: string,
  body: () => Effect.Effect<void, E, Layer.Success<ReturnType<typeof coordinator>> | Scope.Scope>,
  faults: ReadonlyArray<ReactorTest.Fault> = [],
) => layer(coordinator(faults))(name, (it) => it.effect(name, body));

/** Tokens from the simulated key, and a session one of them created, with no connection. */
const created = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const service = yield* CoordinatorClient.CoordinatorClient;
  const minted = service.tokens({
    apiKey: test.apiKey,
    modelName: H3.modelName,
    maxSessionDuration: "5 minutes",
  });
  const grant = yield* minted.create;
  const signaling = service.signaling(Effect.succeed(grant.jwt));
  const { sessionId } = yield* signaling.create({ name: H3.modelName });
  return { id: sessionId, signaling, tokens: minted };
});

describe("playlists", () => {
  const playlist = [
    "#EXTM3U",
    '#EXT-X-MAP:URI="init.mp4"',
    "#EXTINF:1.0,",
    "seg-0.m4s",
    "#EXTINF:1.0,",
    "https://cdn.fixture/clips/seg-1.m4s",
    "#EXT-X-ENDLIST",
  ].join("\n");
  const base = "https://api.fixture/clips/c.m3u8";
  const refusal = (text: string, maxSegments?: number) => {
    const parsed = CoordinatorClient.parsePlaylist({ text, baseUrl: base, maxSegments });
    return Result.isFailure(parsed) ? parsed.failure.reason._tag : "parsed";
  };

  it("resolves an fMP4 playlist's init and media segments against its URL", () => {
    assert.deepStrictEqual(
      CoordinatorClient.parsePlaylist({ text: playlist, baseUrl: base }),
      Result.succeed([
        { kind: "init", url: "https://api.fixture/clips/init.mp4" },
        { kind: "media", url: "https://api.fixture/clips/seg-0.m4s" },
        { kind: "media", url: "https://cdn.fixture/clips/seg-1.m4s" },
      ]),
    );
  });

  it("refuses what byte concatenation cannot assemble", () => {
    assert.strictEqual(refusal("not a playlist"), "Protocol");
    assert.strictEqual(
      refusal("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv.m3u8"),
      "UnsupportedCapability",
    );
    assert.strictEqual(
      refusal('#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k"\nseg.ts'),
      "UnsupportedCapability",
    );
    assert.strictEqual(refusal("#EXTM3U\nseg-0.m4s"), "Protocol");
    assert.strictEqual(refusal("#EXTM3U\nhttps://user:pass@cdn.fixture/seg.ts"), "Protocol");
    assert.strictEqual(refusal("#EXTM3U\na.ts\nb.ts", 1), "Overflow");
  });
});

/** A CoordinatorClient whose downloads carry a token bound to `sessionId`, minted now. */
const downloader = (sessionId: string) =>
  Effect.gen(function* () {
    const grant = yield* (yield* tokens).bind(sessionId);
    return yield* CoordinatorClient.make({ credential: Effect.succeed(grant.jwt) });
  });

const recorder = (faults: ReadonlyArray<ReactorTest.Fault>) =>
  environment({ timing, recorder: true, faults });

layer(recorder([{ _tag: "LateRecording", nth: 1, by: Duration.seconds(1) }]))(
  "a recording",
  (it) => {
    it.effect(
      "is awaited, then joined from its segments; only the coordinator gets the token",
      () =>
        Effect.gen(function* () {
          yield* Effect.forkScoped(ReactorTest.flow());
          const session = yield* connect;
          yield* Effect.sleep("3 seconds");
          const clip = yield* session.requestRecordingClip(3);
          const service = yield* downloader(session.id);
          const before = (yield* requests).length;
          const started = yield* Clock.currentTimeMillis;
          const downloaded = yield* service.downloadClip(clip);
          // The pending playlist named a Retry-After of one second, sooner than the two a
          // playlist that names none waits.
          assert.isBelow((yield* Clock.currentTimeMillis) - started, 2_000);
          // The init segment, then two of two seconds each, in order.
          assert.deepStrictEqual(
            downloaded.segments.map((segment) => segment.kind),
            ["init", "media", "media"],
          );
          assert.deepStrictEqual(
            [
              downloaded.bytes.byteLength,
              downloaded.bytes[0],
              downloaded.bytes[8],
              downloaded.bytes.at(-1),
            ],
            [8 + 2 * 1_024, 255, 0, 1],
          );
          // A pending playlist reads the session before asking again; the segments' origin gets no token.
          const playlist = new URL(clip.playlistUrl).pathname;
          assert.deepStrictEqual((yield* requests).slice(before), [
            [playlist, "token"],
            [`/sessions/${session.id}`, "token"],
            [playlist, "token"],
            ...downloaded.segments.map((segment) => [new URL(segment.url).pathname, undefined]),
          ]);
          assert.isTrue(
            downloaded.segments.every((segment) => !segment.url.startsWith(service.apiUrl)),
          );
        }),
    );
  },
);

// Reactor's docs: a recording is kept 24 h and plays after its session ends; it may finish then.
layer(
  recorder([
    { _tag: "LateRecording", nth: 1, by: Duration.seconds(5) },
    { _tag: "LateRecording", nth: 2, by: Duration.minutes(1) },
  ]),
)("a recording of an ended session", (it) => {
  it.effect("is awaited until its predicted ready time, and not after", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const session = yield* connect;
      yield* Effect.sleep("3 seconds");
      const finishing = yield* session.recording;
      const overdue = yield* session.recording;
      const service = yield* downloader(session.id);
      yield* session.close;
      // The init segment and two of its three seconds.
      assert.strictEqual((yield* service.downloadClip(finishing)).segments.length, 3);
      const late = yield* Effect.flip(service.downloadClip(overdue));
      assert.strictEqual(late.reason._tag, "TerminalSession");
    }),
  );
});

// The session does not reconnect itself, so it stays without a connection.
layer(
  environment({
    timing,
    recorder: true,
    faults: [
      { _tag: "Disconnect", nth: 1, after: Duration.seconds(1) },
      { _tag: "LateRecording", nth: 1, by: Duration.seconds(20) },
    ],
    reconnect: false,
  }),
)("a recording of a session without a connection", (it) => {
  // An INACTIVE session has only lost its connection, so its recording is still awaited.
  it.effect("is awaited past its predicted ready time", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const session = yield* connect;
      const clip = yield* session.recording;
      const service = yield* downloader(session.id);
      const started = yield* Clock.currentTimeMillis;
      // The init segment and one of media: the recording was asked for as the session connected.
      assert.strictEqual((yield* service.downloadClip(clip)).segments.length, 2);
      assert.isAtLeast((yield* Clock.currentTimeMillis) - started, 20_000);
      const test = yield* ReactorTest.ReactorTest;
      assert.deepStrictEqual(
        (yield* test.sessions).map((info) => info.state),
        ["INACTIVE"],
      );
    }),
  );
});

layer(recorder([{ _tag: "LateRecording", nth: 2, by: Duration.minutes(1) }]))(
  "a download's bounds",
  (it) => {
    it.effect("bound the joined bytes and the whole download", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
        const session = yield* connect;
        const service = yield* downloader(session.id);
        const bounded = yield* Effect.flip(
          service.downloadClip(yield* session.recording, { maxTotalBytes: 1_000 }),
        );
        assert.strictEqual(bounded.reason._tag, "Overflow");
        const late = yield* Effect.flip(
          service.downloadClip(yield* session.recording, { downloadTimeout: "5 seconds" }),
        );
        assert.strictEqual(late.reason._tag, "Timeout");
        // A deadline `Duration` cannot parse, a NaN it reads as zero, and a negative.
        const clip = yield* session.recording;
        const huge: number = 10 ** 999;
        const bad: ReadonlyArray<Duration.Input> = [`${huge} seconds`, Number.NaN, -5];
        for (const input of bad) {
          const refused = yield* Effect.flip(
            service.downloadClip(clip, { downloadTimeout: input }),
          );
          assert.deepStrictEqual(
            [refused.reason._tag, refused.context.outcome],
            ["InvalidInput", "not-submitted"],
            Inspectable.toStringUnknown(input),
          );
        }
        // A bound is a whole number of bytes or segments.
        const fractional = yield* Effect.flip(
          service.downloadClip(clip, { maxTotalBytes: 1_000.5 }),
        );
        assert.deepStrictEqual(
          [fractional.reason._tag, fractional.context.outcome],
          ["InvalidInput", "not-submitted"],
        );
      }),
    );
  },
);

alone("a request whose token comes too late, or not at all, was never sent", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const service = yield* CoordinatorClient.CoordinatorClient;
    const { id } = yield* created;
    const before = (yield* requests).length;
    const slow = service.signaling(
      Effect.sleep("20 seconds").pipe(Effect.as(Redacted.make("late"))),
    );
    const reading = yield* Effect.forkChild(Effect.flip(slow.read(id)));
    const ending = yield* Effect.forkChild(slow.terminate(id));
    const late = yield* Fiber.join(reading);
    const unminted = yield* Effect.flip(
      service.signaling(Effect.fail(ReactorError.fromCode("Http", "token: HTTP 500"))).read(id),
    );
    assert.deepStrictEqual(
      [late.reason._tag, late.context.outcome, unminted.context.outcome],
      ["Timeout", "not-submitted", "not-submitted"],
    );
    assert.isFalse((yield* Fiber.join(ending)).attempted);
    assert.strictEqual((yield* requests).length, before);
    assert.deepStrictEqual(
      (yield* test.sessions).map((info) => [info.state, info.deletes]),
      [["ACTIVE", 0]],
    );
  }),
);

alone("termination is confirmed by the independent read, not by the DELETE response", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const { id, signaling } = yield* created;
    const ended = yield* signaling.terminate(id);
    assert.deepStrictEqual(
      [ended.confirmed, ended.evidence, ended.deleteStatus, ended.state],
      [true, "terminal", 200, "CLOSED"],
    );
    // Paid run tokens 7bc779d4: ending a session the account does not have answers 404.
    const server = yield* CoordinatorClient.make({ apiKey: test.apiKey });
    const absent = yield* server.terminate("sess_unknown");
    assert.deepStrictEqual(
      [absent.confirmed, absent.evidence, absent.deleteStatus],
      [true, "absent", 404],
    );
    yield* test.inject({ _tag: "IgnoreDelete" });
    const other = yield* created;
    const running = yield* other.signaling.terminate(other.id);
    assert.deepStrictEqual([running.confirmed, running.state], [false, "ACTIVE"]);
  }),
);

// A coordinator can lose a running session for one read, so a 404 alone does not prove the end.
alone(
  "a session missing from one read, after a DELETE that did not take, is not confirmed ended",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const { id, signaling } = yield* created;
      yield* test.inject({ _tag: "IgnoreDelete" });
      yield* test.inject({ _tag: "MissingSession", nth: 1 });
      const ended = yield* signaling.terminate(id);
      assert.deepStrictEqual([ended.confirmed, ended.state], [false, "ACTIVE"]);
    }),
);

// A server that holds only the key can ask whether a session it recorded still runs, and not end it.
alone("a CoordinatorClient with only the key inspects a session with the key", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const { id } = yield* created;
    const server = yield* CoordinatorClient.make({ apiKey: test.apiKey });
    yield* server.inspect(id);
    assert.deepStrictEqual((yield* requests).at(-1), [`/sessions/${id}`, "key"]);
  }),
);

alone("termination stays unconfirmed when the DELETE was refused", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const first = yield* created;
    const second = yield* created;
    // A token acts only on the sessions it created or was bound to.
    const refused = yield* first.signaling.terminate(second.id);
    assert.deepStrictEqual(
      [refused.confirmed, refused.deleteStatus, refused.error?.reason],
      [false, 403, "Http"],
    );
  }),
);

// The session does not reconnect itself, so it stays without a connection.
layer(
  environment({
    timing,
    faults: [{ _tag: "Disconnect", nth: 1, after: Duration.seconds(1) }, { _tag: "IgnoreDelete" }],
    reconnect: false,
  }),
)("termination of a session without a connection", (it) => {
  // Paid run tokens 83d17eb7: INACTIVE is a session without a connection, still running.
  it.effect("is not confirmed while the session reads INACTIVE", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      yield* session.changes.pipe(
        Stream.filter((snapshot) => snapshot.status === "disconnected"),
        Stream.runHead,
      );
      const server = yield* CoordinatorClient.make({ apiKey: test.apiKey });
      const dropped = yield* server.terminate(session.id);
      assert.deepStrictEqual([dropped.confirmed, dropped.state], [false, "INACTIVE"]);
    }),
  );
});

layer(coordinator())("tokens", (it) => {
  const request = { modelName: H3.modelName, maxSessionDuration: "60 seconds" } as const;
  const mint = (options: Omit<CoordinatorClient.TokenOptions, "modelName">) =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const service = yield* CoordinatorClient.CoordinatorClient;
      return yield* service.mintToken({ ...request, apiKey: test.apiKey, ...options });
    });

  it.effect("returns the grant Reactor echoes, and never sends the API key as a bearer", () =>
    Effect.gen(function* () {
      const grant = yield* mint({ expiresAfter: "10 minutes" });
      assert.deepStrictEqual(grant.granted, {
        models: [H3.modelName],
        maxSessions: 1,
        maxSessionSeconds: 60,
        bound: [],
      });
      assert.strictEqual(grant.maxSessionSeconds, 60);
      assert.strictEqual(grant.expiresAt - (yield* Clock.currentTimeMillis) / 1000, 600);
      assert.deepStrictEqual(yield* requests, [["/tokens", undefined]]);
    }),
  );

  // Reactor documents the reply's echo, not the token's claims; a session outlives its token.
  it.effect("takes a token shorter than its session, and one whose reply echoes nothing", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const brief = yield* mint({ expiresAfter: "30 seconds" });
      assert.strictEqual(brief.expiresAt - (yield* Clock.currentTimeMillis) / 1000, 30);
      yield* test.inject({ _tag: "OverGrant", nth: 1, grant: "silent" });
      assert.isUndefined((yield* mint({})).granted);
    }),
  );

  it.effect("refuses a grant wider than asked, and a token already expired", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const { id } = yield* created;
      const refusal = (grant: "longer" | "uncapped" | "bound" | "expired") =>
        Effect.gen(function* () {
          yield* test.inject({ _tag: "OverGrant", nth: 1, grant });
          const error = yield* Effect.flip(mint({ bind: [id], maxSessions: 2 }));
          return `${error.reason._tag}/${error.context.outcome ?? ""}`;
        });
      assert.deepStrictEqual(
        [
          yield* refusal("longer"),
          yield* refusal("uncapped"),
          yield* refusal("bound"),
          yield* refusal("expired"),
        ],
        Array.from({ length: 4 }, () => "Protocol/replied"),
      );
    }),
  );

  it.effect("mints an uncapped session only when asked, and a bound token that creates none", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const missing = yield* Effect.flip(mint({ maxSessionDuration: undefined }));
      assert.deepStrictEqual(
        [missing.reason._tag, missing.context.outcome],
        ["InvalidInput", "not-submitted"],
      );
      const uncapped = yield* mint({ maxSessionDuration: "unlimited" });
      assert.deepStrictEqual(
        [uncapped.maxSessionSeconds, uncapped.granted?.maxSessionSeconds],
        [undefined, undefined],
      );
      const { id, tokens: minted } = yield* created;
      const bound = yield* minted.bind(id);
      assert.deepStrictEqual([bound.granted?.bound, bound.granted?.maxSessions], [[id], 1]);
      const service = yield* CoordinatorClient.CoordinatorClient;
      const refused = yield* Effect.flip(
        service.signaling(Effect.succeed(bound.jwt)).create({ name: H3.modelName }),
      );
      assert.strictEqual(refused.reason._tag === "Http" ? refused.reason.status : undefined, 403);
    }),
  );
});

// Reactor's docs: 10 sessions a minute, three back to back; 429 says when to retry.
alone("a refusal carries its status and delay, and keeps the body out of its message", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    yield* Effect.replicateEffect(created, 3);
    const error = yield* Effect.flip(created);
    assert.deepStrictEqual(
      [error.reason._tag, error.reason._tag === "Http" ? error.reason.status : undefined],
      ["Http", 429],
    );
    assert.isTrue(error.isRetryable);
    assert.isDefined(error.retryAfter);
    assert.notInclude(error.message, "too many sessions");
    const json = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(error);
    assert.notInclude(json, "too many sessions");
    assert.include(
      error.reason._tag === "Http" ? Redacted.value(error.reason.body ?? Redacted.make("")) : "",
      "too many sessions",
    );
  }),
);

// A create answered 2xx without naming a session may still have made one: what the reply said is
// kept, redacted as provider text is, for whoever must find that session.
alone(
  "a create reply that names no session keeps its body redacted, and out of its message",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const error = yield* Effect.flip(created);
      assert.deepStrictEqual([error.reason._tag, error.context.outcome], ["Protocol", "unknown"]);
      assert.notInclude(error.message, "PENDING");
      const json = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(error);
      assert.notInclude(json, "PENDING");
      const detail =
        error.context.detail === undefined ? undefined : Redacted.value(error.context.detail);
      assert.deepStrictEqual(Predicate.hasProperty(detail, "body") ? detail.body : undefined, {
        state: "PENDING",
      });
    }),
  [{ _tag: "UnnamedAllocation" }],
);

// A create nobody answered may have allocated, for all its caller can tell: its deadline ends it
// with the outcome unknown, so nothing retries it as a create that was never sent.
alone(
  "a create never answered ends at its deadline with its outcome unknown",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const sentAt = yield* Clock.currentTimeMillis;
      const error = yield* Effect.flip(created);
      assert.deepStrictEqual([error.reason._tag, error.context.outcome], ["Timeout", "unknown"]);
      assert.strictEqual((yield* Clock.currentTimeMillis) - sentAt, 15_000);
      assert.lengthOf(yield* test.sessions, 0);
    }),
  [{ _tag: "StallAllocation" }],
);

// A read answered 404 once need not mean the session ended: the next read may find it running.
alone(
  "a session the coordinator loses for one read is found again by the next",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const { id } = yield* created;
      const server = yield* CoordinatorClient.make({ apiKey: test.apiKey });
      const lost = yield* Effect.flip(server.inspect(id));
      assert.deepStrictEqual(
        [lost.reason._tag, lost.reason._tag === "Http" ? lost.reason.status : undefined],
        ["Http", 404],
      );
      assert.notStrictEqual((yield* server.inspect(id)).state, "CLOSED");
    }),
  [{ _tag: "MissingSession", nth: 1 }],
);

// The billing page shows rates per minute; the live pricing endpoint states H3's per second.
it.effect("reads a model's rate in the unit its pricing states it", () =>
  Effect.gen(function* () {
    const pricing = (rate: object) => ({
      settings: { credits_per_dollar: 10_000, currency_code: "USD" },
      models: [{ name: "h3-reference-to-video-turbo-realtime", rate }],
    });
    const model = "reactor/h3-reference-to-video-turbo-realtime";
    const perSecond = yield* CoordinatorClient.modelRate(
      pricing({ amount_per_sec: 125, unit: "credits", denomination: "second" }),
      model,
    );
    const perMinute = yield* CoordinatorClient.modelRate(
      pricing({ amount_per_min: 7_500, unit: "credits", denomination: "minute" }),
      model,
    );
    assert.deepStrictEqual(
      [perSecond, perMinute],
      [
        { creditsPerDollar: 10_000, creditsPerSecond: 125, per: "second" },
        { creditsPerDollar: 10_000, creditsPerSecond: 125, per: "minute" },
      ],
    );
  }),
);

// Reactor's JS SDK types class a 5xx SERVER_ERROR as recoverable, and 401/403 and 409 as not.
it("a server error is retryable; a refusal of authority or a conflict is not", () => {
  const retryable = (status: number) =>
    ReactorError.make({
      reason: Http.make({ message: `HTTP ${String(status)}`, status }),
      context: { outcome: "replied" },
    }).isRetryable;
  assert.deepStrictEqual(
    [retryable(502), retryable(504), retryable(403), retryable(409)],
    [true, true, false, false],
  );
});

// Reactor's FAQ asks for the SDK version in bug reports; its logs read it from client_info.
layer(Layer.merge(coordinator(), NodeFileSystem.layer))("the SDK version", (it) => {
  it.effect("names this package's version to Reactor", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const fs = yield* FileSystem.FileSystem;
      const manifest = yield* fs.readFileString(
        fileURLToPath(new URL("../package.json", import.meta.url)),
      );
      const { version } = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
      )(manifest);
      yield* created;
      const test = yield* ReactorTest.ReactorTest;
      assert.deepStrictEqual(
        (yield* test.sessions).map((info) => info.client?.sdkVersion),
        [version],
      );
    }),
  );
});
