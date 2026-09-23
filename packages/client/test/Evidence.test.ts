/** Evidence codecs: reports persist as JSON without provider text, and decode back. */
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Redacted, Result, Schema } from "effect";
import {
  ClipEnded,
  CommandFailure,
  CommandFailureFromJson,
  Failure,
  Http,
  IceFailed,
  Native,
  PolicyFailure,
  ReactorError,
  ReactorErrorFromJson,
  Remote,
  TransportFailed,
} from "../src/errors.js";
import type { ReactorErrorReason } from "../src/errors.js";
import { CloseReport, Termination } from "../src/index.js";
import * as Orchestration from "../src/orchestration/index.js";

const secret = "provider text that must not persist";
const reasons: readonly ReactorErrorReason[] = [
  new Failure({ _tag: "Timeout", message: "timed out" }),
  new Http({
    message: "coordinator failed",
    status: 429,
    retryAfter: Duration.seconds(2),
    body: secret,
  }),
  new Remote({ _tag: "RecorderDisabled", message: "clip failed", remoteCode: "E1", body: secret }),
  new Native({ message: "native failed", status: 3, backendMessage: Redacted.make(secret) }),
  new IceFailed({ message: "ice failed", pairs: 2, candidateTypes: ["host", "relay"] }),
  new TransportFailed({ message: "dtls failed", pairs: 1 }),
  new ClipEnded({
    message: "clip ended by clip_popped",
    clipId: "c-1",
    lifecycle: "clip_popped",
    transportGeneration: 4n,
  }),
];
const failure = (reason: ReactorErrorReason) =>
  new ReactorError({
    reason,
    context: { operation: "close", sessionId: "s-1", generation: 3n, detail: { secret } },
  });
const persisted = <T, E>(schema: Schema.Codec<T, E>, value: T) =>
  JSON.parse(
    JSON.stringify(Schema.encodeUnknownSync(Schema.toCodecJson(schema))(value)),
  ) as unknown;
const restored = <T, E>(schema: Schema.Codec<T, E>, json: unknown): T =>
  Schema.decodeUnknownSync(Schema.toCodecJson(schema))(json);

const termination: Termination = {
  attempted: true,
  responseReceived: true,
  confirmed: false,
  evidence: null,
  deleteStatus: 503,
  state: null,
  error: failure(reasons[1]!),
};
const report: CloseReport = {
  localClosed: true,
  allocation: "known",
  ownership: "owned",
  sessionId: "s-1",
  remote: termination,
  unpublishSubmitted: ["track-a"],
  unresolvedPublications: [],
  localErrors: reasons.map(failure),
};

describe("evidence codecs", () => {
  it("encodes every reason as the failure's diagnostic JSON, and decodes it back", () => {
    for (const reason of reasons) {
      const error = failure(reason);
      const json = Schema.encodeSync(ReactorErrorFromJson)(error);
      expect(json).toEqual(error.toJSON());
      expect(JSON.stringify(json)).not.toContain(secret);
      const decoded = Schema.decodeUnknownSync(ReactorErrorFromJson)(json);
      expect(ReactorError.is(decoded)).toBe(true);
      expect(decoded.reason._tag).toBe(reason._tag);
      expect(decoded.context.generation).toBe(3n);
      expect(decoded.toJSON()).toEqual(error.toJSON());
    }
  });

  it("round-trips a close report through JSON, keeping its failures as failures", () => {
    const json = persisted(CloseReport, report);
    expect(JSON.stringify(json)).not.toContain(secret);
    const back = restored(CloseReport, json);
    expect({ ...back, remote: { ...back.remote, error: undefined }, localErrors: [] }).toEqual({
      ...report,
      remote: { ...termination, error: undefined },
      localErrors: [],
    });
    expect(back.localErrors.map((error) => error.reason._tag)).toEqual(
      reasons.map((reason) => reason._tag),
    );
    expect(back.remote.error?.retryAfter).toEqual(Duration.seconds(2));
  });

  it.effect("keeps each policy cleanup result of an orchestration report", () =>
    Effect.gen(function* () {
      const refused = PolicyFailure.refuse("Busy", "session busy", "setAutoplay");
      const command = CommandFailure.from(ReactorError.fromCode("Timeout", "no reply"), {
        operation: "pause",
        outcome: "unknown",
        requestId: "r-1",
        generation: 2n,
      });
      const cleanup: Orchestration.CleanupReport = {
        sessions: [
          {
            lease: report,
            policy: [
              { operation: "stop", result: Result.succeed(undefined) },
              { operation: "setAutoplay", result: Result.fail(refused) },
              { operation: "pause", result: Result.fail(command) },
            ],
          },
        ],
      };
      const back = restored(
        Orchestration.CleanupReport,
        persisted(Orchestration.CleanupReport, cleanup),
      );
      const [stop, autoplay, pause] = back.sessions[0]!.policy;
      expect(Result.isSuccess(stop!.result)).toBe(true);
      const autoplayFailure = yield* Effect.flip(Effect.fromResult(autoplay!.result));
      expect(PolicyFailure.is(autoplayFailure) && autoplayFailure.reason._tag).toBe("Busy");
      const pauseFailure = yield* Effect.flip(Effect.fromResult(pause!.result));
      expect(CommandFailure.is(pauseFailure) && pauseFailure.context).toEqual(command.context);
    }),
  );

  it("refuses JSON that is not a failure's diagnostic form", () => {
    const decode = Schema.decodeUnknownResult(CommandFailureFromJson);
    // A replied command names its request: the class refuses it without one.
    const noRequest = {
      _tag: "CommandFailure",
      message: "no reply",
      reason: { _tag: "Timeout", message: "no reply" },
      context: { operation: "pause", outcome: "replied", generation: "2" },
    };
    expect(Result.isFailure(decode(noRequest))).toBe(true);
    const badGeneration = {
      ...noRequest,
      context: { ...noRequest.context, requestId: "r-1", generation: "two" },
    };
    expect(Result.isFailure(decode(badGeneration))).toBe(true);
    expect(
      Result.isSuccess(
        decode({ ...badGeneration, context: { ...badGeneration.context, generation: "2" } }),
      ),
    ).toBe(true);
  });

  it("encodes an allocation record, which has no token field", () => {
    const allocation: Orchestration.Allocation = {
      sessionId: "s-1",
      ownership: "owned",
      model: "h3",
      expiresAt: 4_102_444_800,
    };
    expect(persisted(Orchestration.Allocation, allocation)).toEqual(allocation);
    expect(
      Result.isFailure(
        Schema.decodeResult(Orchestration.Allocation)({
          ...allocation,
          expiresAt: Infinity,
        }),
      ),
    ).toBe(true);
  });
});
