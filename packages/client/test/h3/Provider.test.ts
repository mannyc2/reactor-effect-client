import { describe, expect, test } from "bun:test";
import { Cause, Crypto, Effect, Exit, Fiber, Result, Scope, Stream } from "effect";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as H3 from "../../src/h3/index.js";
import { ReactorError } from "../../src/errors.js";
import { CommandFailure } from "../../src/session/commands.js";
import type { JsonObject } from "../../src/json.js";
import { pngBytes } from "../../src/testing/Png.js";
import { fixture, fixtureClip, gate } from "./ProviderSession.js";
import type { Fixture, Script } from "./ProviderSession.js";
import { at, providerSchema } from "./ProviderSchema.js";

const options: H3.Options = { commandTimeoutMs: 80, setupTimeoutMs: 500, reconcileWindowMs: 30 };
const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(NodeCrypto.layer))));
const capture = (provider: H3.Provider) =>
  Effect.gen(function* () {
    const observation = yield* provider.observe({ capacity: 1024 });
    const all: H3.ProviderEvent[] = [];
    yield* observation.events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          all.push(event);
        }),
      ),
      Effect.forkScoped,
    );
    return all;
  });
const setup = (script: Script = {}, bounds: H3.Options = {}) =>
  Effect.gen(function* () {
    const fake = yield* fixture(script);
    const provider = yield* H3.make(fake.session, { ...options, ...bounds });
    return { fake, provider, events: yield* capture(provider) };
  });
const waitFor = (predicate: () => Effect.Effect<boolean>) =>
  Effect.gen(function* () {
    while (!(yield* predicate())) yield* Effect.sleep(1);
  }).pipe(Effect.timeout(1000));
const observed = (provider: H3.Provider, sequence: bigint) =>
  waitFor(() => provider.current.pipe(Effect.map((snapshot) => snapshot.revision >= sequence)));
const ready = (snapshot: H3.ProviderSnapshot) => {
  expect(snapshot._tag).toBe("Ready");
  if (snapshot._tag !== "Ready") throw new Error("Expected complete provider state and queue");
  return snapshot;
};
const request = (fields: Partial<H3.Request> = {}): H3.Request => ({
  prompt: "A blue paper boat on clear water.",
  seconds: 7,
  ...fields,
});
const bytesReference = (): H3.Reference => ({ _tag: "Bytes", bytes: pngBytes(64, 48) });

describe("canonical H3 provider acquisition", () => {
  test("uses only schema, state and queue reads and leaves connected Session ownership to its caller", () =>
    run(
      Effect.gen(function* () {
        const fake = yield* fixture();
        yield* Effect.scoped(
          Effect.gen(function* () {
            const provider = yield* H3.make(fake.session, options);
            expect(fake.reads).toEqual(["schema"]);
            expect(fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
            expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
            expect(provider.contract).toMatchObject({
              documentedVersion: "0.5.5",
              subset: "prompt-and-images",
              deployment: {
                title: "H3 hand-authored offline fixture",
                version: "fixture-declaration",
              },
            });
            expect(ready(yield* provider.current).state.autoplay).toBe(false);
            expect(ready(yield* provider.current).state.flush_on_clip_end).toBe(true);
            expect(fake.subscribers()).toBe(1);
          }),
        );
        expect(fake.subscribers()).toBe(0);
        expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
        expect(fake.calls.some((call) => call.command === "reset")).toBe(false);
      }),
    ));

  test("retains every required field and foreign generation, playout, history, and metadata", () =>
    run(
      Effect.gen(function* () {
        const generation = fixtureClip(),
          playout = fixtureClip({ ready: true }),
          history = fixtureClip({ ready: true });
        const { provider, fake } = yield* setup({
          initialGeneration: [generation],
          initialPlayout: [playout],
          initialHistory: [history],
        });
        const snapshot = ready(yield* provider.current);
        expect<unknown>(snapshot.queue).toEqual(fake.queue());
        expect<unknown>(snapshot.state).toEqual(fake.state());
        expect(snapshot.clips.map((entry) => entry.clip.clip_id)).toEqual([
          generation.clip_id,
          playout.clip_id,
          history.clip_id,
        ]);
        expect(snapshot.clips.every((entry) => entry.lifecycle === null)).toBe(true);
        expect(yield* provider.acceptances).toEqual([]);
        expect("originalClipRequest" in snapshot.clips[0]!).toBe(false);
        expect("building" in snapshot).toBe(false);
        expect(Object.isFrozen(snapshot.queue.generation[0])).toBe(true);
      }),
    ));

  test("retained history may also describe a clip waiting in playout", () =>
    run(
      Effect.gen(function* () {
        const clip = fixtureClip({ ready: true });
        const { provider } = yield* setup({ initialPlayout: [clip], initialHistory: [clip] });
        const snapshot = ready(yield* provider.current);
        expect(snapshot.queue.playout[0]).toEqual(clip);
        expect(snapshot.queue.history[0]).toEqual(clip);
        expect(snapshot.clips).toHaveLength(1);
      }),
    ));

  test("failing acquisition immediately releases observation and cannot connect or close the Session", () =>
    run(
      Effect.gen(function* () {
        const fake = yield* fixture({
          schema: {
            openapi: "3.1.0",
            description: "enqueue clip_queued reference_images state_update queue_update",
          },
        });
        const result = yield* Effect.result(H3.make(fake.session, options));
        expect(Result.isFailure(result)).toBe(true);
        expect(fake.calls).toEqual([]);
        expect(fake.subscribers()).toBe(0);
        expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
      }),
    ));

  test("interrupting acquisition joins its local reader and never resets someone else's Session", () =>
    run(
      Effect.gen(function* () {
        const entered = yield* gate();
        const fake = yield* fixture({
          command: { get_state: () => entered.release.pipe(Effect.andThen(Effect.never)) },
        });
        const acquiring = yield* H3.make(fake.session, options).pipe(Effect.forkScoped);
        yield* entered.wait;
        yield* Fiber.interrupt(acquiring);
        expect(fake.subscribers()).toBe(0);
        expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
        expect(fake.calls.map((call) => call.command)).toEqual(["get_state"]);
      }),
    ));
});

describe("H3 structural deployment compatibility", () => {
  const negative: readonly [string, (document: JsonObject) => void][] = [
    [
      "prose names only",
      (doc) => {
        at(doc).paths = {};
        at(doc).description = "enqueue reference_images clip_queued queue_update state_update";
      },
    ],
    [
      "wrong argument type",
      (doc) => {
        at(
          doc,
          "paths",
          "/events/move",
          "post",
          "requestBody",
          "content",
          "application/json",
          "schema",
          "properties",
        ).position = { type: "string" };
      },
    ],
    [
      "integer-only duration input",
      (doc) => {
        at(
          doc,
          "paths",
          "/events/enqueue",
          "post",
          "requestBody",
          "content",
          "application/json",
          "schema",
          "properties",
        ).seconds = { type: "integer" };
      },
    ],
    [
      "prompt-only input excluded",
      (doc) => {
        at(
          doc,
          "paths",
          "/events/enqueue",
          "post",
          "requestBody",
          "content",
          "application/json",
          "schema",
          "properties",
        ).reference_images = {
          type: "array",
          minItems: 1,
          items: { $ref: "#/components/schemas/ReactorUploadReference" },
        };
      },
    ],
    [
      "missing successful response",
      (doc) => {
        at(doc, "paths", "/events/enqueue", "post").responses = {
          "202": { description: "Accepted" },
        };
      },
    ],
    [
      "reply without required position",
      (doc) => {
        at(doc, "components", "schemas", "ClipMoved").required = ["clip", "queue"];
      },
    ],
    [
      "queue fields are optional",
      (doc) => {
        at(doc, "components", "schemas", "QueueUpdate").required = [];
      },
    ],
    [
      "clip fields are partial",
      (doc) => {
        at(doc, "components", "schemas", "ClipInfo").required = ["clip_id"];
      },
    ],
    [
      "nullable playing ID omitted",
      (doc) => {
        at(doc, "components", "schemas", "StateUpdate", "properties").playing_clip_id = {
          type: "string",
        };
      },
    ],
    [
      "required unsupported audio",
      (doc) => {
        at(
          doc,
          "paths",
          "/events/enqueue",
          "post",
          "requestBody",
          "content",
          "application/json",
          "schema",
        ).required = ["reference_audio"];
      },
    ],
    [
      "wrong reply identity",
      (doc) => {
        at(
          doc,
          "paths",
          "/events/set_autoplay",
          "post",
          "responses",
          "200",
          "content",
          "application/json",
        ).schema = { $ref: "#/components/schemas/FlushAccepted" };
      },
    ],
    [
      "external schema reference",
      (doc) => {
        at(doc, "components", "schemas").ClipInfo = {
          $ref: "https://fixture.invalid/never-follow-this",
        };
      },
    ],
    [
      "cyclic schema reference",
      (doc) => {
        at(doc, "components", "schemas").ClipInfo = { $ref: "#/components/schemas/ClipInfo" };
      },
    ],
    [
      "missing play command",
      (doc) => {
        delete at(doc, "paths")["/events/play"];
      },
    ],
    [
      "missing state seed",
      (doc) => {
        delete at(doc, "components", "schemas", "StateUpdate", "properties").seed;
      },
    ],
  ];
  for (const [name, mutate] of negative)
    test(`rejects ${name} before any command`, () =>
      run(
        Effect.gen(function* () {
          const schema = providerSchema();
          mutate(schema);
          const fake = yield* fixture({ schema });
          const result = yield* Effect.result(H3.make(fake.session, options));
          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) expect(result.failure.code).toBe("UnsupportedCapability");
          expect(fake.calls).toEqual([]);
          expect(fake.subscribers()).toBe(0);
        }),
      ));
});

describe("H3 request capture and references", () => {
  test("allows prompt-only input and preserves optional defaults and opaque metadata", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const acceptance = yield* provider.enqueue({
          prompt: "Only a prompt.",
          metadata: "an opaque caller string",
        });
        const call = fake.calls.find((call) => call.command === "enqueue")!;
        expect(call.args.reference_images).toEqual([]);
        expect(call.args.seconds).toBeUndefined();
        expect(call.args.seed).toBeUndefined();
        expect(call.args.position).toBeUndefined();
        expect(call.args.continue_from_clip_id).toBeUndefined();
        expect(call.args.starting_frame).toBeUndefined();
        expect(call.args.ending_frame).toBeUndefined();
        expect(JSON.parse(acceptance.clip.metadata)).toMatchObject({
          caller: "an opaque caller string",
          submission: acceptance.submissionId,
        });
        expect(acceptance.clip.clip_id).toBe(fake.accepted[0]!.clip_id);
        expect(acceptance.clip.seconds).toBe(fake.accepted[0]!.seconds);
        expect(fake.uploaded).toEqual([]);
        expect(ready(yield* provider.current).queue.generation[0]!.clip_id).toBe(
          acceptance.clip.clip_id,
        );
      }),
    ));

  test("validates image bytes once, uploads distinct content once and preserves ordered duplicates", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const first = yield* H3.validateReference(bytesReference());
        const second = yield* H3.validateReference({ _tag: "Bytes", bytes: pngBytes(48, 64) });
        const accepted = yield* provider.enqueue(request({ references: [first, second, first] }));
        yield* provider.enqueue(request({ references: [first] }));
        expect(fake.uploaded).toHaveLength(2);
        const call = fake.calls.find((call) => call.command === "enqueue")!;
        const refs = call.args.reference_images as readonly JsonObject[];
        expect(refs).toHaveLength(3);
        expect(refs[0]).toEqual(refs[2]);
        expect(refs[0]!.upload_id).not.toBe(refs[1]!.upload_id);
        expect(Object.keys(refs[0]!).sort()).toEqual(["mime_type", "name", "size", "upload_id"]);
        expect(typeof refs[0]!.size).toBe("number");
        expect(accepted.clip.seconds).toBeCloseTo(175 / 24, 8);
      }),
    ));

  test("uploaded wire references require bigint sizes and avoid filesystem or repeat uploads", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const file = {
          upload_id: "11111111-1111-4111-8111-111111111111",
          name: "image.png",
          mime_type: "image/png",
          size: 123n,
        };
        yield* provider.enqueue(request({ references: [{ _tag: "Uploaded", file }] }));
        expect(fake.uploaded).toEqual([]);
        const call = fake.calls.find((call) => call.command === "enqueue")!;
        expect(call.args.reference_images).toEqual([{ ...file, size: 123 }]);
        for (const bad of [
          { ...file, size: 123 },
          { ...file, size: -1n },
          { ...file, size: 100000000000000000000n },
          { ...file, upload_id: "wrong" },
          { ...file, mime_type: "audio/wav" },
        ]) {
          const result = yield* Effect.result(
            H3.validateReference({ _tag: "Uploaded", file: bad as never }),
          );
          expect(Result.isFailure(result)).toBe(true);
        }
      }),
    ));

  test("prepare is inert, captures detached bytes and sends exactly once after caller mutation", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const bytes = pngBytes(64, 48),
          expected = new Uint8Array(bytes);
        const mutable = {
          prompt: "captured prompt",
          references: [{ _tag: "Bytes" as const, bytes }],
          seconds: 7,
          metadata: "captured metadata",
        };
        const prepared = yield* provider.prepare(mutable);
        expect(yield* prepared.state).toEqual({ _tag: "Prepared" });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
        expect(fake.uploaded).toHaveLength(0);
        mutable.prompt = "mutated prompt";
        mutable.metadata = "mutated metadata";
        bytes.fill(0);
        const first = yield* prepared.submit,
          second = yield* prepared.submit;
        expect(second).toBe(first);
        expect(fake.uploaded[0]!.bytes).toEqual(expected);
        expect(first.clip.prompt).toBe("captured prompt");
        expect(JSON.parse(first.clip.metadata).caller).toBe("captured metadata");
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
        expect((yield* prepared.state)._tag).toBe("Completed");
      }),
    ));

  test("rejects invalid inputs, oversized metadata, audio and FastH3 fields before upload or dispatch", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const cyclic: Record<string, unknown> = { prompt: "prompt" };
        cyclic.metadata = cyclic;
        const invalid: unknown[] = [
          null,
          cyclic,
          {
            get prompt() {
              throw new Error("secret input");
            },
          },
          request({ prompt: "" }),
          request({ prompt: "   " }),
          request({ seconds: NaN }),
          request({ seconds: 4 }),
          request({ seconds: Infinity }),
          request({ seed: 1.5 }),
          request({ position: -1 }),
          request({ metadata: "x".repeat(2000) }),
          request({ references: Array.from({ length: 10 }, bytesReference) }),
          request({ references: [{ _tag: "Bytes", bytes: pngBytes(10, 100) }] }),
          request({ references: [{ _tag: "Bytes", bytes: new Uint8Array([1, 2, 3]) }] }),
          { ...request(), startingFrame: bytesReference() },
          { ...request(), endingFrame: bytesReference() },
          { ...request(), reference_audio: {} },
          { ...request(), referenceAudios: [] },
          { ...request(), metadata: {} },
        ];
        for (const input of invalid) {
          const result = yield* Effect.result(provider.enqueue(input as H3.Request));
          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) {
            expect(result.failure).toBeInstanceOf(CommandFailure);
            expect(result.failure.context.outcome).toBe("not-submitted");
            expect(result.failure.context.operation).toBe("enqueue");
            expect(JSON.stringify(result.failure)).not.toContain("secret input");
          }
        }
        expect(fake.uploaded).toHaveLength(0);
        expect(fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      }),
    ));

  test("forwards valid continuation UUIDs without inventing a local retention requirement", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        yield* provider.enqueue(request({ continueFrom: id }));
        expect(
          fake.calls.find((call) => call.command === "enqueue")!.args.continue_from_clip_id,
        ).toBe(id);
      }),
    ));

  test("keeps the provider tokenizer authoritative and uses only an explicit SDK prompt byte bound", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({}, { maxPromptBytes: 14000 });
        yield* provider.enqueue(request({ prompt: "word ".repeat(2500) }));
        const result = yield* Effect.result(
          provider.enqueue(request({ prompt: "x".repeat(14001) })),
        );
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));
});

describe("H3 command and observation authority", () => {
  test("applies each returned model envelope only through Session.observe and retains duplicate visibility", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider, events } = yield* setup();
        const accepted = yield* provider.enqueue(request());
        const source = fake.returns.find(
          (reply) => reply.kind === "message" && reply.type === "clip_queued",
        )!;
        yield* waitFor(() =>
          Effect.succeed(
            events.some((event) => event._tag === "Message" && event.source === source),
          ),
        );
        expect(
          events.filter((event) => event._tag === "Message" && event.source === source),
        ).toHaveLength(1);
        expect(events.filter((event) => event._tag === "Acceptance")).toHaveLength(1);
        yield* fake.replay(source);
        yield* waitFor(() =>
          Effect.succeed(
            events.filter((event) => event._tag === "Message" && event.source === source).length ===
              2,
          ),
        );
        expect(
          events
            .filter((event) => event._tag === "Message" && event.source === source)
            .map((event) => event._tag === "Message" && event.disposition),
        ).toEqual(["applied", "duplicate"]);
        expect(yield* provider.acceptances).toHaveLength(1);
        expect(
          ready(yield* provider.current).clips.filter(
            (entry) => entry.clip.clip_id === accepted.clip.clip_id,
          ),
        ).toHaveLength(1);
      }),
    ));

  test("an ACK followed by a body with the same request id is accepted exactly once", () =>
    run(
      Effect.gen(function* () {
        let pendingClip: ReturnType<typeof fixtureClip> | undefined;
        let commandId = "";
        const { fake, provider, events } = yield* setup(
          {
            command: {
              enqueue: ({ call }) =>
                Effect.sync(() => {
                  commandId = call.requestId;
                  pendingClip = fixtureClip({
                    prompt: String(call.args.prompt),
                    metadata: String(call.args.metadata),
                  });
                  return undefined;
                }),
            },
          },
          { reconcileWindowMs: 200 },
        );
        const fiber = yield* provider.enqueue(request()).pipe(Effect.forkScoped);
        yield* waitFor(() =>
          Effect.succeed(
            events.some(
              (event) => event._tag === "Acknowledged" && event.source.requestId === commandId,
            ),
          ),
        );
        expect(yield* provider.acceptances).toHaveLength(0);
        const body = yield* fake.emit(
          "clip_queued",
          { clip: { ...pendingClip! } },
          { requestId: commandId, correlation: "duplicate" },
        );
        const accepted = yield* Fiber.join(fiber);
        expect(accepted.clip.clip_id).toBe(pendingClip!.clip_id);
        expect(accepted.evidence.source).toBe(body);
        expect(accepted.evidence.kind).toBe("metadata");
        yield* fake.emit(
          "clip_queued",
          { clip: { ...pendingClip! } },
          { requestId: commandId, correlation: "duplicate" },
        );
        yield* waitFor(() =>
          Effect.succeed(
            events.filter(
              (event) => event._tag === "Message" && event.source.requestId === commandId,
            ).length === 2,
          ),
        );
        expect(
          events
            .filter((event) => event._tag === "Message" && event.source.requestId === commandId)
            .map((event) => event._tag === "Message" && event.disposition),
        ).toEqual(["applied", "duplicate"]);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("an ACK alone is unknown and never resubmitted by an inert submission", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: { enqueue: () => Effect.succeed(undefined) },
        });
        const prepared = yield* provider.prepare(request());
        const first = yield* Effect.result(prepared.submit),
          second = yield* Effect.result(prepared.submit);
        expect(Result.isFailure(first)).toBe(true);
        expect(second).toEqual(first);
        if (Result.isFailure(first))
          expect(first.failure.context).toMatchObject({
            operation: "enqueue",
            outcome: "unknown",
            requestId: "fixture-command-3",
            generation: 1n,
          });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
        expect(yield* provider.acceptances).toEqual([]);
        expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
      }),
    ));

  test("uses explicit command outcomes rather than guessing from failure codes", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: { enqueue: ({ fail }) => Effect.fail(fail("unknown", "InvalidInput")) },
        });
        const unknown = yield* Effect.result(provider.enqueue(request()));
        expect(Result.isFailure(unknown)).toBe(true);
        if (Result.isFailure(unknown)) {
          expect(unknown.failure.code).toBe("InvalidInput");
          expect(unknown.failure.context).toMatchObject({
            outcome: "unknown",
            operation: "enqueue",
            generation: 1n,
          });
          expect(unknown.failure.context.requestId).toBe("fixture-command-3");
        }
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
        for (const outcome of ["not-submitted", "replied"] as const) {
          const next = yield* setup({
            command: { enqueue: ({ fail }) => Effect.fail(fail(outcome, "Native")) },
          });
          const result = yield* Effect.result(next.provider.enqueue(request()));
          expect(Result.isFailure(result) && result.failure.context.outcome).toBe(outcome);
        }
      }),
    ));

  test("correlated rejection is replied while a delayed unrelated command_error cannot reject another request", () =>
    run(
      Effect.gen(function* () {
        const direct = yield* setup({
          command: {
            enqueue: () =>
              Effect.succeed({
                type: "command_error",
                data: { command: "enqueue", reason: "full" },
              }),
          },
        });
        const rejected = yield* Effect.result(direct.provider.enqueue(request()));
        expect(Result.isFailure(rejected) && rejected.failure.context.outcome).toBe("replied");
        const unrelated = yield* setup({
          command: {
            enqueue: ({ fake }) =>
              fake
                .emit("command_error", { command: "enqueue", reason: "earlier request" })
                .pipe(Effect.as(undefined)),
          },
        });
        const uncertain = yield* Effect.result(unrelated.provider.enqueue(request()));
        expect(Result.isFailure(uncertain) && uncertain.failure.context.outcome).toBe("unknown");
        expect(
          unrelated.events.some(
            (event) =>
              event._tag === "Message" &&
              event.message.type === "command_error" &&
              event.source.correlation === "unsolicited",
          ),
        ).toBe(true);
      }),
    ));

  test("late metadata evidence can prove acceptance after the command loses its result", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: {
            enqueue: ({ fake, call, fail }) =>
              Effect.gen(function* () {
                const clip = fixtureClip({
                  prompt: String(call.args.prompt),
                  metadata: String(call.args.metadata),
                });
                yield* fake.emit(
                  "clip_generated",
                  { clip: { ...clip, ready: true } },
                  { requestId: call.requestId, correlation: "late" },
                );
                return yield* Effect.fail(fail("unknown", "Disconnected"));
              }),
          },
        });
        const accepted = yield* provider.enqueue(request());
        expect(accepted.evidence.kind).toBe("metadata");
        expect(accepted.evidence.source.correlation).toBe("late");
        expect(accepted.clip.ready).toBe(true);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("two providers sharing one Session have different acceptance namespaces and retain each other's clips", () =>
    run(
      Effect.gen(function* () {
        const fake = yield* fixture();
        const first = yield* H3.make(fake.session, options),
          second = yield* H3.make(fake.session, options);
        const a = yield* first.enqueue(request({ metadata: "first" }));
        const b = yield* second.enqueue(request({ metadata: "second" }));
        expect(JSON.parse(a.clip.metadata).namespace).not.toBe(
          JSON.parse(b.clip.metadata).namespace,
        );
        expect((yield* first.acceptances).map((entry) => entry.submissionId)).toEqual([
          a.submissionId,
        ]);
        expect((yield* second.acceptances).map((entry) => entry.submissionId)).toEqual([
          b.submissionId,
        ]);
        yield* first.refresh;
        yield* second.refresh;
        expect(ready(yield* first.current).queue.generation.map((clip) => clip.clip_id)).toEqual([
          a.clip.clip_id,
          b.clip.clip_id,
        ]);
        expect(ready(yield* second.current).queue.generation.map((clip) => clip.clip_id)).toEqual([
          a.clip.clip_id,
          b.clip.clip_id,
        ]);
      }),
    ));

  test("foreign or changed acceptance metadata cannot settle a local enqueue", () =>
    run(
      Effect.gen(function* () {
        const { provider, fake, events } = yield* setup({
          command: {
            enqueue: ({ call }) =>
              Effect.succeed({
                type: "clip_queued",
                data: {
                  clip: {
                    ...fixtureClip({
                      prompt: String(call.args.prompt),
                      metadata: JSON.stringify({
                        reactor_effect_h3: 1,
                        namespace: "someone-else",
                        submission: "unrelated",
                        caller: "",
                      }),
                    }),
                  },
                },
              }),
          },
        });
        const result = yield* Effect.result(provider.enqueue(request()));
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
        expect(yield* provider.acceptances).toEqual([]);
        expect((yield* provider.current).clips).toHaveLength(1);
        expect(events.filter((event) => event._tag === "Acceptance")).toHaveLength(0);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("a command result never substitutes for a missing observation", () =>
    run(
      Effect.gen(function* () {
        const fake = yield* fixture({ omitObservation: true });
        const result = yield* Effect.result(
          H3.make(fake.session, { ...options, commandTimeoutMs: 10 }),
        );
        expect(Result.isFailure(result)).toBe(true);
        expect(fake.calls.map((call) => call.command)).toEqual(["get_state"]);
        expect(fake.subscribers()).toBe(0);
      }),
    ));
});

describe("H3 full-snapshot freshness and lifecycle", () => {
  test("known acceptance makes stale empty snapshots Synchronizing until the explicit read barrier", () =>
    run(
      Effect.gen(function* () {
        let accepted: ReturnType<typeof fixtureClip> | undefined,
          failReads = false;
        const { fake, provider } = yield* setup({
          command: {
            enqueue: ({ call }) =>
              Effect.sync(() => {
                accepted = fixtureClip({
                  prompt: String(call.args.prompt),
                  metadata: String(call.args.metadata),
                });
                return { type: "clip_queued", data: { clip: { ...accepted } } };
              }),
            get_state: ({ fake, fail }) =>
              failReads
                ? Effect.fail(fail("unknown"))
                : Effect.succeed({
                    type: "state_update",
                    data: fake.state({ generation_queued: accepted === undefined ? 0 : 1 }),
                  }),
            get_queue: () =>
              Effect.succeed({
                type: "queue_update",
                data: {
                  generation: accepted === undefined ? [] : [{ ...accepted }],
                  playout: [],
                  history: [],
                },
              }),
          },
        });
        const result = yield* provider.enqueue(request());
        const waiting = yield* provider.current;
        expect(waiting._tag).toBe("Synchronizing");
        expect("queue" in waiting).toBe(false);
        if (waiting._tag === "Synchronizing")
          expect(waiting.lastFacts?.queue.generation).toEqual([]);
        const blocked = yield* Effect.result(provider.enqueue(request()));
        expect(Result.isFailure(blocked) && blocked.failure.context.outcome).toBe("not-submitted");
        failReads = true;
        expect(Result.isFailure(yield* Effect.result(provider.refresh))).toBe(true);
        expect(yield* provider.acceptance(result.submissionId)).toBe(result);
        failReads = false;
        yield* provider.refresh;
        expect(ready(yield* provider.current).queue.generation[0]!.clip_id).toBe(
          result.clip.clip_id,
        );
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("inconsistent full state and queue never claim readiness", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const event = yield* fake.emit("state_update", fake.state({ generation_queued: 2 }));
        yield* observed(provider, event.sequence);
        expect((yield* provider.current)._tag).toBe("Synchronizing");
        const update = yield* fake.emit("queue_update", {
          generation: [{ ...fixtureClip() }, { ...fixtureClip() }],
          playout: [],
          history: [],
        });
        yield* observed(provider, update.sequence);
        expect(ready(yield* provider.current).queue.generation).toHaveLength(2);
      }),
    ));

  test("early starts and finishes cannot be reversed by a late queued/generated message", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider, events } = yield* setup({
          command: {
            enqueue: ({ fake, call }) =>
              Effect.gen(function* () {
                const clip = fixtureClip({
                  prompt: String(call.args.prompt),
                  metadata: String(call.args.metadata),
                  ready: true,
                });
                yield* fake.emit("clip_started", { clip: { ...clip } });
                yield* fake.emit("clip_finished", {
                  clip: { ...clip },
                  seconds_sent: clip.seconds,
                });
                yield* fake.emit("state_update", fake.state());
                yield* fake.emit("queue_update", { generation: [], playout: [], history: [] });
                return { type: "clip_queued", data: { clip: { ...clip, ready: false } } };
              }),
          },
        });
        const accepted = yield* provider.enqueue(request());
        const source = yield* fake.emit("clip_generated", {
          clip: { ...accepted.clip, ready: true },
        });
        yield* observed(provider, source.sequence);
        const snapshot = ready(yield* provider.current);
        expect(snapshot.queue.generation).toEqual([]);
        expect(snapshot.queue.playout).toEqual([]);
        expect(snapshot.state.playing).toBe(false);
        expect(snapshot.clips[0]!.lifecycle).toBe("clip_finished");
        // The captured event stream is appended by its own consumer; wait for both
        // late duplicates to be observed before asserting on the array.
        const duplicateQueued = () =>
          events.some(
            (event) =>
              event._tag === "Message" &&
              event.message.type === "clip_queued" &&
              event.disposition === "duplicate",
          );
        const duplicateGenerated = () =>
          events.some(
            (event) =>
              event._tag === "Message" &&
              event.source === source &&
              event.disposition === "duplicate",
          );
        yield* waitFor(() => Effect.succeed(duplicateQueued() && duplicateGenerated()));
        expect(duplicateQueued()).toBe(true);
        expect(duplicateGenerated()).toBe(true);
        expect(events.some((event) => (event as { _tag: string })._tag === "Building")).toBe(false);
      }),
    ));

  test("foreign lifecycle and failures stay observable without an original request", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider, events } = yield* setup();
        const clip = fixtureClip();
        const source = yield* fake.emit("clip_failed", {
          clip: { ...clip },
          reason: "fixture rejected generation",
        });
        yield* observed(provider, source.sequence);
        expect((yield* provider.current).clips[0]!.clip.metadata).toBe("foreign metadata");
        expect((yield* provider.current).clips[0]!.lifecycle).toBe("clip_failed");
        expect(
          events.some(
            (event) =>
              event._tag === "Message" &&
              event.message.type === "clip_failed" &&
              event.message.data.reason === "fixture rejected generation",
          ),
        ).toBe(true);
        expect(yield* provider.acceptances).toEqual([]);
      }),
    ));

  test("disconnect is visible without automatic reconnect; stale generations cannot change refreshed facts", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider, events } = yield* setup();
        yield* fake.status("disconnected");
        yield* waitFor(() =>
          provider.current.pipe(Effect.map((snapshot) => snapshot._tag === "Unavailable")),
        );
        expect(fake.lifecycleCalls.reconnect).toBe(0);
        const before = fake.calls.length;
        expect(Result.isFailure(yield* Effect.result(provider.enqueue(request())))).toBe(true);
        expect(fake.calls.length).toBe(before);
        yield* fake.status("ready", 2n);
        yield* waitFor(() =>
          provider.current.pipe(Effect.map((snapshot) => snapshot.transportGeneration === 2n)),
        );
        expect((yield* provider.current)._tag).toBe("Synchronizing");
        yield* provider.refresh;
        const stale = yield* fake.emit(
          "queue_update",
          { generation: [{ ...fixtureClip() }], playout: [], history: [] },
          { generation: 1n, correlation: "stale-generation" },
        );
        yield* waitFor(() =>
          Effect.succeed(
            events.some((event) => event._tag === "Message" && event.source === stale),
          ),
        );
        expect(ready(yield* provider.current).queue.generation).toEqual([]);
        expect(
          events.some(
            (event) =>
              event._tag === "Message" && event.source === stale && event.disposition === "stale",
          ),
        ).toBe(true);
        expect(fake.lifecycleCalls.reconnect).toBe(0);
      }),
    ));
});

describe("H3 explicit commands", () => {
  test("move sends only clip_id and position and accepts the reported clamped append position", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const first = yield* provider.enqueue(request()),
          second = yield* provider.enqueue(request());
        const moved = yield* provider.move(first.clip.clip_id, 9999);
        expect(moved.value).toMatchObject({ queue: "generation", position: 1 });
        expect(fake.calls.find((call) => call.command === "move")!.args).toEqual({
          clip_id: first.clip.clip_id,
          position: 9999,
        });
        expect(ready(yield* provider.current).queue.generation.map((clip) => clip.clip_id)).toEqual(
          [second.clip.clip_id, first.clip.clip_id],
        );
        const back = yield* provider.move(first.clip.clip_id, 0);
        expect(back.value.position).toBe(0);
        expect(ready(yield* provider.current).queue.generation[0]!.clip_id).toBe(
          first.clip.clip_id,
        );
      }),
    ));

  test("play and empty-argument stop preserve ACK versus later lifecycle facts without autoplay policy", () =>
    run(
      Effect.gen(function* () {
        const clip = fixtureClip({ ready: true });
        const { fake, provider, events } = yield* setup({ initialPlayout: [clip] });
        const played = yield* provider.play();
        expect(played._tag).toBe("Acknowledged");
        expect(ready(yield* provider.current).state.playing_clip_id).toBe(clip.clip_id);
        const stopped = yield* provider.stop;
        expect(stopped._tag).toBe("Acknowledged");
        expect(fake.calls.find((call) => call.command === "stop")!.args).toEqual({});
        expect(fake.calls.map((call) => call.command)).toEqual([
          "get_state",
          "get_queue",
          "play",
          "stop",
        ]);
        expect(ready(yield* provider.current).state.playing).toBe(false);
        yield* waitFor(() =>
          Effect.succeed(
            events.some(
              (event) => event._tag === "Message" && event.message.type === "clip_stopped",
            ),
          ),
        );
        expect(
          events.some((event) => event._tag === "Message" && event.message.type === "clip_stopped"),
        ).toBe(true);
      }),
    ));

  test("pop, settings and reset expose actual named replies and change nothing implicitly", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const clip = yield* provider.enqueue(request());
        const popped = yield* provider.pop(clip.clip.clip_id);
        expect(popped.value.clip.clip_id).toBe(clip.clip.clip_id);
        expect(ready(yield* provider.current).queue.generation).toEqual([]);
        expect((yield* provider.setSeed(42)).value.seed).toBe(42);
        const length = yield* provider.setClipSeconds(7);
        expect(length.value).toEqual({ clip_seconds: 175 / 24, frames: 175 });
        expect((yield* provider.setCanvas("9:16")).value).toEqual({
          aspect: "9:16",
          width: 768,
          height: 1344,
        });
        expect((yield* provider.setAutoplay(true)).value.enabled).toBe(true);
        expect((yield* provider.setFlushOnClipEnd(false)).value.enabled).toBe(false);
        expect((yield* provider.reset).value).toEqual({ cleared_clips: 0, was_playing: false });
        expect(fake.calls.map((call) => call.command)).toEqual([
          "get_state",
          "get_queue",
          "enqueue",
          "pop",
          "set_seed",
          "set_clip_seconds",
          "set_canvas",
          "set_autoplay",
          "set_flush_on_clip_end",
          "reset",
        ]);
      }),
    ));

  test("locally invalid command arguments are lazy not-submitted CommandFailure values", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const validId = fixtureClip().clip_id;
        const operations: readonly Effect.Effect<unknown, CommandFailure>[] = [
          provider.move(validId, -1),
          provider.move("bad", 0),
          provider.pop("bad"),
          provider.play("bad"),
          provider.setSeed(NaN),
          provider.setClipSeconds(Infinity),
          provider.setCanvas("wrong" as H3.Aspect),
          provider.setAutoplay(null as never),
          provider.setFlushOnClipEnd("yes" as never),
        ];
        for (const operation of operations) {
          const exit = yield* Effect.exit(operation);
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            const error = Cause.findErrorOption(exit.cause);
            expect(error._tag).toBe("Some");
            if (error._tag === "Some") {
              expect(error.value).toBeInstanceOf(CommandFailure);
              expect(error.value.context.outcome).toBe("not-submitted");
            }
          }
        }
        expect(fake.calls).toHaveLength(2);
      }),
    ));

  test("named mutation ACKs do not fabricate payloads even when a subsequent refresh proves changed state", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: { set_autoplay: ({ defaults }) => defaults().pipe(Effect.as(undefined)) },
        });
        const result = yield* Effect.result(provider.setAutoplay(true));
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
        yield* provider.refresh;
        expect(ready(yield* provider.current).state.autoplay).toBe(true);
        expect(fake.calls.filter((call) => call.command === "set_autoplay")).toHaveLength(1);
      }),
    ));
});

describe("H3 preparation, cancellation and bounds", () => {
  test("prepareFrom performs host work, validation, upload, commit and result under one submission", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const steps: string[] = [];
        const submission = yield* provider.prepareFrom(
          Effect.acquireRelease(
            Effect.sync(() => {
              steps.push("host acquire");
              return request({ references: [bytesReference()] });
            }),
            () =>
              Effect.sync(() => {
                steps.push("host release");
              }),
          ),
          {
            commit: (id) =>
              Effect.sync(() => {
                steps.push("commit");
                expect(fake.uploaded).toHaveLength(1);
                expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
                expect(id).toBe(submission.id);
              }),
            result: (id, result) =>
              Effect.sync(() => {
                steps.push("result");
                expect(id).toBe(submission.id);
                expect(Result.isSuccess(result)).toBe(true);
              }),
          },
        );
        expect(steps).toEqual([]);
        const first = yield* submission.submit,
          second = yield* submission.submit;
        expect(second).toBe(first);
        expect(steps).toEqual(["host acquire", "commit", "result", "host release"]);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("a failed result observer runs once and cannot replace a known acceptance", () =>
    run(
      Effect.gen(function* () {
        const { provider, events } = yield* setup();
        let hooks = 0,
          recorded: H3.Acceptance | undefined;
        const submission = yield* provider.prepare(request(), {
          result: (_, result) =>
            Effect.sync(() => {
              hooks++;
              if (Result.isSuccess(result)) recorded = result.success;
            }).pipe(Effect.andThen(Effect.die(new Error("private hook defect")))),
        });
        const accepted = yield* submission.submit;
        expect(recorded).toBeDefined();
        if (recorded === undefined)
          throw new Error("The hook must observe the original acceptance");
        expect(accepted).toBe(recorded);
        expect(yield* submission.submit).toBe(accepted);
        expect(hooks).toBe(1);
        yield* waitFor(() =>
          Effect.succeed(
            events.some(
              (event) =>
                event._tag === "Diagnostic" && event.error.context.operation === "H3 result hook",
            ),
          ),
        );
        const diagnostic = events.find(
          (event) =>
            event._tag === "Diagnostic" && event.error.context.operation === "H3 result hook",
        );
        expect(diagnostic?._tag).toBe("Diagnostic");
        if (diagnostic?._tag === "Diagnostic") {
          expect(JSON.stringify(diagnostic.error)).not.toContain("private hook defect");
          expect(diagnostic.error.context.detail).toBeDefined();
        }
      }),
    ));

  test("a stalled result observer is bounded and cannot replace the exact primary rejection", () =>
    run(
      Effect.gen(function* () {
        const { provider, fake } = yield* setup(
          { command: { enqueue: ({ fail }) => Effect.fail(fail("replied", "Remote")) } },
          { resultHookTimeoutMs: 10 },
        );
        let hooks = 0,
          recorded: CommandFailure | undefined;
        const submission = yield* provider.prepare(request(), {
          result: (_, result) =>
            Effect.sync(() => {
              hooks++;
              if (Result.isFailure(result)) recorded = result.failure;
            }).pipe(Effect.andThen(Effect.never)),
        });
        const result = yield* Effect.result(submission.submit);
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) expect(result.failure).toBe(recorded!);
        expect(yield* Effect.result(submission.submit)).toEqual(result);
        expect(hooks).toBe(1);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("cancelling provisional host work releases its resources and permits a later clean attempt", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const held = yield* gate();
        let entered = false,
          released = 0,
          attempts = 0,
          commits = 0;
        const submission = yield* provider.prepareFrom(
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              Effect.sync(() => {
                entered = true;
                attempts++;
              }),
              () =>
                Effect.sync(() => {
                  released++;
                }),
            );
            yield* held.wait;
            return request();
          }),
          {
            commit: () =>
              Effect.sync(() => {
                commits++;
              }),
          },
        );
        const pending = yield* submission.submit.pipe(Effect.forkScoped);
        yield* waitFor(() => Effect.succeed(entered));
        yield* Fiber.interrupt(pending);
        expect(released).toBe(1);
        expect(commits).toBe(0);
        expect(yield* submission.state).toEqual({ _tag: "Prepared" });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
        yield* held.release;
        yield* submission.submit;
        expect(attempts).toBe(2);
        expect(released).toBe(2);
        expect(commits).toBe(1);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("cancelling upload prework never dispatches the clip later", () =>
    run(
      Effect.gen(function* () {
        const held = yield* gate();
        const { fake, provider } = yield* setup({
          upload: () =>
            held.wait.pipe(
              Effect.andThen(
                Effect.fail(new ReactorError({ code: "Upload", message: "fixture upload ends" })),
              ),
            ),
        });
        const prepared = yield* provider.prepare(request({ references: [bytesReference()] }));
        const pending = yield* prepared.submit.pipe(Effect.forkScoped);
        yield* waitFor(() => Effect.succeed(fake.uploaded.length === 1));
        yield* Fiber.interrupt(pending);
        yield* held.release;
        yield* Effect.sleep(5);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
        expect(yield* prepared.state).toEqual({ _tag: "Prepared" });
      }),
    ));

  test("a commit-hook refusal sends no command and leaves preparation retryable", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        let refuse = true;
        const prepared = yield* provider.prepare(request(), {
          commit: () =>
            refuse
              ? Effect.fail(
                  CommandFailure.from(
                    new ReactorError({ code: "InvalidState", message: "affinity refused" }),
                    {
                      operation: "enqueue",
                      outcome: "not-submitted",
                    },
                  ),
                )
              : Effect.void,
        });
        const result = yield* Effect.result(prepared.submit);
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
        expect(yield* prepared.state).toEqual({ _tag: "Prepared" });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
        refuse = false;
        yield* prepared.submit;
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("caller cancellation after commit does not cancel or replay a late accepted enqueue", () =>
    run(
      Effect.gen(function* () {
        const held = yield* gate();
        let resultHooks = 0;
        const { fake, provider } = yield* setup(
          { command: { enqueue: ({ defaults }) => held.wait.pipe(Effect.andThen(defaults())) } },
          { commandTimeoutMs: 1000 },
        );
        const prepared = yield* provider.prepare(request(), {
          result: () =>
            Effect.sync(() => {
              resultHooks++;
            }),
        });
        const pending = yield* prepared.submit.pipe(Effect.forkScoped);
        yield* waitFor(() => Effect.succeed(fake.calls.some((call) => call.command === "enqueue")));
        expect((yield* prepared.state)._tag).toBe("Committed");
        yield* Fiber.interrupt(pending);
        yield* held.release;
        const accepted = yield* prepared.submit;
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
        expect(resultHooks).toBe(1);
        expect(yield* provider.acceptance(prepared.id)).toBe(accepted);
      }),
    ));

  test("a known acceptance stays known when Session observation ends during reconciliation", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: {
            enqueue: ({ defaults, fake, fail }) =>
              Effect.gen(function* () {
                yield* defaults();
                yield* Effect.sleep(5);
                yield* fake.failObservation(
                  new ReactorError({ code: "Closed", message: "fixture source ended" }),
                );
                return yield* Effect.fail(fail("unknown", "Disconnected"));
              }),
          },
        });
        const accepted = yield* provider.enqueue(request());
        expect(accepted.clip.clip_id).toBe(fake.accepted[0]!.clip_id);
        expect((yield* provider.current)._tag).toBe("Unavailable");
        expect(yield* provider.acceptance(accepted.submissionId)).toBe(accepted);
      }),
    ));

  test("pending acceptance and retained annotation counts are bounded without replay", () =>
    run(
      Effect.gen(function* () {
        const held = yield* gate();
        const { fake, provider } = yield* setup(
          { command: { enqueue: ({ defaults }) => held.wait.pipe(Effect.andThen(defaults())) } },
          { maxPending: 1, maxAcceptances: 1, commandTimeoutMs: 1000 },
        );
        const first = yield* provider.prepare(request());
        const inflight = yield* first.submit.pipe(Effect.forkScoped);
        yield* waitFor(() => Effect.succeed(fake.calls.some((call) => call.command === "enqueue")));
        const other = yield* provider.prepare(request());
        const overflow = yield* Effect.result(other.submit);
        expect(Result.isFailure(overflow) && overflow.failure.code).toBe("Overflow");
        expect(Result.isFailure(overflow) && overflow.failure.context.outcome).toBe(
          "not-submitted",
        );
        yield* held.release;
        const firstAccepted = yield* Fiber.join(inflight);
        const secondAccepted = yield* other.submit;
        expect(yield* provider.acceptance(firstAccepted.submissionId)).toBeUndefined();
        expect(yield* provider.acceptances).toEqual([secondAccepted]);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(2);
      }),
    ));

  test("malformed known payloads and missing clip duration fail observation with sanitized diagnostics", () =>
    run(
      Effect.gen(function* () {
        for (const [type, data] of [
          ["clip_generated", { clip: { clip_id: fixtureClip().clip_id, ready: true } }],
          ["queue_update", { generation: [] }],
          ["state_update", { playing: false }],
          ["clip_finished", { clip: { ...fixtureClip() } }],
        ] as const) {
          const { fake, provider } = yield* setup();
          yield* fake.emit(type, { ...data, private: "secret fixture token" });
          const error = yield* provider.failure;
          expect(error.code).toBe("Protocol");
          expect(JSON.stringify(error)).not.toContain("secret fixture token");
          expect((yield* provider.current)._tag).toBe("Unavailable");
          expect(Result.isFailure(yield* Effect.result(provider.enqueue(request())))).toBe(true);
        }
      }),
    ));

  test("a malformed correlated acceptance stays unknown and does not create a clip or replay", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: {
            enqueue: () =>
              Effect.succeed({
                type: "clip_queued",
                data: { clip: { clip_id: fixtureClip().clip_id } },
              }),
          },
        });
        const result = yield* Effect.result(provider.enqueue(request()));
        expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
        expect(yield* provider.acceptances).toEqual([]);
        expect((yield* provider.current).clips).toEqual([]);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("foreign clip retention fails visibly at its bound instead of growing forever", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({}, { maxTrackedClips: 1 });
        const source = yield* fake.emit("clip_failed", {
          clip: { ...fixtureClip() },
          reason: "fixture",
        });
        yield* observed(provider, source.sequence);
        yield* fake.emit("clip_failed", { clip: { ...fixtureClip() }, reason: "fixture" });
        const error = yield* provider.failure;
        expect(error.code).toBe("Overflow");
        expect((yield* provider.current).clips).toHaveLength(1);
      }),
    ));

  test("retained provider bytes have a separate bound from clip counts", () =>
    run(
      Effect.gen(function* () {
        const fake = yield* fixture();
        const result = yield* Effect.result(
          H3.make(fake.session, { ...options, maxRetainedBytes: 10 }),
        );
        expect(Result.isFailure(result)).toBe(true);
        expect(fake.subscribers()).toBe(0);
      }),
    ));

  test("one slow observer fails with Overflow without blocking provider commands or new observers", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const held = yield* gate();
        let blocked = false;
        const observation = yield* provider.observe({ capacity: 1 });
        const reader = yield* observation.events.pipe(
          Stream.runForEach(() => {
            if (blocked) return Effect.void;
            blocked = true;
            return held.wait;
          }),
          Effect.forkScoped,
        );
        yield* fake.emit("future_event", { first: true });
        yield* waitFor(() => Effect.succeed(blocked));
        for (let i = 0; i < 5; i++) yield* fake.emit("future_event", { index: i });
        yield* Effect.sleep(5);
        yield* held.release;
        const result = yield* Effect.result(Fiber.join(reader));
        expect(Result.isFailure(result) && result.failure.code).toBe("Overflow");
        const next = yield* provider.observe();
        expect(next.initial._tag).toBe("Ready");
        expect((yield* provider.enqueue(request())).clip.clip_id).toBe(fake.accepted[0]!.clip_id);
      }),
    ));

  test("closing the provider joins local resources while leaving the borrowed Session intact", async () => {
    let retained: H3.Provider | undefined, fake: Fixture | undefined;
    await run(
      Effect.gen(function* () {
        const setup = yield* fixture();
        fake = setup;
        retained = yield* H3.make(setup.session, options);
      }),
    );
    expect(fake!.subscribers()).toBe(0);
    expect(fake!.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
    const result = await Effect.runPromise(Effect.result(retained!.enqueue(request())));
    expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
    expect(Result.isFailure(result) && result.failure.code).toBe("Closed");
  });
});
