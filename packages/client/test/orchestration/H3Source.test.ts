import { expect, test } from "vitest";
import { Clock, Effect, Fiber, Option, Result, Stream } from "effect";
import * as H3 from "../../src/h3/index.js";
import { bindSession, fromH3, isLocalClip } from "../../src/orchestration/h3-source.js";
import type { H3SourceOptions } from "../../src/orchestration/h3-source.js";
import { captureRequest, ClipId } from "../../src/orchestration/request.js";
import { keyedRequest, keyFromProviderMetadata } from "../../src/orchestration/scheduler-key.js";
import { isIdle } from "../../src/orchestration/queries.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import type { EngineEvent } from "../../src/orchestration/types.js";
import type { CloseReport, Session } from "../../src/session/index.js";
import type { MediaGeneration } from "../../src/session/media.js";
import type { JsonObject } from "../../src/json.js";
import { ReactorError } from "../../src/errors.js";
import { dataUri, pngBytes } from "../../src/testing/Png.js";
import { wavBytes } from "../../src/testing/Wav.js";
import { fixture, fixtureClip, metadataOf, textArg } from "../h3/ProviderSession.js";
import type { Script } from "../h3/ProviderSession.js";
import {
  cleanPressure,
  failure,
  gate,
  member,
  request,
  run,
  until,
  untilEffect,
  refusal,
} from "./SourceFixture.js";
import { steppableWall } from "./WallClock.js";

const media: MediaGeneration = {
  generation: 1n,
  tracks: [],
  retired: Effect.never,
  video: () => Stream.never,
  audio: () => Stream.never,
  snapshot: Effect.succeed(cleanPressure),
};
const setup = (
  script: Script = {},
  options: Omit<H3SourceOptions, "media"> = {},
  ownership: Session["ownership"] = "attached",
) =>
  Effect.gen(function* () {
    const fake = yield* fixture(script);
    let closedLease: CloseReport | undefined;
    const session: Session = {
      ...fake.session,
      ownership,
      close: fake.session.close.pipe(
        Effect.map((report) => {
          closedLease = ownership === "attached" ? report : { ...report, ownership };
          return closedLease;
        }),
      ),
    };
    const provider = yield* H3.make(session, {
      replyTimeout: 100,
      setupTimeout: 1000,
      reconcileWindow: 20,
    });
    const source = yield* fromH3(session, provider, { media: Effect.succeed(media), ...options });
    const events: EngineEvent[] = [];
    yield* source.events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          events.push(event);
        }),
      ),
      Effect.forkScoped,
    );
    yield* Effect.yieldNow;
    const emit = (type: string, data: JsonObject) =>
      Effect.gen(function* () {
        const event = yield* fake.emit(type, data);
        yield* untilEffect(
          provider.current.pipe(Effect.map((snapshot) => snapshot.revision >= event.sequence)),
        );
        yield* Effect.sleep(1);
      });
    return { source, provider, fake, session, events, emit, closedLease: () => closedLease };
  });

test("sameSessionAs remains local scheduling metadata and never enters an H3 command", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const input = request({ sameSessionAs: ClipId.make("source-affinity-only") });
      const prepared = yield* source.prepareRouted({ request: input, position: undefined });
      yield* prepared.submit;
      const args = fake.calls.find((call) => call.command === "enqueue")!.args;
      expect(args.sameSessionAs).toBeUndefined();
      expect(args.same_session_as).toBeUndefined();
      expect(args.continue_from_clip_id).toBeUndefined();
    }),
  ));

test("a scheduler key reaches provider metadata while the application request stays intact", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const input = request({ metadata: { editorial: { segment: "opening" } } });
      const keyed = yield* keyedRequest(input, "opening-1");
      const prepared = yield* source.prepareRouted({ request: keyed, position: undefined });
      const clipId = yield* prepared.submit;
      const call = fake.calls.find((entry) => entry.command === "enqueue")!;
      const envelope = metadataOf(textArg(call.args.metadata));
      const caller = metadataOf(textArg(envelope.caller));
      expect(caller).toMatchObject({
        reactor_effect_scheduler: 1,
        key: "opening-1",
        application: { editorial: { segment: "opening" } },
      });
      const clip = (yield* source.state).queued.find((entry) => entry.clipId === clipId)!;
      expect(keyFromProviderMetadata(clip.provider.metadata)).toBe("opening-1");
      expect(input.metadata).toEqual({ editorial: { segment: "opening" } });
      expect(clip.request?.metadata).toEqual(input.metadata);
    }),
  ));

test("H3 source has no implicit autoplay, flush, canvas, connect or reconnect policy", () =>
  run(
    Effect.gen(function* () {
      const { source, provider, fake } = yield* setup();
      expect(fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      expect(fake.lifecycleCalls).toEqual({ connect: 0, reconnect: 0, close: 0 });
      const snapshot = yield* provider.current;
      expect(snapshot._tag === "Ready" && snapshot.state.autoplay).toBe(false);
      expect(snapshot._tag === "Ready" && snapshot.state.flush_on_clip_end).toBe(true);
      expect((yield* source.state).building).toEqual(Option.none());
      expect(yield* source.media).toMatchObject({ generation: 1n, videoFramesPerSecond: 24 });
    }),
  ));

test("explicit canvas and hold policy changes only the requested settings", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, provider } = yield* setup({}, { canvas: "9:16", holdLastFrame: true });
      expect(
        fake.calls
          .filter((call) => call.command.startsWith("set_"))
          .map((call) => [call.command, call.args]),
      ).toEqual([
        ["set_canvas", { aspect: "9:16" }],
        ["set_flush_on_clip_end", { enabled: false }],
      ]);
      expect((yield* source.state).canvas).toEqual(Option.some("9:16"));
      const snapshot = yield* provider.current;
      expect(snapshot._tag === "Ready" && snapshot.state.autoplay).toBe(false);
    }),
  ));

test("H3 source preserves exact foreign queue order and does not fabricate application annotations or build starts", () =>
  run(
    Effect.gen(function* () {
      const a = fixtureClip(),
        b = fixtureClip(),
        c = fixtureClip({ ready: true });
      const { source, events } = yield* setup({ initialGeneration: [b, a], initialPlayout: [c] });
      const snapshot = yield* source.state;
      expect(snapshot.generationOrder).toEqual([ClipId.make(b.clip_id), ClipId.make(a.clip_id)]);
      expect(snapshot.queued.map((clip) => clip.provider)).toEqual([b, a]);
      expect(snapshot.ready.map((clip) => clip.provider)).toEqual([c]);
      expect(
        snapshot.queued.every(
          (clip) =>
            clip.request === undefined && clip.seq === undefined && clip.enqueuedAt === undefined,
        ),
      ).toBe(true);
      expect(snapshot.queued.every((clip) => !isLocalClip(clip))).toBe(true);
      expect(Option.isNone(snapshot.building)).toBe(true);
      expect(events).toEqual([]);
      expect(isIdle(snapshot)).toBe(false);
    }),
  ));

test("foreign startup playback may lack a record and never receives an invented start time", () =>
  run(
    Effect.gen(function* () {
      const id = fixtureClip().clip_id;
      const { source, fake, events } = yield* setup({
        command: {
          get_state: ({ fake }) =>
            Effect.succeed({
              type: "state_update",
              data: fake.state({ playing: true, playing_clip_id: id }),
            }),
        },
      });
      expect((yield* source.state).playing).toEqual(
        Option.some({ clipId: ClipId.make(id), record: Option.none(), startedAt: Option.none() }),
      );
      const before = fake.calls.length;
      const result = yield* Effect.result(source.setCanvas("1:1"));
      expect(Result.isFailure(result) && refusal(result.failure)).toBe("Busy");
      expect(fake.calls).toHaveLength(before);
      expect(events).toEqual([]);
    }),
  ));

test("source preparation snapshots metadata, preserves URI order and shares one provider submission owner", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, provider } = yield* setup();
      const first = dataUri(pngBytes(64, 48)),
        second = dataUri(pngBytes(48, 64));
      const metadata = { nested: { original: true } },
        hooks: string[] = [];
      const input = member("sequence", true, "final", {
        references: [{ uri: first }, { uri: second }, { uri: first }],
        metadata,
      });
      const prepared = yield* source.prepareRouted(
        { request: input, position: 9999 },
        {
          commit: (id) =>
            Effect.sync(() => {
              hooks.push(`commit:${id}`);
            }),
          result: (id) =>
            Effect.sync(() => {
              hooks.push(`result:${id}`);
            }),
        },
      );
      metadata.nested.original = false;
      expect(fake.uploaded).toEqual([]);
      expect(fake.calls).toHaveLength(2);
      const firstId = yield* prepared.submit;
      expect(firstId).toMatch(/^[0-9a-f-]{36}$/);
      expect(yield* prepared.submit).toBe(firstId);
      expect(hooks).toEqual([`commit:${prepared.id}`, `result:${prepared.id}`]);
      expect((yield* provider.acceptance(prepared.id))?.clip.clip_id).toBe(firstId);
      const call = fake.calls.find((call) => call.command === "enqueue")!;
      const refs = call.args.reference_images as readonly JsonObject[];
      expect(refs[0]).toEqual(refs[2]);
      expect(refs[0]).not.toEqual(refs[1]);
      expect(fake.uploaded).toHaveLength(2);
      expect(call.args.position).toBe(9999);
      expect(call.args.seconds).toBe(7);
      expect(call.args.sequence).toBeUndefined();
      expect(call.args.before).toBeUndefined();
      expect(JSON.parse(textArg(metadataOf(textArg(call.args.metadata)).caller))).toEqual({
        nested: { original: true },
      });
      const clip = (yield* source.state).queued.find((clip) => clip.clipId === firstId)!;
      expect(isLocalClip(clip)).toBe(true);
      expect(clip.durationSeconds).toBeCloseTo(175 / 24, 8);
      expect(clip.request?.metadata).toEqual({ nested: { original: true } });
      expect(clip.request?.sequence).toEqual({ id: "sequence", final: true, memberId: "final" });
      expect((yield* source.state).building).toEqual(Option.none());
    }),
  ));

test("a local clip's record holds the captured request itself, so a lineup knows its own clips", () =>
  run(
    Effect.gen(function* () {
      const { source } = yield* setup();
      const captured = yield* captureRequest(request());
      const prepared = yield* source.prepareRouted({ request: captured, position: undefined });
      const clipId = yield* prepared.submit;
      const clip = (yield* source.state).queued.find((entry) => entry.clipId === clipId)!;
      expect(clip.request).toBe(captured);
    }),
  ));

test("source preparation loads audio URIs and sends them as reference_audios beside the images", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const voice = dataUri(wavBytes(3));
      const id = yield* (yield* source.prepareRouted({
        request: request({
          references: [{ uri: dataUri(pngBytes(32, 32)) }],
          audio: [{ uri: voice }, { uri: voice }],
        }),
        position: undefined,
      })).submit;
      expect(fake.uploaded.map((upload) => upload.mimeType)).toEqual(["image/png", "audio/wav"]);
      const call = fake.calls.find((call) => call.command === "enqueue")!;
      const audio = call.args.reference_audios as readonly JsonObject[];
      expect(audio).toHaveLength(2);
      expect(audio[0]).toEqual(audio[1]);
      const clip = (yield* source.state).queued.find((clip) => clip.clipId === id)!;
      expect(clip.provider.has_reference_audio).toBe(true);
      expect(clip.provider.reference_audio_count).toBe(2);
      expect(clip.request?.audio).toEqual([{ uri: voice }, { uri: voice }]);
    }),
  ));

test("source commit refusal releases its annotation reservation and preserves not-submitted evidence", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup({}, { maxAnnotations: 1 });
      let refused = true;
      const rejected = failure("not-submitted", "router refused");
      const prepared = yield* source.prepareRouted(
        { request: request(), position: undefined },
        { commit: () => (refused ? Effect.fail(rejected) : Effect.void) },
      );
      const first = yield* Effect.result(prepared.submit);
      expect(Result.isFailure(first) && first.failure).toBe(rejected);
      expect((yield* prepared.state)._tag).toBe("Prepared");
      expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
      refused = false;
      yield* prepared.submit;
      const overflow = yield* source.prepareRouted({ request: request(), position: undefined });
      const result = yield* Effect.result(overflow.submit);
      expect(Result.isFailure(result) && refusal(result.failure)).toBe("AnnotationCapacity");
      expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
    }),
  ));

test("cancelled source reference prework remains inert and does not leave a hidden committed submission", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate;
      const { source, fake } = yield* setup({
        upload: () => held.wait.pipe(Effect.andThen(Effect.never)),
      });
      const prepared = yield* source.prepareRouted({
        request: request({ references: [{ uri: dataUri(pngBytes(32, 32)) }] }),
        position: undefined,
      });
      const caller = yield* prepared.submit.pipe(Effect.forkScoped);
      yield* until(() => fake.uploaded.length === 1);
      yield* Fiber.interrupt(caller);
      yield* held.release;
      expect((yield* prepared.state)._tag).toBe("Prepared");
      expect(fake.calls.filter((call) => call.command === "enqueue")).toEqual([]);
    }),
  ));

test("admission-to-ready time is measured on elapsed time, so a wall-clock step leaves it unchanged", () =>
  run(
    Effect.gen(function* () {
      const wall = yield* steppableWall;
      yield* Effect.gen(function* () {
        const { source, fake, events, emit } = yield* setup();
        yield* (yield* source.prepareRouted({ request: request(), position: undefined })).submit;
        yield* wall.step(60_000);
        yield* emit("clip_generated", { clip: { ...fake.accepted[0]!, ready: true } });
        yield* until(() => events.some((event) => event._tag === "Ready"));
        const ready = events.find((event) => event._tag === "Ready");
        const timing = ready?._tag === "Ready" ? ready.timing : undefined;
        expect(timing?._tag).toBe("Bounded");
        expect(timing?._tag === "Bounded" ? timing.admissionToReadyMs : undefined).toBeLessThan(
          10_000,
        );
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));

test("duplicate lifecycle messages emit once, keep observed timing and never infer a Building event", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, events, emit } = yield* setup();
      const id = yield* (yield* source.prepareRouted({ request: request(), position: undefined }))
        .submit;
      const clip = { ...fake.accepted[0]!, ready: true };
      for (let index = 0; index < 2; index++) yield* emit("clip_generated", { clip });
      for (let index = 0; index < 2; index++) yield* emit("clip_started", { clip });
      yield* emit(
        "state_update",
        fake.state({ generation_queued: 0, playing: true, playing_clip_id: id }),
      );
      yield* emit("queue_update", { generation: [], playout: [], history: [] });
      expect(events.filter((event) => event._tag === "Ready")).toHaveLength(1);
      expect(events.filter((event) => event._tag === "Started")).toHaveLength(1);
      expect(events.some((event) => event._tag === "Building")).toBe(false);
      const generated = events.find((event) => event._tag === "Ready");
      expect(generated?._tag === "Ready" && generated.timing._tag).toBe("Bounded");
      const playing = Option.getOrThrow((yield* source.state).playing);
      expect(playing.clipId).toBe(id);
      expect(Option.isSome(playing.startedAt)).toBe(true);
      expect(playing.startedAtMonotonicMillis).toBeTypeOf("number");
      expect(Option.isSome(playing.record)).toBe(true);
      expect(fake.calls.some((call) => call.command === "play")).toBe(false);
    }),
  ));

test("snapshot-only playback changes preserve unknown start time and do not emit synthetic lifecycle or starvation", () =>
  run(
    Effect.gen(function* () {
      const clip = fixtureClip({ ready: true });
      const { source, fake, emit, events } = yield* setup({ initialPlayout: [clip] });
      yield* emit(
        "state_update",
        fake.state({ playing: true, playing_clip_id: clip.clip_id, playout_queued: 0 }),
      );
      yield* emit("queue_update", { generation: [], playout: [], history: [] });
      const playing = Option.getOrThrow((yield* source.state).playing);
      expect(Option.isSome(playing.record)).toBe(true);
      expect(playing.startedAt).toEqual(Option.none());
      expect(playing.startedAtMonotonicMillis).toBeUndefined();
      yield* emit(
        "state_update",
        fake.state({ playing: false, playing_clip_id: null, playout_queued: 0, clips_played: 1 }),
      );
      expect((yield* source.state).playing).toEqual(Option.none());
      expect(events).toEqual([]);
    }),
  ));

test("source stop is faithful and ends playback without autoplay changes, replay or starvation", () =>
  run(
    Effect.gen(function* () {
      const clip = fixtureClip({ ready: true });
      const { source, provider, fake, events } = yield* setup({ initialPlayout: [clip] });
      yield* provider.play(clip.clip_id);
      yield* until(() => events.some((event) => event._tag === "Started"));
      const before = fake.calls.length;
      yield* source.stop;
      yield* until(() => events.some((event) => event._tag === "Ended"));
      expect(fake.calls.slice(before).map((call) => [call.command, call.args])).toEqual([
        ["stop", {}],
      ]);
      expect(events.filter((event) => event._tag === "Ended")).toMatchObject([
        { _tag: "Ended", clipId: ClipId.make(clip.clip_id), termination: "stopped" },
      ]);
      expect(events.some((event) => event._tag === "Starved")).toBe(false);
      expect((yield* source.state).playing).toEqual(Option.none());
    }),
  ));

test("only an observed natural finish followed by an idle autoplay snapshot reports starvation once", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, events, emit } = yield* setup();
      const clip = fixtureClip({ ready: true });
      yield* emit("state_update", fake.state({ autoplay: true }));
      expect(events.some((event) => event._tag === "Starved")).toBe(false);
      yield* emit("clip_started", { clip: { ...clip } });
      yield* emit("clip_finished", { clip: { ...clip }, seconds_sent: clip.seconds });
      yield* emit("state_update", fake.state({ autoplay: true, clips_played: 1 }));
      yield* emit("queue_update", { generation: [], playout: [], history: [] });
      yield* emit("state_update", fake.state({ autoplay: true, clips_played: 1 }));
      expect(events.filter((event) => event._tag === "Starved")).toHaveLength(1);
      expect(events.filter((event) => event._tag === "Ended")).toHaveLength(1);
      expect((yield* source.state).building).toEqual(Option.none());
    }),
  ));

test("foreign generation and failure remain visible with unknown timing and bounded continuation hints", () =>
  run(
    Effect.gen(function* () {
      const { source, emit, events } = yield* setup();
      const clips = Array.from({ length: 9 }, () => fixtureClip({ ready: true }));
      for (const clip of clips) yield* emit("clip_generated", { clip: { ...clip } });
      expect((yield* source.state).continuable).toEqual(
        clips.slice(1).map((clip) => ClipId.make(clip.clip_id)),
      );
      expect(
        events
          .filter((event) => event._tag === "Ready")
          .every((event) => event.timing._tag === "Unknown"),
      ).toBe(true);
      yield* emit("clip_failed", { clip: { ...clips[8]! }, reason: "content policy fixture" });
      const failed = events.find((event) => event._tag === "Failed");
      expect(failed?._tag === "Failed" && failed.reason).toBe("content policy fixture");
      expect((yield* source.state).failed).toEqual([ClipId.make(clips[8]!.clip_id)]);
      expect((yield* source.state).continuable).not.toContain(clips[8]!.clip_id);
    }),
  ));

test("real H3 removal reports only observed generation or playout membership and missing IDs are local failures", () =>
  run(
    Effect.gen(function* () {
      const ready = fixtureClip({ ready: true });
      const { source, fake } = yield* setup({ initialPlayout: [ready] });
      const id = yield* (yield* source.prepareRouted({ request: request(), position: undefined }))
        .submit;
      expect(yield* source.remove(id)).toBe("generation");
      expect(yield* source.remove(ClipId.make(ready.clip_id))).toBe("ready");
      const before = fake.calls.length;
      const result = yield* Effect.result(source.remove(ClipId.make(fixtureClip().clip_id)));
      expect(Result.isFailure(result) && refusal(result.failure)).toBe("NotFound");
      expect(fake.calls).toHaveLength(before);
      expect((yield* source.state).generationOrder).toEqual([]);
    }),
  ));

test("named mutation acknowledgement without its payload remains unknown even when state changed", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup({
        command: { set_canvas: ({ defaults }) => defaults.pipe(Effect.as(undefined)) },
      });
      const outcome = yield* Effect.result(source.setCanvas("9:16"));
      expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("unknown");
      // The model changed the canvas: its state broadcast follows the bare ACK.
      yield* untilEffect(
        source.state.pipe(Effect.map((state) => Option.getOrUndefined(state.canvas) === "9:16")),
      );
      expect(fake.calls.filter((call) => call.command === "set_canvas")).toHaveLength(1);
    }),
  ));

test("attached cleanup preserves the exact canonical report and cannot reset even with an explicit reset option", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, closedLease } = yield* setup({}, { resetOnClose: true });
      const report = yield* source.close;
      expect(report.lease).toBe(closedLease()!);
      expect(report.policy).toEqual([]);
      expect(report.lease.remote.attempted).toBe(false);
      expect(report.lease.ownership).toBe("attached");
      expect(yield* source.close).toBe(report);
      expect(fake.lifecycleCalls.close).toBe(1);
      expect(fake.calls.some((call) => call.command === "reset")).toBe(false);
    }),
  ));

for (const reset of [false, true] as const)
  test(`owned cleanup ${reset ? "records bounded reset failure" : "leaves reset absent by default"} and still closes the canonical session`, () =>
    run(
      Effect.gen(function* () {
        const { source, fake, closedLease } = yield* setup(
          { command: { reset: () => Effect.never } },
          reset ? { resetOnClose: true } : {},
          "owned",
        );
        const report = yield* source.close;
        expect(report.lease).toBe(closedLease()!);
        expect(report.lease.ownership).toBe("owned");
        expect(fake.lifecycleCalls.close).toBe(1);
        expect(fake.calls.filter((call) => call.command === "reset")).toHaveLength(reset ? 1 : 0);
        expect(report.policy).toHaveLength(reset ? 1 : 0);
        if (reset) {
          expect(report.policy[0]!.operation).toBe("reset");
          const result = report.policy[0]!.result;
          expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
        }
      }),
    ));

test("source observation fails for malformed provider lifecycle without requiring enqueue or a control caller", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const reader = yield* source.events.pipe(Stream.runDrain, Effect.result, Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* fake.emit("clip_generated", { clip: { clip_id: fixtureClip().clip_id, ready: true } });
      const result = yield* Fiber.join(reader).pipe(Effect.timeout(1000));
      expect(Result.isFailure(result) && result.failure.reason._tag).toBe("Protocol");
      expect((yield* source.state).availability).toBe("Unavailable");
      expect(fake.calls.filter((call) => call.command === "enqueue")).toEqual([]);
    }),
  ));

test("a session-bound source derives its provider and media from the one session", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const acquired: Session[] = [];
      const fromSession = bindSession((session) =>
        Effect.sync(() => {
          acquired.push(session);
          return media;
        }),
      );
      const source = yield* fromSession(fake.session, {
        provider: { replyTimeout: 100, setupTimeout: 1000, reconcileWindow: 20 },
      });
      // One provider view, over the same session, is part of the source.
      expect(source.provider.sessionId).toBe(fake.session.id);
      expect(fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      yield* source.provider.refresh;
      // The media is re-derived from the same session on each read.
      expect(yield* source.media).toMatchObject({ generation: 1n });
      expect(acquired.every((session) => session === fake.session)).toBe(true);
      expect(acquired.length).toBeGreaterThanOrEqual(2);
      // Closing the source closes the session.
      yield* source.close;
      expect(fake.lifecycleCalls.close).toBe(1);
    }),
  ));

test("a session-bound source checks its media before any provider command", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const fromSession = bindSession(() =>
        Effect.fail(
          ReactorError.fromCode("UnsupportedCapability", "no decoded media on this host", {
            outcome: "not-submitted",
          }),
        ),
      );
      const result = yield* Effect.result(fromSession(fake.session));
      expect(Result.isFailure(result) && result.failure.reason._tag).toBe("UnsupportedCapability");
      expect(fake.calls).toEqual([]);
      expect(fake.lifecycleCalls.close).toBe(0);
    }),
  ));

test("source refuses mismatched session identity before taking session ownership", () =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const provider = yield* H3.make(fake.session);
      const result = yield* Effect.result(
        fromH3({ ...fake.session, id: "different" }, provider, { media: Effect.succeed(media) }),
      );
      expect(Result.isFailure(result) && result.failure.reason._tag).toBe("InvalidInput");
      expect(fake.lifecycleCalls.close).toBe(0);
    }),
  ));

test("deployment duration limits are read live and invalid lengths never upload or dispatch", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup({
        command: {
          get_state: ({ fake }) =>
            Effect.succeed({
              type: "state_update",
              data: fake.state({ clip_seconds: 7, clip_seconds_min: 6, clip_seconds_max: 12 }),
            }),
        },
      });
      for (const durationSeconds of [5.5, 14]) {
        const prepared = yield* source.prepareRouted({
          request: request({ durationSeconds }),
          position: undefined,
        });
        const outcome = yield* Effect.result(prepared.submit);
        expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("not-submitted");
        expect((yield* prepared.state)._tag).toBe("Prepared");
      }
      expect(fake.calls.filter((call) => call.command === "enqueue")).toEqual([]);
      expect(fake.uploaded).toEqual([]);
    }),
  ));

test("pop ACK without a named model reply cannot claim removal or trigger a retry", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup({ command: { pop: () => Effect.undefined } });
      const clip = yield* (yield* source.prepareRouted({ request: request(), position: undefined }))
        .submit;
      const result = yield* Effect.result(source.remove(clip));
      expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
      expect(fake.calls.filter((call) => call.command === "pop")).toHaveLength(1);
      expect((yield* source.state).generationOrder).toContain(clip);
    }),
  ));

test("one stalled source observer fails with Overflow without blocking other observers or provider dispatch", () =>
  run(
    Effect.gen(function* () {
      const { source, fake, events } = yield* setup({}, { maxAnnotations: 512 });
      const held = yield* gate;
      let entered = false;
      const reader = yield* source.events.pipe(
        Stream.runForEach(() => {
          if (entered) return Effect.void;
          entered = true;
          return held.wait;
        }),
        Effect.result,
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      const first = fixtureClip({ ready: true });
      yield* fake.emit("clip_generated", { clip: { ...first } });
      yield* until(() => entered);
      for (let index = 0; index < 300; index++) {
        yield* fake.emit("clip_generated", { clip: { ...fixtureClip({ ready: true }) } });
        yield* Effect.yieldNow;
      }
      yield* until(() => events.filter((event) => event._tag === "Ready").length === 301);
      yield* held.release;
      const result = yield* Fiber.join(reader).pipe(Effect.timeout(1000));
      expect(Result.isFailure(result) && result.failure.reason._tag).toBe("Overflow");
      // Foreign lifecycle records require the provider's explicit full-snapshot barrier.
      yield* source.refresh;
      const accepted = yield* (yield* source.prepareRouted({
        request: request(),
        position: undefined,
      })).submit;
      expect(accepted).toBe(ClipId.make(fake.accepted[0]!.clip_id));
      expect((yield* source.state).availability).toBe("Ready");
    }),
  ));

test("orchestration establishes autoplay before the first H3 admission without changing flush policy", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const handle = yield* Renewal.make({
        open: Effect.succeed({ source, lifetime: "Infinity" }),
      });
      yield* handle.engine.enqueue(request());
      const commands = fake.calls.map((call) => call.command);
      expect(commands.slice(0, 5)).toEqual([
        "get_state",
        "get_queue",
        "set_autoplay",
        "set_autoplay",
        "enqueue",
      ]);
      expect(
        fake.calls
          .filter((call) => call.command === "set_autoplay")
          .map((call) => call.args.enabled),
      ).toEqual([false, true]);
      expect(commands).not.toContain("set_flush_on_clip_end");
      expect(commands).not.toContain("play");
      yield* handle.close;
      expect(fake.lifecycleCalls.close).toBe(1);
    }),
  ));

for (const outcome of ["replied", "unknown", "not-submitted", "ack", "timeout"] as const)
  test(`explicit autoplay setup ${outcome} fails acquisition and still returns attached cleanup evidence`, () =>
    run(
      Effect.gen(function* () {
        const { source, fake, closedLease } = yield* setup({
          command: {
            set_autoplay: ({ fail }) =>
              outcome === "timeout"
                ? Effect.never
                : outcome === "ack"
                  ? Effect.undefined
                  : Effect.fail(fail(outcome)),
          },
        });
        const result = yield* Effect.result(
          Renewal.make({ open: Effect.succeed({ source, lifetime: "Infinity" }) }),
        );
        expect(Result.isFailure(result)).toBe(true);
        expect(fake.lifecycleCalls.close).toBe(1);
        expect(closedLease()?.ownership).toBe("attached");
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(0);
        expect(fake.calls.filter((call) => call.command === "reset")).toHaveLength(0);
      }),
    ));

test("explicit source reconnect calls the canonical session once and refreshes both provider snapshots", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup();
      const before = fake.calls.length;
      yield* source.reconnect;
      expect(fake.lifecycleCalls.reconnect).toBe(1);
      expect(fake.calls.slice(before).map((call) => call.command)).toEqual([
        "get_state",
        "get_queue",
      ]);
      expect(source.id).toBe(fake.session.id);
    }),
  ));

test("early observed start and finish survive late enqueue acceptance without synthesized ready or build events", () =>
  run(
    Effect.gen(function* () {
      const { source, events } = yield* setup({
        command: {
          enqueue: ({ fake, call }) =>
            Effect.gen(function* () {
              const clip = fixtureClip({
                prompt: textArg(call.args.prompt),
                metadata: textArg(call.args.metadata),
                ready: true,
              });
              yield* fake.emit("clip_started", { clip: { ...clip } });
              yield* fake.emit("clip_finished", { clip: { ...clip }, seconds_sent: clip.seconds });
              yield* fake.emit("state_update", fake.state());
              yield* fake.emit("queue_update", { generation: [], playout: [], history: [] });
              return { type: "clip_queued", data: { clip: { ...clip, ready: false } } };
            }),
        },
      });
      const accepted = yield* (yield* source.prepareRouted({
        request: request(),
        position: undefined,
      })).submit;
      yield* until(() => events.some((event) => event._tag === "Ended"));
      expect(events.map((event) => event._tag)).toEqual(["Started", "Ended"]);
      expect(
        events.filter((event) => event._tag === "Started").map((event) => event.clipId),
      ).toEqual([accepted]);
      const state = yield* source.state;
      expect(state.queued).toEqual([]);
      expect(state.ready).toEqual([]);
      expect(state.building).toEqual(Option.none());
      expect(state.playing).toEqual(Option.none());
    }),
  ));

test("orchestration timing retention fails at its own bound without unbounded foreign observations", () =>
  run(
    Effect.gen(function* () {
      const { source, fake } = yield* setup({}, { maxAnnotations: 1 });
      const reader = yield* source.events.pipe(Stream.runDrain, Effect.result, Effect.forkScoped);
      yield* Effect.yieldNow;
      for (let index = 0; index < 3; index++) {
        yield* fake.emit("clip_generated", { clip: { ...fixtureClip({ ready: true }) } });
        yield* Effect.yieldNow;
      }
      const result = yield* Fiber.join(reader).pipe(Effect.timeout(1000));
      expect(Result.isFailure(result) && result.failure.reason._tag).toBe("Overflow");
      expect(Result.isFailure(result) && result.failure.message).toContain(
        "timing observation bound",
      );
    }),
  ));
