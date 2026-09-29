/**
 * The whole channel, offline: a playout on the simulated Reactor, the programme, the
 * encoder (a real ffmpeg) and the HTTP API on an ephemeral port, driven
 * through the typed client derived from the same `Api`. Sessions are short,
 * so a renewal happens within the test.
 */
// Vitest decides which suites to run while it collects them, synchronously,
// so whether ffmpeg is on PATH is asked with a synchronous child process.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { spawnSync } from "node:child_process";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, layer } from "@effect/vitest";
import { ConfigProvider, Effect, FileSystem, Layer, Option, Stream, SubscriptionRef } from "effect";
import { HttpApiClient } from "effect/unstable/httpapi";
import { Api } from "../src/Api.ts";
import type { ChannelEvent } from "../src/Api.ts";
import { Server } from "../src/App.ts";

/**
 * Short sessions and clips: a renewal is prepared 10 s in, and switches once
 * the first session has played out what it holds (the programme keeps that
 * within the lead).
 */
const TestSettings = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return ConfigProvider.layer(
      ConfigProvider.fromUnknown({
        CHANNEL_MODE: "simulated",
        CHANNEL_SESSION_LENGTH: "30 seconds",
        CHANNEL_RENEWAL_LEAD: "20 seconds",
        CHANNEL_CLIP_SECONDS: 5,
        CHANNEL_EVIDENCE_DIR: yield* fs.makeTempDirectoryScoped(),
      }),
    );
  }),
);

const TestServer = Server.pipe(
  Layer.provide(TestSettings),
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provide(NodeServices.layer),
);

/** The top-level MP4 boxes in a byte stream, by type. */
const boxes = (bytes: Stream.Stream<Uint8Array, unknown>) =>
  bytes.pipe(
    Stream.mapAccum(
      () => new Uint8Array(0),
      (pending, chunk) => {
        const buffer = new Uint8Array(pending.length + chunk.length);
        buffer.set(pending);
        buffer.set(chunk, pending.length);
        const view = new DataView(buffer.buffer);
        const types: string[] = [];
        let offset = 0;
        while (buffer.length - offset >= 8 && buffer.length - offset >= view.getUint32(offset)) {
          types.push(String.fromCharCode(...buffer.subarray(offset + 4, offset + 8)));
          offset += view.getUint32(offset);
        }
        return [buffer.slice(offset), types] as const;
      },
    ),
  );

const hasFfmpeg = spawnSync("ffmpeg", ["-version"]).status === 0;

describe.skipIf(!hasFfmpeg)("livestream", () => {
  layer(TestServer, { excludeTestServices: true, timeout: "30 seconds" })((it) => {
    it.effect(
      "accepts a prompt and shows it starting on the event stream",
      () =>
        Effect.gen(function* () {
          const client = yield* HttpApiClient.make(Api);
          // The feed carries events from when it is opened, so it is opened first.
          const events = yield* client.channel.events();
          const submitted = yield* client.channel.submit({
            payload: { prompt: "A red kite over a green hill" },
          });
          assert.strictEqual(submitted._tag, "Accepted");
          if (submitted._tag !== "Accepted") return;
          const started = yield* events.pipe(
            Stream.filter(
              (event): event is Extract<ChannelEvent, { _tag: "Clip" }> =>
                event._tag === "Clip" && event.clipId === submitted.clipId,
            ),
            Stream.filter((event) => event.phase === "Started"),
            Stream.runHead,
          );
          assert.isTrue(Option.isSome(started));
          assert.strictEqual(
            Option.getOrUndefined(started)?.prompt,
            "A red kite over a green hill",
          );
        }),
      60_000,
    );

    it.effect(
      "streams fragmented MP4 without a break across a renewal",
      () =>
        Effect.gen(function* () {
          const client = yield* HttpApiClient.make(Api);
          // One viewer joins before the renewal and keeps reading through it.
          const seen = yield* SubscriptionRef.make<{
            readonly first: ReadonlyArray<string>;
            readonly moofs: number;
          }>({ first: [], moofs: 0 });
          yield* (yield* client.live()).pipe(
            boxes,
            Stream.runForEach((type) =>
              SubscriptionRef.update(seen, ({ first, moofs }) => ({
                first: first.length < 2 ? [...first, type] : first,
                moofs: type === "moof" ? moofs + 1 : moofs,
              })),
            ),
            Effect.forkScoped,
          );
          const renewal = yield* (yield* client.channel.events()).pipe(
            Stream.filter((event) => event._tag === "Renewal" && event.phase === "Switched"),
            Stream.runHead,
          );
          assert.isTrue(Option.isSome(renewal));
          const before = yield* SubscriptionRef.get(seen);
          assert.deepStrictEqual(before.first, ["ftyp", "moov"]);
          assert.isAbove(before.moofs, 0);
          // The next fragment after the switch reaches the same viewer: no break.
          const after = yield* SubscriptionRef.changes(seen).pipe(
            Stream.filter(({ moofs }) => moofs > before.moofs),
            Stream.runHead,
            Effect.timeout("10 seconds"),
          );
          assert.isTrue(Option.isSome(after));
          const status = yield* client.channel.status();
          assert.isNotNull(status.session);
        }),
      60_000,
    );
  });
});
