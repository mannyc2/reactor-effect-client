import { expect, test } from "bun:test";
import {
  Crypto,
  Effect,
  Fiber,
  FileSystem,
  Path,
  PlatformError,
  Result,
  Scope,
  Stream,
} from "effect";
import { HttpClient } from "effect/unstable/http";
import * as H3 from "../../src/h3/index.js";
import { ReactorError } from "../../src/errors.js";
import { loadReferenceBytes } from "../../src/orchestration/references.js";
import { dataUri, pngBytes } from "../../testing/Png.js";
import * as TestPlatform from "../../testing/Platform.js";
import { fixture, gate } from "./ProviderSession.js";
import type { Script } from "./ProviderSession.js";

type Services =
  | Scope.Scope
  | FileSystem.FileSystem
  | Path.Path
  | Crypto.Crypto
  | HttpClient.HttpClient;
const run = <A, E>(effect: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(TestPlatform.layer))));
const waitFor = (predicate: () => boolean) =>
  Effect.gen(function* () {
    while (!predicate()) yield* Effect.sleep(1);
  }).pipe(Effect.timeout(1000));

/** Source files belong to the fixture. No production preparation may stage a copy. */
const setup = (script: Script = {}, stream?: FileSystem.FileSystem["stream"]) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const http = yield* HttpClient.HttpClient;
    const root = yield* path.fromFileUrl(new URL("../../", import.meta.url));
    const work = path.join(root, "work");
    yield* fs.makeDirectory(work, { recursive: true });
    const directory = yield* fs.makeTempDirectoryScoped({
      directory: work,
      prefix: "h3-source-fixture-",
    });
    const reads: string[] = [],
      releases: string[] = [];
    let stagingAttempts = 0;
    const forbiddenStage = () =>
      Effect.suspend(() => {
        stagingAttempts++;
        return Effect.fail(
          PlatformError.systemError({
            _tag: "PermissionDenied",
            module: "FileSystem",
            method: "makeTempFile",
            description: "This fixture permits reading source files only",
          }),
        );
      });
    const readonlyFs: FileSystem.FileSystem = {
      ...fs,
      makeTempFile: forbiddenStage,
      makeTempFileScoped: forbiddenStage,
      makeTempDirectory: forbiddenStage,
      makeTempDirectoryScoped: forbiddenStage,
      writeFile: forbiddenStage,
      writeFileString: forbiddenStage,
      stream: (name, options) => {
        reads.push(name);
        return (stream ?? fs.stream)(name, options).pipe(
          Stream.ensuring(
            Effect.sync(() => {
              releases.push(name);
            }),
          ),
        );
      },
    };
    const fake = yield* fixture(script);
    const provider = yield* H3.make(fake.session, {
      commandTimeoutMs: 1000,
      setupTimeoutMs: 1000,
      reconcileWindowMs: 100,
    });
    const load = (uri: string, maxBytes = H3.referenceLimits.maxBytes) =>
      loadReferenceBytes(uri, { maxBytes, timeoutMs: 1000 }).pipe(
        Effect.provideService(FileSystem.FileSystem, readonlyFs),
        Effect.provideService(Path.Path, path),
        Effect.provideService(HttpClient.HttpClient, http),
      );
    const prepare = (uri: string) =>
      provider.prepareFrom(
        load(uri).pipe(
          Effect.map((bytes): H3.Request => ({
            prompt: "A reference supplied by the caller's URI loader.",
            references: [{ _tag: "Bytes", bytes }],
            seconds: 7,
          })),
        ),
      );
    return {
      fs,
      path,
      directory,
      reads,
      releases,
      fake,
      provider,
      load,
      prepare,
      stagingAttempts: () => stagingAttempts,
      entries: () => fs.readDirectory(directory).pipe(Effect.map((entries) => entries.sort())),
    };
  });

test("URI preparation rereads changed bytes at the same file path and uploads the new content", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup();
      const source = h.path.join(h.directory, "mutable.png");
      const firstBytes = pngBytes(32, 24),
        secondBytes = pngBytes(48, 32);
      yield* h.fs.writeFile(source, firstBytes);
      const first = yield* h.prepare(source);
      expect(h.reads).toEqual([]);
      const a = yield* first.submit;
      yield* h.fs.writeFile(source, secondBytes);
      const b = yield* (yield* h.prepare(source)).submit;
      expect(h.reads).toEqual([source, source]);
      expect(h.releases).toEqual([source, source]);
      expect(h.fake.uploaded.map((file) => file.bytes)).toEqual([firstBytes, secondBytes]);
      expect(a.clip.clip_id).not.toBe(b.clip.clip_id);
      const requests = h.fake.calls.filter((call) => call.command === "enqueue");
      expect(requests[0]!.args.reference_images).not.toEqual(requests[1]!.args.reference_images);
      expect(yield* h.entries()).toEqual(["mutable.png"]);
      expect(h.stagingAttempts()).toBe(0);
    }),
  ));

test("identical bytes at different URIs are reread but share a content-addressed upload", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup();
      const a = h.path.join(h.directory, "first.png"),
        b = h.path.join(h.directory, "second.png");
      const image = pngBytes(32, 24);
      yield* h.fs.writeFile(a, image);
      yield* h.fs.writeFile(b, image);
      yield* (yield* h.prepare(a)).submit;
      yield* (yield* h.prepare(b)).submit;
      expect(h.reads).toEqual([a, b]);
      expect(h.releases).toEqual([a, b]);
      expect(h.fake.uploaded).toHaveLength(1);
      const calls = h.fake.calls.filter((call) => call.command === "enqueue");
      expect(calls[0]!.args.reference_images).toEqual(calls[1]!.args.reference_images);
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["first.png", "second.png"]);
    }),
  ));

test("reference preparation succeeds without writable staging and preserves source files", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup();
      const source = h.path.join(h.directory, "original.png");
      const image = pngBytes(32, 24);
      yield* h.fs.writeFile(source, image);
      yield* (yield* h.prepare(source)).submit;
      expect(yield* h.fs.readFile(source)).toEqual(image);
      expect(h.stagingAttempts()).toBe(0);
      expect(h.releases).toEqual([source]);
      expect(yield* h.entries()).toEqual(["original.png"]);
    }),
  ));

test("corrupt source bytes fail validation before upload or enqueue and leave no staged files", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup();
      const source = h.path.join(h.directory, "not-an-image.png");
      yield* h.fs.writeFile(source, new Uint8Array([1, 2, 3]));
      const submission = yield* h.prepare(source);
      const result = yield* Effect.result(submission.submit);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure.context).toMatchObject({
          operation: "enqueue",
          outcome: "not-submitted",
        });
      expect(h.fake.uploaded).toHaveLength(0);
      expect(h.fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      expect(h.releases).toEqual([source]);
      expect(yield* submission.state).toEqual({ _tag: "Prepared" });
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["not-an-image.png"]);
    }),
  ));

test("interrupted reference IO closes its stream, never enqueues later, and creates no staging files", () =>
  run(
    Effect.gen(function* () {
      let entered = false,
        released = 0;
      const h = yield* setup({}, () =>
        Stream.concat(
          Stream.fromEffect(
            Effect.sync(() => {
              entered = true;
              return new Uint8Array([137]);
            }),
          ),
          Stream.never,
        ).pipe(
          Stream.ensuring(
            Effect.sync(() => {
              released++;
            }),
          ),
        ),
      );
      const source = h.path.join(h.directory, "read-in-progress.png");
      yield* h.fs.writeFile(source, pngBytes(32, 24));
      const submission = yield* h.prepare(source);
      const pending = yield* submission.submit.pipe(Effect.forkScoped);
      yield* waitFor(() => entered);
      yield* Fiber.interrupt(pending);
      expect(released).toBe(1);
      expect(h.releases).toEqual([source]);
      expect(h.fake.uploaded).toHaveLength(0);
      expect(h.fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      expect(yield* submission.state).toEqual({ _tag: "Prepared" });
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["read-in-progress.png"]);
    }),
  ));

test("upload failure leaves a closed source reader and no staging files or generation dispatch", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup({
        upload: () => Effect.fail(new ReactorError("Upload", "fixture upload failed")),
      });
      const source = h.path.join(h.directory, "upload-fails.png");
      yield* h.fs.writeFile(source, pngBytes(32, 24));
      const result = yield* Effect.result((yield* h.prepare(source)).submit);
      expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
      expect(h.fake.uploaded).toHaveLength(1);
      expect(h.fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      expect(h.releases).toEqual([source]);
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["upload-fails.png"]);
    }),
  ));

test("cancelling after file read while upload waits cannot leave temporary files or enqueue", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate();
      const h = yield* setup({ upload: () => held.wait.pipe(Effect.andThen(Effect.never)) });
      const source = h.path.join(h.directory, "upload-waits.png");
      yield* h.fs.writeFile(source, pngBytes(32, 24));
      const submission = yield* h.prepare(source);
      const pending = yield* submission.submit.pipe(Effect.forkScoped);
      yield* waitFor(() => h.fake.uploaded.length === 1);
      expect(h.releases).toEqual([source]);
      yield* Fiber.interrupt(pending);
      yield* held.release;
      expect(yield* submission.state).toEqual({ _tag: "Prepared" });
      expect(h.fake.calls.map((call) => call.command)).toEqual(["get_state", "get_queue"]);
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["upload-waits.png"]);
    }),
  ));

test("the standalone URI loader retains typed malformed-URL and encoded-byte bounds", () =>
  run(
    Effect.gen(function* () {
      const h = yield* setup();
      const malformed = yield* Effect.result(h.load("file://[%"));
      expect(Result.isFailure(malformed) && malformed.failure.context.outcome).toBe(
        "not-submitted",
      );
      expect(Result.isFailure(malformed) && malformed.failure.code).toBe("Upload");
      const oversized = yield* Effect.result(
        h.load(`data:image/png;base64,${"A".repeat(10000)}`, 8),
      );
      expect(Result.isFailure(oversized) && oversized.failure.message).toContain("byte bound");
      const image = pngBytes(32, 24);
      expect(yield* h.load(dataUri(image))).toEqual(image);
      expect(h.fake.uploaded).toHaveLength(0);
      expect(yield* h.entries()).toEqual([]);
      expect(h.stagingAttempts()).toBe(0);
    }),
  ));

test("concurrent submissions single-flight identical reference uploads without a URI cache", () =>
  run(
    Effect.gen(function* () {
      const entered = yield* gate(),
        held = yield* gate();
      const h = yield* setup({
        upload: (name, mimeType, bytes) =>
          entered.release.pipe(
            Effect.andThen(held.wait),
            Effect.as({
              file: {
                upload_id: "11111111-1111-4111-8111-111111111111",
                name,
                mime_type: mimeType,
                size: BigInt(bytes.length),
              },
              transfer: "confirmed" as const,
              notification: "submitted" as const,
            }),
          ),
      });
      const source = h.path.join(h.directory, "same.png");
      yield* h.fs.writeFile(source, pngBytes(32, 24));
      const first = yield* h.prepare(source),
        second = yield* h.prepare(source);
      const a = yield* first.submit.pipe(Effect.forkScoped);
      yield* entered.wait;
      const b = yield* second.submit.pipe(Effect.forkScoped);
      yield* waitFor(() => h.reads.length === 2 && h.releases.length === 2);
      yield* held.release;
      yield* Effect.all([Fiber.join(a), Fiber.join(b)]);
      expect(h.fake.uploaded).toHaveLength(1);
      expect(h.fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(2);
      expect(h.stagingAttempts()).toBe(0);
      expect(yield* h.entries()).toEqual(["same.png"]);
    }),
  ));
