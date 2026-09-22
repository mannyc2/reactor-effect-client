import { expect, test } from "bun:test"
import { Crypto, Deferred, Effect, Fiber, FileSystem, Path, PlatformError, Result, Scope } from "effect"
import { HttpClient } from "effect/unstable/http"
import { h3ReferenceTurboRealtime } from "../../src/ModelProfile.js"
import { loadReferenceBytes, makeReferenceUploader } from "../../src/engine/References.js"
import { dataUri, pngBytes } from "../../testing/Png.js"
import * as TestPlatform from "../../testing/Platform.js"

type TestEnvironment = Scope.Scope | FileSystem.FileSystem | Path.Path | Crypto.Crypto | HttpClient.HttpClient
const run = <A, E>(effect: Effect.Effect<A, E, TestEnvironment>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(TestPlatform.layer))))

const image = dataUri(pngBytes(32, 24))
const upload = (path: string) => Effect.succeed({
  upload_id: "00000000-0000-4000-8000-000000000001",
  name: path,
  mime_type: "image/png",
  size: 1
})

test("reference loading rejects malformed file URLs and oversized data before decoding as typed errors", () =>
  run(Effect.gen(function* () {
    const malformed = yield* Effect.result(loadReferenceBytes("file://[%", { maxBytes: 1024, timeoutMs: 1000 }))
    expect(Result.isFailure(malformed) && malformed.failure._tag).toBe("ReferenceError")
    const oversized = yield* Effect.result(loadReferenceBytes(`data:image/png;base64,${"A".repeat(10_000)}`, { maxBytes: 8, timeoutMs: 1000 }))
    expect(Result.isFailure(oversized) && oversized.failure.reason).toContain("exceeds 8 bytes")
  })))

test("temporary-directory platform failure remains a typed reference setup error with its cause", () =>
  run(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const platformCause = PlatformError.systemError({
      _tag: "PermissionDenied",
      module: "FileSystem",
      method: "makeTempDirectoryScoped",
      description: "fixture denied"
    })
    const failingFs = {
      ...fs,
      makeTempDirectoryScoped: () => Effect.fail(platformCause)
    }
    const result = yield* Effect.result(makeReferenceUploader(upload, {
      profile: h3ReferenceTurboRealtime,
      load: { maxBytes: h3ReferenceTurboRealtime.references.maxBytes, timeoutMs: 1000 }
    }).pipe(Effect.provideService(FileSystem.FileSystem, failingFs)))
    expect(Result.isFailure(result) && result.failure._tag).toBe("ReferenceError")
    if (Result.isFailure(result)) expect(result.failure.cause).toBe(platformCause)
  })))

test("concurrent prepares of the same reference single-flight through the session cache", () =>
  run(Effect.gen(function* () {
    let uploads = 0
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const uploader = yield* makeReferenceUploader((path) => Effect.gen(function* () {
      uploads++
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
      return yield* upload(path)
    }), {
      profile: h3ReferenceTurboRealtime,
      load: { maxBytes: h3ReferenceTurboRealtime.references.maxBytes, timeoutMs: 1000 }
    })
    const first = yield* uploader.prepare([{ uri: image }]).pipe(Effect.forkScoped)
    yield* Deferred.await(entered)
    const second = yield* uploader.prepare([{ uri: image }]).pipe(Effect.forkScoped)
    yield* Deferred.succeed(release, undefined)
    const [a, b] = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
    expect(uploads).toBe(1)
    expect(a[0]?.upload.upload_id).toBe(b[0]?.upload.upload_id)
  })))

test("cancelling an in-flight upload removes its unique staged file", () =>
  run(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const stagedPath = yield* Deferred.make<string>()
    const hold = yield* Deferred.make<void>()
    const staging = yield* fs.makeTempDirectoryScoped({ prefix: "reactor-reference-test-" })
    const uploader = yield* makeReferenceUploader((path) => Effect.gen(function* () {
      yield* Deferred.succeed(stagedPath, path)
      yield* Deferred.await(hold)
      return yield* upload(path)
    }), {
      profile: h3ReferenceTurboRealtime,
      load: { maxBytes: h3ReferenceTurboRealtime.references.maxBytes, timeoutMs: 1000 },
      stagingDirectory: staging
    })
    const pending = yield* uploader.prepare([{ uri: image }]).pipe(Effect.forkScoped)
    const path = yield* Deferred.await(stagedPath)
    expect(yield* fs.exists(path)).toBe(true)
    yield* Fiber.interrupt(pending)
    expect(yield* fs.exists(path)).toBe(false)
  })))
