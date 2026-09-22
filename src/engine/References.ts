import { Crypto, Data, Effect, FileSystem, Option, Path, Ref, Schema, Scope, Semaphore, Stream } from "effect"
import { HttpClient } from "effect/unstable/http"
import type { ReferenceImage } from "../Clip.js"
import type { UploadReference } from "../Model.js"
import type { ModelProfile } from "../ModelProfile.js"

export class ReferenceError extends Data.TaggedError("ReferenceError")<{
  readonly uri: string
  readonly reason: string
  readonly cause?: unknown
}> {}

export class ReferenceUploadError extends Data.TaggedError("ReferenceUploadError")<{
  readonly uri: string
  readonly reason: string
  readonly cause?: unknown
}> {}

export interface ImageFacts {
  readonly mime: "image/jpeg" | "image/png" | "image/webp"
  readonly width: number
  readonly height: number
  readonly bytes: number
}

const be32 = (b: Uint8Array, o: number) => ((b[o]! << 24) >>> 0) + (b[o + 1]! << 16) + (b[o + 2]! << 8) + b[o + 3]!
const be16 = (b: Uint8Array, o: number) => (b[o]! << 8) + b[o + 1]!
const le24 = (b: Uint8Array, o: number) => b[o]! + (b[o + 1]! << 8) + (b[o + 2]! << 16)
const le16 = (b: Uint8Array, o: number) => b[o]! + (b[o + 1]! << 8)

/** Type and dimensions from the bytes themselves, never from a URI suffix or declared MIME type. */
export const sniffImage = (b: Uint8Array): Option.Option<ImageFacts> => {
  const bytes = b.byteLength
  if (bytes >= 24 && b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47 && b[12] === 0x49 && b[13] === 0x48 && b[14] === 0x44 && b[15] === 0x52) {
    return Option.some({ mime: "image/png", width: be32(b, 16), height: be32(b, 20), bytes })
  }
  if (bytes >= 4 && b[0] === 0xff && b[1] === 0xd8) {
    let o = 2
    while (o + 9 < bytes) {
      if (b[o] !== 0xff) return Option.none()
      const marker = b[o + 1]!
      if (marker === 0xd8 || (marker >= 0xd0 && marker <= 0xd7) || marker === 0x01 || marker === 0xff) { o += marker === 0xff ? 1 : 2; continue }
      const len = be16(b, o + 2)
      if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) {
        return Option.some({ mime: "image/jpeg", height: be16(b, o + 5), width: be16(b, o + 7), bytes })
      }
      if (marker === 0xd9 || marker === 0xda) return Option.none()
      o += 2 + len
    }
    return Option.none()
  }
  if (bytes >= 30 && b[0] === 0x52 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x46 && b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50) {
    const chunk = String.fromCharCode(b[12]!, b[13]!, b[14]!, b[15]!)
    if (chunk === "VP8 ") return Option.some({ mime: "image/webp", width: le16(b, 26) & 0x3fff, height: le16(b, 28) & 0x3fff, bytes })
    if (chunk === "VP8L") {
      const bits = b[21]! | (b[22]! << 8) | (b[23]! << 16) | (b[24]! << 24)
      return Option.some({ mime: "image/webp", width: (bits & 0x3fff) + 1, height: ((bits >>> 14) & 0x3fff) + 1, bytes })
    }
    if (chunk === "VP8X") return Option.some({ mime: "image/webp", width: le24(b, 24) + 1, height: le24(b, 27) + 1, bytes })
  }
  return Option.none()
}

export const validateImage = (profile: ModelProfile, uri: string, facts: ImageFacts): Option.Option<ReferenceError> => {
  const limits = profile.references
  if (!limits.mimeTypes.includes(facts.mime)) return Option.some(new ReferenceError({ uri, reason: `${facts.mime} is not accepted` }))
  if (facts.bytes > limits.maxBytes) return Option.some(new ReferenceError({ uri, reason: `${facts.bytes} bytes exceeds ${limits.maxBytes}` }))
  if (facts.width <= 0 || facts.height <= 0) return Option.some(new ReferenceError({ uri, reason: "image has no dimensions" }))
  if (facts.width * facts.height > limits.maxPixels) return Option.some(new ReferenceError({ uri, reason: `${facts.width}x${facts.height} exceeds ${limits.maxPixels} pixels` }))
  const aspect = facts.width / facts.height
  if (aspect < limits.minAspect || aspect > limits.maxAspect) {
    return Option.some(new ReferenceError({ uri, reason: `aspect ${aspect.toFixed(3)} is outside ${limits.minAspect}-${limits.maxAspect}` }))
  }
  return Option.none()
}

export interface LoadLimits {
  readonly maxBytes: number
  readonly timeoutMs: number
}

const concat = (chunks: ReadonlyArray<Uint8Array>, total: number): Uint8Array => {
  const bytes = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
  return bytes
}

const boundedStream = <E, R>(uri: string, stream: Stream.Stream<Uint8Array, E, R>, maxBytes: number) =>
  Stream.runFoldEffect(stream, () => ({ chunks: [] as Array<Uint8Array>, total: 0 }), (acc, chunk) => {
    const total = acc.total + chunk.byteLength
    if (total > maxBytes) return Effect.fail(new ReferenceError({ uri, reason: `exceeds ${maxBytes} bytes` }))
    if (acc.chunks.length >= 16_384) return Effect.fail(new ReferenceError({ uri, reason: "reference body has too many chunks" }))
    acc.chunks.push(chunk)
    return Effect.succeed({ chunks: acc.chunks, total })
  }).pipe(Effect.map(({ chunks, total }) => concat(chunks, total)))

/** Resolve file/data/http references through Effect Platform services with a hard retained-byte bound. */
export const loadReferenceBytes = (
  uri: string,
  limits: LoadLimits
): Effect.Effect<Uint8Array, ReferenceError, FileSystem.FileSystem | Path.Path | HttpClient.HttpClient> =>
  Effect.gen(function*() {
    const fail = (reason: string, cause?: unknown) => new ReferenceError({ uri, reason, ...(cause === undefined ? {} : { cause }) })
    if (!Number.isSafeInteger(limits.maxBytes) || limits.maxBytes <= 0) return yield* fail("maxBytes must be a positive safe integer")
    if (!Number.isFinite(limits.timeoutMs) || limits.timeoutMs <= 0) return yield* fail("timeoutMs must be positive and finite")
    if (uri.startsWith("data:")) {
      const comma = uri.indexOf(",")
      if (comma < 0 || !uri.slice(0, comma).endsWith(";base64")) return yield* fail("only base64 data URIs are supported")
      const encoded = uri.slice(comma + 1)
      if (encoded.length > 4 * Math.ceil(limits.maxBytes / 3)) return yield* fail(`exceeds ${limits.maxBytes} bytes`)
      const decoded = Schema.decodeUnknownResult(Schema.Uint8ArrayFromBase64)(encoded)
      if (decoded._tag === "Failure") return yield* fail("invalid base64 data URI")
      if (decoded.success.byteLength > limits.maxBytes) return yield* fail(`exceeds ${limits.maxBytes} bytes`)
      return decoded.success
    }
    if (uri.startsWith("file://") || uri.startsWith("/")) {
      const fs = yield* FileSystem.FileSystem
      const url = uri.startsWith("/") ? undefined : yield* Effect.try({
        try: () => new URL(uri),
        catch: (cause) => fail("invalid file URI", cause)
      })
      const path = uri.startsWith("/") ? uri : yield* (yield* Path.Path).fromFileUrl(url!).pipe(
        Effect.mapError(() => fail("invalid file URI"))
      )
      return yield* boundedStream(uri, fs.stream(path, { bytesToRead: limits.maxBytes + 1 }), limits.maxBytes).pipe(
        Effect.mapError((error) => error instanceof ReferenceError ? error : fail("file could not be read", error))
      )
    }
    if (uri.startsWith("http://") || uri.startsWith("https://")) {
      const http = HttpClient.withScope(yield* HttpClient.HttpClient)
      return yield* Effect.scoped(Effect.gen(function*() {
        const response = yield* http.get(uri).pipe(Effect.mapError((cause) => fail("download failed", cause)))
        if (response.status < 200 || response.status >= 300) return yield* fail(`download failed with HTTP ${response.status}`)
        const declared = Number(response.headers["content-length"] ?? "0")
        if (Number.isFinite(declared) && declared > limits.maxBytes) return yield* fail(`exceeds ${limits.maxBytes} bytes`)
        return yield* boundedStream(uri, response.stream, limits.maxBytes).pipe(
          Effect.mapError((error) => error instanceof ReferenceError ? error : fail("download failed", error))
        )
      })).pipe(Effect.timeoutOrElse({ duration: limits.timeoutMs, orElse: () => Effect.fail(fail("download timed out")) }))
    }
    return yield* fail("unsupported URI scheme (use file://, data:, http:// or https://)")
  })

export interface PreparedReference {
  readonly upload: UploadReference
  readonly facts: ImageFacts
}

export interface ReferenceUploaderOptions {
  readonly profile: ModelProfile
  readonly load: LoadLimits
  readonly stagingDirectory?: string
}

const hex = (bytes: Uint8Array): string => Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

/** Load, validate, hash, stage and upload each distinct image once per session. */
export const makeReferenceUploader = (
  uploadFile: (path: string) => Effect.Effect<UploadReference, unknown>,
  options: ReferenceUploaderOptions
): Effect.Effect<{
  readonly prepare: (references: ReadonlyArray<ReferenceImage>) => Effect.Effect<ReadonlyArray<PreparedReference>, ReferenceError | ReferenceUploadError>
  readonly uploaded: Effect.Effect<number>
}, ReferenceError, FileSystem.FileSystem | Path.Path | Crypto.Crypto | HttpClient.HttpClient | Scope.Scope> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const crypto = yield* Crypto.Crypto
    const http = yield* HttpClient.HttpClient
    const staging = options.stagingDirectory ?? (yield* fs.makeTempDirectoryScoped({ prefix: "reactor-refs-" }).pipe(
      Effect.mapError((cause) => new ReferenceError({ uri: "(staging)", reason: "could not create staging directory", cause }))
    ))
    const byUri = yield* Ref.make(new Map<string, PreparedReference>())
    const byHash = yield* Ref.make(new Map<string, PreparedReference>())
    const gate = yield* Semaphore.make(1)
    const maxCacheEntries = 256

    const prepareOne = (ref: ReferenceImage): Effect.Effect<PreparedReference, ReferenceError | ReferenceUploadError> =>
      gate.withPermit(Effect.gen(function*() {
        const cached = (yield* Ref.get(byUri)).get(ref.uri)
        if (cached !== undefined) return cached
        if ((yield* Ref.get(byUri)).size >= maxCacheEntries) {
          return yield* new ReferenceError({ uri: ref.uri, reason: `reference cache exceeds ${maxCacheEntries} distinct URIs` })
        }
        const bytes = yield* loadReferenceBytes(ref.uri, options.load).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.provideService(HttpClient.HttpClient, http)
        )
        const facts = sniffImage(bytes)
        if (Option.isNone(facts)) return yield* new ReferenceError({ uri: ref.uri, reason: "not a JPEG, PNG or WebP image" })
        const invalid = validateImage(options.profile, ref.uri, facts.value)
        if (Option.isSome(invalid)) return yield* invalid.value
        const hash = hex(yield* crypto.digest("SHA-256", bytes).pipe(
          Effect.mapError((cause) => new ReferenceError({ uri: ref.uri, reason: `could not hash image: ${String(cause)}` }))
        ))
        const same = (yield* Ref.get(byHash)).get(hash)
        if (same !== undefined) {
          yield* Ref.update(byUri, (m) => new Map(m).set(ref.uri, same))
          return same
        }
        const extension = facts.value.mime === "image/png" ? "png" : facts.value.mime === "image/webp" ? "webp" : "jpg"
        const upload = yield* Effect.acquireUseRelease(
          fs.makeTempFile({ directory: staging, prefix: "reactor-ref-", suffix: `.${extension}` }).pipe(
            Effect.mapError((cause) => new ReferenceError({ uri: ref.uri, reason: "could not allocate staging file", cause }))
          ),
          (staged) => fs.writeFile(staged, bytes).pipe(
            Effect.mapError((cause) => new ReferenceError({ uri: ref.uri, reason: "could not stage the file", cause })),
            Effect.andThen(uploadFile(staged).pipe(Effect.mapError((cause) => new ReferenceUploadError({ uri: ref.uri, reason: "upload failed", cause }))))
          ),
          (staged) => fs.remove(staged, { force: true }).pipe(
            Effect.catch((cause) => Effect.logWarning("reference staging cleanup failed", { cause: String(cause) }))
          )
        )
        const prepared: PreparedReference = { upload, facts: facts.value }
        yield* Ref.update(byHash, (m) => new Map(m).set(hash, prepared))
        yield* Ref.update(byUri, (m) => new Map(m).set(ref.uri, prepared))
        return prepared
      }))

    const prepare = (references: ReadonlyArray<ReferenceImage>) =>
      Effect.forEach(references, prepareOne, { concurrency: 1 })

    return { prepare, uploaded: Ref.get(byHash).pipe(Effect.map((m) => m.size)) }
  })
