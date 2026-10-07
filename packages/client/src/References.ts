/**
 * Reference images and voice samples, loaded from where an application keeps them: a base64
 * `data:` URI, a file, or an http(s) URL. A read takes at most `maxBytes` within its `timeout`,
 * and its errors and spans never name the location, since a signed URL or a path can carry a
 * secret. Each location needs only its own service: a file a `FileSystem`, a URL an `HttpClient`,
 * a `data:` URI none.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as FileSystem from "effect/FileSystem";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as H3 from "./H3.js";
import * as Deadline from "./internal/deadline.js";
import { bodyWithin } from "./internal/http.js";
import { Http, ReactorError } from "./ReactorError.js";

/** A reference's bytes as a base64 `data:` URI's payload holds them. */
export type DataLocation = { readonly _tag: "Data"; readonly base64: Redacted.Redacted<string> };
/** A reference in a file, by its path. */
export type FileLocation = { readonly _tag: "File"; readonly path: string };
/** A reference at an http(s) URL with no credentials in it. */
export type UrlLocation = { readonly _tag: "Url"; readonly url: Redacted.Redacted<URL> };
/** Where a reference's bytes are. */
export type Location = DataLocation | FileLocation | UrlLocation;

export interface ReadOptions {
  /**
   * The most bytes it takes: 16 MiB by default, the most a session uploads unless
   * `Reactor.layer({ maxUploadBytes })` raises it.
   */
  readonly maxBytes?: number | undefined;
  /** How long one read may take: 10 seconds by default. */
  readonly timeout?: Duration.Input | undefined;
}

/** A session's default upload limit, `Reactor`'s `maxUploadBytes`, which this default follows. */
const defaultMaxBytes = 16 * 1024 * 1024;

const context = { operation: "reference" } as const;

const invalid = (message: string): ReactorError =>
  ReactorError.fromCode("InvalidInput", message, context);

const tooLarge = (maxBytes: number): ReactorError =>
  invalid(`the reference is larger than ${maxBytes} bytes`);

/** What a span says of a location: its kind, never the location itself. */
const schemes = { Data: "data", File: "file", Url: "http" } as const;

const reveal = (value: string | Redacted.Redacted<string>): string =>
  Redacted.isRedacted(value) ? Redacted.value(value) : value;

/** A file, by path: absolute, or relative to the process's working directory. */
export const file = (path: string): FileLocation => ({ _tag: "File", path });

/** An http(s) URL with no credentials in it; refused otherwise, without naming it. */
export const url = (
  value: string | Redacted.Redacted<string>,
): Effect.Effect<UrlLocation, ReactorError> => {
  // A URL parser's error quotes the URL, so it is never kept.
  const refused = () =>
    invalid("the reference URL is malformed, not http(s), or carries credentials");
  return Effect.try({ try: () => new URL(reveal(value)), catch: refused }).pipe(
    Effect.filterOrFail(
      (parsed) =>
        (parsed.protocol === "http:" || parsed.protocol === "https:") &&
        parsed.username === "" &&
        parsed.password === "",
      refused,
    ),
    Effect.map((parsed): UrlLocation => ({ _tag: "Url", url: Redacted.make(parsed) })),
  );
};

/** A base64 `data:` URI's payload; its declared type is ignored, as validation reads the bytes. */
const dataUri = (uri: string): Effect.Effect<DataLocation, ReactorError> => {
  const comma = uri.indexOf(",");
  if (comma < 0 || !/;base64$/i.test(uri.slice(0, comma)))
    return Effect.fail(invalid("a reference data URI must be base64"));
  const location: DataLocation = { _tag: "Data", base64: Redacted.make(uri.slice(comma + 1)) };
  return Effect.succeed(location);
};

/** The path a `file:` URI names on this machine, without the slash before a Windows drive. */
const fileUri = (uri: string): Effect.Effect<FileLocation, ReactorError> => {
  const refused = () => invalid("the reference file URI is malformed or names another host");
  return Effect.try({
    try: () => {
      const parsed = new URL(uri);
      return { host: parsed.hostname, path: decodeURIComponent(parsed.pathname) };
    },
    catch: refused,
  }).pipe(
    Effect.filterOrFail(({ host }) => host === "" || host === "localhost", refused),
    Effect.map(({ path }) => file(/^\/[A-Za-z]:\//.test(path) ? path.slice(1) : path)),
  );
};

/** Where `uri` points, by its scheme or, for a path, its shape. */
const locationOf = (uri: string): Effect.Effect<Location, ReactorError> => {
  if (/^data:/i.test(uri)) return dataUri(uri);
  if (/^file:/i.test(uri)) return fileUri(uri);
  if (/^(?:\/|[A-Za-z]:[\\/])/.test(uri)) return Effect.succeed(file(uri));
  if (/^https?:\/\//i.test(uri)) return url(uri);
  return Effect.fail(
    invalid("a reference must be a data URI, a file URI or absolute path, or an http(s) URL"),
  );
};

/**
 * Where `uri` says a reference is: a base64 `data:` URI (its declared type ignored, since
 * validation reads the bytes), a `file://` URI or an absolute path, or an `http(s)://` URL. It
 * reads nothing; its errors never contain `uri`.
 */
export const locate = Effect.fn("References.locate")(function* (
  uri: string | Redacted.Redacted<string>,
): Effect.fn.Return<Location, ReactorError> {
  const location = yield* locationOf(reveal(uri));
  yield* Effect.annotateCurrentSpan("reactor.reference.scheme", schemes[location._tag]);
  return location;
});

/** The bytes base64 text decodes to: three for every four characters, less its padding. */
const decodedLength = (base64: string): number =>
  (base64.length / 4) * 3 - Number(base64.endsWith("=")) - Number(base64.endsWith("=="));

/** A data URI's bytes, refused by their decoded length before anything is decoded. */
const readData = (
  location: DataLocation,
  maxBytes: number,
): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError> => {
  const base64 = Redacted.value(location.base64);
  if (decodedLength(base64) > maxBytes) return Effect.fail(tooLarge(maxBytes));
  return Effect.fromResult(Base64.decode(base64)).pipe(
    Effect.mapBoth({
      onFailure: () => invalid("the data URI is not base64"),
      onSuccess: (bytes) => new Uint8Array(bytes),
    }),
  );
};

/** A file's bytes, refused by its size before it is read, and again if it grew meanwhile. */
const readFile = Effect.fnUntraced(function* (
  location: FileLocation,
  maxBytes: number,
): Effect.fn.Return<Uint8Array<ArrayBuffer>, ReactorError, FileSystem.FileSystem> {
  const fs = yield* FileSystem.FileSystem;
  // A platform error's message names the path, so it is never kept.
  const unreadable = () => invalid("the reference file could not be read");
  const info = yield* fs.stat(location.path).pipe(Effect.mapError(unreadable));
  if (Number(info.size) > maxBytes) return yield* tooLarge(maxBytes);
  const bytes = yield* fs
    .stream(location.path, { bytesToRead: maxBytes + 1 })
    .pipe(Stream.mkUint8Array, Effect.mapError(unreadable));
  if (bytes.length > maxBytes) return yield* tooLarge(maxBytes);
  return new Uint8Array(bytes);
});

/**
 * A download's bytes. It sends no cookies and opens no span of its own, since an HTTP client's
 * span records the URL; redirects are followed.
 */
const readUrl = Effect.fnUntraced(
  function* (
    location: UrlLocation,
    maxBytes: number,
  ): Effect.fn.Return<Uint8Array<ArrayBuffer>, ReactorError, HttpClient.HttpClient> {
    const client = yield* HttpClient.HttpClient;
    // An HTTP client error's message names the URL, so it is never kept.
    const failed = () =>
      ReactorError.make({
        reason: Http.make({ message: "the reference download failed" }),
        context,
      });
    const response = yield* client.get(Redacted.value(location.url)).pipe(Effect.mapError(failed));
    const { status } = response;
    if (status < 200 || status >= 300)
      return yield* ReactorError.make({
        reason: Http.make({
          message: `the reference download was answered with HTTP ${status}`,
          status,
        }),
        context,
      });
    const bytes = yield* bodyWithin(response, maxBytes).pipe(
      Effect.mapError((error) => (ReactorError.is(error) ? tooLarge(maxBytes) : failed())),
    );
    return new Uint8Array(bytes);
  },
  Effect.provideService(FetchHttpClient.RequestInit, { credentials: "omit" }),
  Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
);

const bytesAt = (
  location: Location,
  maxBytes: number,
): Effect.Effect<
  Uint8Array<ArrayBuffer>,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> => {
  switch (location._tag) {
    case "Data":
      return readData(location, maxBytes);
    case "File":
      return readFile(location, maxBytes);
    case "Url":
      return readUrl(location, maxBytes);
  }
};

const readAt = Effect.fn("References.read")(function* (
  location: Location,
  options: ReadOptions = {},
): Effect.fn.Return<
  Uint8Array<ArrayBuffer>,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  yield* Effect.annotateCurrentSpan("reactor.reference.scheme", schemes[location._tag]);
  const maxBytes = options.maxBytes ?? defaultMaxBytes;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1)
    return yield* invalid("maxBytes must be a positive integer");
  const timeout = yield* Deadline.decode("the reference timeout")(options.timeout ?? "10 seconds");
  const bytes = yield* bytesAt(location, maxBytes).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () =>
        Effect.fail(
          ReactorError.fromCode(
            "Timeout",
            `the reference took longer than ${Duration.format(timeout)}`,
            context,
          ),
        ),
    }),
  );
  yield* Effect.annotateCurrentSpan("reactor.reference.bytes", bytes.length);
  return bytes;
});

/** The bytes at `location`, at most `maxBytes`, within `timeout`, as a fresh copy. */
// A location is plain data the caller holds, never an Effect piped into, so reading one has no
// pipeable form; its overloads give each location only the service it needs.
// @effect-diagnostics-next-line missingPipeableSignature:off
export function read(
  location: DataLocation,
  options?: ReadOptions,
): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError>;
export function read(
  location: FileLocation,
  options?: ReadOptions,
): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError, FileSystem.FileSystem>;
export function read(
  location: UrlLocation,
  options?: ReadOptions,
): Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError, HttpClient.HttpClient>;
export function read(
  location: Location,
  options?: ReadOptions,
): Effect.Effect<
  Uint8Array<ArrayBuffer>,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
>;
export function read(
  location: Location,
  options?: ReadOptions,
): Effect.Effect<
  Uint8Array<ArrayBuffer>,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  return readAt(location, options);
}

/** The bytes at a location, or at the one a URI names. */
const bytesFrom = (
  source: Location | string | Redacted.Redacted<string>,
  options: ReadOptions | undefined,
): Effect.Effect<
  Uint8Array<ArrayBuffer>,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> =>
  Predicate.isString(source) || Redacted.isRedacted(source)
    ? Effect.flatMap(locate(source), (location) => read(location, options))
    : read(source, options);

const imageAt = Effect.fn("References.image")(function* (
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.fn.Return<
  H3.ValidatedReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  const bytes = yield* bytesFrom(source, options);
  return yield* H3.validateReference({ _tag: "Bytes", bytes });
});

/**
 * An image H3 takes as a reference, and FastH3 as an opening or closing frame: read, then
 * validated. A string or `Redacted` string is a URI `locate` reads.
 */
// It takes a location as `read` does, so it has no pipeable form either.
// @effect-diagnostics-next-line missingPipeableSignature:off
export function image(
  location: DataLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedReference, ReactorError>;
export function image(
  location: FileLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedReference, ReactorError, FileSystem.FileSystem>;
export function image(
  location: UrlLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedReference, ReactorError, HttpClient.HttpClient>;
export function image(
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.Effect<
  H3.ValidatedReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
>;
export function image(
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.Effect<
  H3.ValidatedReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  return imageAt(source, options);
}

const audioAt = Effect.fn("References.audio")(function* (
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.fn.Return<
  H3.ValidatedAudioReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  const bytes = yield* bytesFrom(source, options);
  return yield* H3.validateAudioReference({ _tag: "Bytes", bytes });
});

/**
 * A voice sample H3 takes: read, then validated. A string or `Redacted` string is a URI `locate`
 * reads.
 */
// It takes a location as `read` does, so it has no pipeable form either.
// @effect-diagnostics-next-line missingPipeableSignature:off
export function audio(
  location: DataLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedAudioReference, ReactorError>;
export function audio(
  location: FileLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedAudioReference, ReactorError, FileSystem.FileSystem>;
export function audio(
  location: UrlLocation,
  options?: ReadOptions,
): Effect.Effect<H3.ValidatedAudioReference, ReactorError, HttpClient.HttpClient>;
export function audio(
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.Effect<
  H3.ValidatedAudioReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
>;
export function audio(
  source: Location | string | Redacted.Redacted<string>,
  options?: ReadOptions,
): Effect.Effect<
  H3.ValidatedAudioReference,
  ReactorError,
  FileSystem.FileSystem | HttpClient.HttpClient
> {
  return audioAt(source, options);
}
