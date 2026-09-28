/**
 * Reactor's coordinator API served in memory: the routes, headers, status
 * codes and JSON the client uses for an H3 session, answered after the
 * configured latency. Responses are `Uint8Array` bodies, never web streams, so
 * no Promise sits between a request and its reply and `TestClock` stays exact.
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpClient from "effect/unstable/http/HttpClient";
import type * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Authorization, clips, Refusal } from "./sessions.js";
import type { Sessions } from "./sessions.js";

const Token = Schema.Struct({
  authorization_details: Schema.Tuple([Authorization]),
  expires_after: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
});
const Create = Schema.Struct({
  model: Schema.Struct({ name: Schema.String }),
  client_info: Schema.optionalKey(
    Schema.Struct({ sdk_version: Schema.String, sdk_type: Schema.String }),
  ),
  supported_transports: Schema.Array(
    Schema.Struct({ protocol: Schema.String, version: Schema.String }),
  ),
});
const Offer = Schema.Struct({
  sdp_offer: Schema.String,
  track_mapping: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      kind: Schema.String,
      direction: Schema.String,
      mid: Schema.String,
    }),
  ),
});
const UploadRequest = Schema.Struct({
  name: Schema.String,
  mime_type: Schema.String,
  size: Schema.Int.check(Schema.isGreaterThan(0)),
});

/** Where a presigned upload goes: another origin, as hosted Reactor's storage is. */
const storage = "https://uploads.reactor.test";

interface Reply {
  readonly status: number;
  /** JSON, or the bytes of a playlist or a segment. */
  readonly body?: unknown;
  readonly bytes?: { readonly data: Uint8Array; readonly contentType: string };
  /** Seconds, sent as `Retry-After`. */
  readonly retryAfter?: number;
}
const ok = (body: unknown): Reply => ({ status: 200, body });
const media = (data: Uint8Array, contentType: string): Reply => ({
  status: 200,
  bytes: { data, contentType },
});
/** The bearer a request carries. */
const bearerOf = (request: HttpClientRequest.HttpClientRequest): string | undefined =>
  /^Bearer (\S+)$/.exec(
    Option.getOrUndefined(Headers.get(request.headers, "authorization")) ?? "",
  )?.[1];
const refuse = (status: number, code: string, reason: string) =>
  Refusal.make({ status, code, reason });

const text = (request: HttpClientRequest.HttpClientRequest): string => {
  const body = request.body;
  if (body._tag === "Uint8Array") return new TextDecoder().decode(body.body);
  return body._tag === "Raw" && Predicate.isString(body.body) ? body.body : "";
};

const decode = <S extends Schema.Codec<unknown, unknown>>(
  schema: S,
  request: HttpClientRequest.HttpClientRequest,
) =>
  request.pipe(
    text,
    Schema.decodeEffect(Schema.fromJsonString(schema)),
    Effect.mapError(() => refuse(400, "invalid_request", "the body is malformed")),
  );

const route = (
  sessions: Sessions,
  request: HttpClientRequest.HttpClientRequest,
  url: URL,
): Effect.Effect<Reply, Refusal> =>
  Effect.gen(function* () {
    const method = request.method;
    const header = (name: string) => Option.getOrUndefined(Headers.get(request.headers, name));
    if (url.origin === storage) {
      if (method !== "PUT") return yield* refuse(405, "method_not_allowed", "uploads take PUT");
      const received = request.body._tag === "Uint8Array" ? request.body.body.byteLength : 0;
      return yield* Effect.as(sessions.stored(url.pathname.slice(1), received), { status: 200 });
    }
    const jwt = bearerOf(request);
    if (url.origin === clips) {
      const [recording = "", file = ""] = url.pathname.split("/").slice(1);
      if (method !== "GET") return yield* refuse(405, "method_not_allowed", "segments take GET");
      return media(yield* sessions.segment(recording, file), "video/mp4");
    }
    const playlist = /^\/clips\/([\w-]+)\.m3u8$/.exec(url.pathname)?.[1];
    if (playlist !== undefined && method === "GET")
      return Option.match(yield* sessions.playlist(jwt, playlist), {
        // Not ready yet: ask again in a second.
        onNone: (): Reply => ({ status: 202, retryAfter: 1 }),
        onSome: (text) => media(new TextEncoder().encode(text), "application/vnd.apple.mpegurl"),
      });
    if (url.pathname === "/pricing" && method === "GET") return ok(sessions.pricing);
    if (url.pathname === "/tokens" && method === "POST") {
      const body = yield* decode(Token, request);
      const key = header("reactor-api-key");
      return ok(yield* sessions.mint(key, body.authorization_details[0], body.expires_after));
    }
    const [root, id, ...rest] = url.pathname.split("/").slice(1).map(decodeURIComponent);
    if (root !== "sessions") return yield* refuse(404, "not_found", "no such route");
    if (header("reactor-api-version") !== "1")
      return yield* refuse(426, "unsupported_version", "Reactor-API-Version 1 is required");
    if (rest[0] === "transport" && header("reactor-webrtc-version") !== "1.0")
      return yield* refuse(426, "unsupported_version", "Reactor-WebRTC-Version 1.0 is required");
    if (id === undefined || id === "") {
      const body = yield* decode(Create, request);
      const webrtc = body.supported_transports.some(
        ({ protocol, version }) => protocol === "webrtc" && version === "1.0",
      );
      const client =
        body.client_info === undefined
          ? undefined
          : { sdkVersion: body.client_info.sdk_version, sdkType: body.client_info.sdk_type };
      return ok(yield* sessions.create(jwt, body.model.name, webrtc, client));
    }
    if (rest.length === 0 && method === "GET") return ok(yield* sessions.read(jwt, id));
    // Paid runs saw a DELETE answered 200, by token and by API key alike.
    if (rest.length === 0 && method === "DELETE")
      return yield* Effect.as(sessions.remove(jwt, id), { status: 200 });
    if (rest.length === 1 && rest[0] === "uploads" && method === "POST") {
      const body = yield* decode(UploadRequest, request);
      const slot = yield* sessions.upload(jwt, id, body.name, body.size);
      return ok({
        presigned_id: slot,
        presigned_url: `${storage}/${slot}`,
        path: `uploads/${slot}`,
      });
    }
    if (rest[0] !== "transport" || rest[1] !== "webrtc")
      return yield* refuse(404, "not_found", "no such route");
    // `/transport/webrtc/<resource>[/<connection>/<operation>]`
    const cid = Number(rest[3]);
    switch ([method, rest[2], rest[4]].join(" ")) {
      case "GET ice_servers ":
        return ok(yield* sessions.iceServers(jwt, id));
      case "POST connections ":
        return ok(yield* sessions.register(jwt, id));
      case "POST connections ice_candidates":
        return yield* Effect.as(sessions.candidates(jwt, id, cid), { status: 204 });
      case "GET connections sdp_params":
        return Option.match(yield* sessions.answer(jwt, id, cid), {
          onNone: (): Reply => ({ status: 202 }),
          onSome: ok,
        });
      case "POST connections sdp_params":
      case "PUT connections sdp_params": {
        const body = yield* decode(Offer, request);
        return yield* Effect.as(sessions.offer(jwt, id, cid, body.sdp_offer), { status: 204 });
      }
      default:
        return yield* refuse(404, "not_found", "no such route");
    }
  });

/** An `HttpClient` whose every request reaches the simulated coordinator, and is logged. */
export const client = (sessions: Sessions): HttpClient.HttpClient =>
  HttpClient.make((request, url) =>
    Effect.gen(function* () {
      yield* Effect.sleep(yield* sessions.timing.delay("http"));
      const bearer = bearerOf(request);
      const session = /^\/sessions\/([^/]+)/.exec(url.pathname)?.[1];
      yield* sessions.note({
        sessionId: session === undefined ? "" : decodeURIComponent(session),
        kind: "request",
        name: `${request.method} ${url.href}`,
        ...(bearer === undefined
          ? {}
          : { bearer: bearer === sessions.options.apiKey ? "key" : "token" }),
      });
      const reply = yield* route(sessions, request, url).pipe(
        Effect.catchTag("Refusal", ({ status, code, reason, retryAfter }) =>
          Effect.succeed<Reply>({
            status,
            body: {
              error: { code, message: reason },
              ...(retryAfter === undefined ? {} : { retry_after_seconds: retryAfter }),
            },
            ...(retryAfter === undefined ? {} : { retryAfter }),
          }),
        ),
      );
      const headers =
        reply.retryAfter === undefined ? {} : { "retry-after": String(reply.retryAfter) };
      const response =
        reply.bytes !== undefined
          ? HttpServerResponse.uint8Array(reply.bytes.data, {
              status: reply.status,
              headers,
              contentType: reply.bytes.contentType,
            })
          : reply.body === undefined
            ? HttpServerResponse.empty({ status: reply.status, headers })
            : HttpServerResponse.jsonUnsafe(reply.body, { status: reply.status, headers });
      return HttpServerResponse.toClientResponse(response, { request });
    }),
  );
