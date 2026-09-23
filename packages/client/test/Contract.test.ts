/** Coordinator replies decode through exported Schemas; `raw` keeps what the provider sent. */
import { expect, test } from "vitest";
import { Schema } from "effect";
import * as Reactor from "../src/index.js";
import { ReactorError } from "../src/errors.js";
import { parseAnswer, parseDescriptor, parseIce, parseSessionId } from "../src/contract.js";

const reply = {
  session_id: "session-1",
  state: "READY",
  capabilities: {
    protocol_version: "1.0",
    tracks: [{ name: "main_video", kind: "video", direction: "recvonly" }],
    commands: [{ name: "set_seed", description: null, schema: { type: "object" } }],
    emission_fps: null,
  },
  selected_transport: null,
  future_field: { kept: true },
};

const rejection = (evaluate: () => unknown): ReactorError => {
  try {
    evaluate();
  } catch (cause) {
    if (ReactorError.is(cause)) return cause;
    throw cause;
  }
  throw new Error("decoded");
};

test("a descriptor keeps raw, decodes null as absent, and is frozen", () => {
  const descriptor = parseDescriptor(reply);
  expect(descriptor.raw).toEqual(reply);
  expect(descriptor.raw.selected_transport).toBeNull();
  expect("selected_transport" in descriptor).toBe(false);
  expect("emission_fps" in descriptor.capabilities!).toBe(false);
  expect(descriptor.capabilities?.commands).toEqual([
    { name: "set_seed", schema: { type: "object" } },
  ]);
  expect(Object.isFrozen(descriptor)).toBe(true);
  expect(Object.isFrozen(descriptor.raw)).toBe(true);
  expect(Object.isFrozen(descriptor.capabilities?.tracks[0])).toBe(true);
});

test("the exported schemas describe the decoded shapes", () => {
  const decoded = Schema.decodeUnknownSync(Reactor.SessionDescriptor)(reply);
  expect(decoded).toEqual({
    session_id: "session-1",
    state: "READY",
    capabilities: {
      protocol_version: "1.0",
      tracks: [{ name: "main_video", kind: "video", direction: "recvonly" }],
      commands: [{ name: "set_seed", schema: { type: "object" } }],
    },
  });
  expect(Schema.is(Reactor.Track)({ name: "t", kind: "audio", direction: "sendonly" })).toBe(true);
  expect(
    Schema.is(Reactor.Mapping)({ name: "t", kind: "audio", direction: "sendonly", mid: "0" }),
  ).toBe(true);
});

test("a malformed reply is Protocol, with its SchemaError path and without its values", () => {
  const error = rejection(() =>
    parseDescriptor({
      ...reply,
      capabilities: {
        ...reply.capabilities,
        tracks: [{ ...reply.capabilities.tracks[0], kind: "secret-kind" }],
      },
    }),
  );
  expect(error.reason._tag).toBe("Protocol");
  expect(error.message).toBe("invalid session descriptor");
  expect(Schema.isSchemaError(error.context.detail)).toBe(true);
  expect(String(error.context.detail)).toContain('["capabilities"]["tracks"][0]["kind"]');
  expect(String(error.context.detail)).not.toContain("secret-kind");
  const tracks = Array.from({ length: 65 }, (_, index) => ({
    name: `t${index}`,
    kind: "video",
    direction: "recvonly",
  }));
  expect(
    rejection(() => parseDescriptor({ ...reply, capabilities: { protocol_version: "1", tracks } }))
      .reason._tag,
  ).toBe("Protocol");
  const duplicate = [reply.capabilities.tracks[0], reply.capabilities.tracks[0]];
  expect(
    String(
      rejection(() =>
        parseDescriptor({ ...reply, capabilities: { protocol_version: "1", tracks: duplicate } }),
      ).context.detail,
    ),
  ).toContain("duplicate track names");
});

test("a create reply names its session even when the rest of it is malformed", () => {
  expect(parseSessionId({ session_id: "session-1", capabilities: "every track" })).toBe(
    "session-1",
  );
  expect(rejection(() => parseSessionId({ session_id: "" })).message).toBe("invalid create reply");
});

test("ICE servers and the SDP answer decode null credentials and connection ids as absent", () => {
  expect(
    parseIce({
      ice_servers: [
        { uris: ["stun:stun.fixture"], credentials: null },
        { uris: ["turn:turn.fixture"], credentials: { username: "u", password: "p" } },
      ],
    }),
  ).toEqual([
    { urls: ["stun:stun.fixture"] },
    { urls: ["turn:turn.fixture"], username: "u", credential: "p" },
  ]);
  expect(parseAnswer({ sdp_answer: "v=0", connection_id: null })).toEqual({ sdp_answer: "v=0" });
  expect(parseAnswer({ sdp_answer: "v=0", connection_id: 4_294_967_295 })).toEqual({
    sdp_answer: "v=0",
    connection_id: 4_294_967_295,
  });
  expect(
    rejection(() => parseAnswer({ sdp_answer: "v=0", connection_id: 2 ** 32 })).reason._tag,
  ).toBe("Protocol");
});
