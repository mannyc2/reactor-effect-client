/**
 * The browser's peer: the client's `Peer` port on `RTCPeerConnection`, with the
 * browser's own media tracks. Session allocation, commands and correlation stay
 * in the client; this module carries bytes and tracks.
 */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { IceServer, Track } from "reactor-effect-client/Coordinator";
import { PeerFactory } from "reactor-effect-client/Peer";
import type { Channel, MediaTrack, Peer, PeerEvent, Prepared } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { MessageCode } from "reactor-effect-client/ReactorError";

/** SCTP's usual message bound, lowered by the answer when the remote negotiates less. */
const messageBound = 262_144;
/** How much may wait in a data channel's send buffer before `send` refuses. */
const bufferedBound = 1_048_576;

/** A browser API's expected exception as a failure of `code`; the exception stays in `detail`. */
const platformError =
  (code: MessageCode, operation: string) =>
  (cause: unknown): ReactorError =>
    ReactorError.is(cause)
      ? cause
      : ReactorError.fromCode(code, `${operation} failed`, { operation, detail: cause });

const refused = (code: MessageCode, message: string): ReactorError =>
  ReactorError.fromCode(code, message, { outcome: "not-submitted" });

/**
 * One negotiated connection. Platform callbacks read it synchronously, so its
 * received tracks and message bound are plain fields: this is the DOM seam.
 */
interface Connection {
  readonly pc: RTCPeerConnection;
  readonly channels: Readonly<Record<Channel, RTCDataChannel>>;
  readonly sections: ReadonlyMap<string, Section>;
  readonly received: Map<string, MediaStreamTrack>;
  messageLimit: number;
}

interface Section {
  readonly transceiver: RTCRtpTransceiver;
  readonly declared: Track;
}

const iceServers = (servers: ReadonlyArray<IceServer>): RTCIceServer[] =>
  servers.map((server) => ({
    urls: [...server.urls],
    ...(server.username === undefined ? {} : { username: server.username }),
    ...(server.credential === undefined ? {} : { credential: server.credential }),
  }));

/**
 * A peer for one connection generation. Closing it, or its scope, closes the
 * connection and stops every received track and lease.
 */
export const make: Effect.Effect<Peer, never, Scope.Scope> = Effect.gen(function* () {
  let connection: Connection | undefined;
  const leases = new Set<MediaStreamTrack>();

  const close = (): void => {
    const closing = connection;
    connection = undefined;
    if (closing !== undefined) {
      for (const channel of [closing.channels.control, closing.channels.data]) {
        channel.onopen = null;
        channel.onclose = null;
        channel.onerror = null;
        channel.onmessage = null;
        channel.close();
      }
      closing.pc.onconnectionstatechange = null;
      closing.pc.onicecandidate = null;
      closing.pc.ontrack = null;
      closing.pc.close();
      for (const track of closing.received.values()) track.stop();
    }
    for (const track of leases) track.stop();
    leases.clear();
  };
  yield* Effect.addFinalizer(() => Effect.sync(close));

  const current: Effect.Effect<Connection, ReactorError> = Effect.suspend(() =>
    connection === undefined
      ? Effect.fail(refused("InvalidState", "peer is closed"))
      : Effect.succeed(connection),
  );
  /** The connection is still the one `opened`; a close or a newer prepare replaced it otherwise. */
  const still = (opened: Connection, operation: string): Effect.Effect<void, ReactorError> =>
    Effect.suspend(() =>
      connection === opened
        ? Effect.void
        : Effect.fail(ReactorError.fromCode("Disconnected", `peer closed during ${operation}`)),
    );
  const section = (name: string): Effect.Effect<Section, ReactorError> =>
    Effect.flatMap(current, (opened) => {
      const found = opened.sections.get(name);
      return found === undefined
        ? Effect.fail(refused("InvalidState", `unknown track: ${name}`))
        : Effect.succeed(found);
    });

  const open = (
    servers: ReadonlyArray<IceServer>,
    tracks: ReadonlyArray<Track>,
    emit: (event: PeerEvent) => void,
  ): Connection => {
    close();
    const pc = new RTCPeerConnection({ iceServers: iceServers(servers) });
    // Reliable and ordered by default; Reactor expects control before data.
    const channels = {
      control: pc.createDataChannel("control"),
      data: pc.createDataChannel("data"),
    };
    const sections = new Map<string, Section>();
    for (const declared of tracks)
      sections.set(declared.name, {
        declared,
        transceiver: pc.addTransceiver(declared.kind, { direction: declared.direction }),
      });
    const opened: Connection = {
      pc,
      channels,
      sections,
      received: new Map(),
      messageLimit: messageBound,
    };
    const live = (): boolean => connection === opened;
    for (const kind of ["control", "data"] as const) {
      const channel = channels[kind];
      channel.binaryType = "arraybuffer";
      channel.onopen = () => {
        if (live()) emit({ type: "channel", channel: kind, open: true });
      };
      channel.onclose = () => {
        if (live()) emit({ type: "channel", channel: kind, open: false });
      };
      channel.onerror = () => {
        if (live())
          emit({
            type: "error",
            error: ReactorError.fromCode("Disconnected", `${kind} channel error`),
          });
      };
      channel.onmessage = (event: MessageEvent<unknown>) => {
        if (!live()) return;
        if (!(event.data instanceof ArrayBuffer))
          emit({
            type: "error",
            error: ReactorError.fromCode("Protocol", "nonbinary channel message"),
          });
        else if (event.data.byteLength > opened.messageLimit)
          emit({
            type: "error",
            error: ReactorError.fromCode("Overflow", "received message exceeds its bound"),
          });
        else emit({ type: "message", channel: kind, bytes: new Uint8Array(event.data) });
      };
    }
    pc.onconnectionstatechange = () => {
      if (live()) emit({ type: "state", state: pc.connectionState });
    };
    pc.onicecandidate = (event) => {
      if (!live()) return;
      const candidate = event.candidate;
      if (candidate === null) {
        emit({ type: "ice" });
        return;
      }
      emit({
        type: "ice",
        candidate: {
          candidate: candidate.candidate,
          ...(candidate.sdpMid === null ? {} : { sdp_mid: candidate.sdpMid }),
          ...(candidate.sdpMLineIndex === null ? {} : { sdp_mline_index: candidate.sdpMLineIndex }),
        },
      });
    };
    pc.ontrack = (event) => {
      if (!live()) {
        event.track.stop();
        return;
      }
      const name = [...sections].find(([, entry]) => entry.transceiver === event.transceiver)?.[0];
      const mid = event.transceiver.mid;
      if (name === undefined || mid === null) {
        event.track.stop();
        emit({
          type: "error",
          error: ReactorError.fromCode("Protocol", "received track has no declared mapping"),
        });
        return;
      }
      opened.received.get(name)?.stop();
      opened.received.set(name, event.track);
      emit({ type: "track", name, mid });
    };
    connection = opened;
    return opened;
  };

  const prepare = (
    servers: ReadonlyArray<IceServer>,
    tracks: ReadonlyArray<Track>,
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError> =>
    Effect.gen(function* () {
      const opened = yield* Effect.try({
        try: () => open(servers, tracks, emit),
        catch: platformError("Disconnected", "create peer connection"),
      });
      const offer = yield* Effect.tryPromise({
        try: () => opened.pc.createOffer(),
        catch: platformError("Disconnected", "create offer"),
      }).pipe(Effect.tap(() => still(opened, "prepare")));
      const sdp = offer.sdp;
      if (sdp === undefined || sdp.length === 0)
        return yield* ReactorError.fromCode("Protocol", "createOffer returned no SDP");
      yield* Effect.tryPromise({
        try: () => opened.pc.setLocalDescription(offer),
        catch: platformError("Disconnected", "set local description"),
      });
      yield* still(opened, "prepare");
      const mapping = [];
      for (const track of tracks) {
        const mid = opened.sections.get(track.name)?.transceiver.mid;
        if (mid === null || mid === undefined)
          return yield* ReactorError.fromCode(
            "Protocol",
            `missing mid after setLocalDescription: ${track.name}`,
          );
        mapping.push({ ...track, mid });
      }
      return { sdp, mapping };
    }).pipe(
      // An interrupted preparation leaves no half-made connection behind.
      Effect.onInterrupt(() => Effect.sync(close)),
    );

  const answer = (sdp: string): Effect.Effect<void, ReactorError> =>
    Effect.gen(function* () {
      const opened = yield* current;
      yield* Effect.tryPromise({
        try: () => opened.pc.setRemoteDescription({ type: "answer", sdp }),
        catch: platformError("Disconnected", "apply SDP answer"),
      });
      yield* still(opened, "apply SDP answer");
      const negotiated = opened.pc.sctp?.maxMessageSize;
      opened.messageLimit =
        negotiated !== undefined && Number.isFinite(negotiated) && negotiated >= 1
          ? Math.min(Math.floor(negotiated), messageBound)
          : messageBound;
    });

  const send = (kind: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> =>
    Effect.gen(function* () {
      const opened = yield* current;
      const channel = opened.channels[kind];
      if (channel.readyState !== "open")
        return yield* refused("Disconnected", `${kind} channel is not open`);
      if (
        bytes.byteLength > opened.messageLimit ||
        channel.bufferedAmount + bytes.byteLength > bufferedBound
      )
        return yield* refused("Overflow", `${kind} message or send buffer bound exceeded`);
      yield* Effect.try({
        try: () => channel.send(bytes),
        catch: platformError("Disconnected", `send ${kind}`),
      });
    });

  const lease = (name: string): Effect.Effect<MediaTrack, ReactorError, Scope.Scope> =>
    Effect.acquireRelease(
      Effect.flatMap(current, (opened) => {
        const track = opened.received.get(name);
        if (track?.readyState !== "live")
          return Effect.fail(refused("InvalidState", `no live received track: ${name}`));
        const clone = track.clone();
        leases.add(clone);
        return Effect.succeed(clone);
      }),
      (clone) =>
        Effect.sync(() => {
          leases.delete(clone);
          clone.stop();
        }),
    );

  const replace = (name: string, track: MediaTrack | null): Effect.Effect<void, ReactorError> =>
    Effect.gen(function* () {
      const opened = yield* current;
      const entry = yield* section(name);
      if (entry.declared.direction !== "sendonly")
        return yield* refused("InvalidState", `${name} is not an outgoing track`);
      if (track !== null && (track.kind !== entry.declared.kind || track.readyState !== "live"))
        return yield* refused("InvalidState", "outgoing track kind or liveness mismatch");
      yield* Effect.tryPromise({
        // The session hands this host clones of the application's MediaStreamTracks.
        try: () => entry.transceiver.sender.replaceTrack(track as MediaStreamTrack | null),
        catch: platformError("InvalidState", "replace track"),
      });
      yield* still(opened, "replace track");
    });

  const direction = (name: string, active: boolean): Effect.Effect<void, ReactorError> =>
    Effect.map(section(name), (entry) => {
      entry.transceiver.direction = active ? entry.declared.direction : "inactive";
    });

  const maxBitrate = (name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError> =>
    Effect.gen(function* () {
      const entry = yield* section(name);
      if (
        entry.declared.direction !== "sendonly" ||
        !Number.isSafeInteger(bitsPerSecond) ||
        bitsPerSecond < 1
      )
        return yield* refused(
          "InvalidInput",
          "maxBitrate needs an outgoing track and a positive integral bit rate",
        );
      const sender = entry.transceiver.sender;
      const parameters = sender.getParameters();
      if (parameters.encodings.length === 0)
        return yield* refused("UnsupportedCapability", "sender has no encodings yet");
      for (const encoding of parameters.encodings) encoding.maxBitrate = bitsPerSecond;
      yield* Effect.tryPromise({
        try: () => sender.setParameters(parameters),
        catch: platformError("UnsupportedCapability", "sender bitrate"),
      });
    });

  const stats: Effect.Effect<ReadonlyArray<unknown>, ReactorError> = Effect.gen(function* () {
    const opened = yield* current;
    const report = yield* Effect.tryPromise({
      try: () => opened.pc.getStats(),
      catch: platformError("UnsupportedCapability", "WebRTC statistics"),
    });
    yield* still(opened, "statistics");
    const entries: unknown[] = [];
    report.forEach((entry: unknown) => {
      entries.push(entry);
    });
    return entries;
  });

  return {
    media: { _tag: "Tracks", lease, replace },
    prepare,
    answer,
    send,
    direction,
    maxBitrate,
    stats,
    close: Effect.sync(close),
  };
});

/**
 * The browser's `PeerFactory`. Building it checks for WebRTC, so a browser
 * without it fails there, before any session is allocated:
 * `Reactor.layer().pipe(Layer.provide(BrowserPeer.layer))`.
 */
export const layer: Layer.Layer<PeerFactory, ReactorError> = Layer.effect(
  PeerFactory,
  Effect.suspend(() =>
    typeof RTCPeerConnection === "function" && typeof MediaStream === "function"
      ? Effect.succeed(PeerFactory.of({ check: Effect.void, make }))
      : Effect.fail(
          refused(
            "UnsupportedHost",
            "the browser peer needs RTCPeerConnection and MediaStream; on a server, use the native peer",
          ),
        ),
  ),
);
