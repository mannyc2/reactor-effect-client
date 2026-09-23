import * as Effect from "effect/Effect";
import { ReactorError } from "reactor-effect-client";
import type { Track } from "reactor-effect-client";
import { errorOf } from "reactor-effect-client/host";
import type { Peer, PeerEvent, Prepared, Channel } from "reactor-effect-client/host";
export type { Peer, PeerEvent, Prepared, Channel } from "reactor-effect-client/host";
const attempt = <A>(body: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: body, catch: (e) => errorOf(e, "InvalidState") });
export const requireBrowserPeer = (): void => {
  if (typeof RTCPeerConnection !== "function" || typeof MediaStream !== "function")
    throw new ReactorError(
      "UnsupportedHost",
      "Browser transport requires RTCPeerConnection and MediaStream; choose the native peer layer explicitly on a supported server runtime.",
      { outcome: "not-submitted" },
    );
};
export class BrowserPeer implements Peer {
  readonly nativeTracks = true;
  private pc: RTCPeerConnection | undefined;
  private channels: { control: RTCDataChannel; data: RTCDataChannel } | undefined;
  private readonly transceivers = new Map<
    string,
    { transceiver: RTCRtpTransceiver; declared: Track }
  >();
  private readonly received = new Map<string, MediaStreamTrack>();
  private readonly leases = new Set<MediaStreamTrack>();
  private messageLimit = 262_144;
  constructor(private readonly bufferedLimit = 1_048_576) {}
  prepare(
    servers: readonly RTCIceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const pc = yield* attempt(() => {
        requireBrowserPeer();
        self.close();
        const pc = new RTCPeerConnection({ iceServers: [...servers] });
        self.pc = pc;
        // Reliable, ordered defaults; preserve the source's control-before-data ordering.
        const control = pc.createDataChannel("control"),
          data = pc.createDataChannel("data");
        self.channels = { control, data };
        const live = (): boolean => self.pc === pc;
        const wire = (channel: RTCDataChannel, kind: Channel): void => {
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
                error: new ReactorError("Disconnected", `${kind} channel error`),
              });
          };
          channel.onmessage = (event: MessageEvent<unknown>) => {
            if (!live()) return;
            if (!(event.data instanceof ArrayBuffer)) {
              emit({
                type: "error",
                error: new ReactorError("Protocol", "nonbinary channel message"),
              });
              return;
            }
            if (event.data.byteLength > self.messageLimit) {
              emit({
                type: "error",
                error: new ReactorError(
                  "Overflow",
                  "received message exceeds negotiated/local bound",
                ),
              });
              return;
            }
            emit({ type: "message", channel: kind, bytes: new Uint8Array(event.data) });
          };
        };
        wire(control, "control");
        wire(data, "data");
        for (const declared of tracks)
          self.transceivers.set(declared.name, {
            declared,
            transceiver: pc.addTransceiver(declared.kind, { direction: declared.direction }),
          });
        pc.onconnectionstatechange = () => {
          if (live()) emit({ type: "state", state: pc.connectionState });
        };
        pc.onicecandidate = (event) => {
          if (!live()) return;
          if (event.candidate === null) {
            emit({ type: "ice" });
            return;
          }
          const c = event.candidate;
          emit({
            type: "ice",
            candidate: {
              candidate: c.candidate,
              ...(c.sdpMid === null ? {} : { sdp_mid: c.sdpMid }),
              ...(c.sdpMLineIndex === null ? {} : { sdp_mline_index: c.sdpMLineIndex }),
            },
          });
        };
        pc.ontrack = (event) => {
          if (!live()) {
            event.track.stop();
            return;
          }
          const entry = [...self.transceivers.entries()].find(
            ([, item]) => item.transceiver === event.transceiver,
          );
          if (entry === undefined || event.transceiver.mid === null) {
            event.track.stop();
            emit({
              type: "error",
              error: new ReactorError("Protocol", "received track has no declared mapping"),
            });
            return;
          }
          const [name] = entry;
          self.received.get(name)?.stop();
          self.received.set(name, event.track);
          emit({ type: "track", name, mid: event.transceiver.mid });
        };
        return pc;
      });
      return yield* Effect.tryPromise({
        try: async (signal) => {
          const abort = (): void => {
            if (self.pc === pc) self.close();
          };
          signal.addEventListener("abort", abort, { once: true });
          try {
            const offer = await pc.createOffer();
            if (signal.aborted || self.pc !== pc)
              throw new ReactorError("Aborted", "peer preparation interrupted");
            if (offer.sdp === undefined || offer.sdp.length === 0)
              throw new ReactorError("Protocol", "createOffer returned no SDP");
            await pc.setLocalDescription(offer);
            if (signal.aborted || self.pc !== pc)
              throw new ReactorError("Aborted", "peer preparation interrupted");
            const mapping = tracks.map((track) => {
              const mid = self.transceivers.get(track.name)?.transceiver.mid;
              if (mid == null)
                throw new ReactorError(
                  "Protocol",
                  `missing mid after setLocalDescription: ${track.name}`,
                );
              return Object.freeze({ ...track, mid });
            });
            return { sdp: offer.sdp, mapping: Object.freeze(mapping) };
          } finally {
            signal.removeEventListener("abort", abort);
          }
        },
        catch: (e) => errorOf(e, "Disconnected", "prepare peer"),
      });
    });
  }
  answer(sdp: string): Effect.Effect<void, ReactorError> {
    return Effect.tryPromise({
      try: async () => {
        const pc = this.require();
        await pc.setRemoteDescription({ type: "answer", sdp });
        if (this.pc !== pc)
          throw new ReactorError("Disconnected", "peer closed while applying SDP answer");
        const negotiated = pc.sctp?.maxMessageSize;
        this.messageLimit =
          typeof negotiated === "number" && Number.isFinite(negotiated) && negotiated >= 1
            ? Math.min(Math.floor(negotiated), 262_144)
            : 262_144;
      },
      catch: (e) => errorOf(e, "Disconnected", "apply SDP answer"),
    });
  }
  private require(): RTCPeerConnection {
    if (this.pc === undefined) throw new ReactorError("InvalidState", "peer is closed");
    return this.pc;
  }
  private named(name: string): { transceiver: RTCRtpTransceiver; declared: Track } {
    this.require();
    const entry = this.transceivers.get(name);
    if (entry === undefined) throw new ReactorError("InvalidState", `unknown track: ${name}`);
    return entry;
  }
  send(kind: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> {
    return attempt(() => {
      const channel = this.channels?.[kind];
      if (channel === undefined || channel.readyState !== "open")
        throw new ReactorError("Disconnected", `${kind} channel is not open`, {
          outcome: "not-submitted",
        });
      if (
        bytes.byteLength > this.messageLimit ||
        channel.bufferedAmount + bytes.byteLength > this.bufferedLimit
      )
        throw new ReactorError("Overflow", `${kind} message/send buffer bound exceeded`, {
          outcome: "not-submitted",
        });
      try {
        channel.send(bytes);
      } catch (e) {
        throw errorOf(e, "Disconnected", `send ${kind}`);
      }
    });
  }
  lease(name: string): MediaStreamTrack {
    this.require();
    const track = this.received.get(name);
    if (track === undefined || track.readyState !== "live")
      throw new ReactorError("InvalidState", `no live received track: ${name}`);
    const clone = track.clone();
    this.leases.add(clone);
    return clone;
  }
  release(track: MediaStreamTrack): void {
    this.leases.delete(track);
    track.stop();
  }
  direction(name: string, active: boolean): Effect.Effect<void, ReactorError> {
    return attempt(() => {
      const entry = this.named(name);
      entry.transceiver.direction = active ? entry.declared.direction : "inactive";
    });
  }
  replace(name: string, track: MediaStreamTrack | null): Effect.Effect<void, ReactorError> {
    return Effect.tryPromise({
      try: async () => {
        const entry = this.named(name),
          pc = this.require();
        if (entry.declared.direction !== "sendonly")
          throw new ReactorError("InvalidState", `${name} is not an outgoing track`);
        if (track !== null && (track.kind !== entry.declared.kind || track.readyState !== "live"))
          throw new ReactorError("InvalidState", "outgoing track kind/liveness mismatch");
        await entry.transceiver.sender.replaceTrack(track);
        if (this.pc !== pc)
          throw new ReactorError("Disconnected", "peer closed during replaceTrack");
      },
      catch: (e) => errorOf(e, "InvalidState", "replace track"),
    });
  }
  maxBitrate(name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError> {
    return Effect.tryPromise({
      try: async () => {
        const entry = this.named(name);
        if (
          entry.declared.direction !== "sendonly" ||
          !Number.isSafeInteger(bitsPerSecond) ||
          bitsPerSecond < 1
        )
          throw new ReactorError(
            "InvalidState",
            "maxBitrate requires an outgoing track and positive integral bits/sec",
          );
        const sender = entry.transceiver.sender,
          parameters = sender.getParameters();
        if (parameters.encodings.length === 0)
          throw new ReactorError("UnsupportedCapability", "sender has no mutable encodings yet");
        for (const encoding of parameters.encodings) encoding.maxBitrate = bitsPerSecond;
        await sender.setParameters(parameters);
      },
      catch: (e) => errorOf(e, "UnsupportedCapability", "sender bitrate"),
    });
  }
  stats(): Effect.Effect<readonly unknown[], ReactorError> {
    return Effect.tryPromise({
      try: async () => {
        const pc = this.require(),
          report = await pc.getStats();
        if (this.pc !== pc) throw new ReactorError("Disconnected", "stale stats generation");
        const entries: unknown[] = [];
        report.forEach((entry: unknown) => {
          entries.push(entry);
        });
        return entries;
      },
      catch: (e) => errorOf(e, "UnsupportedCapability", "WebRTC statistics"),
    });
  }
  close(): void {
    const pc = this.pc,
      channels = this.channels;
    this.pc = undefined;
    this.channels = undefined;
    if (channels !== undefined)
      for (const channel of [channels.control, channels.data]) {
        channel.onopen = null;
        channel.onclose = null;
        channel.onerror = null;
        channel.onmessage = null;
        channel.close();
      }
    if (pc !== undefined) {
      pc.onconnectionstatechange = null;
      pc.onicecandidate = null;
      pc.ontrack = null;
      pc.close();
    }
    for (const track of this.received.values()) track.stop();
    for (const track of this.leases) track.stop();
    this.received.clear();
    this.leases.clear();
    this.transceivers.clear();
    this.messageLimit = 262_144;
  }
}
