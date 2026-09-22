import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { ReactorError } from "./errors.js";
import type { IceCandidate, IceServer, Mapping, Track } from "./contract.js";
import type { RawMedia } from "./session/media.js";

export type Channel = "control" | "data";
export type PeerState = "new" | "connecting" | "connected" | "disconnected" | "failed" | "closed";

/** Minimal track lifetime contract. Native peers expose owned frames instead. */
export interface MediaTrack {
  readonly kind: string;
  readonly readyState: "live" | "ended";
  clone(): MediaTrack;
  stop(): void;
}

export type PeerEvent =
  | { readonly type: "state"; readonly state: PeerState }
  | { readonly type: "channel"; readonly channel: Channel; readonly open: boolean }
  | { readonly type: "message"; readonly channel: Channel; readonly bytes: Uint8Array }
  | { readonly type: "ice"; readonly candidate?: IceCandidate }
  | { readonly type: "track"; readonly name: string; readonly mid: string }
  | {
      readonly type: "decoded";
      readonly kind: "video" | "audio";
      readonly name: string;
      readonly mid: string;
    }
  | { readonly type: "error"; readonly error: ReactorError };

export interface Prepared {
  readonly sdp: string;
  readonly mapping: readonly Mapping[];
}

/** Transport only. Coordinator allocation and Reactor protocol state remain session-owned. */
export interface Peer {
  readonly rawMedia?: RawMedia;
  readonly shutdown?: () => Effect.Effect<void, ReactorError>;
  readonly nativeTracks?: boolean;
  readonly mediaSupported?: boolean;
  prepare(
    servers: readonly IceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope>;
  answer(sdp: string): Effect.Effect<void, ReactorError>;
  /** Local submission/transport acknowledgement is never a Reactor application reply. */
  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError>;
  close(): void;
  lease(name: string): MediaTrack;
  release(track: MediaTrack): void;
  direction(name: string, active: boolean): Effect.Effect<void, ReactorError>;
  replace(name: string, track: MediaTrack | null): Effect.Effect<void, ReactorError>;
  maxBitrate(name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError>;
  stats(): Effect.Effect<readonly unknown[], ReactorError>;
}
