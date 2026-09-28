/**
 * The isolated native peer's child process. The parent forks this module for
 * one peer, with advanced serialization on its IPC channel, and it relays one
 * addon `NativePeer` to the parent over Effect RPC. It sees ICE configuration,
 * SDP, channel bytes and media; allocation, credentials, correlation and
 * termination stay in the parent.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { load } from "../addon.js";
import type * as Binding from "../binding.js";
import { open, request, settle } from "../local.js";
import type { Opened } from "../local.js";
import { IsolatedRpcs } from "./protocol.js";

const handlers = IsolatedRpcs.toLayer(
  Effect.gen(function* () {
    const opened = yield* Ref.make<Opened | undefined>(undefined);
    const current = Ref.get(opened).pipe(
      Effect.filterOrFail(
        (peer): peer is Opened => peer !== undefined,
        (): Binding.Failure => ({ class: "Closed", message: "no native peer is open" }),
      ),
    );
    /** One addon call on the open peer; once it reaches this process it runs. */
    const call = (run: (peer: Binding.NativePeer) => Promise<Binding.Reply>) =>
      Effect.flatMap(current, ({ peer }) => settle(() => run(peer)));
    /** One queue, taken only as the parent pulls: a stalled parent leaves the addon's bounds to drop. */
    const queue = <A>(select: (peer: Opened) => Stream.Stream<A>) =>
      current.pipe(Effect.map(select), Stream.unwrap);
    return {
      Open: ({ addon }) =>
        Ref.get(opened).pipe(
          Effect.flatMap((peer) =>
            peer === undefined
              ? load(addon).pipe(
                  Effect.flatMap(open),
                  Effect.flatMap((made) => Ref.set(opened, made)),
                )
              : Effect.void,
          ),
          Effect.mapError(
            (error) =>
              ({
                code: error.reason._tag === "UnsupportedHost" ? "UnsupportedHost" : "Native",
                message: error.message,
              }) as const,
          ),
          Rpc.uninterruptible,
        ),
      Prepare: (payload) =>
        call((peer) => {
          const { servers, tracks } = request(payload);
          return peer.prepare(servers, tracks);
        }).pipe(
          Effect.filterOrFail(
            (reply): reply is Binding.Reply & { readonly prepared: Binding.Prepared } =>
              reply.prepared !== undefined,
            (): Binding.Failure => ({ class: "Protocol", message: "prepare answered no offer" }),
          ),
          Effect.map(({ prepared }) => prepared),
          Rpc.uninterruptible,
        ),
      Answer: ({ sdp }) =>
        call((peer) => peer.answer(sdp)).pipe(Effect.asVoid, Rpc.uninterruptible),
      Direction: ({ name, active }) =>
        call((peer) => peer.direction(name, active)).pipe(Effect.asVoid, Rpc.uninterruptible),
      MaxBitrate: ({ name, bitsPerSecond }) =>
        call((peer) => peer.maxBitrate(name, bitsPerSecond)).pipe(
          Effect.asVoid,
          Rpc.uninterruptible,
        ),
      Send: ({ channel, bytes }) =>
        call((peer) => peer.send(channel, bytes)).pipe(Effect.asVoid, Rpc.uninterruptible),
      Stats: () => call((peer) => peer.stats()).pipe(Effect.map(({ stats }) => stats ?? [])),
      Pressure: () => Effect.map(current, ({ peer }) => peer.pressure()),
      Events: () => queue((peer) => peer.events),
      Video: () => queue((peer) => peer.video),
      Audio: () => queue((peer) => peer.audio),
      // Closing ends every queue's stream, and the join completes the call.
      Shutdown: () =>
        Ref.get(opened).pipe(
          Effect.flatMap((peer) =>
            peer === undefined
              ? Effect.void
              : peer.close.pipe(Effect.andThen(settle(() => peer.peer.shutdown()))),
          ),
          Effect.asVoid,
          Rpc.uninterruptible,
        ),
    };
  }),
);

// The parent owns this process: once its channel closes, nothing can reach this
// peer again, so the child ends at once rather than outlive its parent.
process.once("disconnect", () => {
  process.kill(process.pid, "SIGKILL");
});

// The runner ends when the parent closes the worker, after shutdown.
RpcServer.layer(IsolatedRpcs, { disableFatalDefects: true, disableTracing: true }).pipe(
  Layer.provide(handlers),
  Layer.provide(RpcServer.layerProtocolWorkerRunner),
  Layer.provide(NodeWorkerRunner.layer),
  Layer.launch,
  NodeRuntime.runMain({
    disableErrorReporting: true,
    // The parent ends the runner by closing its worker: that interruption is a
    // clean exit, and it comes at once, before the parent's channel closes and
    // the disconnect guard above would kill this process.
    teardown: (exit, onExit) => {
      const code = Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause) ? 0 : 1;
      onExit(code);
      process.exit(code);
    },
  }),
);
