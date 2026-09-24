/**
 * A free local rehearsal of the hosted qualification: a loopback twin of
 * hosted Reactor, its coordinator and an H3 model, and the host peer that
 * reaches it. It imports only the client's public entry points, `effect` and
 * Node builtins, and runs under Bun and Node 22 or newer.
 *
 *   const twin = await startTwin();
 *   const layer = Reactor.layer({ apiUrl: twin.url }).pipe(
 *     Layer.provideMerge(Layer.mergeAll(Reactor.FetchHttp.layer, services, twinPeers(twin.url))),
 *   );
 *
 * Mint with `twin.apiKey`; read media with `mediaGeneration(session)` from
 * `reactor-effect-client/host`, which is what `Native.media` returns.
 */
export { startTwin } from "./server.js";
export type { Twin, TwinFaults, TwinOptions, TwinRate, TwinSession } from "./server.js";
export { twinPeers } from "./peer.js";
