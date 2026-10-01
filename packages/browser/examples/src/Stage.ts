import { Context } from "effect";
import type { Crypto, Effect, Scope } from "effect";
import type { Reactor } from "reactor-effect-client/Reactor";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import type { Session } from "reactor-effect-client/Session";

/** A fault the Studio can arm in the Reactor beneath it, and what it shows. */
export interface Fault {
  readonly label: string;
  readonly shows: string;
  readonly arm: Effect.Effect<void>;
}

/**
 * What differs between hosted Reactor and the simulated one, so the Studio's
 * own code is the same for both: how a session's media reaches the page
 * (the browser's tracks in media elements, or decoded frames on a canvas),
 * how the page describes the mode, and what faults it can arm.
 */
export class Stage extends Context.Service<
  Stage,
  {
    readonly mode: "Live" | "Offline";
    /** What runs where and what it costs, in a sentence or two. */
    readonly about: string;
    /**
     * Shows the session's current connection generation until the scope
     * closes or the generation ends; the Studio calls it again for the next.
     */
    readonly picture: (session: Session) => Effect.Effect<void, ReactorError, Scope.Scope>;
    /** Plays the current generation's sound the same way. Start it from a click. */
    readonly sound: (session: Session) => Effect.Effect<void, ReactorError, Scope.Scope>;
    /** Hosted Reactor takes none. */
    readonly faults: ReadonlyArray<Fault>;
  }
>()("reactor-effect-browser-examples/Stage") {}

/** What the Studio runs on, which a page provides as one layer, live or offline. */
export type Services = Reactor | Stage | Crypto.Crypto;
