/**
 * Trickled ICE. A generation's gathered candidates wait in its link, at most
 * 256 of them and 256 KiB, until its connection is registered; from then on
 * they are posted in batches of whatever gathered meanwhile, the last batch
 * marking the end of gathering.
 */
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { IceCandidate } from "../../Peer.js";
import { ReactorError } from "../../ReactorError.js";
import { take } from "../queue.js";
import type { Generation } from "./generation.js";
import type { Connection, Core, Link } from "./model.js";
import { isKnown } from "./model.js";

export const make = ({
  core,
  generation,
}: {
  readonly core: Core;
  readonly generation: Generation;
}) => {
  const { current, guard, background } = generation;

  /** Buffers a candidate, or with none the end of gathering; refused past the bounds or the end. */
  const gathered = Effect.fnUntraced(function* (
    c: Connection,
    candidate: IceCandidate | undefined,
  ) {
    const overflow = yield* Ref.modify(c.link, (link) => {
      if (candidate === undefined) return [false, { ...link, iceDone: true }] as const;
      const iceBytes = link.iceBytes + candidate.candidate.length * 2;
      if (link.ice.length >= 256 || iceBytes > 262_144 || link.finalSent)
        return [true, link] as const;
      return [false, { ...link, ice: [...link.ice, candidate], iceBytes }] as const;
    });
    if (overflow)
      return yield* ReactorError.fromCode(
        "Overflow",
        "ICE buffer bound or candidate after the final batch",
      );
    yield* Queue.offer(c.iceWake, undefined);
  });

  /** Posts the batch buffered now, if any: false when there was none. */
  const post = (c: Connection, sessionId: string, connectionId: number) =>
    Ref.modify(c.link, (link) => {
      const final = link.iceDone && !link.finalSent;
      if (link.ice.length === 0 && !final) return [undefined, link] as const;
      return [
        { candidates: link.ice, final },
        { ...link, ice: [], iceBytes: 0 },
      ] as const;
    }).pipe(
      Effect.flatMap((batch) =>
        batch === undefined
          ? Effect.succeed(false)
          : guard(
              c,
              core.signaling.ice(sessionId, connectionId, batch.candidates, batch.final),
            ).pipe(
              Effect.andThen(
                batch.final
                  ? Ref.update(c.link, (link): Link => ({ ...link, finalSent: true }))
                  : Effect.void,
              ),
              Effect.as(true),
            ),
      ),
    );

  /** Posts every buffered batch, once `c`'s connection is registered. */
  const flush = Effect.fnUntraced(function* (c: Connection) {
    yield* current(c);
    const remote = (yield* SubscriptionRef.get(core.state)).remote;
    const connectionId = (yield* Ref.get(c.link)).connectionId;
    if (!isKnown(remote) || connectionId === undefined) return;
    yield* post(c, remote.id, connectionId).pipe(Effect.repeat({ while: (posted) => posted }));
  });

  /** Posts what gathered before registration, then each later batch as it gathers. */
  const trickle = (c: Connection) =>
    flush(c).pipe(
      Effect.andThen(background(c, take(c.iceWake).pipe(Effect.andThen(flush(c)), Effect.forever))),
    );

  return { gathered, trickle };
};

export type Ice = ReturnType<typeof make>;
