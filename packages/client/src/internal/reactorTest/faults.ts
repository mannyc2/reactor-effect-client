/** The faults armed in a simulated Reactor, each counting the occurrences it matches. */
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import type { Fault } from "../../ReactorTest.js";

export const make = (initial: ReadonlyArray<Fault>) =>
  Effect.map(
    Ref.make(initial.map((fault) => ({ fault, seen: 0 }))),
    (armed) =>
      ({
        /** The first fault this occurrence trips, as its `nth` selects; every match counts it. */
        trip: (matches: (fault: Fault) => boolean) =>
          Ref.modify(armed, (all) => {
            let hit: Fault | undefined;
            const next = all.map((entry) => {
              if (!matches(entry.fault)) return entry;
              const seen = entry.seen + 1;
              const nth = "nth" in entry.fault ? entry.fault.nth : undefined;
              if (hit === undefined && (nth === undefined || nth === seen)) hit = entry.fault;
              return { ...entry, seen };
            });
            return [hit, next] as const;
          }),
        /** A standing fault, which applies without being counted. */
        standing: (matches: (fault: Fault) => boolean) =>
          Effect.map(Ref.get(armed), (all) => all.find((entry) => matches(entry.fault))?.fault),
        arm: (fault: Fault) => Ref.update(armed, (all) => [...all, { fault, seen: 0 }]),
      }) as const,
  );

export type Faults = Effect.Success<ReturnType<typeof make>>;
