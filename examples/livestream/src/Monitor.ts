import {
  Clock,
  Context,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Stream,
  SubscriptionRef,
} from "effect";
import { Playout } from "reactor-effect-client";
import { ChannelStatus } from "./Api.ts";
import type { AsRunEntry, Outcome, Switch } from "./Api.ts";
import { Broadcast } from "./Broadcast.ts";
import { housePrompt } from "./Channel.ts";
import { Programme } from "./Programme.ts";
import { Settings } from "./Settings.ts";

/** A clip of the as-run log as the events left it; its prompt is read with the status. */
interface Aired {
  readonly key: Playout.ItemKey | null;
  readonly house: number | null;
  readonly startedAt: number | null;
  readonly seconds: number | null;
  readonly session: string | null;
  readonly pictureAt: number | null;
  readonly outcome: Outcome | null;
}

interface Opened {
  readonly openedAt: number;
  readonly endsAt: number | null;
  readonly reconnecting: boolean;
  readonly reconnects: number;
}

/** What the playout's events have told the channel since it started listening. */
interface Seen {
  /** Newest first. */
  readonly asRun: ReadonlyArray<Aired>;
  /** Viewers' items not yet started, by the phase last reported. */
  readonly phases: ReadonlyMap<string, "Accepted" | "Building" | "Ready">;
  readonly sessions: ReadonlyMap<string, Opened>;
  readonly switches: ReadonlyArray<Switch>;
  readonly replaced: number;
  readonly viewers: number;
  readonly offAir: string | null;
}

const initial: Seen = {
  asRun: [],
  phases: new Map(),
  sessions: new Map(),
  switches: [],
  replaced: 0,
  viewers: 0,
  offAir: null,
};

/** A viewer's item by key, or a house clip by index. */
type Which = { readonly key: Playout.ItemKey } | { readonly house: number };

const matches = (aired: Aired, which: Which): boolean =>
  "key" in which ? aired.key === which.key : aired.house === which.house;

const without = <V>(map: ReadonlyMap<string, V>, key: string): ReadonlyMap<string, V> => {
  const rest = new Map(map);
  rest.delete(key);
  return rest;
};

/** The as-run log keeps the latest 30 clips. */
const record = (seen: Seen, aired: Aired): Seen => ({
  ...seen,
  asRun: [aired, ...seen.asRun].slice(0, 30),
});

/** A clip's outcome; one that never started is logged with it. */
const settle = (seen: Seen, which: Which, outcome: Outcome): Seen => {
  const phases = "key" in which ? without(seen.phases, which.key) : seen.phases;
  if (!seen.asRun.some((aired) => matches(aired, which)))
    return record(
      { ...seen, phases },
      {
        key: "key" in which ? which.key : null,
        house: "house" in which ? which.house : null,
        startedAt: null,
        seconds: null,
        session: null,
        pictureAt: null,
        outcome,
      },
    );
  return {
    ...seen,
    phases,
    asRun: seen.asRun.map((aired) => (matches(aired, which) ? { ...aired, outcome } : aired)),
  };
};

const started = (seen: Seen, which: Which, aired: Omit<Aired, "key" | "house">): Seen =>
  record(
    { ...seen, phases: "key" in which ? without(seen.phases, which.key) : seen.phases },
    {
      key: "key" in which ? which.key : null,
      house: "house" in which ? which.house : null,
      ...aired,
    },
  );

const asRun = (seen: Seen, { key, at, status }: Playout.AsRunEvent): Seen => {
  switch (status._tag) {
    case "Accepted":
    case "Building":
    case "Ready":
      return { ...seen, phases: new Map(seen.phases).set(key, status._tag) };
    case "Started":
      return started(
        seen,
        { key },
        {
          startedAt: status.at,
          seconds: status.seconds,
          session: status.sessionId,
          pictureAt: null,
          outcome: null,
        },
      );
    case "Ended":
      return settle(
        seen,
        { key },
        {
          _tag: "Ended",
          at: status.at,
          termination: status.termination,
          airedSeconds: status.airedSeconds,
        },
      );
    case "Dropped":
      return settle(seen, { key }, { _tag: "Dropped", at, reason: status.reason });
    case "Failed":
      return settle(seen, { key }, { _tag: "Failed", at, reason: status.reason._tag });
    case "Unobserved":
      return settle(seen, { key }, { _tag: "Unobserved", at });
    // Until it is terminal, a later fact can still settle it.
    case "Unknown":
      return status.terminal === true ? settle(seen, { key }, { _tag: "Unknown", at }) : seen;
  }
};

const session = (seen: Seen, event: Playout.SessionEvent, now: number): Seen => {
  const update = (sessionId: string, change: (opened: Opened) => Opened): Seen => {
    const opened = seen.sessions.get(sessionId);
    return opened === undefined
      ? seen
      : { ...seen, sessions: new Map(seen.sessions).set(sessionId, change(opened)) };
  };
  switch (event._tag) {
    // The playout counts a session's lifetime from this event; the latest few are kept.
    case "Opened":
      return {
        ...seen,
        sessions: new Map(
          [
            ...seen.sessions,
            [
              event.sessionId,
              {
                openedAt: now,
                endsAt:
                  event.lifetimeSeconds === undefined ? null : now + event.lifetimeSeconds * 1000,
                reconnecting: false,
                reconnects: 0,
              },
            ] as const,
          ].slice(-4),
        ),
      };
    case "Switched":
      return {
        ...seen,
        switches: [
          { from: event.from, to: event.to, at: now, decision: event.decision },
          ...seen.switches,
        ].slice(0, 5),
      };
    case "Replaced":
      return { ...seen, replaced: seen.replaced + 1 };
    case "Reconnecting":
      return update(event.sessionId, (opened) => ({ ...opened, reconnecting: true }));
    case "Reconnected":
      return update(event.sessionId, (opened) => ({
        ...opened,
        reconnecting: false,
        reconnects: opened.reconnects + 1,
      }));
    // A failed setup shows as no new session; a moderated item settles through as-run.
    case "SetupFailed":
    case "Moderated":
      return seen;
  }
};

/** The channel's record after one playout event, observed at `now`. */
const step = (seen: Seen, event: Playout.Event, now: number): Seen => {
  switch (event._tag) {
    case "AsRun":
      return asRun(seen, event.event);
    case "Session":
      return session(seen, event.event, now);
    case "Filler":
      // A house clip that fails on air is reported ended then, with no termination.
      return event.phase === "Started"
        ? started(
            seen,
            { house: event.index },
            {
              startedAt: event.at,
              seconds: event.seconds ?? null,
              session: null,
              pictureAt: null,
              outcome: null,
            },
          )
        : settle(
            seen,
            { house: event.index },
            { _tag: "Ended", at: event.at, termination: null, airedSeconds: null },
          );
    // Read from the playout's state with the status.
    case "Starved":
    case "Cue":
    case "ReaderOverflow":
      return seen;
  }
};

/** A clip up next, as the status lists it. */
type Upcoming = ChannelStatus["upNext"][number];

/** A clip's phase as up next shows it, from how far the forecast has it from air. */
const phaseOf = (state: Playout.ForecastedClip["state"]): "Accepted" | "Building" | "Ready" => {
  switch (state) {
    case "playing":
    case "ready":
      return "Ready";
    case "building":
      return "Building";
    case "queued":
      return "Accepted";
  }
};

/** The clip a start event names, when it names one. */
const startOf = (
  event: Playout.Event,
):
  | { readonly which: Which; readonly at: number; readonly seconds: number | undefined }
  | undefined => {
  if (event._tag === "Filler" && event.phase === "Started")
    return { which: { house: event.index }, at: event.at, seconds: event.seconds };
  if (event._tag === "AsRun" && event.event.status._tag === "Started")
    return {
      which: { key: event.event.key },
      at: event.event.status.at,
      seconds: event.event.status.seconds,
    };
  return undefined;
};

/** The first frame the broadcast received after a clip's reported start, before its end. */
const pictured = (seen: Seen, which: Which, at: number): Seen => ({
  ...seen,
  asRun: seen.asRun.map((aired) =>
    matches(aired, which) &&
    aired.pictureAt === null &&
    (aired.outcome === null || at <= aired.outcome.at)
      ? { ...aired, pictureAt: at }
      : aired,
  ),
});

/** Each session's part in the playout, and the air running dry, by identity only. */
const logged = (event: Playout.Event): Effect.Effect<void> => {
  if (event._tag === "Starved") return Effect.logWarning("nothing left to play");
  if (event._tag !== "Session") return Effect.void;
  const session = event.event;
  switch (session._tag) {
    case "Opened":
      return Effect.logInfo("session opened", {
        sessionId: session.sessionId,
        lifetimeSeconds: session.lifetimeSeconds ?? "uncapped",
      });
    case "Switched":
      return Effect.logInfo("session switched", {
        from: session.from,
        to: session.to,
        decision: session.decision,
      });
    case "Replaced":
      return Effect.logWarning("session replaced", {
        from: session.from,
        carried: session.carried,
      });
    case "SetupFailed":
      return Effect.logWarning("session setup failed", { consecutive: session.consecutive });
    case "Reconnecting":
      return Effect.logWarning("session reconnecting", { sessionId: session.sessionId });
    case "Reconnected":
      return Effect.logInfo("session reconnected", {
        sessionId: session.sessionId,
        afterMillis: session.afterMillis,
      });
    // The verdict's words are the provider's, so only the session is named.
    case "Moderated":
      return Effect.logWarning("session moderated", { sessionId: session.sessionId });
  }
};

/**
 * What the channel has on air, what its playout has lined up, what it aired
 * and the sessions carrying it, for the page and the log. It listens to the
 * playout's events from the start, and reads the rest from the playout's state
 * with each status, so nothing here keeps a schedule of its own.
 */
export class Monitor extends Context.Service<
  Monitor,
  {
    readonly status: Effect.Effect<ChannelStatus>;
    /** The status now, then again at every change the channel observes. */
    readonly feed: Stream.Stream<ChannelStatus>;
  }
>()("reactor-effect-example-livestream/Monitor") {
  static readonly layer = Layer.effect(
    Monitor,
    Effect.gen(function* () {
      const playout = yield* Playout.Playout;
      const programme = yield* Programme;
      const broadcast = yield* Broadcast;
      const { name, mode, lead, clipSeconds } = yield* Settings;
      const seen = yield* SubscriptionRef.make(initial);

      // A provider's start is not proof of picture, so each start waits, for
      // the clip's length, for a frame to reach the broadcast after it.
      const confirm = (which: Which, at: number, seconds: number | undefined) =>
        broadcast.arrivals.pipe(
          Stream.filter((arrival) => arrival >= at),
          Stream.runHead,
          Effect.flatMap((first) =>
            Option.isSome(first)
              ? SubscriptionRef.update(seen, (value) => pictured(value, which, first.value))
              : Effect.void,
          ),
          Effect.timeoutOption(Duration.seconds(seconds ?? clipSeconds)),
        );

      yield* playout.events.pipe(
        Stream.runForEach(
          Effect.fnUntraced(function* (event: Playout.Event) {
            const now = yield* Clock.currentTimeMillis;
            yield* SubscriptionRef.update(seen, (value) => step(value, event, now));
            yield* logged(event);
            const start = startOf(event);
            if (start !== undefined)
              yield* Effect.forkScoped(confirm(start.which, start.at, start.seconds));
          }),
        ),
        Effect.forkScoped,
      );
      yield* broadcast.viewers.pipe(
        Stream.runForEach((viewers) =>
          SubscriptionRef.update(seen, (value) => ({ ...value, viewers })),
        ),
        Effect.forkScoped,
      );
      // Its message is the library's, never provider text.
      yield* playout.failure.pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          SubscriptionRef.update(seen, (value) => ({
            ...value,
            offAir: Exit.isSuccess(exit) ? exit.value.message : "the playout stopped",
          })),
        ),
        Effect.forkScoped,
      );

      // A clip as the status names it, from what the playout's state calls it:
      // a viewer's prompt is the programme's, a house clip's the rotation's.
      const named = (
        clip: Playout.ItemKey | "filler" | "other",
        house: number | null,
      ): Effect.Effect<Pick<AsRunEntry, "key" | "house" | "origin" | "prompt">> => {
        switch (clip) {
          case "filler":
            return Effect.succeed({
              key: null,
              house,
              origin: "house" as const,
              prompt: house === null ? null : housePrompt(house),
            });
          case "other":
            return Effect.succeed({
              key: null,
              house: null,
              origin: "other" as const,
              prompt: null,
            });
          default:
            return Effect.map(programme.prompt(clip), (prompt) => ({
              key: clip,
              house: null,
              origin: "viewer" as const,
              prompt: prompt ?? null,
            }));
        }
      };

      const compose = Effect.fnUntraced(function* (value: Seen) {
        const state = yield* playout.state;
        const at = yield* Clock.currentTimeMillis;

        const entry = (aired: Aired) =>
          Effect.map(named(aired.key ?? "filler", aired.house), (clip): AsRunEntry => ({
            ...aired,
            ...clip,
          }));

        const onAir = state.playing;
        const airing =
          onAir === null
            ? undefined
            : value.asRun.find(
                (aired) =>
                  aired.outcome === null &&
                  (onAir.key === "filler" ? aired.house !== null : aired.key === onAir.key),
              );
        const playing =
          onAir === null
            ? null
            : {
                ...(yield* named(onAir.key, airing?.house ?? null)),
                startedAt: onAir.startedAt,
                seconds: onAir.seconds ?? null,
                pictureAt: airing?.pictureAt ?? null,
              };

        // What the playout projects to air after the clip on air, each with its projected start,
        // then the viewers' items it projects to go without airing.
        const forecast = yield* playout.forecast;
        const whose = (tag: Playout.ClipTag | null) => {
          if (tag === null) return named("other", null);
          return tag._tag === "Item" ? named(tag.key, null) : named("filler", tag.index);
        };
        const projected = forecast.clips
          .filter((clip) => clip.state !== "playing")
          .map((clip) =>
            Effect.map(whose(clip.clip), (shown): Upcoming => ({
              ...shown,
              phase: phaseOf(clip.state),
              afterSwitch: clip.session === "replacement",
              startsAt: clip.startsAt,
            })),
          );
        const dropping = forecast.drops.map((drop) =>
          Effect.map(named(drop.key, null), (shown): Upcoming => ({
            ...shown,
            phase: value.phases.get(drop.key) ?? "Accepted",
            afterSwitch: false,
            startsAt: null,
          })),
        );

        return ChannelStatus.make({
          name,
          mode,
          at,
          offAir: value.offAir,
          viewers: value.viewers,
          playing,
          upNext: yield* Effect.all([...projected, ...dropping]),
          asRun: yield* Effect.forEach(value.asRun, entry),
          sessions: state.sessions.map(({ sessionId, role }) => {
            const opened = value.sessions.get(sessionId);
            const endsAt = opened?.endsAt ?? null;
            return {
              sessionId,
              role,
              openedAt: opened?.openedAt ?? null,
              endsAt,
              renewsAt: endsAt === null ? null : endsAt - Duration.toMillis(lead),
              reconnecting: opened?.reconnecting ?? false,
              reconnects: opened?.reconnects ?? 0,
            };
          }),
          switches: value.switches,
          replaced: value.replaced,
          runwaySeconds: state.runwaySeconds,
          starved: state.starved,
          startWithinSeconds: Duration.toSeconds(programme.startWithin),
        });
      });

      return Monitor.of({
        status: Effect.flatMap(SubscriptionRef.get(seen), compose),
        // A slow reader skips to the newest status rather than queue old ones.
        feed: SubscriptionRef.changes(seen).pipe(
          Stream.buffer({ capacity: 1, strategy: "sliding" }),
          Stream.mapEffect(compose),
        ),
      });
    }),
  );
}
