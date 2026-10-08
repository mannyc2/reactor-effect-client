/** Shared policy inputs and the scripted provider used by its property families. */
import { assert } from "@effect/vitest";
import { Array, Config, Effect, Option, Redacted } from "effect";
import * as Policy from "../src/internal/playout/policy.js";
import type { ClipTag, SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { ItemKey } from "../src/Playout.js";
import { CommandFailure, ReactorError } from "../src/ReactorError.js";

const config: Policy.Config = {
  defaultSeconds: 5,
  // The scripted provider builds what is asked.
  builtSeconds: (seconds) => seconds,
  lanes: [
    { name: "urgent", conflict: "queue", cut: true },
    { name: "line", conflict: "queue", cut: false },
  ],
  filler: undefined,
  maxBuildsInFlight: 1,
  maxHistory: 64,
  unknownTimeoutMs: 60_000,
  leadMs: 30_000,
  graceMs: 250,
  maxSetupFailures: 3,
  maxModerations: 2,
};

const key = (value: string) => ItemKey.make(value);
/** Whether two tags name one clip. */
const sameClip = (a: ClipTag, b: ClipTag): boolean =>
  a._tag === "Item"
    ? b._tag === "Item" && a.key === b.key
    : b._tag === "Filler" && a.index === b.index;
// Test fixtures are called directly rather than composed as a public pipeable API.
// @effect-diagnostics-next-line missingPipeableSignature:off
const spec = (name: string, lane = 1, seconds = 5): Policy.Spec => ({
  key: key(name),
  lane,
  request: { prompt: name, seconds },
  seconds,
  fingerprint: name,
  cues: [],
  continuity: false,
  start: { _tag: "Follow" },
});
// Test fixtures are called directly rather than composed as a public pipeable API.
// @effect-diagnostics-next-line missingPipeableSignature:off
const clip = (clipId: string, tag?: ClipTag, seconds = 5): SourceClip => ({ clipId, tag, seconds });
const item = (name: string): ClipTag => ({ _tag: "Item", key: key(name) });
const source = (partial: Partial<SourceState> = {}): SourceState => ({
  available: true,
  building: [],
  ready: [],
  playing: undefined,
  continuable: [],
  ...partial,
});

/** A command that failed: never sent, answered with a refusal, or sent with its reply lost. */
const failed = (outcome: "not-submitted" | "replied" | "unknown"): Policy.CommandResult => ({
  _tag: "Failed",
  cause: CommandFailure.from(
    ReactorError.fromCode("InvalidState", "the provider refused it"),
    outcome === "not-submitted"
      ? { operation: "enqueue", outcome }
      : { operation: "enqueue", outcome, requestId: "request", generation: 1n },
  ),
});
/** The provider failed `value`'s build. */
const buildFailed = (value: SourceClip): SourceEvent => ({
  _tag: "Failed",
  clip: value,
  message: "the build failed",
  provider: Redacted.make("the provider's words"),
});

/**
 * Three builds of the line's items measured at 0.4 s per second, built or requested alike: every
 * lane and filler project with them, and a continued build is projected at 1 s a second.
 */
const lineBuild = { lane: 1, perBuilt: 0.4, perRequested: 0.4 };
const measured: Policy.State = {
  ...Policy.initial,
  samples: {
    build: [lineBuild, lineBuild, lineBuild],
    continued: [],
    length: [],
    overBuilt: [],
    aired: [],
  },
};
/**
 * A provider for the property below: it answers the command in flight as the
 * script says, builds and plays clips when told, opens or refuses the session
 * the plan asks for, and loses sessions. Time passes five seconds at a tick,
 * and to the plan's own deadline at a wake.
 */
const steps = [
  "submit",
  "urgent",
  "withdraw",
  "replace",
  "insert",
  "batch",
  "group",
  "drain",
  "done",
  "unknown",
  "refused",
  "ready",
  "fail",
  "start",
  "end",
  "lost",
  "open",
  "denied",
  "tick",
  "wake",
] as const;
type Script = ReadonlyArray<(typeof steps)[number]>;

/**
 * How long the harness's sessions last, and how long before its cap each is renewed. Sessions
 * of two minutes, renewed 30 s before, leave most scripts one session. Sessions of 20 s,
 * renewed 10 s before, renew and switch within many a script, and two lanes carry commands at
 * once in some.
 */
interface Lifetimes {
  readonly lifetimeMs: number;
  readonly leadMs: number;
}
const lasting: Lifetimes = { lifetimeMs: 120_000, leadMs: 30_000 };
const renewing: Lifetimes = { lifetimeMs: 20_000, leadMs: 10_000 };

const simulate = (script: Script, from: Policy.State, lifetimes: Lifetimes = lasting) => {
  const settings: Policy.Config = {
    ...config,
    leadMs: lifetimes.leadMs,
    filler: {
      floor: 5,
      target: 10,
      clip: ({ index, seconds }) => ({ prompt: `filler ${String(index)}`, seconds }),
      lengths: { min: 5, max: 15 },
      invalid: () => undefined,
      protect: "air",
    },
  };
  let state = from;
  let clock = 0;
  let clips = 0;
  let opened = 0;
  let wanted = 0;
  const actions: Array<Policy.Action> = [];
  const inputs: Array<Policy.Input> = [];
  const sessions = new Map<
    string,
    {
      building: Array<SourceClip>;
      ready: Array<SourceClip>;
      playing: SourceClip | undefined;
      /** Autoplay as the plan last set it, once a change applied; unset before. */
      autoplay?: boolean;
    }
  >();
  const lanes = new Map<string, number>();
  const edits = new Map<
    number,
    {
      readonly batch: boolean;
      readonly keys: Map<number, string>;
      /** For each withdrawal of a group key, the group's parts when it was made. */
      readonly parts: Map<number, ReadonlyArray<string>>;
    }
  >();
  const drains: Array<number> = [];
  const problems: Array<string> = [];
  /** Keys submitted so far, group keys among them, and what a withdrawal may drop. */
  const known: Array<string> = [];
  /**
   * Each group's parts by place, replacements included: a replacement takes
   * its part's place, as 0.7.0 counted "every part's key, replacements included".
   */
  const groups = new Map<string, Array<{ readonly key: string; readonly place: number }>>();
  const partOf = new Map<string, { readonly group: string; readonly place: number }>();
  const named = new Set<string>();
  /** Each replacement's key, and the key it replaces. */
  const replaced = new Map<string, string>();
  /** Each session's command in flight, by session. */
  const outstanding = new Map<string, number>();
  /** Sessions the plan closed: it sends them nothing more. */
  const closed = new Set<string>();
  /** Filler enqueues whose result is still to come, by command. */
  const fillers = new Map<number, { readonly index: number; readonly sessionId: string }>();
  /** Where an enqueue of each filler clip may have applied: in flight, done, or its reply lost. */
  const applied = new Map<number, ReadonlySet<string>>();
  /** When the plan last asked to be woken. */
  let wake: number | undefined;
  /** The wait the latest refused open asked for, and when it was refused. */
  let retryAfter: { readonly at: number; readonly until: number } | undefined;
  /**
   * Failed setups since a session's first clip that the plan may count toward its limit: all but
   * refusals that allocated nothing while the session on air had a clip playing or Ready, which
   * holds the air whatever else, and each session gone before a clip sent to it started.
   */
  let failures = 0;
  const aired = new Set<string>();
  const enqueued = new Set<string>();
  /** Keys submitted to follow a clip, and the clip each follows. */
  const paired = new Map<string, ClipTag>();
  /**
   * Each group member's group and its order in the lane as the plan admitted it: a part's and its
   * replacements' order is their place's, and an insert's falls between the places around it.
   */
  const members = new Map<string, { readonly group: string; readonly order: number }>();
  /**
   * Each group member's seat as the plan kept it, from the end of each step it changed in, by the
   * number of actions then: its order, right behind the later member it sits behind, if any.
   */
  const seats = new Map<
    string,
    Array<{ readonly at: number; readonly seat: ReadonlyArray<number> }>
  >();
  const seatOf = (item: {
    readonly order: number;
    readonly behind?: string | undefined;
  }): ReadonlyArray<number> => {
    const ahead =
      item.behind === undefined ? undefined : state.items.get(ItemKey.make(item.behind));
    return ahead === undefined ? [item.order] : [...seatOf(ahead), item.order];
  };
  /** For each action, the input it answered, by its index in `inputs`. */
  const inputOf: Array<number> = [];
  /**
   * Each clip that started, the clip its item was accepted to follow, and whether the plan had
   * asked for its removal by then.
   */
  const starts: Array<{
    readonly tag: ClipTag | undefined;
    readonly follows: ClipTag | undefined;
    readonly removalAsked: boolean;
  }> = [];
  /** Firm items whose first step at or after their startBy has been taken, by key and order. */
  const due = new Set<string>();
  /**
   * The first step at or after a firm item's startBy withdraws it as late, unless it started, or
   * may have, or a start of it is under way: a play of its clip in flight, or one that succeeded
   * whose start is still to be seen. A withdrawal that came first keeps its reason, as can one the
   * step's own input makes, an edit or a drain, or a break of the item's group, or one of an item
   * that follows a clip. An item whose enqueue outcome is unknown may hold a clip the plan cannot
   * name, so it is checked at the first step it is known again, as a read adopts it, unless it
   * settles as unknown: it may have aired unseen. The scripted provider starts clips when the
   * script says, so only the plan's decision is checked, not start times.
   */
  const decideLate = (before: Policy.State, input: Policy.Input) => {
    if (state.closed) return;
    for (const item of state.items.values()) {
      if (item.spec.window?.firm !== true || item.startBy === undefined || clock < item.startBy)
        continue;
      if (item.phase === "Unknown") continue;
      const id = `${item.spec.key}@${String(item.order)}`;
      if (due.has(id)) continue;
      due.add(id);
      if (item.phase === "Started" || item.startedAt !== undefined) continue;
      const settled = item.phase === "Settled" ? item.status?._tag : undefined;
      if (settled === "Unobserved" || settled === "Unknown") continue;
      const lane = state.sessions.find((value) => value.id === item.sessionId);
      const command = lane?.busy?.command;
      if (
        item.clipId !== undefined &&
        ((command?._tag === "Play" && command.clipId === item.clipId) ||
          lane?.played?.clipId === item.clipId)
      )
        continue;
      // Why it went or is going: the reason it was dropped or is withdrawn, or how it settled.
      const status = item.status;
      let reason: string | undefined = item.withdraw;
      if (item.phase === "Settled")
        reason = status?._tag === "Dropped" ? status.reason : status?._tag;
      if (reason === undefined) {
        problems.push(
          `${item.spec.key}, firm and due by ${String(item.startBy)}, was kept at ${String(clock)}, ${item.phase}`,
        );
        continue;
      }
      const was = before.items.get(item.spec.key);
      const fresh =
        was !== undefined &&
        was.order === item.order &&
        was.phase !== "Settled" &&
        was.withdraw === undefined;
      if (
        fresh &&
        reason !== "late" &&
        item.group === undefined &&
        item.spec.follows === undefined &&
        input._tag !== "Edit" &&
        input._tag !== "Drain"
      )
        problems.push(
          `${item.spec.key}, firm and due by ${String(item.startBy)}, went at ${String(clock)} as ${reason}`,
        );
    }
  };

  const send = (input: Policy.Input, at = clock + 7) => {
    clock = at;
    inputs.push(input);
    if (input._tag === "Source" && input.event._tag === "Started") {
      const { clipId, tag } = input.event.clip;
      starts.push({
        tag,
        follows: tag?._tag === "Item" ? state.items.get(tag.key)?.spec.follows : undefined,
        removalAsked: actions.some(
          (action) =>
            action._tag === "Command" &&
            action.command._tag === "Remove" &&
            action.command.clipId === clipId,
        ),
      });
    }
    if (input._tag === "OpenFailed") {
      if (input.retryAfterMs !== undefined) retryAfter = { at, until: at + input.retryAfterMs };
      const air = sessions.get(state.air ?? "");
      if (input.allocated || !(air?.playing !== undefined || (air?.ready.length ?? 0) > 0))
        failures++;
    }
    if (input._tag === "Source" && input.event._tag === "Started" && !aired.has(input.sessionId)) {
      aired.add(input.sessionId);
      failures = 0;
    }
    if (input._tag === "Result")
      for (const [sessionId, id] of outstanding) if (id === input.id) outstanding.delete(sessionId);
    if (input._tag === "Lost") outstanding.delete(input.sessionId);
    // A filler enqueue refused, sent or not, made no clip there.
    const filler = input._tag === "Result" ? fillers.get(input.id) : undefined;
    if (input._tag === "Result" && filler !== undefined) {
      fillers.delete(input.id);
      const result = input.result;
      if (result._tag === "Failed" && result.cause.context.outcome !== "unknown")
        applied.set(
          filler.index,
          new Set([...(applied.get(filler.index) ?? [])].filter((id) => id !== filler.sessionId)),
        );
    }
    const before = state;
    const result = Policy.step(settings, state, input, { mono: clock, wall: clock });
    state = result.state;
    wake = result.wake;
    decideLate(before, input);
    // Nothing falls due before the wake: a Tick at any instant before it does nothing, and wakes
    // at the same time. It is sampled just after the input, halfway, and just before the wake.
    const gap = result.wake === undefined ? 3_600_000 : result.wake - clock;
    for (const at of new Set([Math.min(1, gap / 2), gap / 2, gap - Math.min(1, gap / 2)])) {
      const quiet = Policy.step(
        settings,
        state,
        { _tag: "Tick" },
        { mono: clock + at, wall: clock + at },
      );
      if (quiet.actions.length > 0 || quiet.wake !== result.wake)
        problems.push(
          `a Tick ${String(at)} ms after ${input._tag} at ${String(clock)}, before its wake at ${String(result.wake)}, ` +
            `gave ${quiet.actions.map((action) => (action._tag === "Command" ? action.command._tag : action._tag === "Emit" ? action.event._tag : action._tag)).join(", ") || "nothing"} and a wake at ${String(quiet.wake)}`,
        );
    }
    for (const item of state.items.values()) {
      if (item.group === undefined) continue;
      const seat = seatOf(item);
      const kept = seats.get(item.spec.key) ?? [];
      const last = kept.at(-1)?.seat;
      if (last === undefined || last.join() !== seat.join())
        seats.set(item.spec.key, [...kept, { at: actions.length + result.actions.length, seat }]);
    }
    for (const action of result.actions) {
      actions.push(action);
      inputOf.push(inputs.length - 1);
      if (action._tag === "Command") {
        const command = action.command;
        if (closed.has(action.sessionId))
          problems.push(`command ${String(action.id)} sent to ${action.sessionId}, closed`);
        if (command._tag === "Autoplay" && command.enabled && action.sessionId !== state.air)
          problems.push(`autoplay turned on for ${action.sessionId}, which is not on air`);
        // A filler clip goes to one session at a time: never where, or while, an enqueue of it
        // on a session still open may have applied.
        if (command._tag === "Enqueue" && command.tag._tag === "Filler") {
          const index = command.tag.index;
          const where = [...(applied.get(index) ?? [])].filter((id) => !closed.has(id));
          if (where.length > 0)
            problems.push(
              `filler ${String(index)} sent to ${action.sessionId} while its enqueue on ${where.join(", ")} may have applied`,
            );
          fillers.set(action.id, { index, sessionId: action.sessionId });
          applied.set(index, new Set([...where, action.sessionId]));
        }
        const unsettled = outstanding.get(action.sessionId);
        if (unsettled !== undefined)
          problems.push(
            `command ${String(action.id)} sent to ${action.sessionId} while ${String(unsettled)} was unsettled there`,
          );
        outstanding.set(action.sessionId, action.id);
        // A cut stops only filler, a clip the plan does not own, or a strictly lower lane's clip.
        if (command._tag === "Stop") {
          const value = sessions.get(action.sessionId);
          const cutter = value?.ready.find((clip) => clip.clipId === state.cutting?.next)?.tag;
          const cut = value?.playing?.clipId === command.clipId ? value.playing.tag : undefined;
          if (
            cutter?._tag === "Item" &&
            cut?._tag === "Item" &&
            !((lanes.get(cut.key) ?? -1) > (lanes.get(cutter.key) ?? -1))
          )
            problems.push(`${cutter.key} cut ${cut.key}, which is not of a lower lane`);
        }
      }
      if (action._tag === "Close") {
        sessions.delete(action.sessionId);
        outstanding.delete(action.sessionId);
        closed.add(action.sessionId);
        if (!aired.has(action.sessionId) && enqueued.has(action.sessionId)) failures++;
      }
      if (action._tag === "Command" && action.command._tag === "Enqueue")
        enqueued.add(action.sessionId);
      // Only enough failed setups that count end the playout.
      if (
        action._tag === "Fail" &&
        (action.cause === "open" || action.cause === "lost") &&
        failures < settings.maxSetupFailures
      )
        problems.push(
          `the plan failed (${action.cause}) at ${String(clock)} after ${String(failures)} failed setups that count`,
        );
      if (action._tag === "Open") {
        wanted++;
        // No open goes out before the wait a refusal asked for.
        if (retryAfter !== undefined && clock < retryAfter.until)
          problems.push(
            `an open went out at ${String(clock)}, though the refusal at ${String(retryAfter.at)} asked to wait until ${String(retryAfter.until)}`,
          );
      }
    }
  };
  const observe = (sessionId: string) => {
    const value = sessions.get(sessionId);
    if (value === undefined) return;
    send({
      _tag: "Source",
      sessionId,
      event: {
        _tag: "State",
        state: source({
          building: [...value.building],
          ready: [...value.ready],
          playing: value.playing,
          continuable: value.ready.map((clip) => clip.clipId),
        }),
      },
    });
  };
  const onAir = () => sessions.get(state.air ?? "");
  const airId = (): string => {
    if (state.air === undefined) throw new Error("the scripted provider has no session on air");
    return state.air;
  };
  const edit = (index: number, list: ReadonlyArray<Policy.EditInput>, batch = false) => {
    const id = 100 + index;
    const keys = new Map<number, string>();
    const parts = new Map<number, ReadonlyArray<string>>();
    const fresh: Array<string> = [];
    /** Replacements it makes, with the place each took in its part's group. */
    const replacing: Array<{ readonly key: string; readonly group: string | undefined }> = [];
    list.forEach((value, position) => {
      if (value._tag === "Withdraw") {
        keys.set(position, value.key);
        const group = groups.get(value.key);
        if (group !== undefined)
          parts.set(
            position,
            group.map((part) => part.key),
          );
        // A group key withdraws its parts; a part key, the parts from its place on.
        const part = partOf.get(value.key);
        const reached =
          groups.get(value.key) ??
          (part === undefined
            ? [{ key: value.key, place: 0 }]
            : (groups.get(part.group) ?? []).filter((other) => other.place >= part.place));
        for (const other of reached) named.add(other.key);
      }
      if (value._tag === "Submit" || value._tag === "Insert" || value._tag === "Replace")
        known.push(value.spec.key);
      // A replacement under a key the plan holds already is that item, not a new one.
      if (value._tag === "Replace" && !state.items.has(value.spec.key)) {
        replaced.set(value.spec.key, value.key);
        const part = partOf.get(value.key);
        const joins = part !== undefined && !partOf.has(value.spec.key);
        if (joins) {
          partOf.set(value.spec.key, part);
          groups.get(part.group)?.push({ key: value.spec.key, place: part.place });
        }
        replacing.push({ key: value.spec.key, group: joins ? part.group : undefined });
      }
      if (value._tag === "SubmitGroup" && !groups.has(value.key)) {
        fresh.push(value.key);
        groups.set(
          value.key,
          value.parts.map((part, place) => ({ key: part.key, place })),
        );
        known.push(value.key);
        value.parts.forEach((part, place) => {
          known.push(part.key);
          if (!partOf.has(part.key)) partOf.set(part.key, { group: value.key, place });
        });
      }
    });
    edits.set(id, { batch, keys, parts });
    const before = actions.length;
    send({ _tag: "Edit", id, edits: list, batch });
    // An item takes its lane when accepted, a replacement its item's and an insert its anchor's;
    // a key the plan holds already keeps the one it has. A replacement or an insert joins its
    // item's or its anchor's group, if it has one.
    for (const action of actions.slice(before))
      if (
        action._tag === "Emit" &&
        action.event._tag === "AsRun" &&
        action.event.event.status._tag === "Accepted"
      ) {
        const admitted = state.items.get(action.event.event.key);
        if (admitted !== undefined) lanes.set(action.event.event.key, admitted.spec.lane);
        if (admitted?.group !== undefined)
          members.set(action.event.event.key, {
            group: admitted.group.key,
            order: admitted.order,
          });
      }
    // A group or a replacement the plan refused is none: its key may yet name an item of its own.
    if (actions.some((action) => action._tag === "Refused" && action.id === id)) {
      for (const group of fresh) {
        groups.delete(group);
        for (const [part, place] of partOf) if (place.group === group) partOf.delete(part);
      }
      for (const { key: next, group } of replacing) {
        replaced.delete(next);
        if (group === undefined) continue;
        partOf.delete(next);
        groups.set(
          group,
          (groups.get(group) ?? []).filter((other) => other.key !== next),
        );
      }
    }
  };
  const lates: ReadonlyArray<Policy.Late> = ["nextBoundary", "drop", { skipAfterMs: 1_000 }];
  // Some items start at an instant, some within a window, and some are held and never released:
  // each brings deadlines of its own.
  const timing = (index: number): Pick<Policy.Spec, "start"> | Pick<Policy.Spec, "window"> =>
    index % 5 === 1
      ? {
          start: {
            _tag: "At",
            time: clock + 2_000 + (index % 3) * 3_000,
            late: Array.getUnsafe(lates, index % 3),
          },
        }
      : index % 5 === 2
        ? {
            window: {
              startByMs: 4_000 + (index % 3) * 4_000,
              firm: index % 4 < 2,
              ...(index % 2 === 0 ? { notBeforeMs: 1_000 } : {}),
            },
          }
        : index % 7 === 6
          ? { start: { _tag: "Manual" } }
          : { start: { _tag: "Follow" } };
  const cued = (name: string, lane: number, index: number): Policy.Spec => ({
    ...spec(name, lane, 5 + (index % 3) * 5),
    cues: index % 2 === 0 ? [{ name: "cue", from: "end", offsetMs: 500 }] : [],
    continuity: index % 4 === 3,
    ...timing(index),
  });

  /** The provider carries out the command in flight on a lane, and answers it. */
  const complete = (busy: {
    readonly id: number;
    readonly command: Policy.Command;
    readonly sessionId: string;
  }) => {
    const value = sessions.get(busy.sessionId);
    const command = busy.command;
    let clipId: string | undefined;
    let refused = false;
    let armed = false;
    if (value !== undefined)
      switch (command._tag) {
        case "Enqueue":
          clipId = `c${String(clips++)}`;
          value.building.push({
            clipId,
            tag: command.tag,
            seconds: command.request.seconds ?? 5,
          });
          break;
        case "Remove":
          // It pops only a queued clip, as H3 does: one that started playing meanwhile stays.
          refused = ![...value.building, ...value.ready].some(
            (clip) => clip.clipId === command.clipId,
          );
          value.building = value.building.filter((clip) => clip.clipId !== command.clipId);
          value.ready = value.ready.filter((clip) => clip.clipId !== command.clipId);
          break;
        case "Move": {
          const moved = value.ready.find((clip) => clip.clipId === command.clipId);
          if (moved !== undefined) {
            value.ready = value.ready.filter((clip) => clip !== moved);
            value.ready.splice(command.position, 0, moved);
          }
          break;
        }
        case "Stop": {
          if (value.playing?.clipId !== command.clipId) break;
          const cut = value.playing;
          value.playing = undefined;
          send({
            _tag: "Source",
            sessionId: busy.sessionId,
            event: { _tag: "Ended", clip: cut, termination: "stopped" },
          });
          break;
        }
        case "Play": {
          const next = value.ready.find((clip) => clip.clipId === command.clipId);
          if (next === undefined || value.playing !== undefined) break;
          value.ready = value.ready.filter((clip) => clip !== next);
          value.playing = next;
          send({
            _tag: "Source",
            sessionId: busy.sessionId,
            event: { _tag: "Started", clip: next },
          });
          break;
        }
        case "Autoplay":
          value.autoplay = command.enabled;
          // As autoplay comes on with nothing playing, H3 arms its Ready head at once.
          armed = command.enabled && value.playing === undefined && value.ready.length > 0;
          break;
      }
    send({
      _tag: "Result",
      id: busy.id,
      result: refused ? failed("replied") : { _tag: "Done", clipId },
    });
    if (armed && value !== undefined) {
      const next = Array.getUnsafe(value.ready.splice(0, 1), 0);
      value.playing = next;
      send({ _tag: "Source", sessionId: busy.sessionId, event: { _tag: "Started", clip: next } });
    }
    observe(busy.sessionId);
  };
  /** The provider opens the session the plan asked for. */
  const open = () => {
    wanted--;
    const sessionId = `s${String(++opened)}`;
    sessions.set(sessionId, { building: [], ready: [], playing: undefined });
    send({ _tag: "Opened", sessionId, lifetimeMs: lifetimes.lifetimeMs });
    observe(sessionId);
  };

  send({ _tag: "Tick" });
  script.forEach((step, index) => {
    // Edits name keys already submitted, group parts among them, as often as fresh ones.
    const name =
      index % 2 === 0 || known.length === 0
        ? `k${String(index % 5)}`
        : Array.getUnsafe(known, (index * 7) % known.length);
    // A command in flight on some lane: the oldest or the newest, so lanes answer in either order.
    const lanes = state.sessions
      .flatMap((value) =>
        value.busy === undefined ? [] : [{ ...value.busy, sessionId: value.id }],
      )
      .sort((a, b) => a.id - b.id);
    const busy = index % 2 === 0 ? lanes[0] : lanes.at(-1);
    // A session with a clip building, the oldest or the newest, so both sessions' builds end.
    const builder = () => {
      const building = [...sessions].filter(([, entry]) => entry.building.length > 0);
      return index % 2 === 0 ? building[0] : building.at(-1);
    };
    switch (step) {
      case "submit":
        return edit(index, [{ _tag: "Submit", spec: cued(name, 1, index) }]);
      case "urgent": {
        // Some wait to air right after the next filler clip, or the one after it, submitted as
        // `place` answers after filler: `Asap`, in the lowest lane.
        if (index % 4 === 0) {
          const follows: ClipTag = {
            _tag: "Filler",
            index: state.filler.index + (index % 8 === 0 ? 1 : 0),
          };
          const urgent = cued(`u${String(index)}`, 1, index);
          paired.set(urgent.key, follows);
          return edit(index, [
            { _tag: "Submit", spec: { ...urgent, start: { _tag: "Asap" }, follows } },
          ]);
        }
        return edit(index, [{ _tag: "Submit", spec: cued(`u${String(index)}`, 0, index) }]);
      }
      case "withdraw":
        return edit(index, [{ _tag: "Withdraw", key: key(name) }]);
      case "replace": {
        // A replacement follows what the item it replaces follows.
        const replacement = cued(`r${String(index)}`, 1, index);
        const follows = paired.get(name);
        if (follows !== undefined) paired.set(replacement.key, follows);
        return edit(index, [{ _tag: "Replace", key: key(name), spec: replacement }]);
      }
      case "insert": {
        // Some inserts after an item must air right after it.
        const inserted = cued(`i${String(index)}`, 1, index);
        const pair = index % 2 === 0 && index % 3 === 0;
        if (pair) paired.set(inserted.key, { _tag: "Item", key: key(name) });
        return edit(index, [
          {
            _tag: "Insert",
            spec: pair
              ? {
                  ...inserted,
                  follows: { _tag: "Item", key: key(name) },
                }
              : inserted,
            anchor: key(name),
            side: index % 2 === 0 ? "after" : "before",
          },
        ]);
      }
      case "batch": {
        // Some batches add an item due by a firm deadline, which the batch does not hold; some an
        // insert beside what the batch adds, firm or not; and some replace an item still waiting.
        const firm = { window: { startByMs: 4_000 + (index % 3) * 4_000, firm: true } };
        const added = { ...cued(`b${String(index)}`, 1, index), ...(index % 3 === 1 ? firm : {}) };
        const inserted = {
          ...cued(`b${String(index)}i`, 1, index + 1),
          ...(index % 4 === 1 ? firm : {}),
        };
        const waiting = [...state.items.values()].filter(
          (item) => item.phase !== "Settled" && item.phase !== "Started" && item.spec.key !== name,
        );
        const target = index % 5 < 2 ? waiting[index % Math.max(1, waiting.length)] : undefined;
        const replacement = cued(`b${String(index)}r`, 1, index);
        // A replacement follows what the item it replaces follows.
        const follows = target === undefined ? undefined : paired.get(target.spec.key);
        if (follows !== undefined) paired.set(replacement.key, follows);
        return edit(
          index,
          [
            { _tag: "Withdraw", key: key(name) },
            { _tag: "Submit", spec: added },
            ...(index % 2 === 1
              ? [
                  {
                    _tag: "Insert",
                    spec: inserted,
                    anchor: added.key,
                    side: index % 8 < 4 ? "after" : "before",
                  } as const,
                ]
              : []),
            ...(target === undefined
              ? []
              : [{ _tag: "Replace", key: target.spec.key, spec: replacement } as const]),
          ],
          true,
        );
      }
      case "group": {
        // The group's timing is its first part's, as the runtime builds it; the rest follow it.
        const { window: _window, ...later } = cued(`g${String(index)}b`, 1, index + 1);
        return edit(index, [
          {
            _tag: "SubmitGroup",
            key: key(`g${String(index)}`),
            lane: 1,
            parts: [cued(`g${String(index)}a`, 1, index), { ...later, start: { _tag: "Follow" } }],
            fingerprint: `g${String(index)}`,
          },
        ]);
      }
      case "drain":
        drains.push(100 + index);
        return send({
          _tag: "Drain",
          id: 100 + index,
          finish: index % 2 === 0 ? "accepted" : "playing",
        });
      case "done":
        return busy === undefined ? send({ _tag: "Tick" }) : complete(busy);
      case "unknown":
      case "refused":
        if (busy === undefined) return send({ _tag: "Tick" });
        return send({
          _tag: "Result",
          id: busy.id,
          result: failed(step === "unknown" ? "unknown" : "replied"),
        });
      case "fail": {
        const [sessionId, value] = builder() ?? [];
        if (sessionId === undefined || value === undefined) return send({ _tag: "Tick" });
        send({
          _tag: "Source",
          sessionId,
          event: buildFailed(Array.getUnsafe(value.building.splice(0, 1), 0)),
        });
        return observe(sessionId);
      }
      case "ready": {
        const [sessionId, value] = builder() ?? [];
        if (sessionId === undefined || value === undefined) return send({ _tag: "Tick" });
        value.ready.push(Array.getUnsafe(value.building.splice(0, 1), 0));
        return observe(sessionId);
      }
      case "start": {
        // The provider brings the air to a clip: it opens the session the plan asked for and,
        // with nothing there to play, carries out the autoplay and enqueue asked of it and
        // finishes the clip it builds first. With autoplay off it starts nothing, as H3 starts
        // nothing then: only a play does.
        if (onAir() === undefined && wanted > 0) open();
        for (let asked = 0; asked < 2 && onAir()?.building.length === 0; asked++) {
          const air = state.sessions.find((value) => value.id === state.air);
          const command = air?.busy?.command._tag;
          if (onAir()?.ready.length !== 0 || air?.busy === undefined) break;
          if (command !== "Autoplay" && command !== "Enqueue") break;
          complete({ ...air.busy, sessionId: air.id });
        }
        const value = onAir();
        if (value === undefined || value.playing !== undefined) return send({ _tag: "Tick" });
        // A removal asked of it lands first, as H3 takes commands in order.
        const pending = state.sessions.find((other) => other.id === state.air)?.busy;
        if (pending?.command._tag === "Remove") complete({ ...pending, sessionId: airId() });
        if (value.ready.length === 0 && value.building.length > 0) {
          value.ready.push(Array.getUnsafe(value.building.splice(0, 1), 0));
          observe(airId());
        }
        if (value.autoplay === false) return send({ _tag: "Tick" });
        const next = value.ready.shift();
        if (next === undefined) return send({ _tag: "Tick" });
        value.playing = next;
        const sessionId = airId();
        send({ _tag: "Source", sessionId, event: { _tag: "Started", clip: next } });
        return observe(sessionId);
      }
      case "end": {
        const value = onAir();
        if (value?.playing === undefined) return send({ _tag: "Tick" });
        const ended = value.playing;
        const sessionId = airId();
        value.playing = undefined;
        send({
          _tag: "Source",
          sessionId,
          event: { _tag: "Ended", clip: ended, termination: "finished" },
        });
        return observe(sessionId);
      }
      case "lost": {
        const sessionId = [...sessions.keys()][0];
        if (sessionId === undefined) return send({ _tag: "Tick" });
        sessions.delete(sessionId);
        return send({ _tag: "Lost", sessionId, reason: "gone" });
      }
      case "open":
        return wanted === 0 ? send({ _tag: "Tick" }) : open();
      case "denied": {
        // The provider refuses the open the plan asked for, and each next one it asks for within
        // 5 s, three at most. All of them allocated a session, or may have, or none did, and some
        // ask to wait before the next.
        if (wanted === 0) return send({ _tag: "Tick" });
        const retryAfterMs = index % 3 === 0 ? 2_000 + (index % 4) * 4_000 : undefined;
        for (let refused = 0; refused < 3 && wanted > 0; refused++) {
          wanted--;
          send({
            _tag: "OpenFailed",
            reason: "refused",
            fatal: false,
            allocated: index % 2 === 0,
            ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
          });
          if (wake !== undefined && wake <= clock + 5_000) send({ _tag: "Tick" }, wake);
        }
        return;
      }
      case "tick":
        clock += 5_000;
        return send({ _tag: "Tick" });
      case "wake":
        return send({ _tag: "Tick" }, wake);
    }
  });
  send({ _tag: "Close" });
  return {
    actions,
    inputs,
    inputOf,
    edits,
    drains,
    problems,
    groups,
    members,
    seats,
    named,
    replaced,
    paired,
    starts,
  };
};

/**
 * The plan's promises for one script, run with no build measured and with
 * three, so that filler protects the air from the start; the property and the
 * pinned counterexamples check them.
 */
// Test fixtures are called directly rather than composed as a public pipeable API.
// @effect-diagnostics-next-line missingPipeableSignature:off
const check = (script: Script, lifetimes: Lifetimes = lasting): void => {
  for (const from of [Policy.initial, measured]) keeps(script, from, lifetimes);
};
/**
 * What a script keeps as it runs, from each start `check` runs it from: nothing falls due before
 * a wake; each lane carries one command at a time, and only to a session still open; autoplay is
 * on only on air; a filler clip goes to one session at a time; no open goes out before a
 * refusal's `Retry-After` has passed; and only enough failed setups that count end the playout.
 */
// Test fixtures are called directly rather than composed as a public pipeable API.
// @effect-diagnostics-next-line missingPipeableSignature:off
const wakes = (script: Script, lifetimes: Lifetimes = lasting): void => {
  for (const from of [Policy.initial, measured]) {
    const { problems } = simulate(script, from, lifetimes);
    assert.deepStrictEqual(problems, [], problems.join("; "));
  }
};
const keeps = (script: Script, from: Policy.State, lifetimes: Lifetimes = lasting): void => {
  const {
    actions,
    inputs,
    inputOf,
    edits,
    drains,
    problems,
    groups,
    members,
    seats,
    named,
    replaced,
    paired,
    starts,
  } = simulate(script, from, lifetimes);
  // What `wakes` checks as the script runs: a refusal is then always attributable.
  assert.deepStrictEqual(problems, []);
  const history = new Map<string, Array<Policy.Action & { readonly _tag: "Emit" }>>();
  for (const action of actions)
    if (action._tag === "Emit" && action.event._tag === "AsRun")
      history.set(action.event.event.key, [...(history.get(action.event.event.key) ?? []), action]);
  const tags = (name: string) =>
    (history.get(name) ?? []).flatMap((action) =>
      action.event._tag === "AsRun" ? [action.event.event.status._tag] : [],
    );
  const terminal = ["Ended", "Dropped", "Failed", "Unobserved"];
  for (const name of history.keys()) {
    const statuses = tags(name);
    assert.strictEqual(statuses[0], "Accepted", name);
    const ends = statuses.filter((status) => terminal.includes(status));
    // At most one terminal status, and nothing after it.
    assert.isAtMost(ends.length, 1, `${name}: ${statuses.join(",")}`);
    if (ends.length === 1)
      assert.strictEqual(statuses.at(-1), ends[0], `${name}: ${statuses.join(",")}`);
  }
  // An enqueue whose outcome is unknown is never sent again, unless its session was lost
  // and the item carried to another.
  for (const name of history.keys()) {
    const events = actions.filter(
      (action) =>
        (action._tag === "Emit" &&
          action.event._tag === "AsRun" &&
          action.event.event.key === name) ||
        (action._tag === "Command" &&
          action.command._tag === "Enqueue" &&
          action.command.tag._tag === "Item" &&
          action.command.tag.key === name),
    );
    let uncertain = false;
    for (const action of events) {
      if (action._tag === "Command")
        assert.isFalse(uncertain, `${name} was sent again after an unknown outcome`);
      else if (action._tag === "Emit" && action.event._tag === "AsRun") {
        const status = action.event.event.status;
        if (status._tag === "Unknown") uncertain = true;
        if (status._tag === "Accepted") uncertain = false;
      }
    }
  }
  // Every withdrawal of an accepted edit is answered, once.
  const accepted = new Set(
    actions.flatMap((action) => (action._tag === "Accepted" ? [action.id] : [])),
  );
  for (const [id, value] of edits)
    if (accepted.has(id))
      for (const position of value.keys.keys())
        assert.strictEqual(
          actions.filter(
            (action) =>
              action._tag === "Withdrawn" && action.id === id && action.index === position,
          ).length,
          1,
          `the withdrawal of ${value.keys.get(position) ?? ""} was answered other than once`,
        );
  // A withdrawal answers what became of its item. A group key's answers for the parts the group
  // had when it was made: `withdrawn` if any was dropped by then, else `already-started` if any
  // started, else `not-found`. A part's replacement made afterwards is no part of it.
  const seen = new Map<string, ReadonlyArray<string>>();
  for (const action of actions) {
    if (action._tag === "Emit" && action.event._tag === "AsRun")
      seen.set(action.event.event.key, [
        ...(seen.get(action.event.event.key) ?? []),
        action.event.event.status._tag,
      ]);
    if (action._tag !== "Withdrawn") continue;
    const name = edits.get(action.id)?.keys.get(action.index);
    const parts = edits.get(action.id)?.parts.get(action.index);
    if (parts === undefined) continue;
    const so = parts.flatMap((part) => seen.get(part) ?? []);
    const expected = so.includes("Dropped")
      ? "withdrawn"
      : so.includes("Started")
        ? "already-started"
        : "not-found";
    assert.strictEqual(action.outcome, expected, `group ${name ?? ""}: ${so.join(",")}`);
  }
  // It answers for the item under that key then: one accepted under it afterwards is another.
  const itemThen = (name: string, position: number): ReadonlyArray<string> => {
    const events = actions.flatMap((action, index) =>
      action._tag === "Emit" && action.event._tag === "AsRun" && action.event.event.key === name
        ? [{ index, status: action.event.event.status._tag }]
        : [],
    );
    const from = events.findLastIndex(
      (event) => event.index < position && event.status === "Accepted",
    );
    const next = events.findIndex(
      (event, index) => index > from && event.index > position && event.status === "Accepted",
    );
    return events.slice(Math.max(0, from), next < 0 ? undefined : next).map(({ status }) => status);
  };
  for (const [position, action] of actions.entries())
    if (action._tag === "Withdrawn") {
      const name = edits.get(action.id)?.keys.get(action.index);
      if (name === undefined || groups.has(name)) continue;
      const statuses = itemThen(name, position);
      if (action.outcome === "withdrawn")
        assert.include(statuses, "Dropped", `${name} withdrawn: ${statuses.join(",")}`);
      if (action.outcome === "already-started")
        assert.include(statuses, "Started", `${name} already started: ${statuses.join(",")}`);
      if (action.outcome === "not-found")
        assert.notInclude(statuses, "Started", `${name} not found: ${statuses.join(",")}`);
    }
  // A group's places are its parts, each with the replacements that took its place. A place is
  // broken once none of its items has started or settled Unobserved, which may have aired, and
  // each failed, was dropped or emitted Unknown, so it can no longer air in order; or once an
  // item of it started and then failed, as a part lost with its session on air does. Each status
  // counts from the action that carries it.
  const asRuns = actions.flatMap((action, index) =>
    action._tag === "Emit" && action.event._tag === "AsRun"
      ? [{ index, key: String(action.event.event.key), status: action.event.event.status }]
      : [],
  );
  /** `name`'s as-run statuses among the actions before `end`. */
  const statusesBefore = (name: string, end: number): ReadonlyArray<string> =>
    asRuns.flatMap((entry) => (entry.key === name && entry.index < end ? [entry.status._tag] : []));
  /** A group's places, each by the keys of its items and its order in the lane. */
  const placesOf = (
    group: string,
  ): ReadonlyArray<{ readonly keys: ReadonlyArray<string>; readonly order: number }> => {
    const byPlace = new Map<number, Array<string>>();
    for (const part of groups.get(group) ?? [])
      byPlace.set(part.place, [...(byPlace.get(part.place) ?? []), part.key]);
    return [...byPlace.values()].flatMap((keys) => {
      const order = keys.map((key) => members.get(key)?.order).find((value) => value !== undefined);
      return order === undefined ? [] : [{ keys, order }];
    });
  };
  /** Whether a place is broken by the action at `end`, among the items it had by then. */
  const brokenBy = (keys: ReadonlyArray<string>, end: number): boolean => {
    const had = keys.map((key) => statusesBefore(key, end)).filter((so) => so.includes("Accepted"));
    if (had.some((so) => so.includes("Started") && so.includes("Failed"))) return true;
    return (
      had.length > 0 &&
      !had.some((so) => so.includes("Started") || so.includes("Unobserved")) &&
      had.every((so) =>
        so.some((status) => status === "Failed" || status === "Dropped" || status === "Unknown"),
      )
    );
  };
  /** The index just past the last action of the step that took the action at `index`. */
  const stepEnd = (index: number): number => {
    let end = index + 1;
    while (end < actions.length && inputOf[end] === inputOf[index]) end++;
    return end;
  };
  /** A member's seat when the action at `index` was taken: as the plan kept it at that step's end. */
  const seatAt = (name: string, order: number, index: number): ReadonlyArray<number> => {
    const end = stepEnd(index);
    return (seats.get(name) ?? []).findLast((entry) => entry.at <= end)?.seat ?? [order];
  };
  /** Seats in their group's order: a seat right behind a member comes after it, and before the next. */
  const compareSeat = (a: ReadonlyArray<number>, b: ReadonlyArray<number>): number => {
    for (let index = 0; index < Math.min(a.length, b.length); index++) {
      const order = (a[index] ?? 0) - (b[index] ?? 0);
      if (order !== 0) return order;
    }
    return a.length - b.length;
  };
  // An item is dropped as withdrawn only when a withdrawal or a drain named it, a place of its
  // group ahead of its seat is broken, then or once the script ends (a withdrawal still landing
  // may drop the item that breaks it later), or it is a replacement whose item started first, as
  // 0.7.0 withdrew it. An insert whose build went out continuing from a later member's clip sits
  // right behind that member, so that member's place is ahead of it.
  if (drains.length === 0) {
    const allowed = (name: string, index: number): boolean => {
      if (named.has(name)) return true;
      for (let old = replaced.get(name); old !== undefined; old = replaced.get(old))
        if (tags(old).includes("Started")) return true;
      const member = members.get(name);
      if (member === undefined) return false;
      const seat = seatAt(name, member.order, index);
      return placesOf(member.group).some(
        (place) =>
          compareSeat([place.order], seat) < 0 &&
          (brokenBy(place.keys, index) || brokenBy(place.keys, Infinity)),
      );
    };
    for (const entry of asRuns)
      if (entry.status._tag === "Dropped" && entry.status.reason === "withdrawn")
        assert.isTrue(
          allowed(entry.key, entry.index),
          `${entry.key} was dropped though nothing withdrew it`,
        );
  }
  // A group's members start in order, and none starts once a place ahead of it is broken: each
  // place ahead had started first, or was broken by the end of the step that started the member,
  // since one read of a session's queues names a clip playing before the one that left them
  // unseen settles. A start the plan had asked to remove is excused, as a follower's is below.
  /** Whether the clip whose start the action at `index` records had been asked to go first. */
  const removalAsked = (index: number): boolean => {
    const input = inputs[inputOf[index] ?? -1];
    const event = input?._tag === "Source" ? input.event : undefined;
    const clipId =
      event?._tag === "Started"
        ? event.clip.clipId
        : event?._tag === "State"
          ? event.state.playing?.clipId
          : undefined;
    return actions
      .slice(0, index)
      .some(
        (action) =>
          action._tag === "Command" &&
          action.command._tag === "Remove" &&
          action.command.clipId === clipId,
      );
  };
  for (const entry of asRuns) {
    const member = members.get(entry.key);
    if (entry.status._tag !== "Started" || member === undefined || removalAsked(entry.index))
      continue;
    const settled = stepEnd(entry.index);
    for (const place of placesOf(member.group)) {
      if (place.order >= member.order) continue;
      const first = place.keys.some((key) => statusesBefore(key, entry.index).includes("Started"));
      const unseen = place.keys.some((key) => statusesBefore(key, settled).includes("Unobserved"));
      assert.isTrue(
        first || unseen || brokenBy(place.keys, settled),
        `${entry.key} started before ${place.keys.join(" or ")}, ahead of it in its group`,
      );
      assert.isFalse(
        brokenBy(place.keys, entry.index),
        `${entry.key} started after ${place.keys.join(" or ")}, ahead of it in its group, broke`,
      );
    }
  }
  // Only an item that follows a clip is dropped as displaced.
  for (const name of history.keys()) {
    const displaced = (history.get(name) ?? []).some(
      (action) =>
        action.event._tag === "AsRun" &&
        action.event.event.status._tag === "Dropped" &&
        action.event.event.status.reason === "displaced",
    );
    if (displaced) assert.isTrue(paired.has(name), `${name} was displaced but follows nothing`);
  }
  // An item that follows a clip starts right after it. The simulated provider ends a clip whenever
  // the script says and, with autoplay on, starts the next at once, where H3 waits out a seam that
  // no removal lands within: a start the plan had asked to remove is excused. With autoplay off it
  // starts nothing, as H3 doesn't. The Playout tests own that race.
  for (const [index, start] of starts.entries()) {
    const follows = start.follows;
    if (follows === undefined || start.removalAsked) continue;
    const previous = starts[index - 1]?.tag;
    assert.isTrue(
      previous !== undefined && sameClip(follows, previous),
      `${start.tag?._tag === "Item" ? start.tag.key : ""} started after ${JSON.stringify(previous)}, not ${JSON.stringify(follows)}`,
    );
  }
  // An item and its replacement never both air.
  for (const [next, old] of replaced)
    assert.isFalse(
      tags(next).includes("Started") && tags(old).includes("Started"),
      `${old} and its replacement ${next} both aired`,
    );
  // Every batch and drain is answered once the playout closes.
  const answered = new Set(
    actions.flatMap((action) =>
      action._tag === "Committed" || action._tag === "Refused" ? [action.id] : [],
    ),
  );
  for (const [id, value] of edits)
    if (value.batch) assert.isTrue(answered.has(id), `batch ${String(id)} never answered`);
  const drained = new Set(
    actions.flatMap((action) => (action._tag === "Drained" ? [action.id] : [])),
  );
  for (const id of drains) assert.isTrue(drained.has(id), `drain ${String(id)} never finished`);
};

/**
 * The seeds the gate runs, so every run of it is the same: 3,000 scripts each,
 * and 1,000 of the longer scripts across renewals. 6763954388057357 found a
 * replacement's withdrawal leaving the replacement on air (pinned above). To
 * explore wider, set `PLAYOUT_PROPERTY_RUNS` to a count and optionally
 * `PLAYOUT_PROPERTY_SEED`; without a seed each run draws one and a failure
 * reports it. A seed that finds something joins this list, and its shrunk
 * script becomes a unit test.
 */
const gateSeeds: ReadonlyArray<string> = ["6763954388057357", "1", "2"];
const explore = Effect.runSync(
  Config.all({
    runs: Config.Int("PLAYOUT_PROPERTY_RUNS").pipe(Config.withDefault(0)),
    seed: Config.String("PLAYOUT_PROPERTY_SEED").pipe(Config.option),
  }),
);
const propertyRuns = (
  gate: number,
): ReadonlyArray<{ readonly seed: string | undefined; readonly runs: number }> =>
  explore.runs > 0
    ? [
        {
          seed: explore.seed.pipe(
            Option.filter((value) => value.length > 0),
            Option.getOrUndefined,
          ),
          runs: explore.runs,
        },
      ]
    : gateSeeds.map((seed) => ({ seed, runs: gate }));

/**
 * Scripts the property falsified, each shrunk: the plan's promises hold for
 * them on every run, whatever the seeds explore.
 */
const counterexamples: ReadonlyArray<Script> = [
  // A replacement's withdrawal left the replacement waiting and was never answered.
  ["unknown", "group", "batch", "replace", "ready", "withdraw"],
  // A batch's withdrawal still pending at close was never answered.
  ["batch", "open", "done", "batch"],
  ["batch", "withdraw", "fail", "batch"],
  ["urgent", "withdraw", "lost", "batch"],
  // A replacement whose item started first is dropped as withdrawn, as 0.7.0 dropped it.
  ["submit", "open", "done", "done", "ready", "replace", "start"],
  // A replacement whose item started first went on air too: its removal, answered as unknown
  // while it built, was asked again only once the queues changed, and its build finishing was no
  // change to them.
  [
    "fail",
    "ready",
    "ready",
    "group",
    "open",
    "replace",
    "unknown",
    "submit",
    "ready",
    "batch",
    "withdraw",
    "refused",
    "done",
    "replace",
    "start",
    "start",
    "end",
    "unknown",
    "start",
  ],
  // A withdrawal of a key before any item had it answers not-found: the item submitted under it
  // afterwards, which airs, is another.
  ["withdraw", "withdraw", "open", "ready", "end", "submit", "done", "done", "ready", "start"],
  // A refused replacement is none: k0 and r0, submitted afterwards, are two items, and both air.
  [
    "replace",
    "batch",
    "lost",
    "tick",
    "insert",
    "insert",
    "done",
    "tick",
    "unknown",
    "unknown",
    "submit",
    "refused",
    "done",
    "batch",
    "fail",
    "unknown",
    "done",
    "end",
    "ready",
    "withdraw",
    "open",
    "batch",
    "start",
    "submit",
    "lost",
    "start",
  ],
  // A group's withdrawal was checked against a part's replacement made after it: the plan's
  // answer from the parts the group had then was right, and the check was not.
  [
    "group",
    "lost",
    "start",
    "withdraw",
    "unknown",
    "replace",
    "withdraw",
    "batch",
    "drain",
    "lost",
    "group",
    "batch",
    "lost",
    "withdraw",
  ],
  // An edit refused as it would miss a deadline took a replaced part's group with it, and the
  // withdrawal of that group's part was never answered.
  [
    "open",
    "done",
    "open",
    "wake",
    "fail",
    "open",
    "batch",
    "group",
    "withdraw",
    "done",
    "end",
    "end",
    "urgent",
    "withdraw",
    "submit",
    "group",
    "fail",
    "replace",
    "lost",
    "batch",
  ],
  // A follower waiting Ready at its session's head, fenced, aired after the wrong clip: the
  // scripted provider started it after an early end, with autoplay off there and the lane busy
  // with another enqueue, where H3 starts nothing and the plan sent no play.
  ["urgent", "insert", "start", "urgent", "start", "ready", "end", "start"],
  // A replacement whose item started first aired too: its removal, refused once the item had left
  // the air, went again only as its build ended, when the provider started it.
  [
    "open",
    "unknown",
    "batch",
    "wake",
    "done",
    "replace",
    "start",
    "start",
    "end",
    "refused",
    "start",
  ],
];

export {
  config,
  key,
  spec,
  clip,
  item,
  source,
  failed,
  measured,
  buildFailed,
  steps,
  renewing,
  check,
  wakes,
  propertyRuns,
  counterexamples,
};
