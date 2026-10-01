/** Shared policy inputs and the scripted provider used by its property families. */
import { assert } from "@effect/vitest";
import { Array, Config, Effect, Option, Redacted } from "effect";
import * as Policy from "../src/internal/playout/policy.js";
import type { ClipTag, SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { ItemKey } from "../src/Playout.js";
import { CommandFailure, ReactorError } from "../src/ReactorError.js";

const config: Policy.Config = {
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

/** Three builds measured at 0.4 s per requested second: a continued one is projected at 1 s. */
const measured: Policy.State = {
  ...Policy.initial,
  samples: { build: [0.4, 0.4, 0.4], continued: [], length: [], aired: [] },
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
    { building: Array<SourceClip>; ready: Array<SourceClip>; playing: SourceClip | undefined }
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
   * Each clip that started, the clip its item was accepted to follow, and whether the plan had
   * asked for its removal by then.
   */
  const starts: Array<{
    readonly tag: ClipTag | undefined;
    readonly follows: ClipTag | undefined;
    readonly removalAsked: boolean;
  }> = [];

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
    const result = Policy.step(settings, state, input, { mono: clock, wall: clock });
    state = result.state;
    wake = result.wake;
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
    for (const action of result.actions) {
      actions.push(action);
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
    // a key the plan holds already keeps the one it has.
    for (const action of actions.slice(before))
      if (
        action._tag === "Emit" &&
        action.event._tag === "AsRun" &&
        action.event.event.status._tag === "Accepted"
      ) {
        const lane = state.items.get(action.event.event.key)?.spec.lane;
        if (lane !== undefined) lanes.set(action.event.event.key, lane);
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
          break;
      }
    send({
      _tag: "Result",
      id: busy.id,
      result: refused ? failed("replied") : { _tag: "Done", clipId },
    });
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
      case "batch":
        return edit(
          index,
          [
            { _tag: "Withdraw", key: key(name) },
            { _tag: "Submit", spec: cued(`b${String(index)}`, 1, index) },
          ],
          true,
        );
      case "group":
        return edit(index, [
          {
            _tag: "SubmitGroup",
            key: key(`g${String(index)}`),
            lane: 1,
            parts: [cued(`g${String(index)}a`, 1, index), cued(`g${String(index)}b`, 1, index + 1)],
            fingerprint: `g${String(index)}`,
          },
        ]);
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
        // finishes the clip it builds first.
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
  return { actions, inputs, edits, drains, problems, groups, named, replaced, paired, starts };
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
  const { actions, edits, drains, problems, groups, named, replaced, paired, starts } = simulate(
    script,
    from,
    lifetimes,
  );
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
  // An item is dropped as withdrawn only when a withdrawal or a drain named it, an earlier
  // part of its group failed or was dropped (a replaced part keeps those after it), or it is
  // a replacement whose item started first, as 0.7.0 withdrew it.
  if (drains.length === 0) {
    const reasons = (name: string) =>
      (history.get(name) ?? []).flatMap((action) =>
        action.event._tag === "AsRun" && action.event.event.status._tag === "Dropped"
          ? [action.event.event.status.reason]
          : [],
      );
    const allowed = new Set(named);
    for (const parts of groups.values()) {
      const broken = parts.filter(
        (part) =>
          tags(part.key).includes("Failed") ||
          reasons(part.key).some((reason) => reason !== "replaced"),
      );
      const from = Math.min(...broken.map((part) => part.place));
      for (const part of parts) if (part.place > from) allowed.add(part.key);
    }
    for (const next of replaced.keys())
      for (let old = replaced.get(next); old !== undefined; old = replaced.get(old))
        if (tags(old).includes("Started")) allowed.add(next);
    for (const name of history.keys())
      if (reasons(name).includes("withdrawn"))
        assert.isTrue(allowed.has(name), `${name} was dropped though nothing withdrew it`);
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
  // the script says and starts the next at once, where H3 waits out a seam that no removal lands
  // within: a start the plan had asked to remove is excused. The Playout tests own that race.
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
