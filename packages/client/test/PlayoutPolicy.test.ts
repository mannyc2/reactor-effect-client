/** The playout's pure policy on its own: inputs in, actions and as-run out, no clock and no I/O. */
import { assert, describe, it } from "@effect/vitest";
import * as Policy from "../src/internal/playout/policy.js";
import type { ClipTag, SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { CommandFailure, ReactorError } from "../src/ReactorError.js";
import {
  buildFailed,
  check,
  clip,
  config,
  counterexamples,
  failed,
  item,
  key,
  measured,
  source,
  spec,
} from "./PlayoutPolicy.js";

/** Runs inputs in order at one-millisecond steps and collects every action. */
const run = (
  inputs: ReadonlyArray<Policy.Input>,
  start: Policy.State = Policy.initial,
  from = 0,
) => {
  let state = start;
  const actions: Array<Policy.Action> = [];
  inputs.forEach((input, index) => {
    const result = Policy.step(config, state, input, { mono: from + index, wall: from + index });
    state = result.state;
    actions.push(...result.actions);
  });
  return { state, actions };
};
const commands = (actions: ReadonlyArray<Policy.Action>) =>
  actions.flatMap((action) => (action._tag === "Command" ? [action] : []));
const statuses = (actions: ReadonlyArray<Policy.Action>, name: string) =>
  actions.flatMap((action) =>
    action._tag === "Emit" && action.event._tag === "AsRun" && action.event.event.key === name
      ? [action.event.event.status._tag]
      : [],
  );
/** A session opened with autoplay already confirmed, so the next command is the plan's. */
const opened = (id = "s1"): ReadonlyArray<Policy.Input> => [
  { _tag: "Opened", sessionId: id, lifetimeMs: 600_000 },
  { _tag: "Source", sessionId: id, event: { _tag: "State", state: source() } },
  { _tag: "Result", id: 1, result: { _tag: "Done" } },
];

/** Steps one input at `at` milliseconds, on both clocks. */
const at = (state: Policy.State, input: Policy.Input, time: number) =>
  Policy.step(config, state, input, { mono: time, wall: time });
/** The command in flight on `sessionId`'s lane, or on the first lane that has one. */
const inFlight = (state: Policy.State, sessionId?: string) =>
  state.sessions.find(
    (value) => value.busy !== undefined && (sessionId === undefined || value.id === sessionId),
  )?.busy;
/** The answer to the command in flight on `sessionId`'s lane, or on the first busy one. */
const answer = (
  state: Policy.State,
  result: Policy.CommandResult,
  sessionId?: string,
): Policy.Input => ({ _tag: "Result", id: inFlight(state, sessionId)?.id ?? -1, result });

/**
 * A policy driven input by input, each at the time given: the state it reached and every action,
 * with shorthands for the inputs most cases send.
 */
const drive = (options: { readonly config?: Policy.Config; readonly from?: Policy.State } = {}) => {
  const settings = options.config ?? config;
  let state = options.from ?? Policy.initial;
  const actions: Array<Policy.Action> = [];
  let clock = 0;
  const send = (input: Policy.Input, time = clock + 1) => {
    clock = Math.max(clock, time);
    const result = Policy.step(settings, state, input, { mono: clock, wall: clock });
    state = result.state;
    actions.push(...result.actions);
    return result;
  };
  return {
    state: () => state,
    actions,
    now: () => clock,
    send,
    /** Opens `id` with `lifetimeMs` and confirms whatever autoplay it asks for first. */
    open: (id = "s1", lifetimeMs = 600_000, time?: number) => {
      send({ _tag: "Opened", sessionId: id, lifetimeMs }, time);
      send({ _tag: "Source", sessionId: id, event: { _tag: "State", state: source() } });
      if (inFlight(state, id)?.command._tag === "Autoplay")
        send(answer(state, { _tag: "Done" }, id));
    },
    submit: (value: Policy.Spec, time?: number) =>
      send(
        {
          _tag: "Edit",
          id: 1000 + actions.length,
          edits: [{ _tag: "Submit", spec: value }],
          batch: false,
        },
        time,
      ),
    edit: (edits: ReadonlyArray<Policy.EditInput>, batch = false, time?: number) =>
      send({ _tag: "Edit", id: 1000 + actions.length, edits, batch }, time),
    /** Answers the command in flight on `id`'s lane, or on the first busy one. */
    reply: (result: Policy.CommandResult, time?: number, id?: string) =>
      send(answer(state, result, id), time),
    observe: (partial: Partial<SourceState>, id = "s1", time?: number) =>
      send(
        { _tag: "Source", sessionId: id, event: { _tag: "State", state: source(partial) } },
        time,
      ),
    event: (event: SourceEvent, id = "s1", time?: number) =>
      send({ _tag: "Source", sessionId: id, event }, time),
    tick: (time?: number) => send({ _tag: "Tick" }, time),
    /** The command in flight on `id`'s lane, or on the first busy one, if any. */
    busy: (id?: string) => inFlight(state, id)?.command,
  };
};

/** A session's queues as a scripted provider holds them, how many clips it made, and its orders. */
interface Queues {
  ready: Array<SourceClip>;
  /** Clips of the items `slow` names: each stays building until the test moves it on. */
  building: Array<SourceClip>;
  readonly slow: Set<string>;
  playing: SourceClip | undefined;
  made: number;
  /** The Ready queue's order after each command. */
  readonly orders: Array<ReadonlyArray<string>>;
}
const queues = (playing?: SourceClip): Queues => ({
  ready: [],
  building: [],
  slow: new Set(),
  playing,
  made: 0,
  orders: [],
});
/** The Ready clip of the item `name` in `held`. */
const readyIn = (held: Queues, name: string): SourceClip => {
  const found = held.ready.find((value) => value.tag?._tag === "Item" && value.tag.key === name);
  if (found === undefined) throw new Error(`${name} is not Ready`);
  return found;
};
const nameOf = (tag: ClipTag): string =>
  tag._tag === "Item" ? tag.key : `filler-${String(tag.index)}`;
/** Shows the plan `sessionId`'s queues as `held` has them, at `time`. */
const shown = (
  policy: ReturnType<typeof drive>,
  sessionId: string,
  held: Queues,
  time?: number,
): void => {
  policy.observe(
    {
      ready: [...held.ready],
      building: [...held.building],
      playing: held.playing,
      continuable: [...held.ready, ...(held.playing === undefined ? [] : [held.playing])].map(
        (value) => value.clipId,
      ),
    },
    sessionId,
    time,
  );
};
/**
 * A boundary at `time` on `sessionId`, as H3 with autoplay on takes it: the clip `held` plays
 * ends, and its Ready head starts 40 ms later.
 */
const boundary = (
  policy: ReturnType<typeof drive>,
  sessionId: string,
  held: Queues,
  time: number,
): void => {
  const ended = held.playing;
  if (ended !== undefined)
    policy.event({ _tag: "Ended", clip: ended, termination: "finished" }, sessionId, time);
  const [head, ...rest] = held.ready;
  held.playing = head;
  held.ready = rest;
  if (head !== undefined) policy.event({ _tag: "Started", clip: head }, sessionId, time + 40);
  shown(policy, sessionId, held, time + 41);
};
/** The items that started, in the order they did. */
const startOrder = (actions: ReadonlyArray<Policy.Action>): ReadonlyArray<string> =>
  actions.flatMap((action) =>
    action._tag === "Emit" &&
    action.event._tag === "AsRun" &&
    action.event.event.status._tag === "Started"
      ? [String(action.event.event.key)]
      : [],
  );
/**
 * Answers up to `rounds` commands on `sessionId` as a provider would: an enqueue's clip is Ready
 * `buildMs` later, or stays building if `held.slow` names its item; a removal or a move applies at
 * once, anything else succeeds. With nothing in flight, time moves on a second. Each enqueue and
 * removal goes into `log`.
 */
const provide = (
  policy: ReturnType<typeof drive>,
  sessionId: string,
  rounds: number,
  held: Queues,
  log: Array<string>,
  buildMs = 2_000,
): void => {
  const show = (time?: number) => {
    held.orders.push(held.ready.map((value) => value.clipId));
    shown(policy, sessionId, held, time);
  };
  for (let round = 0; round < rounds; round++) {
    const busy = policy.busy(sessionId);
    if (busy === undefined) {
      policy.tick(policy.now() + 1_000);
      if (policy.busy(sessionId) === undefined) return;
      continue;
    }
    switch (busy._tag) {
      case "Enqueue": {
        const name = nameOf(busy.tag);
        const clipId = `c-${name}-${String(held.made++)}`;
        log.push(
          `enqueue ${name}${busy.continueFrom === undefined ? "" : ` from ${busy.continueFrom}`}`,
        );
        policy.reply({ _tag: "Done", clipId }, undefined, sessionId);
        const made = clip(clipId, busy.tag, busy.request.seconds);
        if (held.slow.has(name)) {
          held.building = [...held.building, made];
          show();
        } else {
          held.ready = [...held.ready, made];
          show(policy.now() + buildMs);
        }
        break;
      }
      case "Remove": {
        log.push(`remove ${busy.clipId}`);
        policy.reply({ _tag: "Done" }, undefined, sessionId);
        held.ready = held.ready.filter((value) => value.clipId !== busy.clipId);
        show();
        break;
      }
      case "Move": {
        policy.reply({ _tag: "Done" }, undefined, sessionId);
        const moved = held.ready.find((value) => value.clipId === busy.clipId);
        if (moved !== undefined) {
          held.ready = held.ready.filter((value) => value !== moved);
          held.ready.splice(busy.position, 0, moved);
        }
        show();
        break;
      }
      default:
        policy.reply({ _tag: "Done" }, undefined, sessionId);
    }
  }
};

describe("PlayoutPolicy", () => {
  it("opens a session, turns autoplay on, then builds the first item in lane order", () => {
    const { actions } = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("a", 0) }], batch: false },
      ...opened(),
    ]);
    assert.isTrue(actions.some((action) => action._tag === "Open"));
    const [autoplay, build] = commands(actions);
    assert.deepStrictEqual(autoplay?.command, { _tag: "Autoplay", enabled: true });
    assert.deepStrictEqual(
      build?.command._tag === "Enqueue" ? build.command.tag : undefined,
      item("a"),
    );
  });

  it("orders a session's Ready clips by rank and never moves a clip it does not own", () => {
    const { actions } = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("a", 0) }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "ca" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({ ready: [clip("foreign"), clip("cb", item("b")), clip("ca", item("a"))] }),
        },
      },
    ]);
    const move = commands(actions).find((action) => action.command._tag === "Move");
    assert.deepStrictEqual(move?.command, { _tag: "Move", clipId: "ca", position: 1 });
  });

  it("cuts only a strictly lower lane's clip, and only with the cutter at the front", () => {
    const cut = (playing: SourceClip) =>
      commands(
        run([
          ...opened(),
          {
            _tag: "Edit",
            id: 1,
            edits: [{ _tag: "Submit", spec: spec("urgent", 0) }],
            batch: false,
          },
          {
            _tag: "Edit",
            id: 2,
            edits: [{ _tag: "Submit", spec: spec("other", 0) }],
            batch: false,
          },
          { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "cu" } },
          { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: playing } },
          {
            _tag: "Source",
            sessionId: "s1",
            event: {
              _tag: "State",
              state: source({ playing, ready: [clip("cu", item("urgent"))] }),
            },
          },
          { _tag: "Result", id: 3, result: { _tag: "Done" } },
        ]).actions,
      ).find((action) => action.command._tag === "Stop");
    assert.deepStrictEqual(cut(clip("filler", { _tag: "Filler", index: 0 }, 15))?.command, {
      _tag: "Stop",
      clipId: "filler",
    });
    assert.isUndefined(cut(clip("peer", item("other"), 15)));
  });

  // The 0.7.0 scheduler-cut paid run: H3 answered the stop before it reported the clip ended,
  // the scheduler cut the clip again, and that second stop cut the cutter 5 ms after it started.
  it("cuts a playing clip once, though its end is reported after the cut's result", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const playing = clip("long", item("other"), 15);
    policy.submit(spec("other"));
    policy.reply({ _tag: "Done", clipId: "long" });
    policy.event({ _tag: "Started", clip: playing });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "cu" });
    const stale: Partial<SourceState> = { playing, ready: [clip("cu", item("urgent"))] };
    policy.observe(stale);
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "long" });
    policy.reply({ _tag: "Done" });
    policy.observe(stale);
    policy.tick();
    const stops = commands(policy.actions).filter((action) => action.command._tag === "Stop");
    assert.strictEqual(stops.length, 1);
  });

  it("ends a cut whose play died, so autoplay airs the cutter at the next boundary", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const playing = clip("long", item("other"), 15);
    policy.submit(spec("other"));
    policy.reply({ _tag: "Done", clipId: "long" });
    policy.event({ _tag: "Started", clip: playing });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "cu" });
    policy.observe({ playing, ready: [clip("cu", item("urgent"))] });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "long" });
    policy.reply({ _tag: "Done" });
    policy.event({ _tag: "Ended", clip: playing, termination: "stopped" });
    policy.observe({ ready: [clip("cu", item("urgent"))] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Play", clipId: "cu" });
    policy.reply({ _tag: "Died" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
  });

  it("withdraws what has no clip at once and removes a clip before it drops it", () => {
    const early = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Withdraw", key: key("a") }], batch: false },
    ]);
    assert.isTrue(
      early.actions.some((action) => action._tag === "Withdrawn" && action.outcome === "not-found"),
    );
    const later = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "ca" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [clip("ca", item("a"))] }) },
      },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Withdraw", key: key("a") }], batch: false },
      { _tag: "Result", id: 3, result: { _tag: "Done" } },
    ]);
    const remove = commands(later.actions).find((action) => action.command._tag === "Remove");
    assert.deepStrictEqual(remove?.command, { _tag: "Remove", clipId: "ca" });
    assert.deepStrictEqual(statuses(later.actions, "a"), [
      "Accepted",
      "Building",
      "Ready",
      "Dropped",
    ]);
    assert.isTrue(
      later.actions.some((action) => action._tag === "Withdrawn" && action.outcome === "withdrawn"),
    );
  });

  // 0.7.0 took an insert after the playing clip as the next boundary; the rehearsed `edits`
  // check found the playout refusing it.
  it("inserts after the playing item at the next boundary, and refuses one before it", () => {
    const edit = (id: number, name: string, side: "before" | "after"): Policy.Input => ({
      _tag: "Edit",
      id,
      edits: [{ _tag: "Insert", spec: spec(name), anchor: key("p1"), side }],
      batch: false,
    });
    const { state, actions } = run([
      ...opened(),
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("p1") }], batch: false },
      { _tag: "Edit", id: 3, edits: [{ _tag: "Submit", spec: spec("p2") }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "c1" } },
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: clip("c1", item("p1")) } },
      { _tag: "Result", id: 3, result: { _tag: "Done", clipId: "c2" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({ playing: clip("c1", item("p1")), ready: [clip("c2", item("p2"))] }),
        },
      },
      edit(10, "before", "before"),
      edit(11, "next", "after"),
    ]);
    const refused = actions.flatMap((action) => (action._tag === "Refused" ? [action.id] : []));
    assert.deepStrictEqual(refused, [10]);
    assert.strictEqual(state.items.get(key("next"))?.mode, "follow");
    const build = commands(actions).at(-1)?.command;
    assert.deepStrictEqual(build?._tag === "Enqueue" ? build.tag : undefined, item("next"));
    // Once the anchor has ended, the insert still airs ahead of the item that followed it.
    const after = run(
      [
        answer(state, { _tag: "Done", clipId: "cn" }),
        {
          _tag: "Source",
          sessionId: "s1",
          event: { _tag: "Ended", clip: clip("c1", item("p1")), termination: "finished" },
        },
        {
          _tag: "Source",
          sessionId: "s1",
          event: {
            _tag: "State",
            state: source({ ready: [clip("c2", item("p2")), clip("cn", item("next"))] }),
          },
        },
      ],
      state,
      100,
    );
    const move = commands(after.actions).find((action) => action.command._tag === "Move");
    assert.deepStrictEqual(move?.command, { _tag: "Move", clipId: "cn", position: 0 });
  });

  it("counts no air left for a playing clip the provider named without its length", () => {
    const inputs: ReadonlyArray<Policy.Input> = [
      ...opened(),
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({
            playing: { clipId: "adopted", tag: undefined, seconds: undefined },
            ready: [clip("next")],
          }),
        },
      },
    ];
    const { state } = run(inputs);
    const view = Policy.view(config, state, { mono: 10, wall: 10 });
    assert.strictEqual(view.runwaySeconds, 5);
    // It was seen playing at the last input, so it started no later than that.
    assert.deepStrictEqual(view.playing, {
      key: "other",
      startedAt: inputs.length - 1,
      seconds: undefined,
    });
  });

  it("reports as runway the air secured: not a held clip, and still once a replacement opens", () => {
    const policy = drive({ config: { ...config, leadMs: 30_000 } });
    policy.tick(0);
    policy.open("s1", 90_000);
    for (const name of ["a", "b"]) {
      policy.submit(spec(name));
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
    }
    policy.submit({ ...spec("held"), start: { _tag: "Manual" } });
    policy.observe({
      ready: [clip("c-a", item("a")), clip("c-b", item("b")), clip("c-held", item("held"))],
    });
    const runway = () =>
      Policy.view(config, policy.state(), { mono: policy.now(), wall: policy.now() }).runwaySeconds;
    // The held clip airs only once released.
    assert.strictEqual(runway(), 10);
    assert.isTrue(policy.tick(60_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 90_000);
    // The replacement takes new work, but what the session on air holds still airs first.
    assert.strictEqual(runway(), 10);
  });

  it("keeps what plays from a start or an end over a report older than it", () => {
    const filler = (index: number) => clip(`f${String(index)}`, { _tag: "Filler", index });
    const on = (event: SourceEvent): Policy.Input => ({ _tag: "Source", sessionId: "s1", event });
    // H3 answers a start or an end with the facts it held before it, and says they are not current.
    const stale = (playing: SourceClip | undefined) =>
      on({ _tag: "State", state: source({ available: false, playing }) });
    const { actions, state } = run([
      ...opened(),
      on({ _tag: "Started", clip: filler(0) }),
      stale(undefined),
      on({ _tag: "Ended", clip: filler(0), termination: "finished" }),
      stale(filler(0)),
      on({ _tag: "Started", clip: filler(1) }),
      stale(undefined),
      on({ _tag: "State", state: source({ playing: filler(1) }) }),
    ]);
    assert.deepStrictEqual(
      actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Filler"
          ? [`${action.event.phase} ${String(action.event.index)}`]
          : [],
      ),
      ["Started 0", "Ended 0", "Started 1"],
    );
    // Filler 1 started at the eighth input, and a current report of it changes nothing.
    assert.deepStrictEqual(Policy.view(config, state, { mono: 20, wall: 20 }).playing, {
      key: "filler",
      startedAt: 7,
      seconds: 5,
    });
  });

  it("refuses a changed spec under a used key, and a batch with one refused edit changes nothing", () => {
    const { actions, state } = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      {
        _tag: "Edit",
        id: 2,
        edits: [{ _tag: "Submit", spec: { ...spec("a"), fingerprint: "changed" } }],
        batch: false,
      },
      {
        _tag: "Edit",
        id: 3,
        edits: [
          { _tag: "Submit", spec: spec("b") },
          { _tag: "Insert", spec: spec("c"), anchor: key("missing"), side: "after" },
        ],
        batch: true,
      },
    ]);
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Refused" && action.id === 2 && action.refusal._tag === "KeyMismatch",
      ),
    );
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Refused" && action.id === 3 && action.refusal._tag === "InvalidItem",
      ),
    );
    assert.isFalse(state.items.has(key("b")));
  });

  it("switches to a replacement only once the retiring session is idle and its grace has passed", () => {
    const setup = run([
      ...opened(),
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: clip("x") } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ playing: clip("x") }) },
      },
      { _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 },
      { _tag: "Source", sessionId: "s2", event: { _tag: "State", state: source() } },
    ]);
    assert.isFalse(
      setup.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
    );
    const ended = Policy.step(
      config,
      setup.state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "Ended", clip: clip("x"), termination: "finished" },
      },
      { mono: 100, wall: 100 },
    );
    const idle = Policy.step(
      config,
      ended.state,
      { _tag: "Source", sessionId: "s1", event: { _tag: "State", state: source() } },
      { mono: 101, wall: 101 },
    );
    assert.isFalse(idle.actions.some((action) => action._tag === "OnAir"));
    const after = Policy.step(config, idle.state, { _tag: "Tick" }, { mono: 400, wall: 400 });
    assert.isTrue(
      after.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
    );
    assert.isTrue(
      after.actions.some((action) => action._tag === "Close" && action.sessionId === "s1"),
    );
  });

  it("rebuilds a lost session's unaired items and settles its unknown ones for good", () => {
    const { actions } = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      {
        _tag: "Result",
        id: 2,
        result: failed("unknown"),
      },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Result", id: 3, result: { _tag: "Done", clipId: "cb" } },
      { _tag: "Lost", sessionId: "s1", reason: "gone" },
    ]);
    assert.deepStrictEqual(statuses(actions, "a").at(-1), "Unknown");
    assert.deepStrictEqual(statuses(actions, "b").at(-1), "Accepted");
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Emit" &&
          action.event._tag === "Session" &&
          action.event.event._tag === "Replaced",
      ),
    );
  });

  // A session going down refuses what it is sent before its source can say so. Only a change in
  // what the session reports can mend that, so a timer would resend it forever.
  it("sends an enqueue refused unsent again once, when its session's availability changes", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    const enqueues = () =>
      commands(policy.actions).filter((action) => action.command._tag === "Enqueue").length;
    policy.reply(failed("not-submitted"));
    policy.tick(5_000);
    policy.tick(60_000);
    assert.strictEqual(enqueues(), 1);
    policy.event({ _tag: "Reconnecting" });
    policy.event({ _tag: "Reconnected", afterMillis: 900 });
    policy.observe({});
    assert.deepStrictEqual(policy.busy(), {
      _tag: "Enqueue",
      request: spec("a").request,
      tag: item("a"),
      continueFrom: undefined,
    });
    assert.strictEqual(enqueues(), 2);
  });

  it("fails an item refused unsent again while its session reports itself available", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.reply(failed("not-submitted"));
    policy.event({ _tag: "Reconnecting" });
    policy.observe({});
    policy.reply(failed("not-submitted"));
    assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted", "Failed"]);
    const settled = policy.actions.findLast(
      (action) => action._tag === "Emit" && action.event._tag === "AsRun",
    );
    const status =
      settled?._tag === "Emit" && settled.event._tag === "AsRun"
        ? settled.event.event.status
        : undefined;
    assert.strictEqual(status?._tag === "Failed" ? status.reason._tag : status?._tag, "Command");
  });

  // Once Reactor or its moderation has ended a session, the provider refuses what it is sent
  // with why, before its source reports the session lost.
  for (const reason of ["TerminalSession", "Moderated"] as const)
    it(`carries an enqueue an ended session refused unsent to the next session (${reason})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit(spec("a"));
      policy.reply({
        _tag: "Failed",
        cause: CommandFailure.from(ReactorError.fromCode(reason, "the session ended"), {
          operation: "enqueue",
          outcome: "not-submitted",
        }),
      });
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
      policy.open("s2");
      assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted"]);
      assert.deepStrictEqual(policy.busy("s2"), {
        _tag: "Enqueue",
        request: spec("a").request,
        tag: item("a"),
        continueFrom: undefined,
      });
    });

  for (const refusal of ["InvalidState", "Disconnected"] as const)
    it(`drops an item withdrawn while its enqueue was in flight once that fails unsent (${refusal})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit(spec("a"));
      policy.edit([{ _tag: "Withdraw", key: key("a") }]);
      policy.reply({
        _tag: "Failed",
        cause: CommandFailure.from(ReactorError.fromCode(refusal, "the provider refused it"), {
          operation: "enqueue",
          outcome: "not-submitted",
        }),
      });
      assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted", "Dropped"]);
      assert.deepStrictEqual(
        policy.actions.flatMap((action) => (action._tag === "Withdrawn" ? [action.outcome] : [])),
        ["withdrawn"],
      );
    });

  it("removes a retiring session's filler only while it takes commands, and once per refusal", () => {
    const policy = drive();
    const filler = (index: number) => clip(`f${String(index)}`, { _tag: "Filler", index });
    const retiring = { playing: filler(0), ready: [filler(1)] };
    const removes = () =>
      commands(policy.actions).filter(
        (action) => action.command._tag === "Remove" && action.sessionId === "s1",
      ).length;
    policy.tick(0);
    policy.open("s1");
    policy.observe(retiring, "s1");
    policy.open("s2");
    policy.submit(spec("x"));
    assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["x"]);
    policy.reply({ _tag: "Done", clipId: "cx" });
    policy.event({ _tag: "Reconnecting" }, "s1");
    policy.observe({ ready: [clip("cx", item("x"))] }, "s2");
    assert.strictEqual(removes(), 0);
    policy.observe(retiring, "s1");
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "f1" });
    policy.reply({
      _tag: "Failed",
      cause: CommandFailure.from(ReactorError.fromCode("InvalidState", "the clip is armed"), {
        operation: "pop",
        outcome: "replied",
        requestId: "request",
        generation: 1n,
      }),
    });
    policy.observe(retiring, "s1");
    policy.tick();
    assert.strictEqual(removes(), 1);
    // Once its queues change it is asked again, and at once after an answer that may have applied.
    policy.observe({ ...retiring, ready: [filler(1), filler(2)] }, "s1");
    policy.reply(failed("unknown"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "f1" });
  });

  it("a drain withdraws a held Manual item and finishes although it was never released", () => {
    const { actions } = run([
      ...opened(),
      {
        _tag: "Edit",
        id: 1,
        edits: [{ _tag: "Submit", spec: { ...spec("held"), start: { _tag: "Manual" } } }],
        batch: false,
      },
      { _tag: "Drain", id: 2, finish: "accepted" },
      { _tag: "Tick" },
    ]);
    assert.strictEqual(statuses(actions, "held").at(-1), "Dropped");
    assert.isTrue(actions.some((action) => action._tag === "Drained" && action.id === 2));
  });

  it("fires a cue due at a clip's last moment when the clip finishes there", () => {
    const cued: Policy.Spec = { ...spec("a"), cues: [{ name: "out", from: "end", offsetMs: 0 }] };
    let state = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: cued }], batch: false },
    ]).state;
    state = at(state, answer(state, { _tag: "Done", clipId: "ca" }), 10).state;
    const playing = clip("ca", item("a"));
    state = at(
      state,
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: playing } },
      100,
    ).state;
    const ended = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "Ended", clip: playing, termination: "finished" },
      },
      5_100,
    );
    assert.deepStrictEqual(
      ended.actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Cue" ? [action.event.event.name] : [],
      ),
      ["out"],
    );
  });

  it("learns build time only from builds whose dispatch outcome was always known", () => {
    let state = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("known") }], batch: false },
    ]).state;
    state = at(state, answer(state, { _tag: "Done", clipId: "ck" }), 10).state;
    const known = clip("ck", item("known"));
    state = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [known] }) },
      },
      2_000,
    ).state;
    assert.strictEqual(state.samples.build.length, 1);
    state = at(
      state,
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("unsure") }], batch: false },
      2_001,
    ).state;
    state = at(state, answer(state, failed("unknown")), 2_010).state;
    // Its clip turns up Ready much later: that wait includes the uncertainty, so it is no sample.
    const unsure = clip("cu", item("unsure"));
    state = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [known, unsure] }) },
      },
      40_000,
    ).state;
    assert.strictEqual(state.items.get(key("unsure"))?.phase, "Ready");
    assert.strictEqual(state.samples.build.length, 1);
  });

  /**
   * `p1` plays while `p2` builds, each build taking `buildMs`, and `xc`, inserted before `p2`
   * to continue from the clip before it, waits for the build slot. With no continued build
   * measured, its build is projected at 2.5 times the longest independent one.
   */
  const continuing = (buildMs: number) => {
    let state = run(opened()).state;
    const actions: Array<Policy.Action> = [];
    const send = (input: Policy.Input, time: number) => {
      const result = at(state, input, time);
      state = result.state;
      actions.push(...result.actions);
    };
    const observed = (time: number, partial: Partial<SourceState>) =>
      send(
        {
          _tag: "Source",
          sessionId: "s1",
          event: { _tag: "State", state: source({ continuable: ["c1", "c2"], ...partial }) },
        },
        time,
      );
    const c1 = clip("c1", item("p1"));
    const c2 = clip("c2", item("p2"));
    for (const name of ["p1", "p2"])
      send(
        { _tag: "Edit", id: 0, edits: [{ _tag: "Submit", spec: spec(name) }], batch: false },
        10,
      );
    send(answer(state, { _tag: "Done", clipId: "c1" }), 20);
    const played = 10 + buildMs + 40;
    observed(10 + buildMs, { ready: [c1] });
    send({ _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: c1 } }, played);
    observed(played, { playing: c1 });
    send(answer(state, { _tag: "Done", clipId: "c2" }), played + 10);
    send(
      {
        _tag: "Edit",
        id: 0,
        edits: [
          {
            _tag: "Insert",
            spec: { ...spec("xc"), continuity: true },
            anchor: key("p2"),
            side: "before",
          },
        ],
        batch: false,
      },
      played + 50,
    );
    const dispatched = 10 + 2 * buildMs;
    observed(dispatched, { playing: c1, ready: [c2] });
    const build = commands(actions).at(-1)?.command;
    return { state: () => state, actions, send, observed, build, dispatched, c1 };
  };

  // The 0.7.0 scheduler-edits paid run: a continued insert waited behind a build in flight,
  // took 5.45 s to build against about 2.2 s, missed the playing clip's end, and aired after
  // the next clip, which it did not continue from.
  it("continues a build projected to miss its predecessor's end from the clip airing by then", () => {
    // Builds of 2 s: xc is projected Ready 6 s after it is sent, after p1 ends.
    const late = continuing(2_000);
    assert.deepStrictEqual(late.build, {
      _tag: "Enqueue",
      request: spec("xc").request,
      tag: item("xc"),
      continueFrom: "c2",
    });
    // Builds of half a second: xc is projected Ready before p1 ends, so it continues from p1.
    const early = continuing(500);
    assert.strictEqual(early.build?._tag === "Enqueue" && early.build.continueFrom, "c1");
  });

  it("holds a clip continued from the clip after its place behind that clip, and measures it apart", () => {
    const { state, actions, send, observed, dispatched, c1 } = continuing(2_000);
    send(answer(state(), { _tag: "Done", clipId: "cx" }), dispatched + 10);
    // xc is Ready 2 s later, before p1 ends, but it continues from p2 and so airs after it.
    const readyAt = dispatched + 2_000;
    const seen = actions.length;
    observed(readyAt, { playing: c1, ready: [clip("c2", item("p2")), clip("cx", item("xc"))] });
    send({ _tag: "Tick" }, readyAt + 1);
    const moves = commands(actions.slice(seen)).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves, []);
    assert.strictEqual(state().items.get(key("xc"))?.phase, "Ready");
    assert.deepStrictEqual(state().samples.build.length, 2);
    assert.deepStrictEqual(state().samples.continued, [
      { lane: 1, perBuilt: 0.4, perRequested: 0.4 },
    ]);
  });
});

const enqueued = (actions: ReadonlyArray<Policy.Action>, sessionId?: string) =>
  commands(actions).flatMap((action) =>
    action.command._tag === "Enqueue" && (sessionId === undefined || action.sessionId === sessionId)
      ? [action.command.tag._tag === "Item" ? String(action.command.tag.key) : "filler"]
      : [],
  );
const unknown = failed("unknown");
/** A filler floor of 5 s and a target of 10 s, in clips of 5 s. */
const filled: Policy.Config = {
  ...config,
  filler: {
    floor: 5,
    target: 10,
    clip: ({ index }) => ({ prompt: `filler ${String(index)}`, seconds: 5 }),
    lengths: { min: 5, max: 15 },
    invalid: () => undefined,
    protect: "air",
  },
};

describe("PlayoutPolicy, a follower's fence", () => {
  // The fence goes out while a plays with time to spare, but lands only in b's seam, after a state
  // read named b playing: H3 held b armed, and with autoplay off it never starts it.
  it("plays a clip held armed by a fence that landed in its seam", () => {
    const policy = drive();
    policy.open("s1");
    const a = clip("ca", item("a"), 10);
    const b = clip("cb", item("b"));
    policy.submit(spec("a", 1, 10), 10);
    policy.reply({ _tag: "Done", clipId: "ca" }, 20);
    policy.observe({ ready: [a] }, "s1", 30);
    policy.submit(spec("b"), 40);
    policy.reply({ _tag: "Done", clipId: "cb" }, 50);
    policy.event({ _tag: "Started", clip: a }, "s1", 100);
    policy.observe({ playing: a, ready: [b] }, "s1", 100);
    const follower = { ...spec("v"), follows: item("b") };
    policy.edit(
      [{ _tag: "Insert", spec: follower, anchor: key("b"), side: "after" }],
      false,
      9_800,
    );
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
    policy.event({ _tag: "Ended", clip: a, termination: "finished" }, "s1", 10_100);
    policy.observe({ playing: b, ready: [b] }, "s1", 10_120);
    policy.reply({ _tag: "Done" }, 10_130);
    policy.reply({ _tag: "Done", clipId: "cv" }, 10_170);
    policy.observe({ ready: [b], building: [clip("cv", item("v"))] }, "s1", 10_200);
    assert.deepStrictEqual(policy.busy(), { _tag: "Play", clipId: "cb" });
  });
});

// What 0.7.0's scheduler guaranteed and the first Playout lost, found by an independent critique.
describe("PlayoutPolicy, uncertainty and loss", () => {
  // 0.7.0 SchedulerRenewal "uncertain filler on the retiring source does not hold replacement
  // runway", and SchedulerUnknownRecovery's per-source filler identities.
  for (const lifetimeMs of [600_000, Infinity])
    it(`a lost filler reply holds only its own session, replaced by the deadline (lifetime ${String(lifetimeMs)})`, () => {
      const policy = drive({ config: filled });
      policy.tick(0);
      policy.open("s1", lifetimeMs);
      assert.deepStrictEqual(enqueued(policy.actions), ["filler"]);
      policy.reply(unknown, 100);
      policy.submit(spec("a"), 200);
      const deadline = policy.tick(100 + filled.unknownTimeoutMs);
      assert.isTrue(deadline.actions.some((action) => action._tag === "Open"));
      policy.open("s2", lifetimeMs);
      assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["a"]);
    });

  for (const lifetimeMs of [600_000, Infinity])
    it(`a lost item reply opens a replacement at the deadline, not at the cap (lifetime ${String(lifetimeMs)})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open("s1", lifetimeMs);
      policy.submit(spec("lost"), 10);
      policy.reply(unknown, 20);
      policy.submit(spec("next"), 30);
      // The lost reply holds no build slot: what follows builds behind it on the same session.
      assert.deepStrictEqual(enqueued(policy.actions), ["lost", "next"]);
      const deadline = policy.tick(20 + config.unknownTimeoutMs);
      assert.isTrue(deadline.actions.some((action) => action._tag === "Open"));
    });

  // JSON has no Infinity, so a persisted event would read null: a grant leaves an uncapped cap out too.
  it("reports a session no cap ends without a lifetime", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1", Infinity);
    const opened = policy.actions.flatMap((action) =>
      action._tag === "Emit" &&
      action.event._tag === "Session" &&
      action.event.event._tag === "Opened"
        ? [action.event.event]
        : [],
    );
    assert.deepStrictEqual(opened, [{ _tag: "Opened", sessionId: "s1" }]);
  });

  // 0.7.0 SchedulerFates: a session lost is closed, so it cannot bill beside its replacement.
  it("closes a session it lost", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    const lost = policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" });
    assert.isTrue(
      lost.actions.some((action) => action._tag === "Close" && action.sessionId === "s1"),
    );
  });
});
describe("PlayoutPolicy, lanes", () => {
  const fillersOf = (policy: ReturnType<typeof drive>) =>
    commands(policy.actions).flatMap((action) =>
      action.command._tag === "Enqueue" && action.command.tag._tag === "Filler"
        ? [
            `${action.sessionId} ${String(action.command.tag.index)} ${action.command.request.prompt}`,
          ]
        : [],
    );
  /** s1 on air with a clip of its own and filler 0 in flight, and its replacement s2 open. */
  const renewing = () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    // A clip of its own keeps s1 on air while its replacement opens.
    const playing = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: playing });
    policy.observe({ playing });
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000);
    return policy;
  };

  // A filler enqueue whose command is lost holds its own lane for about 20 s, and no other: each
  // clip takes its index as its enqueue goes out, so the replacement sends filler of its own.
  it("sends filler on the replacement's lane while the session on air carries some", () => {
    const policy = renewing();
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1"]);
    policy.reply({ _tag: "Done", clipId: "f0" }, undefined, "s1");
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1"]);
  });

  it("asks for a refused filler clip again as it was, on whichever lane is free", () => {
    const policy = renewing();
    policy.reply(failed("replied"), 30_100, "s1");
    policy.reply({ _tag: "Done", clipId: "f1" }, 30_200, "s2");
    policy.tick(31_100);
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1", "s2 0 filler 0"]);
  });

  it("records a filler clip under the index it was asked for, though moderation moved on", () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    policy.event({ _tag: "Moderated", action: "terminate", categories: ["test"] }, "s1");
    policy.reply({ _tag: "Done", clipId: "f0" }, undefined, "s1");
    assert.strictEqual(policy.state().sessions[0]?.fillers.get("f0")?.index, 0);
  });

  // An enqueue in flight as its session goes made no clip that can still air, so its clip is
  // asked for on the next session: unless moderation flagged it, which would end that one too.
  it("asks for a lost session's filler clip again on the next, unless moderation flagged it", () => {
    for (const flagged of [false, true]) {
      const policy = drive({ config: filled });
      policy.tick(0);
      policy.open("s1");
      if (flagged)
        policy.event({ _tag: "Moderated", action: "terminate", categories: ["test"] }, "s1");
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
      policy.open("s2", 600_000, 2_000);
      assert.deepStrictEqual(
        fillersOf(policy),
        flagged ? ["s1 0 filler 0", "s2 1 filler 1"] : ["s1 0 filler 0", "s2 0 filler 0"],
      );
    }
  });

  // With one refused move remembered for the whole plan, a second session's refusal made the
  // first's move look new, and the two sessions' moves were sent again in turn.
  it("sends a refused move again only once its own session's queues have changed", () => {
    const policy = drive();
    const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
    const moves = () =>
      commands(policy.actions).flatMap((action) =>
        action.command._tag === "Move" ? [`${action.sessionId} ${action.command.clipId}`] : [],
      );
    policy.tick(0);
    policy.open("s1", 60_000);
    const playing = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: playing });
    policy.observe({ playing, ready: [filler("b", 1), filler("a", 0)] });
    policy.reply(failed("replied"), undefined, "s1");
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000);
    policy.observe({ ready: [filler("d", 3), filler("c", 2)] }, "s2");
    policy.reply(failed("replied"), undefined, "s2");
    policy.tick();
    policy.tick();
    assert.deepStrictEqual(moves(), ["s1 a", "s2 c"]);
    // s1's queues change, so its move is asked again, and s2's is not.
    policy.observe({ playing, ready: [filler("b", 1), filler("a", 0), filler("e", 4)] });
    assert.deepStrictEqual(moves(), ["s1 a", "s2 c", "s1 a"]);
  });

  // A provider may refuse autoplay every time, and a source method may throw every time: each is
  // asked again a second later, not at every reply. Until its autoplay is as its role wants it,
  // nothing else goes to the session, whose clips would otherwise air, or not, as they must not.
  it("sends a failed autoplay, or a move that died, again a second later", () => {
    for (const result of [{ _tag: "Died" } as const, failed("replied")]) {
      const policy = drive();
      policy.tick(0);
      policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 });
      policy.submit(spec("a"));
      policy.observe({});
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
      assert.strictEqual(policy.reply(result, 10).wake, 1_010);
      assert.isUndefined(policy.busy());
      policy.tick(1_010);
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
      policy.reply({ _tag: "Done" });
      assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    }
    const policy = drive();
    const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
    policy.tick(0);
    policy.open();
    policy.observe({ playing: clip("x", undefined, 50), ready: [filler("b", 1), filler("a", 0)] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Move", clipId: "a", position: 0 });
    assert.strictEqual(policy.reply({ _tag: "Died" }, 10).wake, 1_010);
    assert.isUndefined(policy.busy());
    policy.tick(1_010);
    assert.deepStrictEqual(policy.busy(), { _tag: "Move", clipId: "a", position: 0 });
  });

  // s2 opens as s1's replacement, and its autoplay off dies at 40 s. Within the second, s1 goes
  // idle or is lost and s2 takes the air: autoplay on is not the value that failed, so it goes.
  it("turns autoplay on as a session takes the air, though its autoplay off just failed", () => {
    for (const how of ["idle", "lost"] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open("s1", 60_000);
      const x = clip("x", undefined, 40);
      policy.event({ _tag: "Started", clip: x }, "s1", 100);
      policy.observe({ playing: x }, "s1", 100);
      assert.isTrue(policy.tick(30_001).actions.some((action) => action._tag === "Open"));
      policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 }, 30_100);
      policy.observe({}, "s2", 30_200);
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: false });
      policy.reply({ _tag: "Died" }, 40_000, "s2");
      if (how === "idle") {
        policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 40_100);
        policy.observe({}, "s1", 40_100);
        policy.tick(40_350);
      } else policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, 40_200);
      assert.isTrue(
        policy.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
      );
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: true });
    }
  });

  // A cut begins with autoplay off, and the cut ends if that fails. One that died, or whose reply
  // was lost, may have applied, so autoplay on goes again; one refused applied nothing.
  it("turns autoplay on again once a cut's autoplay off may have applied", () => {
    const again = { _tag: "Autoplay", enabled: true } as const;
    for (const [result, after] of [
      [{ _tag: "Died" } as const, again],
      [failed("unknown"), again],
      [failed("replied"), undefined],
    ] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open();
      const f = clip("f", { _tag: "Filler", index: 0 }, 30);
      policy.event({ _tag: "Started", clip: f }, "s1", 10);
      policy.observe({ playing: f }, "s1", 10);
      policy.submit(spec("u", 0), 20);
      policy.reply({ _tag: "Done", clipId: "c-u" }, 30);
      policy.observe({ playing: f, ready: [clip("c-u", item("u"))] }, "s1", 40);
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
      policy.reply(result, 50);
      assert.deepStrictEqual(policy.busy(), after);
    }
  });
});

/** Filler with a floor and twice that as its target, asking for the length the plan wants. */
const protecting = (
  protect: "air" | "order",
  lengths = { min: 5, max: 15 },
  floor = 5,
): Policy.Config => ({
  ...config,
  filler: {
    floor,
    target: floor * 2,
    clip: ({ index, seconds }) => ({ prompt: `filler ${String(index)}`, seconds }),
    lengths,
    invalid: () => undefined,
    protect,
  },
});

describe("PlayoutPolicy, air before queue order", () => {
  /** s1 on air playing a clip of its own that may be continued, `rest` seconds of it left. */
  const airing = (settings: Policy.Config, rest: number, from = measured) => {
    const policy = drive({ config: settings, from });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 });
    const playing = clip("x", undefined, rest);
    policy.observe({ playing, continuable: ["x"] });
    if (policy.busy()?._tag === "Autoplay") policy.reply({ _tag: "Done" });
    return { policy, playing };
  };
  const sent = (command: Policy.Command | undefined) =>
    command?._tag !== "Enqueue"
      ? undefined
      : command.tag._tag === "Item"
        ? String(command.tag.key)
        : `filler of ${command.request.seconds?.toFixed(2) ?? "?"} s`;
  const long: Policy.Spec = { ...spec("long", 1, 15), continuity: true };

  // A 15 s continued build, projected at 16 s with its margin, against 12 s secured.
  it("sends a filler clip ahead of a build that would outlast the air secured, then the build", () => {
    const order = airing(protecting("order"), 12).policy;
    order.submit(long);
    assert.strictEqual(sent(order.busy()), "long");
    const { policy, playing } = airing(protecting("air"), 12);
    policy.submit(long);
    // Its own build runs the air down 0.4 s a second, so 6.67 s of it covers the 4 s missing.
    assert.strictEqual(sent(policy.busy()), "filler of 6.67 s");
    const cover = clip("f0", { _tag: "Filler", index: 0 }, 5);
    policy.observe({ playing, building: [cover], continuable: ["x"] });
    policy.reply({ _tag: "Done", clipId: "f0" });
    assert.isUndefined(policy.busy());
    // Built shorter than asked, it leaves the runway short: the item waits for no second one.
    policy.observe({ playing, ready: [cover], continuable: ["x", "f0"] }, "s1", 2_700);
    assert.strictEqual(sent(policy.busy()), "long");
  });

  // At 0.8 s a second, covering the 3 s missing takes more than a 15 s clip, whose build alone
  // outlasts the air; one airing while the item builds, 8 s, leaves the air dark least.
  it("sends a filler clip that airs no longer than the item takes to build", () => {
    const build = { lane: 1, perBuilt: 0.8, perRequested: 0.8 };
    const slow = {
      ...measured,
      samples: {
        build: [build, build, build],
        continued: [],
        length: [],
        overBuilt: [],
        aired: [],
      },
    };
    const { policy } = airing(protecting("air"), 6, slow);
    policy.submit(spec("ten", 1, 10));
    assert.strictEqual(sent(policy.busy()), "filler of 8.00 s");
  });

  it("builds each item at once until three builds were measured", () => {
    const { policy } = airing(protecting("air"), 12, Policy.initial);
    policy.submit(long);
    assert.strictEqual(sent(policy.busy()), "long");
  });

  // Filler only where it covers a build, in clips of at least 10 s: one builds in 4 s, the item
  // in 2 s, so sending it first would leave the air dark longer.
  it("sends no filler clip that would build no sooner than the item", () => {
    const { policy } = airing(protecting("air", { min: 10, max: 15 }, 0), 1);
    policy.submit(spec("short"));
    assert.strictEqual(sent(policy.busy()), "short");
  });

  // A filler clip first would put its projected start at 8.4 s, past the 7 s it must start by,
  // and the plan would drop it as late.
  it("builds an item with a time to meet at once", () => {
    const { policy } = airing(protecting("air", { min: 5, max: 15 }, 0), 3);
    policy.submit({ ...spec("firm", 1, 15), window: { startByMs: 7_000, firm: true } });
    assert.strictEqual(sent(policy.busy()), "firm");
  });

  // The item it continues may not start for a minute, so it cannot be built yet; once it can, the
  // air its build needs is already there.
  it("keeps the runway at the build of the item next in line while that item waits", () => {
    const refill = (protect: "air" | "order") => {
      const { policy } = airing(protecting(protect), 12);
      policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } });
      policy.submit(long);
      return sent(policy.busy());
    };
    assert.isUndefined(refill("order"));
    assert.strictEqual(refill("air"), "filler of 5.00 s");
  });

  // The plan refills as the runway reaches this floor, not only at the clip floor, 11 s later.
  // Here only the plan's own wakes come.
  it("refills as the runway falls to the floor that covers the next item's build", () => {
    const { policy } = airing(protecting("air"), 30);
    policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } });
    let wake = policy.submit(long).wake;
    for (let guard = 0; guard < 10 && policy.busy() === undefined && wake !== undefined; guard++)
      wake = policy.tick(wake).wake;
    // x was seen playing at 2 ms with 30 s left: the runway falls to long's floor, 16 s, at 14 s.
    assert.strictEqual(policy.now(), 14_002);
    assert.strictEqual(sent(policy.busy()), "filler of 5.00 s");
  });

  // A refused filler clip is asked for again as it was: 15 s of it, asked to cover a long build,
  // take 6 s to build, and the 5 s item only 2 s, so the item goes first rather than wait for it.
  it("covers an item with a refused filler clip only if that clip builds sooner", () => {
    const { policy } = airing(protecting("air", { min: 1, max: 15 }, 0), 1);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    policy.submit(spec("short"), 1_100);
    assert.strictEqual(sent(policy.busy()), "short");
  });

  it("projects a refused filler clip's build at the length it is asked for again", () => {
    const { policy, playing } = airing(protecting("air"), 6);
    // x's start is seen, so the runway falls with the time: below the 5 s floor from 1,004 on.
    policy.event({ _tag: "Started", clip: playing }, "s1", 4);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    // Below the floor the refill asks for the shortest clip, and the refused 15 s one goes again.
    policy.tick(1_030);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    // It builds until 7,030, and a 5 s item 2 s after it: one that must start by 8,040 cannot.
    const firm = { ...spec("firm"), window: { startByMs: 7_000, firm: true } };
    const refused = policy
      .submit(firm, 1_040)
      .actions.flatMap((action) => (action._tag === "Refused" ? [action.refusal._tag] : []));
    assert.deepStrictEqual(refused, ["WouldMissDeadline"]);
  });

  // Refused, the clip builds nothing. A 5 s item that must start by 7,040 can, as x ends at
  // 6,002, and is not refused as if 15 s of filler built ahead of it until 6,010.
  it("projects no build for a filler clip the provider refused", () => {
    const { policy } = airing(protecting("air"), 6);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    const firm = { ...spec("firm"), window: { startByMs: 7_000, firm: true } };
    const refused = policy
      .submit(firm, 40)
      .actions.flatMap((action) => (action._tag === "Refused" ? [action.refusal._tag] : []));
    assert.deepStrictEqual(refused, []);
  });

  // A cover goes out for dark air projected, not for the margin alone. A 15 s item builds in 6 s,
  // and nearly 7 s are secured.
  it("sends no filler clip ahead of a build the air secured outlasts, by less than the margin", () => {
    const { policy } = airing(protecting("air"), 7);
    policy.submit(spec("item", 1, 15));
    assert.strictEqual(sent(policy.busy()), "item");
  });

  // An item on a cut lane, or an Asap one, has a time to meet: now. With 5.5 s secured, above the
  // floor, and a 6 s build, only an item without one waits for a cover.
  it("sends a cut lane's item and an Asap one ahead of the cover their builds would need", () => {
    for (const [value, first] of [
      [spec("cutter", 0, 15), "cutter"],
      [{ ...spec("asap", 1, 15), start: { _tag: "Asap" } } as const, "asap"],
      [spec("line", 1, 15), "filler of 5.00 s"],
    ] as const) {
      const { policy } = airing(protecting("air"), 5.5);
      policy.submit(value);
      assert.strictEqual(sent(policy.busy()), first);
    }
  });

  // Released, a Manual item airs as an Asap one does, at the next boundary.
  it("sends a released Manual item ahead of the cover its build would need, as an Asap one", () => {
    const { policy } = airing(protecting("air"), 5.5);
    policy.submit({ ...spec("held", 1, 15), start: { _tag: "Manual" } });
    // Held, it is built ahead only while the runway keeps the floor that covers its build.
    assert.strictEqual(sent(policy.busy()), "filler of 5.00 s");
    policy.send({ _tag: "Release", id: 1, key: key("held") });
    policy.reply({ _tag: "Done", clipId: "f0" });
    assert.strictEqual(sent(policy.busy()), "held");
  });

  // The cover an item had went with its lost session, and on the next no air is secured: it is
  // covered again there.
  it("covers a carried item again on the session it is carried to", () => {
    const { policy, playing } = airing(protecting("air"), 12);
    policy.submit(long);
    assert.strictEqual(sent(policy.busy()), "filler of 6.67 s");
    policy.reply({ _tag: "Done", clipId: "f0" });
    const cover = clip("f0", { _tag: "Filler", index: 0 }, 6.67);
    policy.observe({ playing, ready: [cover], continuable: ["x", "f0"] });
    assert.strictEqual(sent(policy.busy()), "long");
    policy.reply({ _tag: "Done", clipId: "c-long" });
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    policy.open("s2");
    // With no clip to continue there, it builds in 6 s, and a clip that airs as long covers it.
    assert.strictEqual(sent(policy.busy("s2")), "filler of 6.00 s");
  });

  // With 14.5 s left before s2's cap, the refill's shortest clip would air whole there, but the
  // refused 15 s clip it asks for again would not: that waits for a session it fits on.
  it("sends a refused filler clip again only where it airs whole before the cap", () => {
    const { policy } = airing(protecting("air"), 6);
    policy.submit(long, 10);
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, 40);
    policy.open("s2", 16_500, 50);
    policy.tick(1_030);
    assert.strictEqual(sent(policy.busy("s2")), undefined);
  });
});

// ReactorTest numbers clip ids per session, so two sessions' clips can share one.
// A clip is looked up by id only on the session it was reported by or a command went to.
describe("PlayoutPolicy, clip ids", () => {
  const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
  /** s1 on air with a clip of its own and filler 0 in flight, and s2 with filler 1 in flight. */
  const renewing = () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: x });
    policy.observe({ playing: x });
    policy.tick(30_010);
    policy.open("s2", 60_000);
    // Each session names its filler clip c1.
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s1");
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s2");
    return { policy, x };
  };

  it("settles on a removal's result only the clip on the session it went to", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    policy.observe({ playing: filler("c0", 0), ready: [filler("c1", 1)] }, "s1");
    policy.open("s2");
    policy.submit(spec("x"));
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s2");
    policy.observe({ ready: [clip("c1", item("x"))] }, "s2");
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Remove", clipId: "c1" });
    policy.reply({ _tag: "Done" }, undefined, "s1");
    assert.strictEqual(policy.state().items.get(key("x"))?.phase, "Ready");
  });

  it("measures each session's filler build, though their clips share an id", () => {
    const { policy, x } = renewing();
    policy.observe({ playing: x, ready: [filler("c1", 0)] }, "s1");
    policy.observe({ ready: [filler("c1", 1)] }, "s2");
    assert.strictEqual(policy.state().samples.build.length, 2);
  });

  it("forgets a failed filler clip only on its own session", () => {
    const { policy } = renewing();
    policy.event(buildFailed(filler("c1", 0)), "s1");
    policy.observe({ ready: [filler("c1", 1)] }, "s2");
    assert.strictEqual(policy.state().samples.build.length, 1);
  });

  it("adopts an item's clip on its new session, though its clip taken off before had the id", () => {
    const policy = drive();
    const timed = {
      ...spec("timed"),
      start: { _tag: "At", time: 12_000, late: "nextBoundary" },
    } as const;
    policy.tick(0);
    policy.open("s1");
    const x = clip("x", undefined, 5);
    const y = clip("y", undefined, 10);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, ready: [y] }, "s1", 10);
    policy.submit(timed, 20);
    policy.reply({ _tag: "Done", clipId: "c1" }, 30);
    policy.observe({ playing: x, ready: [y, clip("c1", item("timed"))] }, "s1", 40);
    // y goes, and timed is Ready too early: it is taken off, to be built again.
    policy.observe({ playing: x, ready: [clip("c1", item("timed"))] }, "s1", 50);
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Remove", clipId: "c1" });
    policy.reply({ _tag: "Done" }, 60);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" }, 70);
    policy.open("s2", 600_000, 80);
    const z = clip("z", undefined, 20);
    policy.event({ _tag: "Started", clip: z }, "s2", 90);
    policy.observe({ playing: z }, "s2", 90);
    assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["timed"]);
    policy.reply({ _tag: "Done", clipId: "c1" }, 100, "s2");
    policy.observe({ playing: z, ready: [clip("c1", item("timed"))] }, "s2", 110);
    assert.strictEqual(policy.state().items.get(key("timed"))?.phase, "Ready");
  });

  it("cuts a clip on the session on air, though a clip of its id was cut on another", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    const first = clip("c1", { _tag: "Filler", index: 0 }, 15);
    policy.event({ _tag: "Started", clip: first }, "s1", 10);
    policy.submit(spec("u", 0), 20);
    policy.reply({ _tag: "Done", clipId: "c2" }, 30);
    policy.observe({ playing: first, ready: [clip("c2", item("u"))] }, "s1", 40);
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Autoplay", enabled: false });
    // s1 goes before the cut ends: u is built again on s2, which airs filler of its own named c1.
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" }, 50);
    policy.open("s2", 600_000, 60);
    const second = clip("c1", { _tag: "Filler", index: 1 }, 15);
    policy.event({ _tag: "Started", clip: second }, "s2", 70);
    policy.reply({ _tag: "Done", clipId: "c2" }, 80, "s2");
    policy.observe({ playing: second, ready: [clip("c2", item("u"))] }, "s2", 90);
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: false });
  });

  it("keeps a continued clip behind the clip it follows on its own session", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open("s1", 60_000);
    // s1 airs a clip of its own, with another of its own Ready, named c2.
    const x = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, ready: [clip("c2")] }, "s1", 10);
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000, 30_020);
    // p1 and p2 build on s2 in 2 s each, as measured, and p2's clip is named c2 too.
    const ready: Array<SourceClip> = [];
    for (const [name, clipId, at] of [
      ["p1", "c1", 30_030],
      ["p2", "c2", 32_030],
    ] as const) {
      policy.submit(spec(name), at);
      policy.reply({ _tag: "Done", clipId }, at + 10, "s2");
      ready.push(clip(clipId, item(name)));
      policy.observe({ ready: [...ready], continuable: ["c1", "c2"] }, "s2", at + 2_000);
    }
    // xc, projected Ready only after p1 would end, continues from p2 and follows it.
    const continued = { ...spec("xc"), continuity: true };
    policy.edit([{ _tag: "Insert", spec: continued, anchor: key("p2"), side: "before" }]);
    assert.deepStrictEqual(policy.busy("s2"), {
      _tag: "Enqueue",
      request: continued.request,
      tag: item("xc"),
      continueFrom: "c2",
    });
    policy.reply({ _tag: "Done", clipId: "cx" }, 34_050, "s2");
    ready.push(clip("cx", item("xc")));
    policy.observe({ ready, continuable: ["c1", "c2", "cx"] }, "s2", 40_040);
    const moves = commands(policy.actions).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves, []);
  });

  // A clip taken off to be built again stays listed while its removal is refused. Built again
  // with continuity, behind a clip it would outlast, it continued from that old clip of its own
  // and followed it: the next look ranked it behind itself, round and round, and threw.
  it("never continues a clip built again from one of its own still listed", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.submit(spec("x", 1, 30), 10);
    policy.reply({ _tag: "Done", clipId: "c-x" });
    const x = clip("c-x", item("x"), 30);
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    policy.observe({ playing: x }, "s1", 3_001);
    const p2 = { ...spec("p2", 1, 10), continuity: true };
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [spec("p1", 1, 5), p2],
          fingerprint: "g",
        },
      ],
      false,
      3_100,
    );
    const builds = () =>
      commands(policy.actions).flatMap((action) =>
        action.command._tag === "Enqueue" ? [action.command] : [],
      );
    policy.reply({ _tag: "Done", clipId: "c-p1" }, 3_110);
    const p1Clip = clip("c-p1", item("p1"), 5);
    policy.observe({ playing: x, ready: [p1Clip], continuable: ["c-x", "c-p1"] }, "s1", 5_110);
    assert.strictEqual(builds().at(-1)?.continueFrom, "c-p1");
    policy.reply({ _tag: "Done", clipId: "c-p2" }, 5_120);
    const p2Clip = clip("c-p2", item("p2"), 10);
    // A continued build takes a second per second here.
    policy.observe(
      { playing: x, ready: [p1Clip, p2Clip], continuable: ["c-x", "c-p1", "c-p2"] },
      "s1",
      15_120,
    );
    // i goes in before p2 with 2 s of x left, and is still building as p1 starts.
    policy.edit([{ _tag: "Insert", spec: spec("i", 1, 5), anchor: key("p1"), side: "after" }]);
    policy.reply({ _tag: "Done", clipId: "c-i" }, 31_000);
    const iClip = clip("c-i", item("i"), 5);
    policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 33_000);
    policy.event({ _tag: "Started", clip: p1Clip }, "s1", 33_040);
    policy.observe(
      { playing: p1Clip, ready: [p2Clip], building: [iClip], continuable: ["c-p1", "c-p2"] },
      "s1",
      33_041,
    );
    // p2 would start before i: it is taken off, and its removal refused. Asked again, it applies,
    // though the reads that follow still list the clip.
    policy.tick(36_600);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-p2" });
    policy.reply(failed("replied"), 36_610);
    for (let round = 0; round < 2 && policy.busy()?._tag === "Remove"; round++)
      policy.reply({ _tag: "Done" }, 36_620);
    policy.observe(
      { playing: p1Clip, ready: [p2Clip, iClip], continuable: ["c-p1", "c-p2", "c-i"] },
      "s1",
      37_500,
    );
    for (let round = 0; round < 4 && policy.busy()?._tag === "Move"; round++) {
      policy.reply({ _tag: "Done" });
      policy.observe(
        { playing: p1Clip, ready: [iClip, p2Clip], continuable: ["c-p1", "c-i", "c-p2"] },
        "s1",
      );
    }
    policy.reply({ _tag: "Done", clipId: "c-p2-1" });
    const again = clip("c-p2-1", item("p2"), 10);
    policy.event({ _tag: "Ended", clip: p1Clip, termination: "finished" }, "s1", 38_100);
    policy.event({ _tag: "Started", clip: iClip }, "s1", 38_140);
    policy.observe(
      { playing: iClip, ready: [p2Clip, again], continuable: ["c-i", "c-p2", "c-p2-1"] },
      "s1",
      38_141,
    );
    // Its continued build would be Ready only after i ends: it continues from i all the same.
    assert.deepStrictEqual(builds().at(-1), {
      _tag: "Enqueue",
      request: p2.request,
      tag: item("p2"),
      continueFrom: "c-i",
    });
    assert.deepStrictEqual(statuses(policy.actions, "i").at(-1), "Started");
  });
});

const withdrawn = (actions: ReadonlyArray<Policy.Action>) =>
  actions.flatMap((action) => (action._tag === "Withdrawn" ? [action.outcome] : []));

describe("PlayoutPolicy, edits", () => {
  /**
   * `a` starts while its replacement `r` builds, and `r`'s clip is being removed: a replacement
   * whose item started first is dropped.
   */
  const replacedAfterStart = () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    built(policy, ["a"]);
    policy.edit([{ _tag: "Replace", key: key("a"), spec: spec("r") }]);
    policy.reply({ _tag: "Done", clipId: "c-r" });
    const first = clip("c-a", item("a"));
    policy.event({ _tag: "Started", clip: first });
    const building = clip("c-r", item("r"));
    policy.observe({ playing: first, building: [building] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
    return { policy, removing: { first, clip: building } };
  };
  /** Builds `names` one after another on s1, each Ready once built; returns their clip ids. */
  const built = (policy: ReturnType<typeof drive>, names: ReadonlyArray<string>) => {
    const current = policy.state().sessions[0]?.source;
    const ready: Array<SourceClip> = [...(current?.ready ?? [])];
    for (const name of names) {
      const command = policy.busy();
      assert.deepStrictEqual(
        command?._tag === "Enqueue" && command.tag._tag === "Item" ? command.tag.key : command,
        key(name),
      );
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
      const seconds = command?._tag === "Enqueue" ? (command.request.seconds ?? 5) : 5;
      ready.push(clip(`c-${name}`, item(name), seconds));
      policy.observe({ playing: current?.playing, ready: [...ready] });
    }
    return ready;
  };

  // 0.7.0 SchedulerReplace: replacing one part of a group keeps the parts after it.
  it("replacing a group part keeps the parts after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2"), spec("p3")],
        fingerprint: "g",
      },
    ]);
    const ready = built(policy, ["p1", "p2", "p3"]);
    policy.edit([{ _tag: "Replace", key: key("p2"), spec: spec("p2b") }]);
    built(policy, ["p2b"]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-p2" });
    policy.reply({ _tag: "Done" });
    policy.observe({
      ready: [...ready.filter((value) => value.clipId !== "c-p2"), clip("c-p2b", item("p2b"))],
    });
    assert.deepStrictEqual(statuses(policy.actions, "p2").at(-1), "Dropped");
    assert.isUndefined(policy.state().items.get(key("p3"))?.withdraw);
    assert.notDeepEqual(policy.busy(), { _tag: "Remove", clipId: "c-p3" });
  });

  // An enqueue whose reply is lost holds no build slot, so the part after it was built behind it,
  // listed Ready and aired, while the lost one might still air after it. Nothing is sent again,
  // so the group cannot air in order: the parts after it go.
  it("a group's later part waits for an earlier part whose enqueue reply was lost, and goes with the group", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    assert.deepStrictEqual(policy.busy(), {
      _tag: "Enqueue",
      request: spec("p1").request,
      tag: item("p1"),
      continueFrom: undefined,
    });
    policy.reply(failed("unknown"));
    const enqueued = commands(policy.actions).flatMap((action) =>
      action.command._tag === "Enqueue" ? [action.command.tag] : [],
    );
    assert.deepStrictEqual(enqueued, [item("p1")]);
    assert.deepStrictEqual(statuses(policy.actions, "p2"), ["Accepted", "Dropped"]);
    assert.include(
      policy.actions.flatMap((action) =>
        action._tag === "Emit" &&
        action.event._tag === "AsRun" &&
        action.event.event.key === key("p2") &&
        action.event.event.status._tag === "Dropped"
          ? [action.event.event.status.reason]
          : [],
      ),
      "withdrawn",
    );
  });

  // 0.7.0 SchedulerWindows: an At item Ready too early is taken off and built again later.
  it("rebuilds an At item exposed after it was Ready, rather than dropping it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const foreign = clip("x", undefined, 5);
    policy.event({ _tag: "Started", clip: foreign }, "s1", 10);
    policy.observe({ playing: foreign }, "s1", 10);
    policy.submit(spec("cover", 1, 10), 20);
    built(policy, ["cover"]);
    policy.submit(
      { ...spec("timed"), start: { _tag: "At", time: 12_000, late: "nextBoundary" } },
      30,
    );
    built(policy, ["timed"]);
    policy.edit([{ _tag: "Withdraw", key: key("cover") }], false, 40);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-cover" });
    policy.reply({ _tag: "Done" }, 50);
    policy.observe({ playing: foreign, ready: [clip("c-timed", item("timed"))] }, "s1", 60);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-timed" });
    policy.reply({ _tag: "Done" }, 70);
    policy.observe({ playing: foreign }, "s1", 80);
    assert.notInclude(statuses(policy.actions, "timed"), "Dropped");
    assert.strictEqual(policy.state().items.get(key("timed"))?.phase, "Accepted");
  });

  // 0.7.0 SchedulerFates: a withdrawal answers what became of the clip.
  it("answers already-started for a clip that aired before its removal landed", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    built(policy, ["a"]);
    policy.edit([{ _tag: "Withdraw", key: key("a") }]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-a" });
    policy.event({ _tag: "Started", clip: clip("c-a", item("a")) });
    policy.reply(failed("replied"));
    policy.event({ _tag: "Ended", clip: clip("c-a", item("a")), termination: "finished" });
    assert.deepStrictEqual(withdrawn(policy.actions), ["already-started"]);
  });

  it("answers not-found for a clip whose build failed before its removal landed", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.reply({ _tag: "Done", clipId: "c-a" });
    policy.observe({ building: [clip("c-a", item("a"))] });
    policy.edit([{ _tag: "Withdraw", key: key("a") }]);
    policy.reply(failed("replied"));
    policy.event(buildFailed(clip("c-a", item("a"))));
    assert.deepStrictEqual(withdrawn(policy.actions), ["not-found"]);
  });

  // One command at a time: a cut that stopped the playing clip and played its cutter in one call
  // played a cutter withdrawn while the stop was landing, and answered already-started.
  it("drops a cutter withdrawn while its cut stops the playing clip, and never plays it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const long = clip("c-long", item("long"), 15);
    policy.submit(spec("long", 1, 15));
    built(policy, ["long"]);
    policy.event({ _tag: "Started", clip: long });
    policy.observe({ playing: long });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "c-urgent" });
    const cutter = clip("c-urgent", item("urgent"));
    policy.observe({ playing: long, ready: [cutter] });
    // Autoplay goes off first, so nothing starts in the stopped clip's place.
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "c-long" });
    policy.edit([{ _tag: "Withdraw", key: key("urgent") }]);
    policy.event({ _tag: "Ended", clip: long, termination: "stopped" });
    policy.observe({ ready: [cutter] });
    policy.reply({ _tag: "Done" });
    // The plan looks again once the stop has landed: the cutter goes instead of playing.
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-urgent" });
    policy.reply({ _tag: "Done" });
    policy.observe({});
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
    assert.deepStrictEqual(statuses(policy.actions, "urgent").at(-1), "Dropped");
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
    assert.isUndefined(commands(policy.actions).find((action) => action.command._tag === "Play"));
  });

  it("plays the cutter once the stop has landed, then puts autoplay back", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const long = clip("c-long", item("long"), 15);
    policy.submit(spec("long", 1, 15));
    built(policy, ["long"]);
    policy.event({ _tag: "Started", clip: long });
    policy.observe({ playing: long });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "c-urgent" });
    const cutter = clip("c-urgent", item("urgent"));
    policy.observe({ playing: long, ready: [cutter] });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "c-long" });
    // The stop's reply can come before H3 reports the clip ended: the play waits for that.
    policy.reply({ _tag: "Done" });
    assert.isUndefined(policy.busy());
    policy.event({ _tag: "Ended", clip: long, termination: "stopped" });
    policy.observe({ ready: [cutter] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Play", clipId: "c-urgent" });
    policy.reply({ _tag: "Done" });
    policy.event({ _tag: "Started", clip: cutter });
    policy.observe({ playing: cutter });
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
    assert.deepStrictEqual(statuses(policy.actions, "urgent").at(-1), "Started");
  });

  // 0.7.0 answered a group key's withdrawal `withdrawn` if any part was; the first Playout
  // answered with the first part's outcome.
  it("answers withdrawn for a group whose first part aired and whose next part it drops", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    built(policy, ["p1", "p2"]);
    const first = clip("c-p1", item("p1"));
    policy.event({ _tag: "Started", clip: first });
    policy.observe({ playing: first, ready: [clip("c-p2", item("p2"))] });
    policy.edit([{ _tag: "Withdraw", key: key("g") }]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-p2" });
    assert.deepStrictEqual(withdrawn(policy.actions), []);
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(statuses(policy.actions, "p2").at(-1), "Dropped");
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
  });

  // A drain stops admissions, not withdrawals: the critique's withdraw during a drain answered
  // not-found and the item then aired.
  it("withdraws during a drain", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    built(policy, ["a"]);
    policy.send({ _tag: "Drain", id: 500, finish: "accepted" });
    // b's enqueue is in flight: the withdrawal waits for its clip, then removes it.
    policy.edit([{ _tag: "Withdraw", key: key("b") }]);
    assert.isFalse(policy.actions.some((action) => action._tag === "Refused"));
    policy.reply({ _tag: "Done", clipId: "c-b" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-b" });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(statuses(policy.actions, "b").at(-1), "Dropped");
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
  });

  // Found by the property across renewals: a replacement whose item started first is dropped,
  // but its removal, refused while it built, went again only once the session's queues changed,
  // and its build finishing was no change to them. It aired after its item.
  it("asks again for a refused removal once the clip it removes has finished building", () => {
    const { policy, removing } = replacedAfterStart();
    policy.reply(failed("replied"));
    policy.observe({ playing: removing.first, ready: [removing.clip] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // Found by the property: the removal was refused once its item had left the air,
  // its clip still building. It went again only once the queues changed, and with nothing playing
  // or Ready there that change is the build ending, as H3, autoplay on, starts the clip.
  it("asks again at once for a refused removal of the clip H3 starts next", () => {
    const { policy, removing } = replacedAfterStart();
    policy.event({ _tag: "Ended", clip: removing.first, termination: "finished" });
    policy.observe({ building: [removing.clip] });
    policy.reply(failed("replied"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // Ready right behind the clip on air, it starts as that clip ends: the change the
  // refusal waited for.
  it("asks again at once for a refused removal of a clip Ready next behind the clip on air", () => {
    const { policy, removing } = replacedAfterStart();
    policy.observe({ playing: removing.first, ready: [removing.clip] });
    policy.reply(failed("replied"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // A session taking the air turned autoplay on before it removed a replacement it had
  // withdrawn, and H3 arms its Ready head as autoplay comes on.
  it("removes a withdrawn replacement before turning autoplay on as its session takes the air", () => {
    const policy = drive();
    policy.tick(0);
    // s1 lasts 20 s, so its replacement is wanted at once.
    policy.open("s1", 20_000);
    policy.submit(spec("a"));
    policy.reply({ _tag: "Done", clipId: "c-a" }, undefined, "s1");
    const first = clip("c-a", item("a"));
    policy.observe({ ready: [first] }, "s1");
    policy.open("s2");
    policy.edit([{ _tag: "Replace", key: key("a"), spec: spec("r") }]);
    policy.event({ _tag: "Started", clip: first }, "s1");
    policy.observe({ playing: first }, "s1");
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    policy.observe({ ready: [clip("c-r", item("r"))] }, "s2");
    policy.reply({ _tag: "Done", clipId: "c-r" }, undefined, "s2");
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-r" });
  });

  // One whose outcome is unknown was held back the same way, with nothing left to change.
  it("asks again at once for a removal whose outcome is unknown", () => {
    const { policy, removing } = replacedAfterStart();
    policy.observe({ playing: removing.first, ready: [removing.clip] });
    policy.reply(failed("unknown"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // A refused removal waits for its session's queues to change, but a held clip about to air
  // cannot: its removal is asked again then, once, and drops it.
  it("drops a held item withdrawn while its removal was refused, as it comes to air", () => {
    for (const again of [{ _tag: "Done" }, failed("replied")] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit({ ...spec("h"), start: { _tag: "Manual" } }, 10);
      const playing = clip("x", undefined, 10);
      policy.event({ _tag: "Started", clip: playing }, "s1", 20);
      const ready = [clip("y", undefined, 1), clip("c-h", item("h"))];
      policy.observe({ playing, ready }, "s1", 21);
      policy.edit([{ _tag: "Withdraw", key: key("h") }], false, 30);
      policy.reply(failed("replied"), 40);
      // x ends at 10,020 and 1 s of y airs after it: h is 1.5 s from airing at 9,520.
      policy.tick(9_520);
      assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-h" });
      policy.reply(again, 9_530);
      if (again._tag === "Failed") {
        // Refused again, it waits for the queues to change, as any refused removal does.
        assert.isUndefined(policy.busy());
        policy.observe({ playing, ready: [...ready, clip("z")] }, "s1", 9_540);
        policy.reply({ _tag: "Done" }, 9_550);
      }
      assert.deepStrictEqual(statuses(policy.actions, "h").at(-1), "Dropped");
      assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
    }
  });

  // A group key's withdrawal answers for the parts the group had when it was made. A part's
  // replacement made afterwards is no part of it, and its drop leaves that answer as it was.
  it("answers a group's withdrawal from its parts then, not from a replacement made after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    built(policy, ["p1"]);
    const first = clip("c-p1", item("p1"));
    policy.event({ _tag: "Started", clip: first });
    policy.observe({ playing: first });
    // p2's enqueue is in flight: the group's withdrawal waits on it.
    policy.edit([{ _tag: "Withdraw", key: key("g") }]);
    policy.edit([{ _tag: "Replace", key: key("p2"), spec: spec("r") }]);
    policy.edit([{ _tag: "Withdraw", key: key("r") }]);
    policy.reply(failed("unknown"));
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    assert.deepStrictEqual(statuses(policy.actions, "r"), ["Accepted", "Dropped"]);
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn", "already-started"]);
  });

  // An edit refused as it would miss a deadline took back its items only: a part's replacement
  // in it took its whole group with it, so a later withdrawal of a part was never answered, and
  // a withdrawal in it of a key the plan did not know was answered beside the refusal.
  it("leaves the plan as it was when it refuses an edit that would miss a deadline", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    const before = policy.state();
    const refused = policy.edit([
      { _tag: "Replace", key: key("p2"), spec: spec("p2b") },
      { _tag: "Withdraw", key: key("unknown") },
      { _tag: "Submit", spec: { ...spec("late"), window: { startByMs: 1_000, firm: true } } },
    ]);
    const answers = refused.actions.flatMap((action) =>
      action._tag === "Refused" || action._tag === "Withdrawn" ? [action._tag] : [],
    );
    assert.deepStrictEqual(answers, ["Refused"]);
    assert.deepStrictEqual(policy.state().items, before.items);
    assert.deepStrictEqual(policy.state().groups, before.groups);
    assert.strictEqual(policy.state().nextOrder, before.nextOrder);
    const withdrawal = policy.edit([{ _tag: "Withdraw", key: key("p2") }]);
    assert.deepStrictEqual(withdrawn(withdrawal.actions), ["withdrawn"]);
  });
});
describe("PlayoutPolicy, time", () => {
  // The critique's 60 s cap: n10 was cut mid-clip at the cap and later items aired out of order.
  it("builds on a capped session only what can air before its cap, and replaces it at the lead", () => {
    const policy = drive({ config: { ...config, leadMs: 5_000 } });
    policy.tick(0);
    policy.open("s1", 60_000);
    const foreign = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: foreign }, "s1", 1_000);
    policy.observe({ playing: foreign }, "s1", 1_000);
    policy.submit(spec("a"), 1_010);
    assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    policy.reply({ _tag: "Done", clipId: "c-a" }, 1_020);
    policy.observe({ playing: foreign, ready: [clip("c-a", item("a"))] }, "s1", 1_030);
    const next = policy.submit(spec("b"), 1_040);
    assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    // The session holds air nearly to its cap: a replacement opened now would bill idle.
    assert.isFalse(next.actions.some((action) => action._tag === "Open"));
    assert.isTrue(policy.tick(56_000).actions.some((action) => action._tag === "Open"));
  });

  // A refusal that allocated nothing billed nothing, so while a session holds the air it neither
  // pauses renewal nor counts toward the opens that do.
  it("pauses renewal only once enough opens that may bill have failed, and fails with no air", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    policy.tick(0);
    policy.open("s1", 120_000);
    assert.isTrue(policy.tick(60_010).actions.some((action) => action._tag === "Open"));
    // Each retry waits a second longer than the one before.
    const retried = (allocated: boolean) => {
      policy.send({ _tag: "OpenFailed", reason: "refused", fatal: false, allocated });
      return policy.tick(policy.now() + 7_000).actions.some((action) => action._tag === "Open");
    };
    const opened = [true, false, false, true, false, true].map(retried);
    // The third open that may bill pauses renewal while s1 holds the air.
    assert.deepStrictEqual(opened, [true, true, true, true, true, false]);
    const unheld = drive();
    unheld.tick(0);
    for (let attempt = 0; attempt < 3; attempt++)
      unheld.send({ _tag: "OpenFailed", reason: "refused", fatal: false, allocated: false });
    assert.isTrue(unheld.actions.some((action) => action._tag === "Fail"));
  });

  // Two renewal opens are refused with nothing allocated while s1 holds the air, the second
  // asking to wait past s1's cap. They count toward no limit, then or once s1 is gone.
  it("counts no refusal that allocated nothing refused while a session held the air, then or after", () => {
    const policy = drive({ config: { ...config, leadMs: 30_000 } });
    const refuse = (retryAfterMs?: number) =>
      policy.send({
        _tag: "OpenFailed",
        reason: "429",
        fatal: false,
        allocated: false,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      });
    policy.tick(0);
    policy.open("s1", 60_000);
    policy.event({ _tag: "Started", clip: clip("x", undefined, 70) }, "s1", 10);
    policy.tick(30_001);
    refuse();
    policy.tick(policy.now() + 1_000);
    refuse(40_000);
    policy.tick(policy.now() + 40_000);
    // s1's cap has ended it. With nothing holding the air every refusal counts, and the third
    // fails the playout.
    assert.deepStrictEqual(policy.state().sessions, []);
    const failed = [3_000, 4_000, 5_000].map((wait) => {
      refuse();
      const failing = policy.actions.some((action) => action._tag === "Fail");
      policy.tick(policy.now() + wait);
      return failing;
    });
    assert.deepStrictEqual(failed, [false, false, true]);
  });

  // Three renewal opens that may have billed ran out while s1 held the air, and the one open
  // after s1's cap gave s2, which holds the air before it airs a clip. A refusal of s2's renewal
  // that allocated nothing counts toward no limit, so it pauses nothing either.
  it("asks again after a refusal that allocated nothing once the setups that count ran out", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.tick(60_010);
    for (let failure = 0; failure < 3; failure++) {
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 5_000);
    }
    assert.isTrue(policy.state().openingPaused);
    const paused = opens();
    policy.tick(120_010);
    policy.open("s2", 30_000);
    // s2's cap is within the lead, so its renewal opens at once.
    assert.strictEqual(opens(), paused + 2);
    policy.send({ _tag: "OpenFailed", reason: "429", fatal: false, allocated: false });
    policy.tick(policy.now() + 5_000);
    assert.strictEqual(opens(), paused + 3);
  });

  // s1 airs a 5 s clip after another while each renewal open fails 2 s after it is asked, having
  // allocated a session or maybe so. A clip on the session that already held the air says nothing
  // of those opens: only a session's first clip ends their run.
  it("pauses renewal after three opens that may bill, though the session on air airs clips", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    policy.tick(0);
    policy.open("s1", 120_000);
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    const failed: Array<number> = [];
    let asked = opens();
    let failing: number | undefined;
    for (let at = 60_000; at < 119_000; at += 100) {
      if (at % 5_000 === 0)
        policy.event({ _tag: "Started", clip: clip(`x${String(at)}`) }, "s1", at);
      policy.tick(at);
      if (opens() > asked) {
        asked = opens();
        failing = at + 2_000;
      }
      if (failing !== undefined && at >= failing) {
        failing = undefined;
        failed.push(at);
        policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true }, at);
      }
    }
    // Each retry waits a second longer; the third failure pauses opening until s1's cap.
    assert.deepStrictEqual(failed, [62_100, 65_100, 69_100]);
    assert.isTrue(policy.state().openingPaused);
  });

  it("ends a run of failed setups at a session's first clip, seen in a read or at its start", () => {
    for (const how of ["read", "start"] as const) {
      const policy = drive({ config: { ...config, leadMs: 60_000 } });
      policy.tick(0);
      policy.open("s1", 120_000);
      policy.tick(60_010);
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 1_000);
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 2_000);
      policy.open("s2", 120_000);
      assert.strictEqual(policy.state().setupFailures, 2, how);
      const first = clip("y");
      if (how === "read") policy.observe({ playing: first }, "s2");
      else policy.event({ _tag: "Started", clip: first }, "s2");
      assert.strictEqual(policy.state().setupFailures, 0, how);
    }
  });

  // The one more open after a pause waits out the last refusal's Retry-After, though the session
  // on air is gone sooner.
  it("waits out the last refusal's Retry-After for the open after a pause", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    const fail = (retryAfterMs?: number) =>
      policy.send({
        _tag: "OpenFailed",
        reason: "503",
        fatal: false,
        allocated: true,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      });
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.tick(60_010);
    fail();
    policy.tick(policy.now() + 1_000);
    fail();
    policy.tick(policy.now() + 2_000);
    fail(60_000);
    const refused = policy.now();
    const asked = opens();
    const lost = policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, refused + 5_000);
    assert.strictEqual(opens(), asked);
    assert.strictEqual(lost.wake, refused + 60_000);
    policy.tick(refused + 60_000);
    assert.strictEqual(opens(), asked + 1);
  });

  // A session lost before any clip sent to it started is a failed setup, as a failed open is.
  it("waits a second longer for each session lost before any clip sent to it started", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opensIn = (step: Policy.Step) => step.actions.some((action) => action._tag === "Open");
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.event({ _tag: "Started", clip: clip("x", undefined, 70) }, "s1", 10);
    assert.isTrue(opensIn(policy.tick(60_010)));
    const waits: Array<number> = [];
    for (const id of ["r1", "r2"]) {
      // Each replacement is sent an item, then lost.
      policy.open(id, 600_000, policy.now() + 100);
      policy.submit(spec(id));
      const lost = policy.send({ _tag: "Lost", sessionId: id, reason: "gone" });
      const lostAt = policy.now();
      if (!opensIn(lost)) assert.isTrue(opensIn(policy.tick(lost.wake)));
      waits.push(policy.now() - lostAt);
    }
    assert.deepStrictEqual(waits, [1_000, 2_000]);
  });

  // s1's renewal is refused with a Retry-After of 20 s, then s1 is lost before the item sent to it
  // started: a failed setup, whose own wait, 2 s as the second failure in a row, is shorter.
  it("waits out a refusal's Retry-After after a session lost before any clip", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    policy.tick(0);
    policy.open("s1", 30_000);
    policy.submit(spec("a"));
    // s1's cap is within the lead, so its renewal opens at once.
    const asked = opens();
    policy.send({
      _tag: "OpenFailed",
      reason: "429",
      fatal: false,
      allocated: false,
      retryAfterMs: 20_000,
    });
    const refusedAt = policy.now();
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    policy.tick(refusedAt + 19_999);
    assert.strictEqual(opens(), asked);
    policy.tick(refusedAt + 20_000);
    assert.strictEqual(opens(), asked + 1);
  });

  // Six renewal opens are refused with nothing allocated while s1 holds the air, then the
  // replacement is lost before the item sent to it started. Each failure lengthens the wait by a
  // second only up to three, so the next open still goes before s1's cap.
  it("waits at most a second for each of three failures before it opens again", () => {
    const policy = drive();
    const opensIn = (step: Policy.Step) => step.actions.some((action) => action._tag === "Open");
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 70);
    policy.event({ _tag: "Started", clip: x }, "s1", 100);
    policy.observe({ playing: x }, "s1", 100);
    assert.isTrue(opensIn(policy.tick(30_001)));
    const waits: Array<number> = [];
    for (let refusal = 0; refusal < 6; refusal++) {
      const refused = policy.send(
        { _tag: "OpenFailed", reason: "429", fatal: false, allocated: false },
        policy.now() + 100,
      );
      const refusedAt = policy.now();
      assert.isTrue(opensIn(policy.tick(refused.wake)));
      waits.push(policy.now() - refusedAt);
    }
    assert.deepStrictEqual(waits, [1_000, 2_000, 3_000, 3_000, 3_000, 3_000]);
    policy.open("s2", 600_000, policy.now() + 100);
    policy.submit(spec("i", 1, 20));
    assert.strictEqual(policy.busy("s2")?._tag, "Enqueue");
    policy.reply({ _tag: "Done", clipId: "c-i" }, undefined, "s2");
    let step = policy.send({ _tag: "Lost", sessionId: "s2", reason: "gone" }, policy.now() + 2_000);
    const lostAt = policy.now();
    for (let wakes = 0; !opensIn(step) && step.wake !== undefined && wakes < 10; wakes++)
      step = policy.tick(step.wake);
    assert.strictEqual(policy.now() - lostAt, 3_000);
    assert.isBelow(policy.now(), 60_001);
  });

  it("times an end cue from the provider's length, not the requested one", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit({ ...spec("a"), cues: [{ name: "out", from: "end", offsetMs: 0 }] });
    policy.reply({ _tag: "Done", clipId: "c-a" });
    const aired = clip("c-a", item("a"), 5.167);
    policy.event({ _tag: "Started", clip: aired }, "s1", 1_000);
    const cues = (actions: ReadonlyArray<Policy.Action>) =>
      actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Cue" ? [action.event.event.at] : [],
      );
    assert.deepStrictEqual(cues(policy.tick(6_000).actions), []);
    assert.deepStrictEqual(cues(policy.tick(6_167).actions), [6_167]);
  });

  it("does not report starvation while a replacement waits to take the air", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    // Owed air: an item that may not start for a long while.
    policy.submit({ ...spec("later"), window: { notBeforeMs: 3_600_000, firm: false } }, 5);
    const x = clip("x", undefined, 5);
    policy.observe({ ready: [x] }, "s1", 9);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x }, "s1", 10);
    policy.open("s2", 600_000, 20);
    policy.observe({ ready: [clip("y", undefined, 5)] }, "s2", 30);
    policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 5_010);
    policy.observe({}, "s1", 5_011);
    policy.tick(5_300);
    assert.isFalse(
      policy.actions.some((action) => action._tag === "Emit" && action.event._tag === "Starved"),
    );
  });
});

// Each decision that waits on the time comes at its own deadline, which the wake names; the
// property below checks that nothing falls due between wakes.
describe("PlayoutPolicy, wakes", () => {
  it("wakes as the runway falls to the filler's floor, and does not poll below it", () => {
    const policy = drive({ config: protecting("order") });
    policy.tick(0);
    policy.open("s1", 600_000, 1);
    const filler = clip("f0", { _tag: "Filler", index: 0 }, 20);
    policy.observe({ ready: [filler] });
    policy.reply({ _tag: "Done", clipId: "f0" });
    policy.event({ _tag: "Started", clip: filler }, "s1", 1_000);
    // 20 s from 1 s: the runway falls to the floor of 5 s at 16 s.
    assert.strictEqual(policy.observe({ playing: filler }, "s1", 1_001).wake, 16_000);
    const floor = policy.tick(16_000);
    assert.deepStrictEqual(enqueued(floor.actions), ["filler"]);
    // Below the floor nothing else is due until the lead ahead of the session's cap.
    assert.strictEqual(floor.wake, 570_001);
    assert.strictEqual(policy.reply({ _tag: "Done", clipId: "f1" }, 16_010).wake, 570_001);
  });

  it("drops a firm item once its projection reaches its startBy, when it does", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    // a builds from 10 ms to 2,010. b, on the lane that cuts, airs once Ready, not after a: with
    // 2 s of build after a's, it could start by 4,010.
    policy.submit(spec("a"), 10);
    policy.reply({ _tag: "Done", clipId: "c-a" }, 11);
    const firm = { ...spec("b", 0), window: { startByMs: 4_000, firm: true } };
    // Past 2,010 its projection grows with the time, and reaches 4,020 at 2,020.
    assert.strictEqual(policy.submit(firm, 20).wake, 2_020);
    assert.deepStrictEqual(statuses(policy.tick(2_019).actions, "b"), []);
    assert.deepStrictEqual(statuses(policy.tick(2_020).actions, "b"), ["Dropped"]);
  });

  it("drops an At item that has not started by its time, at that time", () => {
    const policy = drive();
    policy.tick(0);
    const at = { ...spec("a"), start: { _tag: "At", time: 5_000, late: "drop" } } as const;
    assert.strictEqual(policy.submit(at, 10).wake, 5_000);
    assert.deepStrictEqual(statuses(policy.tick(5_000).actions, "a"), ["Dropped"]);
  });

  it("takes back a held item Ready behind the clip on air as it comes within the margin", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit({ ...spec("h"), start: { _tag: "Manual" } }, 10);
    const playing = clip("x", undefined, 10);
    policy.event({ _tag: "Started", clip: playing }, "s1", 20);
    // x ends at 10,020 and 1 s of y airs after it: h is 1.5 s from airing at 9,520.
    const ready = [clip("y", undefined, 1), clip("c-h", item("h"))];
    assert.strictEqual(policy.observe({ playing, ready }, "s1", 21).wake, 9_520);
    assert.deepStrictEqual(commands(policy.tick(9_519).actions), []);
    assert.deepStrictEqual(
      commands(policy.tick(9_520).actions).map((action) => action.command),
      [{ _tag: "Remove", clipId: "c-h" }],
    );
  });

  it("builds an At item once the air secured lasts to its time, though nothing plays", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const x = clip("x", undefined, 5);
    policy.event({ _tag: "Started", clip: x }, "s1", 20);
    policy.observe({ playing: x, ready: [clip("y", undefined, 5)] }, "s1", 21);
    const at = { ...spec("a"), start: { _tag: "At", time: 20_000, late: "nextBoundary" } } as const;
    policy.submit(at, 30);
    // x overran its 5 s without an end reported: y's 5 s last to 20 s from 15 s on.
    assert.strictEqual(policy.tick(5_020).wake, 15_000);
    assert.deepStrictEqual(enqueued(policy.tick(15_000).actions), ["a"]);
  });

  it("drops in the same step a firm item that the build it sends makes late", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    // b may not build before 1,010, and has 2 s of build to start by 4,010.
    const firm = { ...spec("b"), window: { notBeforeMs: 1_000, startByMs: 4_000, firm: true } };
    policy.submit(firm, 10);
    // a builds meanwhile, from 20 ms to 2,020: b could then start at 4,020 at the earliest.
    const sent = policy.submit(spec("a"), 20);
    assert.deepStrictEqual(enqueued(sent.actions), ["a"]);
    assert.deepStrictEqual(statuses(sent.actions, "b"), ["Dropped"]);
  });

  // During a renewal the runway measured is the replacement's, which does not fall while the
  // session on air airs: the plan names no wake for it to fall to the floor.
  it("wakes for no floor while the replacement's runway, which does not fall, is above it", () => {
    const policy = drive({ config: protecting("air"), from: measured });
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 60);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, continuable: ["x"] }, "s1", 10);
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 600_000, 30_020);
    const fillers = [0, 1, 2].map((index) =>
      clip(`f${String(index)}`, { _tag: "Filler", index }, index === 2 ? 6.001 : 5),
    );
    policy.observe({ ready: fillers, continuable: ["f0", "f1", "f2"] }, "s2", 30_030);
    policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } }, 30_040);
    const long: Policy.Spec = { ...spec("long", 1, 15), continuity: true };
    // s2's 16.001 s stay Ready 1 ms above long's floor: the next deadline is s1's cap.
    assert.strictEqual(policy.submit(long, 30_050).wake, 60_001);
  });

  it("cuts for no item settled while its clip is still listed Ready", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(
      { ...spec("u", 0), start: { _tag: "At", time: 5_000, late: "nextBoundary" } },
      10,
    );
    const filler = clip("f", { _tag: "Filler", index: 0 }, 20);
    policy.event({ _tag: "Started", clip: filler }, "s1", 20);
    policy.observe({ playing: filler, ready: [clip("c-u", item("u"))] }, "s1", 21);
    policy.edit([{ _tag: "Withdraw", key: key("u") }], false, 30);
    policy.reply({ _tag: "Done" }, 40);
    assert.deepStrictEqual(statuses(policy.actions, "u"), ["Accepted", "Ready", "Dropped"]);
    // Its clip is gone, though no read has shown it yet; its time comes and cuts nothing.
    assert.deepStrictEqual(commands(policy.tick(5_000).actions), []);
  });

  // s1 airs a clip of its own to 55 s, and s2, its replacement, must have aired what it builds by
  // 64 s, its cap less the margin; a is due at 40 s. The 9.98 s that tile the gap to a would end
  // past that, and the tile shrinks to the 9 s that fit at 31 s, with nothing to wake the plan
  // then: it asks for those 9 s at once.
  it("asks a replacement at once for the filler that airs before its cap", () => {
    const policy = drive({ config: protecting("air") });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 60_000 }, 10);
    const x = clip("x", undefined, 55);
    policy.event({ _tag: "Started", clip: x }, "s1", 20);
    policy.observe({ playing: x });
    policy.reply({ _tag: "Done" });
    policy.submit({ ...spec("a"), start: { _tag: "At", time: 40_000, late: "nextBoundary" } }, 30);
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 35_000 }, 30_020);
    policy.observe({}, "s2");
    policy.reply({ _tag: "Done" }, undefined, "s2");
    assert.deepStrictEqual(policy.busy("s2"), {
      _tag: "Enqueue",
      request: { prompt: "filler 0", seconds: 9 },
      tag: { _tag: "Filler", index: 0 },
    });
  });

  // u, which follows filler 0, raised its fence with time to spare, and s1 refused it at 9 s, to
  // be asked again at 10 s. x ends at 10,020: the fence comes down 250 ms before, and filler 0,
  // which the air needs, goes then.
  it("wakes as the clip on air falls to the readiness margin, where a fence comes down", () => {
    const policy = drive({ config: protecting("air"), from: measured });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 }, 10);
    const x = clip("x", undefined, 10);
    policy.event({ _tag: "Started", clip: x }, "s1", 20);
    policy.observe({ playing: x });
    policy.reply({ _tag: "Done" });
    const follower = {
      ...spec("u", 1, 1),
      start: { _tag: "Asap" },
      follows: { _tag: "Filler", index: 0 },
    } as const;
    policy.submit(follower, 3_000);
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
    assert.strictEqual(policy.reply(failed("replied"), 9_000).wake, 9_770);
    assert.deepStrictEqual(commands(policy.tick(9_769).actions), []);
    assert.deepStrictEqual(enqueued(policy.tick(9_770).actions), ["filler"]);
  });

  // u follows a, which holds the build slot. x's rest outlasts u's 0.4 s build by the 1.5 s
  // margin until 1.9 s before x ends at 7,996.3. Counted on from the time rather than back from
  // that end, the instant came out an ulp apart when the plan looked halfway there.
  it("names one wake for a follower's cover, whenever it looks", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 }, 10);
    const x = clip("x", undefined, 7.986);
    policy.event({ _tag: "Started", clip: x }, "s1", 10.3);
    policy.observe({ playing: x });
    policy.reply({ _tag: "Done" });
    policy.submit(spec("a"));
    policy.reply({ _tag: "Done", clipId: "ca" });
    policy.observe({ playing: x, building: [clip("ca", item("a"))] });
    assert.strictEqual(policy.submit({ ...spec("u", 1, 1), follows: item("a") }, 40).wake, 6_096.3);
    assert.strictEqual(policy.tick(3_068.15).wake, 6_096.3);
  });

  // x, a clip of its own, has ended with k Ready behind it, and f, which follows k, holds the
  // build turn until k starts. p's enqueue reply was lost, so x2, which continues from p, waits
  // ahead of f; it fits before s1's cap only until 34,010, and with a 1 s lead no replacement
  // opens first. Nothing builds ahead of f after that either, filler included, until an input or
  // the wake.
  it("holds a follower's turn though a build ahead of it stops fitting before the cap", () => {
    const policy = drive({ config: { ...protecting("air"), leadMs: 1_000 }, from: measured });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 60_000 }, 10);
    const x = clip("x", undefined, 20);
    policy.event({ _tag: "Started", clip: x }, "s1", 20);
    policy.observe({ playing: x });
    policy.reply({ _tag: "Done" });
    policy.submit(spec("k"), 30);
    policy.reply({ _tag: "Done", clipId: "ck" });
    const ready = [clip("ck", item("k"))];
    policy.observe({ playing: x, ready, continuable: ["ck"] });
    policy.submit(spec("p"));
    policy.submit({ ...spec("x2", 1, 15), continuity: true });
    policy.submit({ ...spec("f", 1, 1), follows: item("k") });
    policy.event({ _tag: "Ended", clip: x, termination: "finished" });
    policy.observe({ ready, continuable: ["ck"] });
    policy.reply(failed("unknown"));
    assert.strictEqual(policy.tick(1_000).wake, 59_010);
    assert.deepStrictEqual(commands(policy.tick(34_011).actions), []);
  });
});

describe("PlayoutPolicy, any script", () => {
  for (const script of counterexamples)
    it(`keeps every promise of the plan for ${script.join(", ")}`, () => check(script));
});

// Claims from the critique's delegated pass, each checked here before any fix.
describe("PlayoutPolicy, edit claims", () => {
  /** Builds `names` on s1 one after another, each Ready once built. */
  const build = (policy: ReturnType<typeof drive>, names: ReadonlyArray<string>) => {
    for (const name of names) {
      const command = policy.busy();
      assert.deepStrictEqual(
        command?._tag === "Enqueue" && command.tag._tag === "Item" ? command.tag.key : command,
        key(name),
      );
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
      const current = policy.state().sessions[0]?.source;
      policy.observe({
        playing: current?.playing,
        ready: [...(current?.ready ?? []), clip(`c-${name}`, item(name))],
        continuable: [...(current?.continuable ?? []), `c-${name}`],
      });
    }
  };

  it("a replace lane keeps its Ready item as cover until the new one is Ready", () => {
    const policy = drive({
      config: {
        ...config,
        lanes: [...config.lanes, { name: "news", conflict: "replace", cut: false }],
      },
    });
    policy.tick(0);
    policy.open();
    policy.submit(spec("x", 2));
    build(policy, ["x"]);
    policy.submit(spec("y", 2));
    assert.notInclude(statuses(policy.actions, "x"), "Dropped");
    assert.notDeepEqual(policy.busy(), { _tag: "Remove", clipId: "c-x" });
  });

  it("a continuing replacement continues from the clip before its place, not the one it replaces", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    build(policy, ["a", "b"]);
    policy.edit([{ _tag: "Replace", key: key("b"), spec: { ...spec("b2"), continuity: true } }]);
    const command = policy.busy();
    assert.deepStrictEqual(command?._tag === "Enqueue" ? command.continueFrom : command, "c-a");
  });

  it("refuses an insert under a used key with another anchor", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("a"), side: "after" }]);
    const again = policy.edit([
      { _tag: "Insert", spec: spec("i"), anchor: key("b"), side: "after" },
    ]);
    assert.isTrue(
      again.actions.some(
        (action) => action._tag === "Refused" && action.refusal._tag === "KeyMismatch",
      ),
    );
  });

  it("refuses a group whose part key another item already uses", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("p"));
    build(policy, ["p"]);
    const group = policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p"), spec("q")],
        fingerprint: "g",
      },
    ]);
    assert.isTrue(group.actions.some((action) => action._tag === "Refused"));
    assert.strictEqual(policy.state().items.get(key("p"))?.phase, "Ready");
  });

  // The property's counterexample, shrunk: ["unknown","group","batch","replace","ready","withdraw"].
  // A replacement takes its part's place, so withdrawing it withdraws it and the parts after it,
  // and answers with its own outcome, as 0.7.0 counted "every part's key, replacements included".
  it("withdrawing a group part's replacement withdraws it and the parts after it, and answers", () => {
    const policy = drive();
    policy.tick(0);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("ga"), spec("gb")],
        fingerprint: "g",
      },
    ]);
    policy.edit([{ _tag: "Replace", key: key("ga"), spec: spec("r") }]);
    assert.deepStrictEqual(statuses(policy.actions, "ga").at(-1), "Dropped");
    const withdrawal = policy.edit([{ _tag: "Withdraw", key: key("r") }]);
    assert.deepStrictEqual(statuses(withdrawal.actions, "r"), ["Dropped"]);
    assert.deepStrictEqual(statuses(withdrawal.actions, "gb"), ["Dropped"]);
    assert.deepStrictEqual(withdrawn(withdrawal.actions), ["withdrawn"]);
  });

  it("an insert after a group follows its last part by place, not the replacement added last", () => {
    const policy = drive();
    policy.tick(0);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("ga"), spec("gb")],
        fingerprint: "g",
      },
    ]);
    policy.edit([{ _tag: "Replace", key: key("ga"), spec: spec("r") }]);
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("g"), side: "after" }]);
    assert.strictEqual(policy.state().items.get(key("i"))?.group?.index, 1);
  });

  it("a batch's cover airs only when nothing else can", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    build(policy, ["a", "b"]);
    policy.edit(
      [
        { _tag: "Withdraw", key: key("a") },
        { _tag: "Submit", spec: spec("c") },
      ],
      true,
    );
    // As in 0.7.0, what a pending batch withdraws ranks after the rest of its lane.
    policy.reply({ _tag: "Done", clipId: "c-c" });
    const moves = commands(policy.actions).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves.at(-1)?.command, { _tag: "Move", clipId: "c-b", position: 0 });
  });
});

describe("PlayoutPolicy, groups", () => {
  const group = (
    lane = 1,
    names: ReadonlyArray<string> = ["p1", "p2", "p3"],
  ): Policy.EditInput => ({
    _tag: "SubmitGroup",
    key: key("g"),
    lane,
    parts: names.map((name) => spec(name, lane)),
    fingerprint: "g",
  });

  // A pending batch holds what it adds, a group's first part among them. The part waiting behind it
  // counted no air ahead of it, so it was taken off and built again while the batch waited.
  it("builds each part of a group in a pending batch once, while its first part is held", () => {
    const lanes: Policy.Config = {
      ...config,
      lanes: [...config.lanes, { name: "news", conflict: "replace", cut: false }],
    };
    for (const [how, lane, batch] of [
      ["in a replace lane", 2, false],
      ["in a batched edit", 1, true],
    ] as const) {
      const policy = drive({ config: lanes });
      policy.tick(0);
      policy.open("s1");
      policy.submit(spec("x", 1, 60));
      policy.reply({ _tag: "Done", clipId: "c-x" });
      const x = clip("c-x", item("x"), 60);
      policy.observe({ ready: [x] }, "s1");
      policy.event({ _tag: "Started", clip: x }, "s1", 100);
      policy.observe({ playing: x }, "s1", 101);
      // A replacement opens and takes new work, with autoplay off: what it builds waits there.
      policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 }, 200);
      policy.observe({}, "s2", 201);
      for (let round = 0; round < 3 && policy.busy("s2")?._tag === "Autoplay"; round++)
        policy.reply({ _tag: "Done" }, undefined, "s2");
      policy.edit([group(lane)], batch, 300);
      const log: Array<string> = [];
      provide(policy, "s2", 30, queues(), log);
      assert.deepStrictEqual(log, ["enqueue p1", "enqueue p2", "enqueue p3"], how);
    }
  });

  // A part waiting behind the first, which waits for its time, counted that part as air only once
  // its time had come: 1.5 s before the clip on air ended, it was taken off and built again.
  it("builds a group's second part once while its first waits Ready for its time", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.submit(spec("x", 1, 10), 10);
    policy.reply({ _tag: "Done", clipId: "c-x" });
    const x = clip("c-x", item("x"), 10);
    policy.observe({ ready: [x] }, "s1", 2_010);
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    policy.observe({ playing: x }, "s1", 3_001);
    // It starts at x's end.
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [
            { ...spec("p1"), start: { _tag: "At", time: 13_000, late: "nextBoundary" } },
            spec("p2"),
          ],
          fingerprint: "g",
        },
      ],
      false,
      3_100,
    );
    const held = queues(x);
    const log: Array<string> = [];
    provide(policy, "s1", 4, held, log);
    policy.tick(11_400);
    provide(policy, "s1", 4, held, log);
    policy.tick(12_900);
    provide(policy, "s1", 4, held, log);
    assert.deepStrictEqual(log, ["enqueue p1", "enqueue p2"]);
    for (const order of held.orders) {
      const first = order.findIndex((clipId) => clipId.startsWith("c-p1-"));
      const second = order.findIndex((clipId) => clipId.startsWith("c-p2-"));
      if (first >= 0 && second >= 0)
        assert.isBelow(first, second, `p2 queued ahead of p1: ${order.join(", ")}`);
    }
  });

  // An insert that follows a part waiting behind the part before it airs right after that part, so
  // its continued build continues from that part's clip. The part counted for nothing while it
  // waited, so the insert was built independent.
  it("continues an insert from the waiting part it follows", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("x"));
    policy.reply({ _tag: "Done", clipId: "c-x" });
    const x = clip("c-x", item("x"), 20);
    policy.observe({ ready: [x], continuable: ["c-x"] });
    policy.event({ _tag: "Started", clip: x }, "s1", 100);
    policy.observe({ playing: x, continuable: ["c-x"] }, "s1", 101);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    policy.reply({ _tag: "Done", clipId: "c-p1" });
    const p1 = clip("c-p1", item("p1"));
    policy.observe({ playing: x, ready: [p1], continuable: ["c-x", "c-p1"] });
    policy.reply({ _tag: "Done", clipId: "c-p2" });
    const p2 = clip("c-p2", item("p2"));
    policy.observe({ playing: x, ready: [p1, p2], continuable: ["c-x", "c-p1", "c-p2"] });
    const before = policy.actions.length;
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("y"), continuity: true, follows: item("p2") },
        anchor: key("p2"),
        side: "after",
      },
    ]);
    // The follower's fence goes up first.
    for (let round = 0; round < 4; round++) {
      const busy = policy.busy();
      if (busy === undefined || busy._tag === "Enqueue") break;
      policy.reply({ _tag: "Done" });
    }
    const enqueue = commands(policy.actions.slice(before)).find(
      (action) => action.command._tag === "Enqueue",
    )?.command;
    assert.deepStrictEqual(
      enqueue?._tag === "Enqueue" ? [enqueue.tag, enqueue.continueFrom] : enqueue,
      [item("y"), "c-p2"],
    );
  });

  // A part waiting behind the part before it still airs ahead of what is queued after it, so a
  // follower of a later clip is projected against that clip's end with the part counted. Left out,
  // the end came 5 s early, and the follower, which could be Ready in time, was dropped as late.
  it("projects a follower's clip behind a waiting part where it airs", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    let ready: Array<SourceClip> = [];
    let playing: SourceClip | undefined;
    const building: Array<SourceClip> = [];
    const show = (time?: number) => policy.observe({ playing, ready, building }, "s1", time);
    /** Answers the moves in flight, applying each to the scripted queue. */
    const moves = () => {
      for (let round = 0; round < 6; round++) {
        const busy = policy.busy();
        if (busy?._tag !== "Move") return;
        policy.reply({ _tag: "Done" });
        const moved = ready.find((value) => value.clipId === busy.clipId);
        if (moved === undefined) return;
        ready = ready.filter((value) => value !== moved);
        ready.splice(busy.position, 0, moved);
        show();
      }
    };
    /** Builds `name`, whose enqueue is in flight: Ready 2 s after it went, at 0.4 s a second. */
    const build = (name: string) => {
      const busy = policy.busy();
      assert.deepStrictEqual(busy?._tag === "Enqueue" ? busy.tag : busy, item(name));
      const sentAt = policy.state().items.get(key(name))?.dispatchedAt ?? policy.now();
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
      ready = [...ready, clip(`c-${name}`, item(name))];
      show(sentAt + 2_000);
      moves();
    };
    policy.submit(spec("x"), 10);
    const x = clip("c-x", item("x"), 20);
    build("x");
    ready = [];
    playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 2_100);
    show(2_101);
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [spec("p1"), spec("p2")],
          fingerprint: "g",
        },
      ],
      false,
      15_000,
    );
    build("p1");
    build("p2");
    policy.submit(spec("z"));
    build("z");
    // w holds the build slot for 10 s; y follows z and waits for the slot.
    policy.submit(spec("w", 1, 25));
    policy.reply({ _tag: "Done", clipId: "c-w" });
    building.push(clip("c-w", item("w"), 25));
    show();
    policy.submit({ ...spec("y"), follows: item("z") });
    policy.tick(policy.now() + 50);
    assert.deepStrictEqual(statuses(policy.actions, "y"), ["Accepted"]);
  });

  // An insert that follows a member placed after it in its group would wait for that member to air,
  // while that member waits behind it: neither airs, and the group never ends.
  it("refuses an insert that follows a member of its group placed after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    policy.reply({ _tag: "Done", clipId: "c-p1" });
    for (const [anchor, side] of [
      ["p1", "after"],
      ["p2", "before"],
      ["g", "before"],
    ] as const) {
      const { actions } = policy.edit([
        {
          _tag: "Insert",
          spec: { ...spec("i"), follows: item("p2") },
          anchor: key(anchor),
          side,
        },
      ]);
      const refusal = actions.find((action) => action._tag === "Refused")?.refusal;
      assert.strictEqual(refusal?._tag, "InvalidItem", `${side} ${anchor}`);
      const message = refusal?._tag === "InvalidItem" ? refusal.message : "";
      assert.include(message, "follows");
      assert.notInclude(message, "p2");
      assert.isFalse(policy.state().items.has(key("i")));
    }
    // Following the member before it, it is admitted.
    const { actions } = policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("i"), follows: item("p1") },
        anchor: key("p1"),
        side: "after",
      },
    ]);
    assert.isFalse(actions.some((action) => action._tag === "Refused"));
  });

  const waitsOnItself = (name: string): Policy.Refusal => ({
    _tag: "InvalidItem",
    key: key(name),
    message: "follows waits for a member of its group placed after it",
  });
  const refusalOf = (actions: ReadonlyArray<Policy.Action>) =>
    actions.find((action) => action._tag === "Refused")?.refusal;

  // The check read the plan before the edit: an insert that follows another the same edit adds,
  // placed after it, was admitted, and waited on an insert that waited behind it.
  it("refuses an insert that follows one the same edit places after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([group(1, ["p1", "p2"])]);
    const { actions } = policy.edit([
      { _tag: "Insert", spec: spec("i1"), anchor: key("p1"), side: "after" },
      {
        _tag: "Insert",
        spec: { ...spec("i2"), follows: item("i1") },
        anchor: key("p1"),
        side: "after",
      },
    ]);
    assert.deepStrictEqual(refusalOf(actions), waitsOnItself("i2"));
    assert.isFalse(policy.state().items.has(key("i1")));
  });

  // A replacement of an insert takes the clip the insert follows, if any, else its own: one that
  // follows a later part was admitted, as the check covered inserts alone.
  it("refuses a replacement of an insert that follows a member placed after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([group(1, ["p1", "p2", "p3"])]);
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("p1"), side: "after" }]);
    const { actions } = policy.edit([
      { _tag: "Replace", key: key("i"), spec: { ...spec("i9"), follows: item("p3") } },
    ]);
    assert.deepStrictEqual(refusalOf(actions), waitsOnItself("i9"));
    assert.isFalse(policy.state().items.has(key("i9")));
  });

  // An insert whose continued build falls back to a later part's clip sits right behind that part.
  // The check compared orders, by which an insert before that part, following the first insert,
  // came after it: admitted, it waited on the first insert, which waited behind it.
  it("refuses an insert that follows an insert seated behind a later part", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.edit([group(1, ["p1", "p2", "p3"])], false, 10);
    const log: Array<string> = [];
    const held = queues();
    provide(policy, "s1", 1, held, log);
    const p1 = readyIn(held, "p1");
    held.ready = held.ready.filter((value) => value !== p1);
    held.playing = p1;
    policy.event({ _tag: "Started", clip: p1 }, "s1", policy.now() + 10);
    policy.observe({ ready: held.ready, playing: p1, continuable: [p1.clipId] });
    provide(policy, "s1", 2, held, log, 300);
    const p2 = readyIn(held, "p2");
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("xc"), continuity: true },
        anchor: key("p2"),
        side: "before",
      },
    ]);
    // Ready only after p1 ends, xc continues from p2's clip, right behind p2.
    assert.strictEqual(policy.state().items.get(key("xc"))?.follows, p2.clipId);
    const { actions } = policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("z"), follows: item("xc") },
        anchor: key("p2"),
        side: "before",
      },
    ]);
    assert.deepStrictEqual(refusalOf(actions), waitsOnItself("z"));
    assert.isFalse(policy.state().items.has(key("z")));
  });

  // An insert that follows an item of no group, which follows a later member of the insert's group,
  // waits for that member to air, while that member waits behind the insert. The check read the
  // insert's own `follows` alone: it was admitted, and the group stalled.
  it("refuses an insert whose follows chain reaches a member of its group placed after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([group(1, ["a1", "a2"])]);
    policy.submit({ ...spec("w"), follows: item("a2") });
    // u and v follow each other: a chain into them ends there.
    policy.edit([
      { _tag: "Submit", spec: { ...spec("u"), follows: item("v") } },
      { _tag: "Submit", spec: { ...spec("v"), follows: item("u") } },
    ]);
    const insert = (follows: string) =>
      policy.edit([
        {
          _tag: "Insert",
          spec: { ...spec(`i-${follows}`), follows: item(follows) },
          anchor: key("a1"),
          side: "after",
        },
      ]).actions;
    assert.deepStrictEqual(refusalOf(insert("w")), waitsOnItself("i-w"));
    assert.isFalse(policy.state().items.has(key("i-w")));
    assert.isUndefined(refusalOf(insert("u")));
  });

  // An insert whose continued build continues from a later part's clip airs right behind that
  // part, so it sits there in its group: when that part's place breaks, it goes with the members
  // after it. It stayed, by its own earlier order, and aired alone after its group ended.
  it("withdraws an insert seated behind a part whose place breaks", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [spec("p1"), spec("p2"), spec("p3")],
          fingerprint: "g",
        },
      ],
      false,
      10,
    );
    const log: Array<string> = [];
    const held = queues();
    provide(policy, "s1", 1, held, log);
    // p1 airs; p2 and p3 are built behind it, Ready while most of p1's 5 s is left.
    const p1 = readyIn(held, "p1");
    held.ready = held.ready.filter((value) => value !== p1);
    held.playing = p1;
    policy.event({ _tag: "Started", clip: p1 }, "s1", policy.now() + 10);
    policy.observe({ ready: held.ready, playing: p1, continuable: [p1.clipId] });
    provide(policy, "s1", 2, held, log, 300);
    const p2 = readyIn(held, "p2");
    const before = policy.actions.length;
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("xc"), continuity: true },
        anchor: key("p2"),
        side: "before",
      },
    ]);
    const enqueue = commands(policy.actions.slice(before)).find(
      (action) => action.command._tag === "Enqueue",
    )?.command;
    // Ready only after p1 ends, xc continues from p2's clip instead, right behind p2.
    assert.deepStrictEqual(
      enqueue?._tag === "Enqueue" ? [enqueue.tag, enqueue.continueFrom] : enqueue,
      [item("xc"), p2.clipId],
    );
    policy.reply({ _tag: "Done", clipId: "c-xc" });
    policy.observe({ ready: held.ready, playing: p1, building: [clip("c-xc", item("xc"))] });
    // p2's clip fails, and its place, which had nothing else, is broken.
    policy.event(buildFailed(p2));
    held.ready = [...held.ready.filter((value) => value !== p2), clip("c-xc", item("xc"))];
    policy.observe({ ready: held.ready, playing: p1 });
    const items = policy.state().items;
    assert.strictEqual(items.get(key("p3"))?.withdraw, "withdrawn");
    assert.strictEqual(items.get(key("xc"))?.withdraw, "withdrawn");
  });

  // An insert whose continued build falls back to a later part's clip sits behind that part. Its
  // replacement, not built yet, took the insert's own order instead, before that part, as a place
  // of its own: the part waited behind it, left the air secured, and was taken off as the part
  // before it played, and the insert, its clip continued from the part's, aired ahead of the part.
  it("seats an insert's replacement where the insert sits, behind a later part", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    // With 5 s of x left, i's continued build would be Ready only after p1 ends: it continues
    // from p2's clip, behind p2.
    policy.edit(
      [
        {
          _tag: "Insert",
          spec: { ...spec("i", 1, 12), continuity: true },
          anchor: key("p1"),
          side: "after",
        },
      ],
      false,
      38_000,
    );
    provide(policy, "s1", 4, held, log, 1_000);
    const p2 = readyIn(held, "p2");
    assert.strictEqual(policy.state().items.get(key("i"))?.follows, p2.clipId);
    // The air secured at one instant, before the replacement and after.
    const at = { mono: 40_000, wall: 40_000 };
    const runway = () => Policy.view(config, policy.state(), at).runwaySeconds;
    const secured = runway();
    held.slow.add("ir");
    policy.edit([{ _tag: "Replace", key: key("i"), spec: spec("ir", 1, 6) }]);
    provide(policy, "s1", 6, held, log);
    assert.strictEqual(runway(), secured);
    for (const time of [43_000, 48_100, 53_200]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.notInclude(log, `remove ${p2.clipId}`);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "i"]);
  });

  // An insert whose continued build went out continuing from a later part's clip sits behind that
  // part. It sat there only while that clip was listed as the part's: once the part was taken off,
  // waiting behind an insert still building, the first insert left its seat and aired right after
  // the part before it, its clip continued from one that never aired.
  it("keeps an insert behind the part its build continues from, though that part is taken off", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    policy.edit(
      [
        {
          _tag: "Insert",
          spec: { ...spec("i", 1, 12), continuity: true },
          anchor: key("p1"),
          side: "after",
        },
      ],
      false,
      38_000,
    );
    provide(policy, "s1", 4, held, log, 1_000);
    const p2 = readyIn(held, "p2");
    const i = readyIn(held, "i");
    assert.strictEqual(policy.state().items.get(key("i"))?.follows, p2.clipId);
    // k goes in before p2 and builds for as long as the test runs: p2 waits behind it.
    held.slow.add("k");
    policy.edit([{ _tag: "Insert", spec: spec("k"), anchor: key("p2"), side: "before" }]);
    provide(policy, "s1", 4, held, log);
    // p1 airs from 43 s to 48 s; 1.5 s before its end, p2 is about to air.
    boundary(policy, "s1", held, 43_000);
    policy.tick(46_600);
    provide(policy, "s1", 8, held, log);
    assert.include(log, `remove ${p2.clipId}`);
    boundary(policy, "s1", held, 48_100);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1"]);
    assert.include(log, `remove ${i.clipId}`);
  });

  // A place has one time, the group's: a replacement of a part takes its part's start, window and
  // the clip it follows, as admission gives the part, and none of its own.
  it("gives a part's replacement its part's time, never its own", () => {
    const policy = drive();
    policy.tick(0);
    const own = { follows: item("z"), window: { notBeforeMs: 60_000, firm: false } } as const;
    // z waits to air, so following it is no cause to drop anything.
    policy.submit(spec("z"));
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [{ ...spec("p1"), window: { notBeforeMs: 30_000, firm: false } }, spec("p2")],
        fingerprint: "g",
      },
    ]);
    const timeOf = (name: string) => {
      const value = policy.state().items.get(key(name));
      return [value?.spec.follows, value?.spec.window, value?.notBefore, value?.spec.start];
    };
    const p1 = timeOf("p1");
    policy.edit([
      { _tag: "Replace", key: key("p1"), spec: { ...spec("r1"), ...own, start: { _tag: "Asap" } } },
      { _tag: "Replace", key: key("p2"), spec: { ...spec("r2"), ...own, start: { _tag: "Asap" } } },
    ]);
    assert.deepStrictEqual(timeOf("r1"), p1);
    assert.deepStrictEqual(timeOf("r2"), [undefined, undefined, undefined, { _tag: "Follow" }]);
    assert.deepStrictEqual(
      ["r1", "r2"].map((name) => policy.state().items.get(key(name))?.mode),
      ["follow", "follow"],
    );
  });

  // A part waiting behind an insert not built yet airs only if that insert is Ready ahead of it in
  // time; otherwise it is taken off and built again. The runway counted it as secured air all the
  // same, so the filler floor, the cover sent ahead of a build and the cap read air the plan did
  // not have.
  it("leaves a part waiting behind an insert not built yet out of the runway", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [spec("p1"), spec("p2")],
          fingerprint: "g",
        },
      ],
      false,
      10,
    );
    const log: Array<string> = [];
    const held = queues();
    provide(policy, "s1", 4, held, log);
    // p1 airs with p2 Ready behind it.
    const p1 = readyIn(held, "p1");
    readyIn(held, "p2");
    held.ready = held.ready.filter((value) => value !== p1);
    held.playing = p1;
    const startedAt = policy.now() + 1;
    policy.event({ _tag: "Started", clip: p1 }, "s1", startedAt);
    policy.observe({ ready: held.ready, playing: p1 });
    const runway = () =>
      Policy.view(config, policy.state(), { mono: policy.now(), wall: policy.now() }).runwaySeconds;
    assert.isAbove(runway(), 5);
    // 1.5 s in, xi goes in before p2, and its 10 s build goes out at once.
    policy.edit(
      [{ _tag: "Insert", spec: spec("xi", 1, 25), anchor: key("p2"), side: "before" }],
      false,
      startedAt + 1_500,
    );
    assert.strictEqual(policy.state().items.get(key("xi"))?.phase, "Building");
    assert.isBelow(runway(), 5);
  });

  // A part waiting behind an insert still building airs once the insert has, and ahead of any clip
  // sent now. The runway leaves it out as air not secured, and the cap check did too: a clip that
  // would air past the session's cap behind it was built there all the same, to be cut off there.
  it("counts a part waiting behind an insert still building toward the cap", () => {
    // Two builds may be in flight, so another may go while the insert's does.
    const policy = drive({ config: { ...config, maxBuildsInFlight: 2 } });
    policy.tick(0);
    // What s1 builds must have aired by 35 s, its cap less the margin.
    policy.open("s1", 36_000, 0);
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 1,
          parts: [spec("p1", 1, 20), spec("p2")],
          fingerprint: "g",
        },
      ],
      false,
      10,
    );
    policy.reply({ _tag: "Done", clipId: "c-p1" }, 20);
    policy.reply({ _tag: "Done", clipId: "c-p2" }, 30);
    const p1 = clip("c-p1", item("p1"), 20);
    const p2 = clip("c-p2", item("p2"));
    policy.observe({ ready: [p1, p2] }, "s1", 2_000);
    // p1 airs to 22.1 s, with p2 Ready behind it.
    policy.event({ _tag: "Started", clip: p1 }, "s1", 2_100);
    policy.observe({ playing: p1, ready: [p2] }, "s1", 2_101);
    // xi goes in before p2, and its build goes out at once: p2 airs once xi has, from 27.1 s.
    policy.edit(
      [{ _tag: "Insert", spec: spec("xi"), anchor: key("p2"), side: "before" }],
      false,
      3_000,
    );
    policy.reply({ _tag: "Done", clipId: "c-xi" }, 3_010);
    const xi = clip("c-xi", item("xi"));
    policy.observe({ playing: p1, ready: [p2], building: [xi] }, "s1", 3_020);
    // z, sent now, would air after p2, from 32.1 s to 37.1 s, past the cap: it waits for the
    // replacement, which opens at the lead.
    const before = policy.actions.length;
    policy.submit(spec("z"), 3_030);
    assert.deepStrictEqual(enqueued(policy.actions.slice(before)), []);
    assert.isTrue(policy.tick(6_000).actions.some((action) => action._tag === "Open"));
    policy.open("s2");
    assert.deepStrictEqual(enqueued(policy.actions.slice(before), "s2"), ["z"]);
  });

  // A part waiting behind the part before it, Ready behind that part on their session, airs next
  // once that part starts. It waited at the end of the queue, behind a lower lane's clip, and was
  // moved into its place only once that part had started: a part shorter than the move's round trip
  // let the lower lane's clip air between them.
  it("keeps a waiting part in its place behind the part before it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    const names = () =>
      held.ready.map((value) => (value.tag === undefined ? "" : nameOf(value.tag)));
    policy.submit(spec("x", 0, 30));
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x });
    policy.observe({ playing: x });
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1", 1, 2), spec("p2")],
        fingerprint: "g",
      },
    ]);
    policy.submit(spec("z", 2));
    provide(policy, "s1", 12, held, log);
    assert.deepStrictEqual(names(), ["p1", "p2", "z"]);
    const p1 = readyIn(held, "p1");
    held.ready = held.ready.filter((value) => value !== p1);
    held.playing = p1;
    const before = policy.actions.length;
    policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 30_000);
    policy.event({ _tag: "Started", clip: p1 });
    policy.observe({ ready: held.ready, playing: p1 });
    assert.deepStrictEqual(commands(policy.actions.slice(before)), []);
    assert.deepStrictEqual(names(), ["p2", "z"]);
  });

  /**
   * A group whose first part waits Ready behind x on the session on air while a replacement takes
   * new work: the second part's enqueue, if it goes there, and that part Ready at its head, or
   * still being built there.
   */
  const acrossRenewal = (built = true, later: ReadonlyArray<string> = []) => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open("s1");
    policy.submit(spec("x", 1, 60));
    policy.reply({ _tag: "Done", clipId: "c-x" });
    const x = clip("c-x", item("x"), 60);
    policy.observe({ ready: [x] }, "s1");
    policy.event({ _tag: "Started", clip: x }, "s1", 100);
    policy.observe({ playing: x }, "s1", 101);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1", 1, 1), spec("p2"), ...later.map((name) => spec(name))],
        fingerprint: "g",
      },
    ]);
    // p1's enqueue is in flight on s1 as a replacement opens.
    policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 }, 200);
    policy.observe({}, "s2", 201);
    for (let round = 0; round < 3 && policy.busy("s2")?._tag === "Autoplay"; round++)
      policy.reply({ _tag: "Done" }, undefined, "s2");
    policy.reply({ _tag: "Done", clipId: "c-p1" }, undefined, "s1");
    const p1 = clip("c-p1", item("p1"), 1);
    policy.observe({ playing: x, ready: [p1] }, "s1", 2_300);
    const sent = policy.busy("s2");
    policy.reply({ _tag: "Done", clipId: "c-p2" }, undefined, "s2");
    const ready = [clip("c-p2", item("p2"))];
    policy.observe(built ? { ready } : { building: ready }, "s2", built ? 4_300 : 2_400);
    // Each later part builds behind it there, 2 s apart.
    for (const name of later) {
      policy.reply({ _tag: "Done", clipId: `c-${name}` }, undefined, "s2");
      ready.push(clip(`c-${name}`, item(name)));
      policy.observe({ ready: [...ready] }, "s2", policy.now() + 2_000);
    }
    return { policy, sent };
  };

  // A replacement takes new work while the first part waits Ready on the session on air, whose
  // clips air first. The second part waited for the first to start before it was sent, so a first
  // part shorter than its build left the air dark at the switch.
  it("builds a later part on the replacement while the part before it waits Ready on air", () => {
    const { policy, sent } = acrossRenewal();
    assert.deepStrictEqual(sent?._tag === "Enqueue" ? sent.tag : sent, item("p2"));
    // Ready at the head of the replacement's queue, it is not taken off: p1 airs first, on s1.
    assert.isUndefined(policy.busy("s2"));
    assert.deepStrictEqual(statuses(policy.actions, "p2"), ["Accepted", "Building", "Ready"]);
  });

  // Once s1 is lost with p1, to be built again, s2 takes the air with nothing playing and p2 Ready
  // at its head, or still being built there. Its autoplay would start p2 the moment it went on, or
  // as its build ended, before p1: p2 is taken off first, to be built again behind p1.
  it("takes a later part off a replacement taking the air before its autoplay goes on", () => {
    for (const built of [true, false]) {
      const { policy } = acrossRenewal(built);
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" }, `${built}`);
      policy.reply({ _tag: "Done" }, undefined, "s2");
      assert.strictEqual(policy.state().items.get(key("p2"))?.phase, "Accepted");
    }
  });

  // Taken off, p2's clip was no longer its item's, and the plan stopped at it: with its removal's
  // reply ahead of the read that showed it gone, autoplay came on, and H3 started p3 before p1.
  it("takes each later part off a replacement taking the air, though a read lists one taken off", () => {
    const { policy } = acrossRenewal(true, ["p3"]);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" });
    policy.reply({ _tag: "Done" }, undefined, "s2");
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p3" });
    policy.reply({ _tag: "Done" }, undefined, "s2");
    policy.observe({}, "s2");
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: true });
  });

  // s1 is lost with p1 on air: p1 fails, its place breaks, and p2, Ready at the head of s2 as s2
  // takes the air, is withdrawn. Its removal held autoplay off for as long as it was refused, and
  // for as long as each removal's outcome stayed unknown: a provider that kept failing it kept the
  // air dark.
  it("holds autoplay off for a removal that does not apply a second at most, and asks for it still", () => {
    for (const outcome of ["replied", "unknown"] as const) {
      const { policy } = acrossRenewal();
      const x = clip("c-x", item("x"), 60);
      const p1 = clip("c-p1", item("p1"), 1);
      policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 60_100);
      policy.event({ _tag: "Started", clip: p1 }, "s1", 60_140);
      policy.observe({ playing: p1 }, "s1", 60_141);
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, 60_500);
      assert.deepStrictEqual(statuses(policy.actions, "p2").at(-1), "Ready");
      assert.strictEqual(policy.state().items.get(key("p2"))?.withdraw, "withdrawn");
      for (const at of [60_600, 60_900, 61_200, 61_500])
        if (policy.busy("s2")?._tag === "Remove") policy.reply(failed(outcome), at, "s2");
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" }, outcome);
      // A second after the first failure, autoplay goes on, and the removal is asked again.
      policy.reply(failed(outcome), 61_600, "s2");
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: true }, outcome);
      policy.reply({ _tag: "Done" }, 61_610, "s2");
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" }, outcome);
    }
  });

  // Refused, the removal of a clip taken off was never asked again, and autoplay came on with it at
  // the head: H3 started p2 before p1.
  it("asks again for a refused removal of a part taken off before autoplay comes on", () => {
    const { policy } = acrossRenewal(true, ["p3"]);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" });
    policy.reply(failed("replied"), undefined, "s2");
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" });
    policy.reply({ _tag: "Done" }, undefined, "s2");
    policy.observe({ ready: [clip("c-p3", item("p3"))] }, "s2");
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p3" });
  });

  // w, urgent, waits Ready ahead of p2 on s2; it is withdrawn, and its removal is refused once.
  // As s2 takes the air after s1 is lost with p1, w's removal goes again and applies, its reply
  // ahead of the read that shows w gone. The clip still listed counted as air ahead of p2, so p2
  // was not taken off: autoplay came on, and H3 started p2 before p1 was built again.
  it("counts a clip whose removal applied as no air, though a read still lists it", () => {
    const { policy } = acrossRenewal();
    policy.submit(spec("w", 0));
    policy.reply({ _tag: "Done", clipId: "c-w" }, undefined, "s2");
    const w = clip("c-w", item("w"));
    const p2 = clip("c-p2", item("p2"));
    policy.observe({ ready: [p2, w] }, "s2", policy.now() + 2_000);
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Move", clipId: "c-w", position: 0 });
    policy.observe({ ready: [w, p2] }, "s2");
    policy.reply({ _tag: "Done" }, undefined, "s2");
    policy.edit([{ _tag: "Withdraw", key: key("w") }]);
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-w" });
    policy.reply(failed("replied"), undefined, "s2");
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-w" });
    policy.reply({ _tag: "Done" }, undefined, "s2");
    // p2 waits behind p1, which is to be built again: it is taken off before autoplay comes on.
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Remove", clipId: "c-p2" });
    const now = { mono: policy.now(), wall: policy.now() };
    assert.strictEqual(Policy.view(config, policy.state(), now).runwaySeconds, 0);
  });

  const moved = (actions: ReadonlyArray<Policy.Action>) =>
    commands(actions).filter((action) => action.command._tag === "Move").length;

  // An insert a pending batch withdraws ranks behind the rest of its lane until the batch commits,
  // and the part waiting behind it ranked at its own place: each queue read moved the part ahead of
  // the insert, which took it out of turn, and back, and the part aired before the insert.
  it("keeps a part behind an insert that a pending batch withdraws", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 60));
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    shown(policy, "s1", held);
    policy.edit([group(1, ["p1", "p2"])]);
    provide(policy, "s1", 10, held, log);
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("p1"), side: "after" }]);
    provide(policy, "s1", 10, held, log);
    const before = moved(policy.actions);
    // z builds for as long as the test runs, and i goes once it is Ready.
    held.slow.add("z");
    policy.edit(
      [
        { _tag: "Submit", spec: spec("z") },
        { _tag: "Withdraw", key: key("i") },
      ],
      true,
    );
    provide(policy, "s1", 12, held, log);
    policy.submit(spec("w"));
    provide(policy, "s1", 12, held, log);
    boundary(policy, "s1", held, 63_000);
    provide(policy, "s1", 6, held, log);
    boundary(policy, "s1", held, 68_100);
    provide(policy, "s1", 6, held, log);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "i"]);
    assert.isAtMost(moved(policy.actions) - before, 2);
  });

  // An insert whose continued build would end after the parts around it continues from a lower
  // lane's clip behind them and airs right behind it, while it still holds the part after it. That
  // part ranked at its own place once the insert was Ready ahead of it, and behind it otherwise:
  // each queue read moved one past the other.
  it("keeps a part behind an insert that continues from a lower lane's clip", () => {
    const lanes: Policy.Config = {
      ...config,
      lanes: [...config.lanes, { name: "low", conflict: "queue", cut: false }],
    };
    const policy = drive({ config: lanes, from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    policy.submit(spec("y", 2, 10), 13_200);
    provide(policy, "s1", 12, held, log);
    // With 5 s of x left, i's continued build of 21 s ends after p1 and p2 would: it continues
    // from y, and follows it.
    const continued = { ...spec("i", 1, 20), continuity: true };
    policy.edit(
      [{ _tag: "Insert", spec: continued, anchor: key("p1"), side: "after" }],
      false,
      38_000,
    );
    const before = moved(policy.actions);
    provide(policy, "s1", 12, held, log);
    assert.include(log, "enqueue i from c-y-3");
    for (const time of [43_000, 48_100, 58_200, 78_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "y", "i", "p2"]);
    assert.isAtMost(moved(policy.actions) - before, 4);
  });

  // A part waiting behind a part held for its time does not air before the clips behind it, though
  // it is Ready at its place ahead of them: the reader that projects when a followed clip ends
  // counted it ahead of that clip, and a follower whose continued build would be Ready only after
  // that clip ended continued from it.
  it("counts no part waiting behind a held part ahead of a clip a follower continues from", () => {
    const lanes: Policy.Config = {
      ...config,
      lanes: [...config.lanes, { name: "low", conflict: "queue", cut: false }],
    };
    const policy = drive({ config: lanes, from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 60), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 30_000);
    shown(policy, "s1", held, 30_001);
    // x ends at 90,000, z airs to 100,000 and b to 130,000; the group starts at 110,000.
    const first: Policy.Spec = {
      ...spec("p1", 2),
      start: { _tag: "At", time: 110_000, late: "nextBoundary" },
    };
    policy.edit(
      [
        {
          _tag: "SubmitGroup",
          key: key("g"),
          lane: 2,
          parts: [first, spec("p2", 2)],
          fingerprint: "g",
        },
      ],
      false,
      30_100,
    );
    policy.submit(spec("z", 2, 10), 30_200);
    policy.submit(spec("b", 2, 30), 30_300);
    provide(policy, "s1", 30, held, log);
    assert.deepStrictEqual(
      held.ready.map((value) => value.clipId),
      ["c-z-1", "c-b-2", "c-p1-3", "c-p2-4"],
    );
    // With 10 s of x left, y's continued build of 23 s would end after z does.
    policy.submit({ ...spec("y", 1, 22), continuity: true, follows: item("z") }, 80_000);
    provide(policy, "s1", 3, held, log);
    assert.include(log, "enqueue y");
  });

  // A part's replacement still building beside the part, Ready ahead, took the part behind them
  // out of turn: it was moved behind a lower lane's clip, which aired between the two parts, and it
  // left the air secured.
  it("keeps a part in turn behind a part whose replacement is still building", () => {
    const lanes: Policy.Config = {
      ...config,
      lanes: [...config.lanes, { name: "low", conflict: "queue", cut: false }],
    };
    const policy = drive({ config: lanes });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 20));
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    shown(policy, "s1", held);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1", 1, 2), spec("p2")],
        fingerprint: "g",
      },
    ]);
    policy.submit(spec("y", 2));
    provide(policy, "s1", 10, held, log);
    const runway = () => Policy.view(lanes, policy.state(), { mono: 0, wall: 0 }).runwaySeconds;
    const secured = runway();
    held.slow.add("p1r");
    policy.edit([{ _tag: "Replace", key: key("p1"), spec: spec("p1r", 1, 2) }]);
    assert.strictEqual(runway(), secured);
    const before = moved(policy.actions);
    provide(policy, "s1", 10, held, log);
    boundary(policy, "s1", held, 23_000);
    boundary(policy, "s1", held, 25_000);
    provide(policy, "s1", 6, held, log);
    assert.strictEqual(moved(policy.actions) - before, 0);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2"]);
  });

  // The same replacement left the part behind it out of the air secured, and filler went out to
  // cover air that was there.
  it("sends no filler for a part behind a part whose replacement is still building", () => {
    const covering: Policy.Config = {
      ...config,
      maxBuildsInFlight: 2,
      filler: {
        floor: 40,
        target: 40,
        clip: ({ index }) => ({ prompt: `filler ${String(index)}`, seconds: 5 }),
        lengths: { min: 5, max: 15 },
        invalid: () => undefined,
        protect: "air",
      },
    };
    const policy = drive({ config: covering });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 20));
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1", 1, 2), spec("p2", 1, 30)],
        fingerprint: "g",
      },
    ]);
    provide(policy, "s1", 12, held, log);
    boundary(policy, "s1", held, policy.now() + 1);
    provide(policy, "s1", 12, held, log);
    const now = { mono: policy.now(), wall: policy.now() };
    const secured = () => Policy.view(covering, policy.state(), now).runwaySeconds;
    const before = secured();
    held.slow.add("p1r");
    const sent = log.length;
    policy.edit([{ _tag: "Replace", key: key("p1"), spec: spec("p1r", 1, 2) }]);
    provide(policy, "s1", 8, held, log);
    assert.deepStrictEqual(log.slice(sent), ["enqueue p1r"]);
    assert.strictEqual(secured(), before);
  });

  /** A lane below the line, whose clips air after its parts. */
  const low: Policy.Config = {
    ...config,
    lanes: [...config.lanes, { name: "low", conflict: "queue", cut: false }],
  };
  /** x airs from 3 s to 23 s on s1, and the group `parts` is submitted, with y behind it in `low`. */
  const lowBehind = (parts: ReadonlyArray<Policy.Spec>) => {
    const policy = drive({ config: low });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 20));
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    shown(policy, "s1", held);
    policy.edit([{ _tag: "SubmitGroup", key: key("g"), lane: 1, parts, fingerprint: "g" }]);
    provide(policy, "s1", 10, held, log);
    return { policy, held, log };
  };
  /** Whether y waited behind p2 in every order `held`'s queue took from `from` on. */
  const yBehindP2 = (held: Queues, from: number) =>
    held.orders.slice(from).every((order) => {
      const y = order.findIndex((clipId) => clipId.startsWith("c-y-"));
      return y < 0 || y > order.findIndex((clipId) => clipId.startsWith("c-p2-"));
    });

  // A part's replacement its batch holds back ranked behind everything that airs, and the part
  // waiting behind their place ranked just behind it there, though the part it replaces, Ready
  // ahead, airs first: a lower lane's clip was moved ahead of the waiting part, and aired between.
  it("keeps a part in place behind a part whose replacement its batch holds back", () => {
    const { policy, held, log } = lowBehind([spec("p1", 1, 2), spec("p2")]);
    policy.submit(spec("y", 2));
    provide(policy, "s1", 10, held, log);
    const from = held.orders.length;
    // z never builds, so the batch stays open with p1r held by it.
    policy.edit(
      [
        { _tag: "Replace", key: key("p1"), spec: spec("p1r", 1, 2) },
        { _tag: "Submit", spec: { ...spec("z"), start: { _tag: "Manual" } } },
      ],
      true,
    );
    provide(policy, "s1", 10, held, log);
    assert.isTrue(yBehindP2(held, from), held.orders.slice(from).join(" | "));
    // x ends, and p1 airs and ends before anything is answered.
    boundary(policy, "s1", held, 23_000);
    boundary(policy, "s1", held, 25_000);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2"]);
  });

  // An insert that follows the part it is inserted after waits behind it by rank. Its replacement
  // took the clip it follows but not the anchor, so it waited for that part's clip to air, ranked
  // behind everything that airs, and the part behind it went there too: a lower lane's clip was
  // moved ahead of that part.
  it("keeps an insert's replacement, and the part behind it, in place behind the part it follows", () => {
    const { policy, held, log } = lowBehind([spec("p1", 1, 4), spec("p2")]);
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("i", 1, 2), follows: item("p1") },
        anchor: key("p1"),
        side: "after",
      },
    ]);
    provide(policy, "s1", 10, held, log);
    policy.submit(spec("y", 2));
    provide(policy, "s1", 10, held, log);
    const from = held.orders.length;
    policy.edit([{ _tag: "Replace", key: key("i"), spec: spec("ir", 1, 2) }]);
    provide(policy, "s1", 10, held, log);
    assert.isTrue(yBehindP2(held, from), held.orders.slice(from).join(" | "));
    for (const time of [23_000, 27_100, 29_200]) boundary(policy, "s1", held, time);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "ir", "p2"]);
  });

  /**
   * x airs from 13 s to 43 s on s1, with p1 and p2 Ready behind it. With 5 s of x left, i's
   * continued build would be Ready only after p1 ends: it continues from p2's clip, and sits
   * behind p2. Measured builds take 0.4 s a second; i's takes 1 s.
   */
  const seatedInsert = (settings: Policy.Config = config, from: Policy.State = measured) => {
    const policy = drive({ config: settings, from });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    policy.edit(
      [
        {
          _tag: "Insert",
          spec: { ...spec("i", 1, 12), continuity: true },
          anchor: key("p1"),
          side: "after",
        },
      ],
      false,
      38_000,
    );
    provide(policy, "s1", 4, held, log, 1_000);
    assert.strictEqual(policy.state().items.get(key("i"))?.follows, readyIn(held, "p2").clipId);
    return { policy, held, log };
  };
  /** x ends, then p1, then p2 (5 s each), then i (12 s), as H3 with autoplay on takes them. */
  const playOut = (policy: ReturnType<typeof drive>, held: Queues, log: Array<string>) => {
    for (const time of [43_000, 48_100, 53_200, 65_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
  };

  // The insert's replacement sat behind p2 only until it was built. One continued from p2's clip,
  // as the clip before its seat, then aired ahead of p2; one that follows p2 waited for p2 ahead
  // of it, and was dropped as displaced, with the insert already gone as replaced.
  it("keeps an insert's replacement behind the part the insert sits behind, built or not", () => {
    for (const next of [
      { ...spec("ir", 1, 6), continuity: true },
      { ...spec("ir", 1, 3), follows: item("p2") },
    ]) {
      const { policy, held, log } = seatedInsert();
      policy.edit([{ _tag: "Replace", key: key("i"), spec: next }]);
      provide(policy, "s1", 8, held, log);
      playOut(policy, held, log);
      const how = next.continuity ? "continued" : "following p2";
      assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "ir"], how);
    }
  });

  // A replacement its batch holds back, Ready, left the insert's seat for its own order, before p2,
  // while the insert stayed as cover: p2 waited behind the held replacement, and y, a lower lane's
  // clip, was moved ahead of p2 and aired between the parts.
  it("keeps a part in place behind an insert whose Ready replacement its batch holds back", () => {
    const { policy, held, log } = seatedInsert(low);
    policy.submit(spec("y", 2));
    provide(policy, "s1", 4, held, log);
    const from = held.orders.length;
    // z never builds, so the batch stays open, and i airs as cover.
    policy.edit(
      [
        { _tag: "Replace", key: key("i"), spec: spec("ir", 1, 6) },
        { _tag: "Submit", spec: { ...spec("z"), start: { _tag: "Manual" } } },
      ],
      true,
    );
    provide(policy, "s1", 8, held, log);
    assert.strictEqual(policy.state().items.get(key("ir"))?.phase, "Ready");
    assert.isTrue(yBehindP2(held, from), held.orders.slice(from).join(" | "));
    playOut(policy, held, log);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "i", "y"]);
  });

  // An insert placed right after the insert that sits behind p2 sat at its own order, before p2:
  // it aired before its anchor, and one that follows its anchor was refused as waiting for a
  // member placed after it.
  it("seats an insert placed after one that sits behind a later part right after it", () => {
    for (const k of [spec("k", 1, 3), { ...spec("k", 1, 3), follows: item("i") }]) {
      const { policy, held, log } = seatedInsert();
      const { actions } = policy.edit([
        { _tag: "Insert", spec: k, anchor: key("i"), side: "after" },
      ]);
      assert.isUndefined(refusalOf(actions));
      provide(policy, "s1", 6, held, log);
      playOut(policy, held, log);
      const how = k.follows === undefined ? "after i" : "following i";
      assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "i", "k"], how);
    }
  });

  // An insert that follows the insert placed right before it ranked held while that one built, and
  // the part behind it ranked held too: a lower lane's clip was moved ahead of the part.
  it("keeps a part in place behind the follower of an insert still building", () => {
    const { policy, held, log } = lowBehind([spec("p1", 1, 4), spec("p2")]);
    policy.submit(spec("y", 2));
    provide(policy, "s1", 10, held, log);
    held.slow.add("i");
    policy.edit([{ _tag: "Insert", spec: spec("i", 1, 2), anchor: key("p1"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    const from = held.orders.length;
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("k", 1, 2), follows: item("i") },
        anchor: key("i"),
        side: "after",
      },
    ]);
    provide(policy, "s1", 6, held, log);
    assert.isTrue(yBehindP2(held, from), held.orders.slice(from).join(" | "));
    // i's build ends, and k's follows.
    held.ready = [...held.ready, ...held.building];
    held.building = [];
    held.slow.clear();
    shown(policy, "s1", held);
    provide(policy, "s1", 10, held, log);
    assert.isTrue(yBehindP2(held, from), held.orders.slice(from).join(" | "));
    for (const time of [23_000, 27_100, 29_200, 31_300]) boundary(policy, "s1", held, time);
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "i", "k", "p2"]);
  });

  // Two inserts that each follow a later part of the other's group, each placed before a part the
  // other's chain waits on: the walk read follows links alone and admitted the second, and neither
  // group aired past its first part.
  it("refuses an insert whose follows chain reaches a later part through another group's order", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    for (const [name, parts] of [
      ["g", ["a1", "a2"]],
      ["h", ["b1", "b2"]],
    ] as const)
      policy.edit([
        {
          _tag: "SubmitGroup",
          key: key(name),
          lane: 1,
          parts: parts.map((part) => spec(part)),
          fingerprint: name,
        },
      ]);
    const insert = (name: string, anchor: string, follows: string) =>
      policy.edit([
        {
          _tag: "Insert",
          spec: { ...spec(name), follows: item(follows) },
          anchor: key(anchor),
          side: "after",
        },
      ]).actions;
    assert.isUndefined(refusalOf(insert("j", "b1", "a2")));
    assert.deepStrictEqual(refusalOf(insert("i", "a1", "b2")), waitsOnItself("i"));
  });

  // n goes in before b1 with no follows, and w, after a1 in another group, follows b2. n was sent
  // only once w, the item before it in the lane, was; w waited for b2's clip; and b2 waited behind
  // n: with builds unmeasured, neither group aired past a1.
  it("sends a group's insert once the member of its group before it is sent", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    for (const [name, parts] of [
      ["g", ["a1"]],
      ["h", ["b1", "b2"]],
    ] as const)
      policy.edit([
        {
          _tag: "SubmitGroup",
          key: key(name),
          lane: 1,
          parts: parts.map((part) => spec(part)),
          fingerprint: name,
        },
      ]);
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("w"), follows: item("b2") },
        anchor: key("a1"),
        side: "after",
      },
    ]);
    policy.edit([{ _tag: "Insert", spec: spec("n"), anchor: key("b1"), side: "before" }]);
    const log: Array<string> = [];
    const held = queues();
    for (let round = 0; round < 8; round++) {
      provide(policy, "s1", 10, held, log);
      boundary(policy, "s1", held, policy.now() + 6_000);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["a1", "n", "b1", "b2", "w"]);
  });

  // A replacement s2 takes new work while i sits behind p2 on s1. m goes in before p2 and is built
  // on s2, and i is replaced by ir, continued: the only clip ranked before ir there was m's, so ir
  // continued from it and was seated behind m, ahead of p2, whose clip airs in between.
  it("never seats a replacement earlier for a clip built on the session taking new work", () => {
    const { policy, log } = seatedInsert();
    policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 });
    policy.observe({}, "s2");
    for (let round = 0; round < 3 && policy.busy("s2")?._tag === "Autoplay"; round++)
      policy.reply({ _tag: "Done" }, undefined, "s2");
    const replacement = queues();
    policy.edit([{ _tag: "Insert", spec: spec("m", 1, 3), anchor: key("p2"), side: "before" }]);
    provide(policy, "s2", 4, replacement, log);
    assert.include(log, "enqueue m");
    policy.edit([
      { _tag: "Replace", key: key("i"), spec: { ...spec("ir", 1, 6), continuity: true } },
    ]);
    provide(policy, "s2", 4, replacement, log);
    assert.include(log, "enqueue ir");
    assert.strictEqual(policy.state().items.get(key("ir"))?.behind, key("p2"));
  });

  // k sits behind p2 beside i, placed after it, and q's continued build falls back to i's clip, so
  // q sits behind i. Every seat one deep behind p2 sorted ahead of every seat two deep: k aired
  // between i and q, whose clip continues from i's.
  it("seats an insert continued from a seated insert's clip right behind that one", () => {
    // Continued builds measured at 1 s a second, so q's would end only as i airs.
    const continued = { lane: 1, perBuilt: 1, perRequested: 1 };
    const slow = {
      ...measured,
      samples: { ...measured.samples, continued: [continued, continued, continued] },
    };
    const { policy, held, log } = seatedInsert(config, slow);
    policy.edit([{ _tag: "Insert", spec: spec("k", 1, 3), anchor: key("i"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("q", 1, 20), continuity: true },
        anchor: key("p1"),
        side: "after",
      },
    ]);
    provide(policy, "s1", 4, held, log);
    assert.strictEqual(policy.state().items.get(key("q"))?.behind, key("i"));
    for (const time of [43_000, 48_100, 53_200, 65_300, 85_400]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "i", "q", "k"]);
  });

  // k goes in after i, an insert after p1, and i is replaced by ir, continued, whose build falls
  // back to p2's clip, so ir sits behind p2. k, placed beside the item ir replaces, stayed at its
  // own order and aired before p2 and ir.
  it("seats the insert placed after a replaced insert beside its replacement", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    policy.edit([{ _tag: "Insert", spec: spec("i", 1, 3), anchor: key("p1"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    policy.edit([{ _tag: "Insert", spec: spec("k", 1, 3), anchor: key("i"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    // With 5 s of x left, ir's continued build of 18 s would be Ready only after p2 has begun.
    policy.edit(
      [{ _tag: "Replace", key: key("i"), spec: { ...spec("ir", 1, 18), continuity: true } }],
      false,
      38_000,
    );
    provide(policy, "s1", 8, held, log);
    assert.include(log, `enqueue ir from ${readyIn(held, "p2").clipId}`);
    for (const time of [43_000, 48_100, 53_200, 71_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "ir", "k"]);
  });

  /** x airs from 13 s to 43 s; p1 and p2 (5 s), then i (3 s), placed after p1, are Ready. */
  const besideInsert = () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    policy.edit([{ _tag: "Insert", spec: spec("i", 1, 3), anchor: key("p1"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    return { policy, held, log };
  };
  /** With 5 s of x left, i is replaced by ir, whose continued build of 18 s falls back to p2. */
  const replaceBehind = (policy: ReturnType<typeof drive>) =>
    policy.edit(
      [{ _tag: "Replace", key: key("i"), spec: { ...spec("ir", 1, 18), continuity: true } }],
      false,
      38_000,
    );

  // k, after i, went behind p2 with ir as soon as ir's build went out. i then started first and
  // ir went as withdrawn, but k stayed behind p2, apart from the insert it was placed after.
  it("keeps an insert beside the item it was placed after when that item's replacement loses", () => {
    const { policy, held, log } = besideInsert();
    policy.edit([{ _tag: "Insert", spec: spec("k", 1, 3), anchor: key("i"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    held.slow.add("ir");
    replaceBehind(policy);
    provide(policy, "s1", 6, held, log);
    for (const time of [43_000, 48_100, 51_200, 54_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(statuses(policy.actions, "ir").at(-1), "Dropped");
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "i", "k", "p2"]);
  });

  // k went in after i while ir, its replacement, was building behind p2. It took i's seat, its
  // own order, and stayed there once ir took i's place: it aired before p2 and ir.
  it("moves an insert placed beside a replaced item with its replacement once that takes the place", () => {
    const { policy, held, log } = besideInsert();
    held.slow.add("ir");
    replaceBehind(policy);
    provide(policy, "s1", 4, held, log);
    policy.edit([{ _tag: "Insert", spec: spec("k", 1, 3), anchor: key("i"), side: "after" }]);
    provide(policy, "s1", 4, held, log);
    // ir's build ends while x plays, and ir takes i's place.
    held.ready = [...held.ready, ...held.building];
    held.building = [];
    held.slow.clear();
    shown(policy, "s1", held);
    provide(policy, "s1", 6, held, log);
    for (const time of [43_000, 48_100, 53_200, 71_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "ir", "k"]);
  });

  // During a renewal, a continued build on the replacement went independent because a clip Ready
  // on the session on air ranked between it and the clip before it there, though the session on
  // air airs all of its own first.
  it("continues a build on a replacement from its clip before it, past a clip on air", () => {
    const policy = drive({ config: low });
    policy.tick(0);
    policy.open("s1");
    const log: Array<string> = [];
    const air = queues();
    policy.submit(spec("x", 2, 30));
    provide(policy, "s1", 1, air, log);
    const x = readyIn(air, "x");
    air.ready = [];
    air.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 3_000);
    shown(policy, "s1", air);
    policy.submit(spec("a", 2));
    provide(policy, "s1", 4, air, log);
    readyIn(air, "a");
    policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 });
    policy.observe({}, "s2");
    for (let round = 0; round < 3 && policy.busy("s2")?._tag === "Autoplay"; round++)
      policy.reply({ _tag: "Done" }, undefined, "s2");
    const replacement = queues();
    policy.submit(spec("b", 1));
    provide(policy, "s2", 4, replacement, log);
    policy.submit({ ...spec("c", 2), continuity: true });
    provide(policy, "s2", 4, replacement, log);
    assert.include(log, `enqueue c from ${readyIn(replacement, "b").clipId}`);
  });

  // A clip placed right after an insert that sits behind a later part was projected at its own
  // order, ahead of that part, and so never right after the insert.
  it("places a clip right after an insert that sits behind a later part", () => {
    const { policy } = seatedInsert();
    const { actions } = policy.send({
      _tag: "Place",
      id: 1,
      probe: { seconds: 20, continuity: false, submitInMs: 0 },
    });
    const placement = actions.find((action) => action._tag === "Placed");
    assert.deepStrictEqual(
      placement?._tag === "Placed" ? placement.placement?.after : placement,
      item("i"),
    );
  });

  // k went in before i, an insert held unbuilt by its window, and i was replaced by ir: as nothing
  // was built for i, ir took its place at once, before ir's build fell back to p2's clip and sat
  // behind p2. k, placed beside the item ir replaced, stayed at its own order.
  it("moves an insert beside a replaced item that was never built with the replacement's seat", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    const log: Array<string> = [];
    const held = queues();
    policy.submit(spec("x", 1, 30), 10);
    provide(policy, "s1", 1, held, log);
    const x = readyIn(held, "x");
    held.ready = [];
    held.playing = x;
    policy.event({ _tag: "Started", clip: x }, "s1", 13_000);
    shown(policy, "s1", held, 13_001);
    policy.edit([group(1, ["p1", "p2"])], false, 13_100);
    provide(policy, "s1", 6, held, log);
    const later = { notBeforeMs: 120_000, firm: false } as const;
    policy.edit([
      {
        _tag: "Insert",
        spec: { ...spec("i", 1, 3), window: later },
        anchor: key("p1"),
        side: "after",
      },
    ]);
    policy.edit([{ _tag: "Insert", spec: spec("k", 1, 3), anchor: key("i"), side: "before" }]);
    provide(policy, "s1", 4, held, log);
    readyIn(held, "k");
    replaceBehind(policy);
    provide(policy, "s1", 8, held, log);
    assert.include(log, `enqueue ir from ${readyIn(held, "p2").clipId}`);
    assert.strictEqual(policy.state().items.get(key("k"))?.behind, key("p2"));
    for (const time of [43_000, 48_100, 53_200, 56_300]) {
      boundary(policy, "s1", held, time);
      provide(policy, "s1", 6, held, log);
    }
    assert.deepStrictEqual(startOrder(policy.actions), ["x", "p1", "p2", "k", "ir"]);
  });
});
