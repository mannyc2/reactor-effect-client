/** The playout's pure policy on its own: inputs in, actions and as-run out, no clock and no I/O. */
import { assert, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Policy from "../src/internal/playout/policy.js";
import type { ClipTag, SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { ItemKey } from "../src/Playout.js";

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
/** The answer to the command in flight. */
const answer = (state: Policy.State, result: Policy.CommandResult): Policy.Input => ({
  _tag: "Result",
  id: state.busy?.id ?? -1,
  result,
});

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
      if (state.busy?.command._tag === "Autoplay") send(answer(state, { _tag: "Done" }));
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
    /** Answers the command in flight. */
    reply: (result: Policy.CommandResult, time?: number) => send(answer(state, result), time),
    observe: (partial: Partial<SourceState>, id = "s1", time?: number) =>
      send(
        { _tag: "Source", sessionId: id, event: { _tag: "State", state: source(partial) } },
        time,
      ),
    event: (event: SourceEvent, id = "s1", time?: number) =>
      send({ _tag: "Source", sessionId: id, event }, time),
    tick: (time?: number) => send({ _tag: "Tick" }, time),
    /** The command in flight, if any. */
    busy: () => state.busy?.command,
  };
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
        ]).actions,
      ).find((action) => action.command._tag === "Cut");
    assert.deepStrictEqual(cut(clip("filler", { _tag: "Filler", index: 0 }, 15))?.command, {
      _tag: "Cut",
      clipId: "filler",
      next: "cu",
    });
    assert.isUndefined(cut(clip("peer", item("other"), 15)));
  });

  // The 0.7.0 scheduler-cut paid run: H3 answered the stop before it reported the clip ended,
  // the scheduler cut the clip again, and that second stop cut the cutter 5 ms after it started.
  it("cuts a playing clip once, though its end is reported after the cut's result", () => {
    const playing = clip("long", item("other"), 15);
    const stale: Policy.Input = {
      _tag: "Source",
      sessionId: "s1",
      event: {
        _tag: "State",
        state: source({ playing, ready: [clip("cu", item("urgent"))] }),
      },
    };
    const { state, actions } = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("other") }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "long" } },
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: playing } },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("urgent", 0) }], batch: false },
      { _tag: "Result", id: 3, result: { _tag: "Done", clipId: "cu" } },
      stale,
    ]);
    assert.deepStrictEqual(commands(actions).at(-1)?.command, {
      _tag: "Cut",
      clipId: "long",
      next: "cu",
    });
    const after = run([answer(state, { _tag: "Done" }), stale, { _tag: "Tick" }], state, 10);
    assert.isUndefined(commands(after.actions).find((action) => action.command._tag === "Cut"));
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
        { _tag: "Result", id: state.busy?.id ?? -1, result: { _tag: "Done", clipId: "cn" } },
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
    const { state } = run([
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
    ]);
    const view = Policy.view(config, state, { mono: 10, wall: 10 });
    assert.strictEqual(view.runwaySeconds, 5);
    assert.strictEqual(view.playing, "other");
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
        result: { _tag: "Failed", outcome: "unknown", retryable: false, reason: "lost reply" },
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
    const lost = { _tag: "Failed", outcome: "unknown", retryable: false, reason: "lost" } as const;
    state = at(state, answer(state, lost), 2_010).state;
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
    assert.deepStrictEqual(state().samples.continued, [0.4]);
  });

  // Any sequence of edits and provider answers keeps the plan's promises.
  it.effect.prop(
    "one command at a time, no enqueue sent twice for a key, one terminal status each",
    [
      Schema.Array(
        Schema.Literals(["submit", "withdraw", "done", "unknown", "refused", "ready", "lost"]),
      ).check(Schema.isMaxLength(40)),
    ],
    ([script]) =>
      Effect.sync(() => {
        let state = run(opened()).state;
        const actions: Array<Policy.Action> = [];
        let clock = 10;
        script.forEach((step, index) => {
          const busy = state.busy;
          const input: Policy.Input =
            step === "submit"
              ? {
                  _tag: "Edit",
                  id: 100 + index,
                  edits: [{ _tag: "Submit", spec: spec(`k${index % 5}`) }],
                  batch: false,
                }
              : step === "withdraw"
                ? {
                    _tag: "Edit",
                    id: 100 + index,
                    edits: [{ _tag: "Withdraw", key: key(`k${index % 5}`) }],
                    batch: false,
                  }
                : step === "lost"
                  ? { _tag: "Lost", sessionId: state.sessions[0]?.id ?? "s1", reason: "gone" }
                  : step === "ready"
                    ? {
                        _tag: "Source",
                        sessionId: state.sessions[0]?.id ?? "s1",
                        event: { _tag: "State", state: source() },
                      }
                    : busy === undefined
                      ? { _tag: "Tick" }
                      : {
                          _tag: "Result",
                          id: busy.id,
                          result:
                            step === "done"
                              ? { _tag: "Done", clipId: `c${index}` }
                              : {
                                  _tag: "Failed",
                                  outcome: step === "unknown" ? "unknown" : "not-submitted",
                                  retryable: step === "refused",
                                  reason: step,
                                },
                        };
          const result = Policy.step(config, state, input, { mono: clock, wall: clock });
          clock += 7;
          state = result.state;
          actions.push(...result.actions);
        });
        const sent = commands(actions);
        // A command is issued only when none is in flight: ids strictly increase.
        for (let index = 1; index < sent.length; index++)
          assert.isAbove(sent[index]!.id, sent[index - 1]!.id);
        const byKey = new Map<string, Array<string>>();
        for (const action of actions)
          if (action._tag === "Emit" && action.event._tag === "AsRun")
            byKey.set(action.event.event.key, [
              ...(byKey.get(action.event.event.key) ?? []),
              action.event.event.status._tag,
            ]);
        for (const [name, history] of byKey) {
          assert.strictEqual(history[0], "Accepted", name);
          const terminal = history.filter((status) =>
            ["Ended", "Dropped", "Failed", "Unobserved"].includes(status),
          );
          assert.isAtMost(terminal.length, 1, `${name}: ${history.join(",")}`);
        }
        // An enqueue whose outcome is unknown is never sent again.
        const unknown = new Set(
          actions.flatMap((action) =>
            action._tag === "Emit" &&
            action.event._tag === "AsRun" &&
            action.event.event.status._tag === "Unknown"
              ? [action.event.event.key]
              : [],
          ),
        );
        for (const name of unknown) {
          const after = actions.findIndex(
            (action) =>
              action._tag === "Emit" &&
              action.event._tag === "AsRun" &&
              action.event.event.key === name &&
              action.event.event.status._tag === "Unknown",
          );
          const again = actions
            .slice(after)
            .some(
              (action) =>
                action._tag === "Command" &&
                action.command._tag === "Enqueue" &&
                action.command.tag._tag === "Item" &&
                action.command.tag.key === name,
            );
          const carried = actions
            .slice(after)
            .some(
              (action) =>
                action._tag === "Emit" &&
                action.event._tag === "AsRun" &&
                action.event.event.key === name &&
                action.event.event.status._tag === "Accepted",
            );
          assert.isFalse(again && !carried, `${name} was sent again after an unknown outcome`);
        }
      }),
  );
});

const enqueued = (actions: ReadonlyArray<Policy.Action>, sessionId?: string) =>
  commands(actions).flatMap((action) =>
    action.command._tag === "Enqueue" && (sessionId === undefined || action.sessionId === sessionId)
      ? [action.command.tag._tag === "Item" ? String(action.command.tag.key) : "filler"]
      : [],
  );
const unknown: Policy.CommandResult = {
  _tag: "Failed",
  outcome: "unknown",
  retryable: false,
  reason: "the reply was lost",
};

// What 0.7.0's scheduler guaranteed and the first Playout lost, found by an independent critique.
describe("PlayoutPolicy, uncertainty and loss", () => {
  const filled: Policy.Config = {
    ...config,
    filler: {
      floor: 5,
      target: 10,
      clip: ({ index }) => ({ prompt: `filler ${String(index)}`, seconds: 5 }),
      lengths: { min: 5, max: 15 },
    },
  };

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
      assert.deepStrictEqual(enqueued(policy.actions), ["lost"]);
      const deadline = policy.tick(20 + config.unknownTimeoutMs);
      assert.isTrue(deadline.actions.some((action) => action._tag === "Open"));
      policy.open("s2", lifetimeMs);
      assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["next"]);
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
