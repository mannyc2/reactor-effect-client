/**
 * Where the show is seen. On a terminal, the session's newest frame is drawn with the status under
 * it, on the alternate screen, and the terminal is given back however the show ends. Anywhere else
 * (a pipe, a CI log) each change is a line of text and no frame is drawn.
 */
import { Console, Duration, Effect, Scope, Stdio, Stream, SubscriptionRef, Terminal } from "effect";
import type { PlatformError } from "effect/PlatformError";
import type { VideoFrame } from "reactor-effect-client/Media";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import * as Picture from "./Picture.ts";

/** Where a clip is: being sent, then each fact H3 reports about it. */
export type Phase = "sending" | "accepted" | "generated" | "playing" | "ended" | "failed";

export interface Screen {
  /** The clip at `index` in the playlist reached `phase`. */
  readonly clip: (index: number, phase: Phase) => Effect.Effect<void>;
  /** Shows these frames from now on: drawn on a terminal, named by the first one in a log. */
  readonly show: (
    frames: Stream.Stream<VideoFrame, ReactorError>,
  ) => Effect.Effect<void, never, Scope.Scope>;
}

interface View {
  readonly session: string;
  readonly prompts: ReadonlyArray<string>;
  readonly phases: ReadonlyArray<Phase>;
  /** The newest frame, once one has arrived. */
  readonly frame: VideoFrame | undefined;
  /** Why the frames stopped, if they have. */
  readonly stopped: string | undefined;
}

/** What the picture is: its size and pixel format, or why there is none. */
const video = (view: View): string => {
  if (view.stopped !== undefined) return `video stopped (${view.stopped})`;
  if (view.frame === undefined) return "no video yet";
  return `${view.frame.width}x${view.frame.height} ${view.frame.format}`;
};

/** The status under the picture: the session and each clip's phase, then what is playing. */
const status = (view: View): readonly [string, string] => {
  // A hosted session's id is a UUID: its first group is enough to tell sessions apart.
  const session = view.session.split("-", 1)[0] ?? view.session;
  const clips = view.phases.map((phase, index) => `${index + 1} ${phase}`).join(" · ");
  const playing = view.phases.indexOf("playing");
  return [
    `session ${session} · ${clips} · ${video(view)}`,
    playing === -1 ? "" : `▶ clip ${playing + 1}: ${view.prompts[playing] ?? ""}`,
  ];
};

/** In a log, a clip's step as its own line. */
const logged = (prompts: ReadonlyArray<string>): Screen => ({
  clip: (index, phase) =>
    Console.log(
      phase === "playing"
        ? `clip ${index + 1} playing: ${prompts[index] ?? ""}`
        : `clip ${index + 1} ${phase}`,
    ),
  show: (frames) =>
    frames.pipe(
      Stream.take(1),
      Stream.runForEach((frame) =>
        Console.log(`video ${frame.width}x${frame.height} ${frame.format}`),
      ),
      Effect.catch((error) => Console.log(`video stopped (${error.reason._tag})`)),
      Effect.forkScoped,
      Effect.asVoid,
    ),
});

const csi = "\u001b[";
/** The alternate screen, with the cursor hidden. */
const enter = `${csi}?1049h${csi}?25l`;
/** The main screen again, with the cursor shown and the colours reset. */
const leave = `${csi}0m${csi}?25h${csi}?1049l`;
/**
 * Synchronized output: a terminal that supports it holds its repaint until the whole screen is
 * written, so the picture never tears; one that does not ignores it.
 */
const hold = `${csi}?2026h`;
const unhold = `${csi}?2026l`;
/** About twelve redraws a second: smooth to the eye, half the frames H3 sends. */
const redrawEvery = Duration.millis(1000 / 12);

/** `text` on screen row `row`, counted from 1, with the rest of the row cleared. */
const at = (row: number, text: string) => `${csi}${row};1H${text}${csi}K`;

/** At most `columns` characters of `text`, so that a status line never wraps. */
const cut = (text: string, columns: number) => Array.from(text).slice(0, columns).join("");

/** The newest frame's picture within `space`, or none before the first frame. */
const picture = (frame: VideoFrame | undefined, space: Picture.Cells): Picture.Picture =>
  frame === undefined ? { columns: 0, rows: [] } : Picture.render({ frame, space });

/** The whole screen: the picture at the top, centred, and the status under it. */
const compose = (view: View, space: Picture.Cells): string => {
  const lines = status(view);
  const { columns, rows } = picture(view.frame, {
    columns: space.columns,
    rows: Math.max(0, space.rows - lines.length),
  });
  const margin = " ".repeat(Math.max(0, Math.floor((space.columns - columns) / 2)));
  return [
    hold,
    ...rows.map((row, index) => at(index + 1, margin + row)),
    ...lines.map((line, index) => at(rows.length + index + 1, cut(line, space.columns))),
    // Clears what a larger picture left below, before the terminal was resized.
    `${csi}J`,
    unhold,
  ].join("");
};

/** On a terminal: the newest view drawn about twelve times a second, on the alternate screen. */
const drawn = Effect.fnUntraced(function* (
  terminal: Terminal.Terminal,
  start: View,
): Effect.fn.Return<Screen, PlatformError, Scope.Scope> {
  const view = yield* SubscriptionRef.make(start);
  yield* Effect.acquireRelease(terminal.display(enter), () =>
    SubscriptionRef.get(view).pipe(
      // Back on the main screen, the last status stays as the show's record.
      Effect.flatMap((last) => terminal.display(`${leave}${status(last)[0]}\n`)),
      // A terminal that cannot be given back is a defect to report, not to pass over.
      Effect.orDie,
    ),
  );
  // Forked after the screen is taken, so it stops before the screen is given back. A write
  // that fails, as when the terminal has gone, ends the drawing; the show and its cleanup go on.
  yield* SubscriptionRef.changes(view).pipe(
    // Only the newest view is drawn: frames come 24 a second, faster than the terminal is redrawn.
    Stream.buffer({ capacity: 1, strategy: "sliding" }),
    Stream.runForEach((next) =>
      Effect.gen(function* () {
        const space = { columns: yield* terminal.columns, rows: yield* terminal.rows };
        yield* terminal.display(compose(next, space));
        yield* Effect.sleep(redrawEvery);
      }),
    ),
    Effect.forkScoped,
  );
  return {
    clip: (index, phase) =>
      SubscriptionRef.update(view, (current) => ({
        ...current,
        phases: current.phases.map((before, position) => (position === index ? phase : before)),
      })),
    show: (frames) =>
      frames.pipe(
        Stream.runForEach((frame) =>
          SubscriptionRef.update(view, (current) => ({ ...current, frame })),
        ),
        // The frames end with their connection, as when Reactor ends the session: the last
        // picture stays, and the status says why.
        Effect.catch((error) =>
          SubscriptionRef.update(view, (current) => ({ ...current, stopped: error.reason._tag })),
        ),
        Effect.forkScoped,
        Effect.asVoid,
      ),
  };
});

/**
 * The screen for one session's show of `prompts`: drawn when standard output is a terminal,
 * logged otherwise.
 */
export const make = Effect.fnUntraced(function* (show: {
  readonly session: string;
  readonly prompts: ReadonlyArray<string>;
}): Effect.fn.Return<Screen, PlatformError, Scope.Scope | Stdio.Stdio | Terminal.Terminal> {
  const stdio = yield* Stdio.Stdio;
  if (!(yield* stdio.stdoutIsTerminal)) return logged(show.prompts);
  return yield* drawn(yield* Terminal.Terminal, {
    ...show,
    phases: show.prompts.map((): Phase => "sending"),
    frame: undefined,
    stopped: undefined,
  });
});
