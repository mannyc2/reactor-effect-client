/**
 * A decoded video frame as terminal text: rows of upper-half blocks in 24-bit colour, two pixels
 * to a cell, the top one the block's colour and the bottom one its background. Pure, and cheap
 * enough to redraw a frame several times a second: each half cell samples the one pixel at its
 * centre, and nothing is allocated per pixel.
 */
import type { VideoFrame } from "reactor-effect-client/Media";

/** A size in terminal cells. */
export interface Cells {
  readonly columns: number;
  readonly rows: number;
}

/** A frame as text: one string per row of cells, each ending in a colour reset. */
export interface Picture {
  readonly columns: number;
  readonly rows: ReadonlyArray<string>;
}

/**
 * The largest picture of a frame that fits in `space`, its shape kept. A terminal cell is about
 * twice as tall as it is wide and shows two pixels, one above the other, so its pixels are square.
 */
const fit = (frame: VideoFrame, space: Cells): Cells => {
  const columns = Math.min(
    space.columns,
    Math.floor((2 * space.rows * frame.width) / frame.height),
  );
  const rows = Math.min(space.rows, Math.round((columns * frame.height) / (2 * frame.width)));
  return { columns, rows };
};

const upperHalfBlock = "▀";
const reset = "\u001b[0m";
const foreground = (rgb: number) => `\u001b[38;2;${rgb >> 16};${(rgb >> 8) & 255};${rgb & 255}m`;
const background = (rgb: number) => `\u001b[48;2;${rgb >> 16};${(rgb >> 8) & 255};${rgb & 255}m`;

/** `frame` drawn as large as fits in `space`, its shape kept. */
export const render = (input: { readonly frame: VideoFrame; readonly space: Cells }): Picture => {
  const { data, width, height } = input.frame;
  const size = fit(input.frame, input.space);
  // Four bytes a pixel, in the frame's own channel order.
  const red = input.frame.format === "RGBA" ? 0 : 2;
  const blue = 2 - red;
  const rgb = (at: number) =>
    ((data[at + red] ?? 0) << 16) | ((data[at + 1] ?? 0) << 8) | (data[at + blue] ?? 0);
  // Where each column of cells, and each half row, takes its pixel from.
  const columns = Array.from(
    { length: size.columns },
    (_, column) => Math.floor(((column + 0.5) * width) / size.columns) * 4,
  );
  const halfRow = (half: number) =>
    Math.floor(((half + 0.5) * height) / (2 * size.rows)) * width * 4;
  const rows: Array<string> = [];
  for (let row = 0; row < size.rows; row++) {
    const top = halfRow(2 * row);
    const bottom = halfRow(2 * row + 1);
    let text = "";
    // A colour is sent only where it changes along the row.
    let upper = -1;
    let lower = -1;
    for (const column of columns) {
      const above = rgb(top + column);
      const below = rgb(bottom + column);
      if (above !== upper) text += foreground(above);
      if (below !== lower) text += background(below);
      upper = above;
      lower = below;
      text += upperHalfBlock;
    }
    rows.push(text + reset);
  }
  return { columns: size.columns, rows };
};
