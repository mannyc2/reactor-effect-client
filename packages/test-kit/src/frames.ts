/**
 * Decoded-media buffer ownership, the same on every host. Each frame's bytes
 * must be the whole of an ArrayBuffer of their own: offset 0, no slack before
 * or after them, and no buffer shared with another frame. A typed view passes
 * a type check without this: a pooled or IPC-delivered view can sit inside a
 * larger buffer, which `.buffer` exposes and a Transferable would move whole.
 *
 * `bytes` selects the view to check, so the check assumes no pixel or sample
 * format; a host checks its declared format separately. This module needs no
 * DOM or Node types, so every host's tests can import it.
 */
export const assertExactFrames = <A>(
  frames: readonly A[],
  bytes: (frame: A) => ArrayBufferView,
): void => {
  const seen = new Set<ArrayBufferLike>();
  frames.forEach((frame, index) => {
    const view = bytes(frame);
    if (view.byteOffset !== 0)
      throw new Error(`frame ${index} starts at byte ${view.byteOffset} of its buffer, not 0`);
    if (view.buffer.byteLength !== view.byteLength)
      throw new Error(
        `frame ${index} holds ${view.byteLength} bytes of a ${view.buffer.byteLength}-byte buffer`,
      );
    if (seen.has(view.buffer)) throw new Error(`frame ${index} shares its buffer with another`);
    seen.add(view.buffer);
  });
};
