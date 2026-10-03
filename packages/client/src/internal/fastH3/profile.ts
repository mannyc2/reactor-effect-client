/** FastH3's documented request bounds and the frame grid observed on hosted FastH3. */
export const modelName = "reactor/fast-h3" as const;
export const documentedVersion = "1.7.2" as const;
export const source = "https://docs.reactor.inc/model-api-reference/fast-h3/schema";
export const requestSeconds = { min: 5.167, max: 14.375 } as const;
export const defaultSeconds = 14.375;
export const fps = 24;
export const frameGrid = { min: 124, step: 17, max: 345 } as const;
export const prompt = { maxTokens: 1_024 } as const;
export const tracks = { video: "main_video", audio: "main_audio" } as const;
export const canvases = {
  "16:9": { width: 1344, height: 768 },
  // These three pixel sizes follow H3; FastH3 documents their aspects but no pixel sizes.
  "1:1": { width: 768, height: 768 },
  "9:16": { width: 768, height: 1344 },
  "4:3": { width: 1024, height: 768 },
} as const;
export type CanvasAspect = keyof typeof canvases;

/** An accepted request's frame count, aligned upward onto FastH3's grid. */
export const snapFrames = (seconds: number): number => {
  // The documented minimum rounds 124/24 to three decimals; it still builds 124 frames.
  if (seconds <= requestSeconds.min) return frameGrid.min;
  const frames = Math.ceil(seconds * fps);
  return Math.min(
    frameGrid.max,
    frameGrid.min + Math.ceil((frames - frameGrid.min) / frameGrid.step) * frameGrid.step,
  );
};
export const builtSeconds = (seconds: number): number => snapFrames(seconds) / fps;
