/** Built-in AudioWorklet module. No package imports, network, decoder or polyfill.
 * One credit authorizes one render block: MessagePort never queues an unbounded PCM history.
 * The standard worklet globals are absent from TypeScript's DOM library. */
declare const currentFrame: number;
declare const sampleRate: number;
declare abstract class AudioWorkletProcessor {
  readonly port: MessagePort;
}
declare function registerProcessor(name: string, processor: new () => AudioWorkletProcessor): void;
class ReactorPcmTap extends AudioWorkletProcessor {
  private credit = false;
  private stopped = false;
  private skipped = 0;
  constructor() {
    super();
    this.port.onmessage = (event: MessageEvent<unknown>) => {
      if (event.data === "pull") this.credit = true;
      if (event.data === "stop") { this.stopped = true; this.port.onmessage = null; this.port.close(); }
    };
  }
  process(inputs: Float32Array[][], outputs: Float32Array[][]): boolean {
    // A connected silent output keeps the graph rendering without monitoring the received audio.
    for (const output of outputs) for (const plane of output) plane.fill(0);
    if (this.stopped) return false;
    const input = inputs[0], first = input?.[0];
    if (input === undefined || first === undefined || first.length === 0) return true;
    if (!this.credit) { this.skipped = Math.min(Number.MAX_SAFE_INTEGER, this.skipped + first.length); return true; }
    this.credit = false;
    if (input.length > 32 || first.length > 16384) {
      this.port.postMessage({ type: "error", message: "render quantum exceeds PCM bounds" }); return true;
    }
    const planes = input.map((plane) => new Float32Array(plane).buffer);
    this.port.postMessage({ type: "pcm", planes, frames: first.length, sampleRate, frame: currentFrame,
      skipped: this.skipped }, planes);
    this.skipped = 0;
    return true;
  }
}
registerProcessor("reactor-pcm-tap-v1", ReactorPcmTap);
export {};
