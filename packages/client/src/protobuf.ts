import { ReactorError, positiveLimit } from "./errors.js";
export interface WireLimits {
  readonly bytes: number;
  readonly depth: number;
  readonly fields: number;
}
export const wireLimits: WireLimits = { bytes: 262_144, depth: 64, fields: 65_536 };
export interface UnknownFields {
  _unknown?: readonly Uint8Array[];
}
export interface Field {
  readonly number: number;
  readonly wire: number;
  readonly start: number;
}
interface Budget {
  fields: number;
}
const fail = (s: string): never => {
  throw new ReactorError("Protocol", s);
};
const validate = (l: WireLimits): void => {
  positiveLimit(l.bytes, "wire bytes", 16 * 1024 * 1024);
  positiveLimit(l.depth, "wire depth", 256);
  positiveLimit(l.fields, "wire fields", 1_000_000);
};
export const checkedString = (s: string, max: number): string => {
  if (typeof s !== "string" || s.length > max) fail("string exceeds wire bound");
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const next = s.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) fail("unpaired UTF-16 surrogate");
    } else if (c >= 0xdc00 && c <= 0xdfff) fail("unpaired UTF-16 surrogate");
  }
  return s;
};
/** Strict, bounded reader, retaining unknown fields including legacy groups. */
export class Reader {
  private offset = 0;
  constructor(
    readonly input: Uint8Array,
    readonly limits: WireLimits = wireLimits,
    readonly depth = 0,
    private readonly budget: Budget = { fields: 0 },
  ) {
    validate(limits);
    if (input.byteLength > limits.bytes || depth > limits.depth) fail("protobuf size/depth limit");
  }
  get done(): boolean {
    return this.offset === this.input.length;
  }
  private need(n: number): void {
    if (n < 0 || n > this.input.length - this.offset) fail("truncated protobuf field");
  }
  varint(): bigint {
    let n = 0n;
    for (let i = 0; i < 10; i++) {
      this.need(1);
      const b = this.input[this.offset++];
      if (b === undefined) return fail("truncated varint");
      if (i === 9 && b > 1) fail("varint exceeds 64 bits");
      n |= BigInt(b & 127) << BigInt(7 * i);
      if (b < 128) return n;
    }
    return fail("unterminated varint");
  }
  field(): Field {
    if (++this.budget.fields > this.limits.fields) fail("protobuf field count limit");
    const start = this.offset,
      tag = this.varint();
    if (tag > 0xffffffffn) fail("invalid protobuf tag");
    const number = Number(tag >> 3n),
      wire = Number(tag & 7n);
    if (number === 0 || wire > 5) fail("invalid protobuf tag/wire type");
    return { number, wire, start };
  }
  expect(f: Field, wire: number): void {
    if (f.wire !== wire) fail(`wrong wire type for field ${f.number}`);
  }
  int64(): bigint {
    return BigInt.asIntN(64, this.varint());
  }
  int32(): number {
    return Number(BigInt.asIntN(32, this.varint()));
  }
  bool(): boolean {
    return this.varint() !== 0n;
  }
  double(): number {
    this.need(8);
    const n = new DataView(this.input.buffer, this.input.byteOffset + this.offset, 8).getFloat64(
      0,
      true,
    );
    this.offset += 8;
    return n;
  }
  bytes(): Uint8Array {
    const n = this.varint();
    if (n > BigInt(this.input.length - this.offset))
      return fail("truncated length-delimited field");
    const size = Number(n),
      b = this.input.subarray(this.offset, this.offset + size);
    this.offset += size;
    return b;
  }
  string(): string {
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(this.bytes());
    } catch (e) {
      if (e instanceof ReactorError) throw e;
      return fail("invalid UTF-8");
    }
  }
  child(): Reader {
    return new Reader(this.bytes(), this.limits, this.depth + 1, this.budget);
  }
  unknown(f: Field): Uint8Array {
    this.skip(f, 0);
    return this.input.slice(f.start, this.offset);
  }
  private skip(f: Field, groupDepth: number): void {
    switch (f.wire) {
      case 0:
        this.varint();
        break;
      case 1:
        this.need(8);
        this.offset += 8;
        break;
      case 2:
        this.bytes();
        break;
      case 3: {
        if (this.depth + groupDepth + 1 > this.limits.depth) fail("protobuf group depth limit");
        while (!this.done) {
          const next = this.field();
          if (next.wire === 4) {
            if (next.number !== f.number) fail("mismatched protobuf end group");
            return;
          }
          this.skip(next, groupDepth + 1);
        }
        fail("unterminated protobuf group");
        break;
      }
      case 4:
        fail("unexpected protobuf end group");
        break;
      case 5:
        this.need(4);
        this.offset += 4;
        break;
    }
  }
}
/** No external protobuf runtime. Child writers share depth and field budgets. */
export class Writer {
  private readonly chunks: Uint8Array[] = [];
  private size = 0;
  constructor(
    readonly limits: WireLimits = wireLimits,
    readonly depth = 0,
    private readonly budget: Budget = { fields: 0 },
  ) {
    validate(limits);
    if (depth > limits.depth) fail("protobuf depth limit");
  }
  raw(b: Uint8Array): void {
    if (b.length > this.limits.bytes - this.size) fail("protobuf message size limit");
    this.chunks.push(b);
    this.size += b.length;
  }
  varint(n: bigint): void {
    if (n < 0n || n > 0xffffffffffffffffn) fail("varint outside uint64 range");
    const bytes: number[] = [];
    do {
      const b = Number(n & 127n);
      n >>= 7n;
      bytes.push(n ? b | 128 : b);
    } while (n);
    this.raw(new Uint8Array(bytes));
  }
  tag(field: number, wire: number): void {
    if (++this.budget.fields > this.limits.fields) fail("protobuf field count limit");
    this.varint(BigInt(field * 8 + wire));
  }
  int64(field: number, value: bigint): void {
    if (typeof value !== "bigint" || value < -(1n << 63n) || value >= 1n << 63n)
      fail("int64 out of range");
    this.tag(field, 0);
    this.varint(BigInt.asUintN(64, value));
  }
  int32(field: number, value: number): void {
    if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647)
      fail("enum/int32 out of range");
    this.tag(field, 0);
    this.varint(BigInt.asUintN(64, BigInt(value)));
  }
  bool(field: number, value: boolean): void {
    this.tag(field, 0);
    this.varint(value ? 1n : 0n);
  }
  double(field: number, value: number): void {
    if (typeof value !== "number") fail("double must be a number");
    const b = new Uint8Array(8);
    new DataView(b.buffer).setFloat64(0, value, true);
    this.tag(field, 1);
    this.raw(b);
  }
  string(field: number, value: string): void {
    const b = new TextEncoder().encode(checkedString(value, this.limits.bytes));
    this.tag(field, 2);
    this.varint(BigInt(b.length));
    this.raw(b);
  }
  message(field: number, write: (writer: Writer) => void): void {
    const child = new Writer(this.limits, this.depth + 1, this.budget);
    write(child);
    const b = child.finish();
    this.tag(field, 2);
    this.varint(BigInt(b.length));
    this.raw(b);
  }
  unknown(value: UnknownFields): void {
    for (const b of value._unknown ?? []) {
      const reader = new Reader(b, this.limits, this.depth);
      while (!reader.done) {
        reader.unknown(reader.field());
        if (++this.budget.fields > this.limits.fields) fail("protobuf field count limit");
      }
      this.raw(b);
    }
  }
  finish(): Uint8Array<ArrayBuffer> {
    const b = new Uint8Array(this.size);
    let offset = 0;
    for (const chunk of this.chunks) {
      b.set(chunk, offset);
      offset += chunk.length;
    }
    return b;
  }
}
