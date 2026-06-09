// Minimal Apache Avro binary codec (the subset Ring needs), pure TypeScript.
// Encoding matches the Avro spec, so it interoperates with the Rust broker and
// the other connectors.

const U64 = (1n << 64n) - 1n;

export class Encoder {
  private bytes: number[] = [];

  private varint(zz: bigint): void {
    while (zz & ~0x7fn) {
      this.bytes.push(Number((zz & 0x7fn) | 0x80n));
      zz >>= 7n;
    }
    this.bytes.push(Number(zz & 0x7fn));
  }

  putLong(v: number | bigint): this {
    const b = BigInt(v);
    this.varint(((b << 1n) ^ (b >> 63n)) & U64);
    return this;
  }
  putInt(v: number): this {
    return this.putLong(v);
  }
  putBool(v: boolean): this {
    this.bytes.push(v ? 1 : 0);
    return this;
  }
  putDouble(v: number): this {
    const buf = new ArrayBuffer(8);
    new DataView(buf).setFloat64(0, v, true);
    for (const x of new Uint8Array(buf)) this.bytes.push(x);
    return this;
  }
  putBytes(b: Uint8Array): this {
    this.putLong(b.length);
    for (const x of b) this.bytes.push(x);
    return this;
  }
  putString(s: string): this {
    return this.putBytes(new TextEncoder().encode(s));
  }
  arrayStart(count: number | bigint): this {
    return this.putLong(count);
  }
  arrayEnd(): this {
    return this.putLong(0);
  }
  bytes_(): Uint8Array {
    return new Uint8Array(this.bytes);
  }
}

export class Decoder {
  private pos = 0;
  constructor(private data: Uint8Array) {}

  private varint(): bigint {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      const b = this.data[this.pos++];
      result |= BigInt(b & 0x7f) << shift;
      if (!(b & 0x80)) break;
      shift += 7n;
    }
    return result;
  }

  long(): bigint {
    const n = this.varint();
    return (n >> 1n) ^ -(n & 1n);
  }
  int(): number {
    return Number(this.long());
  }
  bool(): boolean {
    return this.data[this.pos++] !== 0;
  }
  double(): number {
    const dv = new DataView(this.data.buffer, this.data.byteOffset + this.pos, 8);
    this.pos += 8;
    return dv.getFloat64(0, true);
  }
  bytes(): Uint8Array {
    const n = Number(this.long());
    const out = this.data.slice(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  string(): string {
    return new TextDecoder().decode(this.bytes());
  }
  arrayCount(): bigint {
    return this.long();
  }
}
