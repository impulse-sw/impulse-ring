// Ring — TypeScript/JavaScript connector (by Impulse).
//
// JavaScript cannot mmap shared memory or do cross-process atomics/futex in pure
// JS, so this connector binds the native C connector (libimpulse_ring) via Bun's
// built-in FFI. The Avro codec is pure TypeScript (see avro.ts); the transport
// (shm rings, frames, control plane) is the proven C core.
//
// Scope: the client surface — connect, publish, list, subscribe, call. Exposing
// a function (being an RPC server) is not supported through the FFI binding,
// because the C service runs the handler on its own thread and would have to call
// synchronously back into the JS VM; do that from Rust/C/Go/Python instead.
//
// Run with Bun. Linux only (Tier 0: arm64/amd64).

import { dlopen, FFIType, ptr, toArrayBuffer } from "bun:ffi";

import { Decoder, Encoder } from "./avro";

export { Decoder, Encoder };

const LIB =
  process.env.IMPULSE_RING_LIB ?? `${import.meta.dir}/../../c/build/libimpulse_ring.so`;

const { i32, i64, u64, ptr: PTR, cstring, void: VOID } = FFIType;

const lib = dlopen(LIB, {
  ir_connect: { args: [cstring], returns: PTR },
  ir_connect_with_reply_cap: { args: [cstring, u64], returns: PTR },
  ir_reply_capacity: { args: [PTR], returns: u64 },
  ir_disconnect: { args: [PTR], returns: VOID },
  ir_last_error: { args: [PTR], returns: cstring },
  ir_free: { args: [PTR], returns: VOID },
  ir_publish_channel: { args: [PTR, cstring, cstring, cstring], returns: PTR },
  ir_publish_channel_with_arena: { args: [PTR, cstring, cstring, cstring, u64], returns: PTR },
  ir_publish: { args: [PTR, PTR, u64], returns: i32 },
  ir_publisher_free: { args: [PTR], returns: VOID },
  ir_publisher_capacity: { args: [PTR], returns: u64 },
  ir_list_channels: { args: [PTR, PTR, u64, PTR], returns: i32 },
  ir_subscribe: { args: [PTR, i64, cstring], returns: PTR },
  ir_recv: { args: [PTR, i32, PTR, PTR], returns: i32 },
  ir_subscriber_free: { args: [PTR], returns: VOID },
  ir_subscriber_capacity: { args: [PTR], returns: u64 },
  ir_call: { args: [PTR, cstring, cstring, PTR, u64, i32, PTR, PTR], returns: i32 },
  ir_function_arena_capacity: { args: [PTR, cstring, cstring, PTR], returns: i32 },
  ir_broker_epoch: { args: [PTR], returns: i64 },
  ir_broker_restarted: { args: [PTR], returns: i32 },
  ir_set_auto_reconnect: { args: [PTR, i32], returns: VOID },
});
const S = lib.symbols;

const IR_OK = 0;
// sizeof(ir_channel_info): i64 + char[256] + char[256] + u64 + int, 8-aligned.
const CHAN_STRIDE = 536;

export class RingError extends Error {}

function cstr(s: string): Uint8Array {
  return new TextEncoder().encode(s + "\0");
}

function readCField(buf: Uint8Array, off: number, max: number): string {
  let end = off;
  while (end < off + max && buf[end] !== 0) end++;
  return new TextDecoder().decode(buf.subarray(off, end));
}

export interface ChannelInfo {
  channelId: bigint;
  name: string;
  ownerApp: string;
  schemaFp: bigint;
  requiresKey: boolean;
}

export class Publisher {
  constructor(private handle: number | bigint) {}
  publish(body: Uint8Array): void {
    const rc = S.ir_publish(this.handle, ptr(body), BigInt(body.length));
    if (rc !== IR_OK) throw new RingError("publish failed");
  }
  // Bytes of data region the channel's arena holds, as the broker allocated it
  // (clamped and rounded, so not necessarily what was asked for). A message
  // bigger than this can never be published, so a publisher that cuts a stream
  // into messages sizes them from here.
  capacity(): number {
    return Number(S.ir_publisher_capacity(this.handle));
  }
  close(): void {
    S.ir_publisher_free(this.handle);
  }
}

export class Subscriber {
  constructor(private handle: number | bigint) {}
  // Returns the next message body, or null on timeout.
  recv(timeoutMs: number): Uint8Array | null {
    const bodyOut = new BigUint64Array(1);
    const lenOut = new BigUint64Array(1);
    const rc = S.ir_recv(this.handle, timeoutMs, ptr(bodyOut), ptr(lenOut));
    if (rc === 0) return null;
    if (rc < 0) throw new RingError("recv failed");
    const p = Number(bodyOut[0]);
    const len = Number(lenOut[0]);
    const out = new Uint8Array(toArrayBuffer(p, 0, len)).slice();
    S.ir_free(p);
    return out;
  }
  // Bytes of data region the channel's arena holds. Both ends read it out of the
  // same ring header, so neither has to assume what the other kept to.
  capacity(): number {
    return Number(S.ir_subscriber_capacity(this.handle));
  }
  close(): void {
    S.ir_subscriber_free(this.handle);
  }
}

export class Connection {
  private handle: number | bigint;

  // `replyCap` sizes this connection's reply ring in bytes (0 = the connector
  // default, 64 KiB). Every reply travels through that one ring — control
  // replies and RPC responses alike — so it is the ceiling on a response: a
  // function whose answer does not fit fails to deliver it. The value is raised
  // to the default if smaller, capped at 128 MiB, and rounded up to a power of
  // two (a ring's capacity must be one).
  constructor(appName: string, replyCap = 0) {
    this.handle = S.ir_connect_with_reply_cap(cstr(appName), BigInt(replyCap));
    if (!this.handle) throw new RingError("connect failed (is impulsed running?)");
  }

  // Bytes of data region this connection's reply ring holds.
  replyCapacity(): number {
    return Number(S.ir_reply_capacity(this.handle));
  }

  private err(prefix: string): RingError {
    const e = S.ir_last_error(this.handle);
    return new RingError(`${prefix}: ${e ? String(e) : "unknown"}`);
  }

  close(): void {
    if (this.handle) {
      S.ir_disconnect(this.handle);
      this.handle = 0;
    }
  }

  // `arenaCap` requests an arena capacity in bytes (0 = broker default, 512
  // KiB): the publisher sizes it, because the publisher is the side that knows
  // how big its messages are. The broker clamps it to [256 KiB, 128 MiB] and
  // rounds up to a power of two; Publisher.capacity reports what it got.
  publishChannel(name: string, schemaJson: string, key = "", arenaCap = 0): Publisher {
    const h = S.ir_publish_channel_with_arena(
      this.handle,
      cstr(name),
      cstr(schemaJson),
      key ? cstr(key) : null,
      BigInt(arenaCap),
    );
    if (!h) throw this.err("publishChannel");
    return new Publisher(h);
  }

  // Capacity of a function's request arena, as the broker allocated it: what a
  // caller sizes its requests by. Costs one control round-trip, so remember the
  // answer rather than asking per call.
  functionArenaCapacity(fnName: string, key = ""): number {
    const out = new BigUint64Array(1);
    const rc = S.ir_function_arena_capacity(this.handle, cstr(fnName), key ? cstr(key) : null, ptr(out));
    if (rc !== IR_OK) throw this.err("functionArenaCapacity");
    return Number(out[0]);
  }

  listChannels(max = 64): ChannelInfo[] {
    const buf = new Uint8Array(max * CHAN_STRIDE);
    const countOut = new BigUint64Array(1);
    if (S.ir_list_channels(this.handle, ptr(buf), BigInt(max), ptr(countOut)) !== IR_OK)
      throw this.err("listChannels");
    const count = Number(countOut[0]);
    const dv = new DataView(buf.buffer);
    const out: ChannelInfo[] = [];
    for (let i = 0; i < count; i++) {
      const b = i * CHAN_STRIDE;
      out.push({
        channelId: dv.getBigInt64(b + 0, true),
        name: readCField(buf, b + 8, 256),
        ownerApp: readCField(buf, b + 264, 256),
        schemaFp: dv.getBigUint64(b + 520, true),
        requiresKey: dv.getInt32(b + 528, true) !== 0,
      });
    }
    return out;
  }

  subscribe(channelId: bigint | number, key = ""): Subscriber {
    const h = S.ir_subscribe(this.handle, BigInt(channelId), key ? cstr(key) : null);
    if (!h) throw this.err("subscribe");
    return new Subscriber(h);
  }

  call(fnName: string, req: Uint8Array, key = "", timeoutMs = 5000): Uint8Array {
    const respOut = new BigUint64Array(1);
    const lenOut = new BigUint64Array(1);
    const rc = S.ir_call(
      this.handle,
      cstr(fnName),
      key ? cstr(key) : null,
      ptr(req),
      BigInt(req.length),
      timeoutMs,
      ptr(respOut),
      ptr(lenOut),
    );
    if (rc !== IR_OK) throw this.err("call");
    const p = Number(respOut[0]);
    const len = Number(lenOut[0]);
    const out = new Uint8Array(toArrayBuffer(p, 0, len)).slice();
    S.ir_free(p);
    return out;
  }

  // ---- broker-restart recovery ----
  // The underlying C connection transparently reconnects and replays its
  // published channels / exposed functions when impulsed restarts, so calls
  // through this client keep working across a restart; these expose and control
  // that behaviour.

  // The broker epoch this connection is attached under (changes on a restart).
  brokerEpoch(): bigint {
    return BigInt(S.ir_broker_epoch(this.handle));
  }

  // True if impulsed has restarted (or is currently unreachable) since connect.
  brokerRestarted(): boolean {
    return S.ir_broker_restarted(this.handle) !== 0;
  }

  // Enable/disable transparent reconnect on a detected restart (default: on).
  setAutoReconnect(on: boolean): void {
    S.ir_set_auto_reconnect(this.handle, on ? 1 : 0);
  }
}
