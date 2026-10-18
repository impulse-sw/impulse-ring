# Ring — Bootstrap & Handshake (v1)

How a connector discovers and joins the bus **without any socket**. The only
rendezvous point is the well-known control segment name.

## Broker startup (`impulsed`)

1. Garbage-collect stale segments: scan `/dev/shm` for `impulse-ring.*` and
   `shm_unlink` them (recovers leaks from a crashed prior run).
2. `shm_open("/impulse-ring.ctl.v1", O_CREAT|O_RDWR)`, size it to hold the
   superblock + submission ring, and format both.
3. Write `version`, `broker_pid`, `epoch`; publish `magic` **last** with a
   Release store so a racing connector only proceeds once the segment is ready.
4. Consume the submission ring forever, replying into each client's reply ring.

On graceful shutdown the broker `shm_unlink`s the control segment and every
arena it created.

## Connector registration

1. `shm_open("/impulse-ring.ctl.v1", O_RDWR)`; `mmap`; validate `magic` and
   `version`. Failure here means the broker is not running.
2. Attach the submission ring at offset 64 as a **producer**.
3. Pick a random `u64` **nonce**. Create your own reply segment
   `/impulse-ring.cli.<nonce>.v1` and format a ring at offset 0; you are its
   sole **consumer**, the broker and remote services are its producers.
4. Start a **dispatcher** that drains the reply ring and routes each frame to
   the waiter registered under the frame's `correlation_id`.
5. Send a `Register` record (carrying `correlation_id`, `app_name`, `nonce`,
   `reply_segment`, `heartbeat_ms`) into the submission ring.
6. The broker opens your reply segment (by the name you sent), assigns a
   `client_id`, and writes `RegisterReply` back. Match it by `correlation_id`.

There is no chicken-and-egg problem: the client creates its reply segment
*before* registering and tells the broker its name, so the broker can always
answer.

## Data plane (peer-to-peer)

The broker is never on the data hot path. After the control handshake:

* **Publish/subscribe:** the broker creates and formats the channel arena and
  returns its name. The publisher attaches as the ring's producer; the
  subscriber attaches as its consumer. Messages flow directly between them. The
  publisher sizes the arena (`PublishChannel.arena_cap`, `0` = default), because
  it is the side that knows how big its frames are; the broker clamps the request
  to `[MIN_ARENA_CAP, MAX_ARENA_CAP]` and rounds it up to a power of two, and both
  ends read what it actually allocated out of the ring header. A publisher that
  is finished with a channel sends `Unpublish` so the name becomes free and the
  arena is reclaimed (see **Liveness & cleanup**); otherwise the channel lives
  as long as the connection that published it.
* **RPC:** `ExposeFunction` creates the function's request arena (service is the
  consumer). One thread reads that arena, because it is single-consumer, and
  hands each request to a pool of workers (`DEFAULT_SERVICE_WORKERS`, lazily
  spawned): a handler that waits — a long poll, a slow store — must not stop the
  other callers of the same function, and a handler that panics answers an error
  instead of ending the service. Responses may therefore leave out of order,
  which costs nothing: a caller matches them by `correlation_id`, and a reply
  ring serialises its producers. A caller `LookupFunction`s to learn the arena
  name and the request/
  response fingerprints, validates compatibility, then writes an `RpcRequest`
  (with its own `reply_segment` and a fresh `correlation_id`) into the request
  arena. The service runs the handler and writes an `RpcResponse` into the
  caller's reply segment; the caller's dispatcher resolves the pending call. The
  caller sizes its own reply segment, so the ceiling on an inline response is a
  property of the ring the two sides share rather than a constant each assumes.
  A caller is expected to resolve a function **once per `(fn_name, access_key)`**
  and keep the answer, rather than look it up per call: a lookup is a control
  round-trip, and the broker serves control messages from a single thread, so a
  caller that resolves per call puts the broker on the hot path of every request
  it makes — and competes there with every other application's control traffic.
  A resolution is dropped when a call to it stops being answered (the callee may
  have restarted onto a fresh arena) and when the broker restarts.

## Liveness & cleanup

* Connectors may send periodic `Heartbeat`s. (Milestone 1 reaping is lenient.)
* A publisher releases a channel with `Unpublish { client_id, channel }`:
  addressed **by name**, because `channel_id` is the broker's and is reassigned
  on every restart, while the name is the publisher's own. The broker honours a
  release only from the channel's owner, and answers nothing — the publisher is
  usually going away as it sends this. A release that is lost costs only what it
  cost before the message existed, since the arena is reclaimed anyway when the
  client departs.

  A channel **nobody has subscribed to yet** is not reclaimed at once but kept
  resolvable for a grace period (30 s in `impulsed`): a subscriber learns the
  channel's name from a response it may still be reading, so a stream small
  enough to be published in full before anyone subscribed would otherwise lose
  that race. Once anybody has subscribed, a release takes effect immediately —
  the subscriber keeps its own mapping of an unlinked arena (POSIX) and drains
  what is left. A name inside its grace period does not refuse a fresh publish
  of the same name.

  Releasing matters most to an application that publishes a channel per
  streamed body, which is what an HTTP-over-Ring layer does for every body too
  large to travel inline: without it, each one leaves an arena behind for the
  lifetime of the process.
* Each connector `shm_unlink`s its own reply segment on disconnect.
* The broker `shm_unlink`s the control segment and all arenas on shutdown; its
  startup scan recovers anything left by a crash.

## Broker restart & reconnect

When `impulsed` restarts it garbage-collects **all** Ring segments (including
every connector's reply segment and arenas) and writes a fresh `epoch` into the
new control superblock (offset 16, the broker start time in nanoseconds). An
already-connected connector is therefore left with a dead submission ring, a
reply segment the new broker never opened, and a `client_id` it never issued —
every subsequent call simply times out.

A connector detects this **without a socket** by re-reading `epoch`:

1. Record `epoch` at attach time.
2. Re-open the control segment **by name** (a fresh `shm_open`; the cached
   mapping still points at the unlinked pre-restart segment) and read `epoch`.
   A different value means the broker restarted; a failed open means it is
   currently down. Connectors do this both proactively (a background watcher
   polls the epoch, so an idle RPC server recovers too) and lazily (a control or
   RPC call that stops being answered triggers the same check).
3. On a confirmed restart, **reconnect**: re-attach the control segment, create a
   new reply segment, re-`Register` under the same `app_name` (obtaining a new
   `client_id`), then **replay** the connection's own registrations — re-publish
   each channel it published and re-`ExposeFunction` each function it exposed —
   rebinding the live publisher/service handles to the new arenas. The failed
   operation is then retried once.

Subscribers are not auto-replayed: a channel's `channel_id` is not stable across
a restart and its publisher lives in another process, so a consumer re-resolves
the channel by name and re-`Subscribe`s. This recovery is the connector's
responsibility (the wire protocol is unchanged); see the native Rust connector
for the reference implementation.
