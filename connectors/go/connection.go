package impulsering

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ChannelInfo describes a channel returned by ListChannels.
type ChannelInfo struct {
	ChannelID   int64
	Name        string
	OwnerApp    string
	SchemaFP    uint64
	RequiresKey bool
}

// Handler serves an exposed function: it decodes the Avro request body and
// returns the Avro response body (or an error to signal a remote failure).
type Handler func(req []byte) ([]byte, error)

type reply struct {
	fp   uint64
	body []byte
}

// chanReg is a published channel, tracked so it can be replayed (re-published,
// ring rebound) after a broker restart. The Publisher reads through reg.mu.
type chanReg struct {
	mu         sync.Mutex
	name       string
	schemaJSON string
	key        string
	seg        *segment
	r          *ring
	schemaFP   uint64
}

// svcReg is an exposed function, tracked so it can be replayed after a restart.
// The serve goroutine reads the request ring through reg.mu.
type svcReg struct {
	mu         sync.Mutex
	seg        *segment
	reqRing    *ring
	reqFP      int64
	respFP     int64
	name       string
	reqSchema  string
	respSchema string
	key        string
	arenaCap   int64
	handler    Handler
}

// Connection is a live connection to the Ring broker.
type Connection struct {
	appName string

	// Transport (current broker generation), guarded by txMu.
	txMu      sync.RWMutex
	ctl       *segment
	sub       *ring
	replySeg  *segment
	replyRing *ring
	replyName string
	clientID  int64
	nonce     int64
	epoch     int64

	mu      sync.Mutex
	pending map[int64]chan reply
	running atomic.Bool

	autoReconnect atomic.Bool
	reconnectMu   sync.Mutex

	// Registrations to replay after a reconnect, guarded by regMu.
	regMu    sync.Mutex
	channels []*chanReg
	services []*svcReg
	retired  []*segment // segments kept mapped until Close (see reconnect)

	wg sync.WaitGroup
}

// Publisher is the producer end of a channel.
type Publisher struct {
	c   *Connection
	reg *chanReg
}

// Subscriber is the consumer end of a channel.
type Subscriber struct {
	seg      *segment
	r        *ring
	SchemaFP uint64
}

func randID() int64 {
	var b [8]byte
	rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]) >> 1) // positive 63-bit
}

// Connect attaches to the broker and registers appName on the bus.
func Connect(appName string) (*Connection, error) {
	c := &Connection{appName: appName, pending: make(map[int64]chan reply)}
	c.running.Store(true)
	c.autoReconnect.Store(true)

	if err := c.bootstrap(); err != nil {
		return nil, err
	}
	c.wg.Add(2)
	go c.dispatch()
	go c.watch()

	if err := c.doRegister(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// bootstrap opens a fresh control segment + a new reply segment and installs
// them as the transport (clientID reset to 0). The previous control/reply
// segments are retired (kept mapped) so background goroutines that copied a ring
// pointer out of them stay valid until Close.
func (c *Connection) bootstrap() error {
	ctl, err := segOpen(controlName)
	if err != nil {
		return fmt.Errorf("cannot open control segment (is impulsed running?): %w", err)
	}
	for i := 0; i < len(ctlMagic); i++ {
		if ctl.data[i] != ctlMagic[i] {
			ctl.close()
			return errf("control magic mismatch")
		}
	}
	sub, err := ringAttach(ctl.data, submissionBase)
	if err != nil {
		ctl.close()
		return err
	}
	epoch := int64(binary.LittleEndian.Uint64(ctl.data[ctlEpochOff : ctlEpochOff+8]))

	nonce := randID()
	replyName := fmt.Sprintf("/impulse-ring.cli.%d.v1", nonce)
	replySeg, err := segCreate(replyName, ringBytes(replyCap))
	if err != nil {
		ctl.close()
		return err
	}
	replyRing := ringFormat(replySeg.data, 0, replyCap)

	c.txMu.Lock()
	if c.ctl != nil {
		c.retired = append(c.retired, c.ctl)
	}
	if c.replySeg != nil {
		c.retired = append(c.retired, c.replySeg)
	}
	c.ctl = ctl
	c.sub = sub
	c.replySeg = replySeg
	c.replyRing = replyRing
	c.replyName = replyName
	c.nonce = nonce
	c.epoch = epoch
	c.clientID = 0
	c.txMu.Unlock()
	return nil
}

func (c *Connection) dispatch() {
	defer c.wg.Done()
	for c.running.Load() {
		// Re-read the reply ring each tick so a reconnect's swap is picked up.
		c.txMu.RLock()
		replyRing := c.replyRing
		c.txMu.RUnlock()
		rec := replyRing.popBlocking(100)
		if rec == nil {
			continue
		}
		fp, body, err := decodeFrame(rec)
		if err != nil {
			continue
		}
		corr := peekLong(body)
		c.mu.Lock()
		ch := c.pending[corr]
		delete(c.pending, corr)
		c.mu.Unlock()
		if ch != nil {
			ch <- reply{fp: fp, body: body}
		}
	}
}

// watch proactively detects a broker restart by polling the live control-segment
// epoch, so an idle connection (notably a pure RPC server) recovers without a
// failed call to trigger it.
func (c *Connection) watch() {
	defer c.wg.Done()
	for c.running.Load() {
		for i := 0; i < 25 && c.running.Load(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if !c.running.Load() || !c.autoReconnect.Load() {
			continue
		}
		c.txMu.RLock()
		observed := c.epoch
		c.txMu.RUnlock()
		if live, err := liveBrokerEpoch(); err == nil && live != observed {
			c.reconnect(observed)
		}
	}
}

func (c *Connection) slot(corr int64) chan reply {
	ch := make(chan reply, 1)
	c.mu.Lock()
	c.pending[corr] = ch
	c.mu.Unlock()
	return ch
}

func (c *Connection) drop(corr int64) {
	c.mu.Lock()
	delete(c.pending, corr)
	c.mu.Unlock()
}

func (c *Connection) clientId() int64 {
	c.txMu.RLock()
	defer c.txMu.RUnlock()
	return c.clientID
}

func (c *Connection) replyNameTx() string {
	c.txMu.RLock()
	defer c.txMu.RUnlock()
	return c.replyName
}

func (c *Connection) epochTx() int64 {
	c.txMu.RLock()
	defer c.txMu.RUnlock()
	return c.epoch
}

// submit pushes a framed control message onto the current submission ring.
func (c *Connection) submit(frame []byte, timeoutMs int) bool {
	c.txMu.RLock()
	sub := c.sub
	c.txMu.RUnlock()
	return sub.push(frame, timeoutMs)
}

func (c *Connection) controlCall(fp uint64, body []byte, corr int64, timeoutMs int) (reply, error) {
	ch := c.slot(corr)
	if !c.submit(encodeFrame(fp, body), 2000) {
		c.drop(corr)
		return reply{}, errf("submission ring full (broker stuck?)")
	}
	select {
	case r := <-ch:
		return r, nil
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		c.drop(corr)
		return reply{}, errf("timed out waiting for broker reply")
	}
}

// liveBrokerEpoch opens the control segment freshly and reads the live broker
// epoch (the cached mapping still points at the unlinked pre-restart segment).
func liveBrokerEpoch() (int64, error) {
	ctl, err := segOpen(controlName)
	if err != nil {
		return 0, err
	}
	defer ctl.close()
	for i := 0; i < len(ctlMagic); i++ {
		if ctl.data[i] != ctlMagic[i] {
			return 0, errf("control magic mismatch")
		}
	}
	return int64(binary.LittleEndian.Uint64(ctl.data[ctlEpochOff : ctlEpochOff+8])), nil
}

// brokerUnreachable reports whether err means the broker stopped answering.
func brokerUnreachable(err error) bool {
	s := err.Error()
	return strings.Contains(s, "timed out") || strings.Contains(s, "submission ring full")
}

// withReconnect runs op; if it fails because the broker stopped answering and the
// live epoch has actually changed, it reconnects and retries op once.
func withReconnect[T any](c *Connection, op func() (T, error)) (T, error) {
	v, err := op()
	if err == nil {
		return v, nil
	}
	if !c.autoReconnect.Load() || !brokerUnreachable(err) {
		return v, err
	}
	observed := c.epochTx()
	live, lerr := liveBrokerEpoch()
	if lerr != nil || live == observed {
		return v, err
	}
	if rerr := c.reconnect(observed); rerr != nil {
		return v, err
	}
	return op()
}

func (c *Connection) doRegister() error {
	corr := randID()
	c.txMu.RLock()
	nonce := c.nonce
	replyName := c.replyName
	c.txMu.RUnlock()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutString(c.appName)
	e.PutLong(nonce)
	e.PutString(replyName)
	e.PutLong(1000)
	e.PutLong(int64(os.Getpid())) // pid: lets the broker reclaim our names if we die without unregistering
	r, err := c.controlCall(fpRegister, e.Bytes(), corr, 5000)
	if err != nil {
		return err
	}
	d := NewDecoder(r.body)
	d.Long()
	clientID := d.Long()
	status := d.Int()
	msg := d.String()
	if status != stOK {
		return errf("register rejected: %s", msg)
	}
	c.txMu.Lock()
	c.clientID = clientID
	c.txMu.Unlock()
	return nil
}

// reconnect re-bootstraps onto a restarted broker and replays every owned
// channel/function. Single-flight: a caller that lost the race observes the
// bumped epoch and returns without doing anything.
func (c *Connection) reconnect(observed int64) error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if c.epochTx() != observed {
		return nil // someone else reconnected
	}
	if err := c.bootstrap(); err != nil {
		return err
	}
	// Detach pending slots bound to the dead broker; their waiters will time out.
	c.mu.Lock()
	c.pending = make(map[int64]chan reply)
	c.mu.Unlock()
	if err := c.doRegister(); err != nil {
		return err
	}
	c.replay()
	return nil
}

// replay re-establishes every owned channel and function on the fresh broker.
func (c *Connection) replay() {
	c.regMu.Lock()
	chans := append([]*chanReg(nil), c.channels...)
	svcs := append([]*svcReg(nil), c.services...)
	c.regMu.Unlock()

	for _, cr := range chans {
		seg, rr, fp, err := c.doPublish(cr.name, cr.schemaJSON, cr.key)
		if err != nil {
			continue
		}
		cr.mu.Lock()
		old := cr.seg
		cr.seg, cr.r, cr.schemaFP = seg, rr, fp
		cr.mu.Unlock()
		c.retire(old)
	}
	for _, sv := range svcs {
		seg, rr, reqFP, respFP, err := c.doExpose(sv.name, sv.reqSchema, sv.respSchema, sv.key, sv.arenaCap)
		if err != nil {
			continue
		}
		sv.mu.Lock()
		old := sv.seg
		sv.seg, sv.reqRing, sv.reqFP, sv.respFP = seg, rr, reqFP, respFP
		sv.mu.Unlock()
		c.retire(old)
	}
}

func (c *Connection) retire(seg *segment) {
	if seg == nil {
		return
	}
	c.txMu.Lock()
	c.retired = append(c.retired, seg)
	c.txMu.Unlock()
}

// Close unregisters the app, stops background goroutines, and releases segments.
func (c *Connection) Close() {
	e := NewEncoder()
	e.PutLong(randID())
	e.PutLong(c.clientId())
	c.submit(encodeFrame(fpUnregister, e.Bytes()), 200)
	c.running.Store(false)
	c.wg.Wait()
	// Safe now that all goroutines have stopped reading any ring.
	for _, s := range c.retired {
		s.close()
	}
	c.regMu.Lock()
	for _, cr := range c.channels {
		if cr.seg != nil {
			cr.seg.close()
		}
	}
	c.regMu.Unlock()
	c.replySeg.close()
	c.ctl.close()
}

// doPublish performs the PublishChannel round-trip and attaches the arena ring.
func (c *Connection) doPublish(name, schemaJSON, key string) (*segment, *ring, uint64, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientId())
	e.PutString(name)
	e.PutString(schemaJSON)
	e.PutString(key)
	r, err := c.controlCall(fpPublish, e.Bytes(), corr, 5000)
	if err != nil {
		return nil, nil, 0, err
	}
	d := NewDecoder(r.body)
	d.Long()
	d.Long() // channel_id
	schemaFP := d.Long()
	arena := d.String()
	status := d.Int()
	msg := d.String()
	if status != stOK {
		return nil, nil, 0, errf("publish failed: %s", msg)
	}
	seg, err := segOpen(arena)
	if err != nil {
		return nil, nil, 0, err
	}
	rr, err := ringAttach(seg.data, 0)
	if err != nil {
		seg.close()
		return nil, nil, 0, err
	}
	return seg, rr, uint64(schemaFP), nil
}

// PublishChannel publishes a channel with the given Avro schema. key may be ""
// for a public channel.
func (c *Connection) PublishChannel(name, schemaJSON, key string) (*Publisher, error) {
	reg, err := withReconnect(c, func() (*chanReg, error) {
		seg, rr, fp, e := c.doPublish(name, schemaJSON, key)
		if e != nil {
			return nil, e
		}
		return &chanReg{name: name, schemaJSON: schemaJSON, key: key, seg: seg, r: rr, schemaFP: fp}, nil
	})
	if err != nil {
		return nil, err
	}
	c.regMu.Lock()
	c.channels = append(c.channels, reg)
	c.regMu.Unlock()
	return &Publisher{c: c, reg: reg}, nil
}

// Publish sends one Avro-encoded message.
func (p *Publisher) Publish(body []byte) error {
	p.reg.mu.Lock()
	r := p.reg.r
	fp := p.reg.schemaFP
	p.reg.mu.Unlock()
	if !r.push(encodeFrame(fp, body), 1000) {
		return errf("channel full (slow subscriber)")
	}
	return nil
}

// Close releases the publisher's mapping and stops it being replayed.
func (p *Publisher) Close() {
	c := p.c
	c.regMu.Lock()
	for i, cr := range c.channels {
		if cr == p.reg {
			c.channels = append(c.channels[:i], c.channels[i+1:]...)
			break
		}
	}
	c.regMu.Unlock()
	p.reg.mu.Lock()
	seg := p.reg.seg
	p.reg.seg = nil
	p.reg.mu.Unlock()
	if seg != nil {
		seg.close()
	}
}

// ListChannels returns all channels on the bus.
func (c *Connection) ListChannels() ([]ChannelInfo, error) {
	return withReconnect(c, func() ([]ChannelInfo, error) {
		corr := randID()
		e := NewEncoder()
		e.PutLong(corr)
		e.PutLong(c.clientId())
		r, err := c.controlCall(fpList, e.Bytes(), corr, 5000)
		if err != nil {
			return nil, err
		}
		d := NewDecoder(r.body)
		d.Long()
		var out []ChannelInfo
		for {
			count := d.ArrayCount()
			if count == 0 {
				break
			}
			if count < 0 {
				count = -count
				d.Long() // block byte-size (unused)
			}
			for i := int64(0); i < count; i++ {
				out = append(out, ChannelInfo{
					ChannelID:   d.Long(),
					Name:        d.String(),
					OwnerApp:    d.String(),
					SchemaFP:    uint64(d.Long()),
					RequiresKey: d.Bool(),
				})
			}
		}
		return out, nil
	})
}

// Subscribe subscribes to a channel by id. key may be "" for a public channel.
//
// A subscriber is not auto-replayed across a broker restart (a channel's id
// changes and its publisher lives in another process); re-subscribe by resolving
// the channel by name again.
func (c *Connection) Subscribe(channelID int64, key string) (*Subscriber, error) {
	return withReconnect(c, func() (*Subscriber, error) {
		corr := randID()
		e := NewEncoder()
		e.PutLong(corr)
		e.PutLong(c.clientId())
		e.PutLong(channelID)
		e.PutString(key)
		e.PutLong(0) // expected_fp = 0: broker owns fingerprints
		r, err := c.controlCall(fpSubscribe, e.Bytes(), corr, 5000)
		if err != nil {
			return nil, err
		}
		d := NewDecoder(r.body)
		d.Long()
		arena := d.String()
		schemaFP := d.Long()
		status := d.Int()
		msg := d.String()
		if status != stOK {
			return nil, errf("subscribe failed: %s", msg)
		}
		seg, err := segOpen(arena)
		if err != nil {
			return nil, err
		}
		rr, err := ringAttach(seg.data, 0)
		if err != nil {
			seg.close()
			return nil, err
		}
		return &Subscriber{seg: seg, r: rr, SchemaFP: uint64(schemaFP)}, nil
	})
}

// Recv returns the next message body, or nil if none arrives within timeoutMs.
func (s *Subscriber) Recv(timeoutMs int) ([]byte, error) {
	rec := s.r.popBlocking(timeoutMs)
	if rec == nil {
		return nil, nil
	}
	fp, body, err := decodeFrame(rec)
	if err != nil {
		return nil, err
	}
	if fp != s.SchemaFP {
		return nil, errf("message schema mismatch: %#x != %#x", fp, s.SchemaFP)
	}
	return append([]byte(nil), body...), nil
}

// Close releases the subscriber's mapping.
func (s *Subscriber) Close() { s.seg.close() }

// ExposeFunction serves name with handler on a background goroutine, using the
// broker's default request-arena size. See ExposeFunctionWithArena to size it.
func (c *Connection) ExposeFunction(name, reqSchema, respSchema, key string, handler Handler) error {
	return c.ExposeFunctionWithArena(name, reqSchema, respSchema, key, 0, handler)
}

// doExpose performs the ExposeFunction round-trip and attaches the request arena.
func (c *Connection) doExpose(name, reqSchema, respSchema, key string, reqArenaCap int64) (*segment, *ring, int64, int64, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientId())
	e.PutString(name)
	e.PutString(reqSchema)
	e.PutString(respSchema)
	e.PutString(key)
	e.PutLong(reqArenaCap)
	r, err := c.controlCall(fpExpose, e.Bytes(), corr, 5000)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	d := NewDecoder(r.body)
	d.Long()
	d.Long() // fn_id
	reqFP := d.Long()
	respFP := d.Long()
	reqArena := d.String()
	status := d.Int()
	msg := d.String()
	if status != stOK {
		return nil, nil, 0, 0, errf("expose failed: %s", msg)
	}
	seg, err := segOpen(reqArena)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	reqRing, err := ringAttach(seg.data, 0)
	if err != nil {
		seg.close()
		return nil, nil, 0, 0, err
	}
	return seg, reqRing, reqFP, respFP, nil
}

// ExposeFunctionWithArena serves name with handler, requesting a request-arena
// capacity of reqArenaCap bytes (0 = broker default). The broker clamps the
// value to [256 KiB, 128 MiB] and rounds it up to a power of two.
func (c *Connection) ExposeFunctionWithArena(name, reqSchema, respSchema, key string, reqArenaCap int64, handler Handler) error {
	sv, err := withReconnect(c, func() (*svcReg, error) {
		seg, rr, reqFP, respFP, e := c.doExpose(name, reqSchema, respSchema, key, reqArenaCap)
		if e != nil {
			return nil, e
		}
		return &svcReg{
			seg: seg, reqRing: rr, reqFP: reqFP, respFP: respFP,
			name: name, reqSchema: reqSchema, respSchema: respSchema, key: key,
			arenaCap: reqArenaCap, handler: handler,
		}, nil
	})
	if err != nil {
		return err
	}
	c.regMu.Lock()
	c.services = append(c.services, sv)
	c.regMu.Unlock()
	c.wg.Add(1)
	go c.serve(sv)
	return nil
}

func (c *Connection) serve(sv *svcReg) {
	defer c.wg.Done()
	caller := make(map[string]*segment)
	callerRing := make(map[string]*ring)
	defer func() {
		for _, s := range caller {
			s.close()
		}
	}()
	for c.running.Load() {
		// Snapshot the request ring + fingerprints under the lock so a reconnect's
		// re-expose (which swaps the arena) is picked up on the next iteration.
		sv.mu.Lock()
		reqRing := sv.reqRing
		reqFP := sv.reqFP
		respFP := sv.respFP
		sv.mu.Unlock()
		rec := reqRing.popBlocking(100)
		if rec == nil {
			continue
		}
		_, body, err := decodeFrame(rec)
		if err != nil {
			continue
		}
		d := NewDecoder(body)
		corr := d.Long()
		d.Long() // caller_id
		replySeg := d.String()
		argFP := d.Long()
		args := d.Bytes()

		status := stOK
		var result []byte
		if argFP != reqFP {
			status = stMismatch
		} else if res, herr := sv.handler(args); herr != nil {
			status = stInternal
		} else {
			result = res
		}

		e := NewEncoder()
		e.PutLong(corr)
		e.PutInt(int32(status))
		e.PutString("")
		if status == stOK {
			e.PutLong(respFP)
		} else {
			e.PutLong(0)
		}
		e.PutBytes(result)

		rr := callerRing[replySeg]
		if rr == nil {
			cs, oerr := segOpen(replySeg)
			if oerr != nil {
				continue
			}
			cr, aerr := ringAttach(cs.data, 0)
			if aerr != nil {
				cs.close()
				continue
			}
			caller[replySeg] = cs
			callerRing[replySeg] = cr
			rr = cr
		}
		rr.push(encodeFrame(fpRPCResponse, e.Bytes()), 2000)
	}
}

// Call invokes a remote function and blocks for the response. key may be "".
func (c *Connection) Call(fnName, key string, req []byte, timeoutMs int) ([]byte, error) {
	return withReconnect(c, func() ([]byte, error) {
		return c.callOnce(fnName, key, req, timeoutMs)
	})
}

func (c *Connection) callOnce(fnName, key string, req []byte, timeoutMs int) ([]byte, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientId())
	e.PutString(fnName)
	e.PutString(key)
	r, err := c.controlCall(fpLookup, e.Bytes(), corr, 5000)
	if err != nil {
		return nil, err
	}
	d := NewDecoder(r.body)
	d.Long()
	d.Long() // fn_id
	reqFP := d.Long()
	respFP := d.Long()
	reqArena := d.String()
	status := d.Int()
	msg := d.String()
	if status != stOK {
		return nil, errf("lookup failed: %s", msg)
	}

	seg, err := segOpen(reqArena)
	if err != nil {
		return nil, err
	}
	defer seg.close()
	fnRing, err := ringAttach(seg.data, 0)
	if err != nil {
		return nil, err
	}

	rpcCorr := randID()
	ch := c.slot(rpcCorr)
	req2 := NewEncoder()
	req2.PutLong(rpcCorr)
	req2.PutLong(c.clientId())
	req2.PutString(c.replyNameTx())
	req2.PutLong(reqFP) // arg_fp = broker-derived request fingerprint
	req2.PutBytes(req)
	if !fnRing.push(encodeFrame(fpRPCRequest, req2.Bytes()), 2000) {
		c.drop(rpcCorr)
		return nil, errf("function request ring full")
	}

	select {
	case rr := <-ch:
		dd := NewDecoder(rr.body)
		dd.Long()
		rstatus := dd.Int()
		rmsg := dd.String()
		resultFP := dd.Long()
		result := dd.Bytes()
		if rstatus != stOK {
			return nil, errf("remote error: %s", rmsg)
		}
		if resultFP != respFP {
			return nil, errf("response schema mismatch")
		}
		return result, nil
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		c.drop(rpcCorr)
		return nil, errf("rpc call timed out")
	}
}

// BrokerEpoch is the broker epoch this connection is attached under. It changes
// whenever impulsed restarts; after a reconnect it tracks the new broker.
func (c *Connection) BrokerEpoch() int64 { return c.epochTx() }

// BrokerRestarted reports whether the live broker epoch differs from the one
// this connection attached under (i.e. impulsed restarted), or the broker is
// currently unreachable.
func (c *Connection) BrokerRestarted() bool {
	live, err := liveBrokerEpoch()
	if err != nil {
		return true
	}
	return live != c.epochTx()
}

// SetAutoReconnect enables or disables transparent reconnect on a detected
// broker restart (default: enabled).
func (c *Connection) SetAutoReconnect(on bool) { c.autoReconnect.Store(on) }
