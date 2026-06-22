package impulsering

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
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

// Connection is a live connection to the Ring broker.
type Connection struct {
	ctl       *segment
	sub       *ring
	replySeg  *segment
	replyRing *ring
	replyName string
	clientID  int64

	mu      sync.Mutex
	pending map[int64]chan reply
	running atomic.Bool
	wg      sync.WaitGroup
}

// Publisher is the producer end of a channel.
type Publisher struct {
	seg      *segment
	r        *ring
	schemaFP uint64
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
	ctl, err := segOpen(controlName)
	if err != nil {
		return nil, fmt.Errorf("cannot open control segment (is impulsed running?): %w", err)
	}
	for i := 0; i < len(ctlMagic); i++ {
		if ctl.data[i] != ctlMagic[i] {
			ctl.close()
			return nil, errf("control magic mismatch")
		}
	}
	sub, err := ringAttach(ctl.data, submissionBase)
	if err != nil {
		ctl.close()
		return nil, err
	}

	nonce := randID()
	replyName := fmt.Sprintf("/impulse-ring.cli.%d.v1", nonce)
	replySeg, err := segCreate(replyName, ringBytes(replyCap))
	if err != nil {
		ctl.close()
		return nil, err
	}
	replyRing := ringFormat(replySeg.data, 0, replyCap)

	c := &Connection{
		ctl:       ctl,
		sub:       sub,
		replySeg:  replySeg,
		replyRing: replyRing,
		replyName: replyName,
		pending:   make(map[int64]chan reply),
	}
	c.running.Store(true)
	c.wg.Add(1)
	go c.dispatch()

	if err := c.register(appName, nonce); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Connection) dispatch() {
	defer c.wg.Done()
	for c.running.Load() {
		rec := c.replyRing.popBlocking(100)
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

func (c *Connection) controlCall(fp uint64, body []byte, corr int64, timeoutMs int) (reply, error) {
	ch := c.slot(corr)
	if !c.sub.push(encodeFrame(fp, body), 2000) {
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

func (c *Connection) register(appName string, nonce int64) error {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutString(appName)
	e.PutLong(nonce)
	e.PutString(c.replyName)
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
	c.clientID = clientID
	return nil
}

// Close unregisters the app, stops background goroutines, and releases segments.
func (c *Connection) Close() {
	e := NewEncoder()
	e.PutLong(randID())
	e.PutLong(c.clientID)
	c.sub.push(encodeFrame(fpUnregister, e.Bytes()), 200)
	c.running.Store(false)
	c.wg.Wait()
	c.replySeg.close()
	c.ctl.close()
}

// PublishChannel publishes a channel with the given Avro schema. key may be ""
// for a public channel.
func (c *Connection) PublishChannel(name, schemaJSON, key string) (*Publisher, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientID)
	e.PutString(name)
	e.PutString(schemaJSON)
	e.PutString(key)
	r, err := c.controlCall(fpPublish, e.Bytes(), corr, 5000)
	if err != nil {
		return nil, err
	}
	d := NewDecoder(r.body)
	d.Long()
	d.Long() // channel_id
	schemaFP := d.Long()
	arena := d.String()
	status := d.Int()
	msg := d.String()
	if status != stOK {
		return nil, errf("publish failed: %s", msg)
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
	return &Publisher{seg: seg, r: rr, schemaFP: uint64(schemaFP)}, nil
}

// Publish sends one Avro-encoded message.
func (p *Publisher) Publish(body []byte) error {
	if !p.r.push(encodeFrame(p.schemaFP, body), 1000) {
		return errf("channel full (slow subscriber)")
	}
	return nil
}

// Close releases the publisher's mapping.
func (p *Publisher) Close() { p.seg.close() }

// ListChannels returns all channels on the bus.
func (c *Connection) ListChannels() ([]ChannelInfo, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientID)
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
}

// Subscribe subscribes to a channel by id. key may be "" for a public channel.
func (c *Connection) Subscribe(channelID int64, key string) (*Subscriber, error) {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientID)
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

// ExposeFunction serves name with handler on a background goroutine.
func (c *Connection) ExposeFunction(name, reqSchema, respSchema, key string, handler Handler) error {
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientID)
	e.PutString(name)
	e.PutString(reqSchema)
	e.PutString(respSchema)
	e.PutString(key)
	r, err := c.controlCall(fpExpose, e.Bytes(), corr, 5000)
	if err != nil {
		return err
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
		return errf("expose failed: %s", msg)
	}
	seg, err := segOpen(reqArena)
	if err != nil {
		return err
	}
	reqRing, err := ringAttach(seg.data, 0)
	if err != nil {
		seg.close()
		return err
	}
	c.wg.Add(1)
	go c.serve(seg, reqRing, reqFP, respFP, handler)
	return nil
}

func (c *Connection) serve(seg *segment, reqRing *ring, reqFP, respFP int64, handler Handler) {
	defer c.wg.Done()
	caller := make(map[string]*segment)
	callerRing := make(map[string]*ring)
	defer func() {
		for _, s := range caller {
			s.close()
		}
		seg.close()
	}()
	for c.running.Load() {
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
		} else if res, herr := handler(args); herr != nil {
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
	corr := randID()
	e := NewEncoder()
	e.PutLong(corr)
	e.PutLong(c.clientID)
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
	req2.PutLong(c.clientID)
	req2.PutString(c.replyName)
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
