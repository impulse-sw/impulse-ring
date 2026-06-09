package impulsering

import (
	"time"
)

// Ring header field offsets (relative to base) — see spec/wire-format.md.
const (
	rMagic     = 0
	rCap       = 4
	rHead      = 64
	rProdLock  = 72
	rSpaceSeq  = 76
	rTail      = 128
	rDataSeq   = 136
	ringHeader = 192
	ringMagic  = 0x474E4952 // "RING"
	spinLimit  = 256
	maxWakeAll = 0x7FFFFFFF
)

func ringBytes(capacity int) int { return ringHeader + capacity }

type ring struct {
	data []byte
	base int
	cap  uint64
	mask uint64
}

func ringFormat(data []byte, base, capacity int) *ring {
	r := &ring{data: data, base: base, cap: uint64(capacity), mask: uint64(capacity) - 1}
	u64(data, base+rHead).Store(0)
	u64(data, base+rTail).Store(0)
	u32(data, base+rProdLock).Store(0)
	u32(data, base+rSpaceSeq).Store(0)
	u32(data, base+rDataSeq).Store(0)
	u32(data, base+rCap).Store(uint32(capacity))
	u32(data, base+rMagic).Store(ringMagic) // published last
	return r
}

func ringAttach(data []byte, base int) (*ring, error) {
	if u32(data, base+rMagic).Load() != ringMagic {
		return nil, errf("ring magic mismatch / not formatted")
	}
	capacity := u32(data, base+rCap).Load()
	if capacity == 0 || capacity&(capacity-1) != 0 {
		return nil, errf("ring capacity invalid")
	}
	return &ring{data: data, base: base, cap: uint64(capacity), mask: uint64(capacity) - 1}, nil
}

func (r *ring) dataOff() int { return r.base + ringHeader }

func (r *ring) writeWrapped(pos uint64, src []byte) {
	idx := int(pos & r.mask)
	off := r.dataOff()
	first := int(r.cap) - idx
	if first > len(src) {
		first = len(src)
	}
	copy(r.data[off+idx:], src[:first])
	if first < len(src) {
		copy(r.data[off:], src[first:])
	}
}

func (r *ring) readWrapped(pos uint64, n int) []byte {
	out := make([]byte, n)
	idx := int(pos & r.mask)
	off := r.dataOff()
	first := int(r.cap) - idx
	if first > n {
		first = n
	}
	copy(out, r.data[off+idx:off+idx+first])
	if first < n {
		copy(out[first:], r.data[off:off+(n-first)])
	}
	return out
}

// 3-state futex mutex on the producer side.
func (r *ring) lock() {
	lock := u32(r.data, r.base+rProdLock)
	if lock.CompareAndSwap(0, 1) {
		return
	}
	for lock.Swap(2) != 0 {
		futexWait(r.data, r.base+rProdLock, 2, 50)
	}
}

func (r *ring) unlock() {
	if u32(r.data, r.base+rProdLock).Swap(0) == 2 {
		futexWake(r.data, r.base+rProdLock, 1)
	}
}

// pushLocked returns 0 on success, 1 if full, -1 if the record can never fit.
func (r *ring) pushLocked(rec []byte) int {
	need := uint64(4 + len(rec))
	if need > r.cap {
		return -1
	}
	head := u64(r.data, r.base+rHead).Load()
	tail := u64(r.data, r.base+rTail).Load()
	if r.cap-(head-tail) < need {
		return 1
	}
	var lb [4]byte
	l := uint32(len(rec))
	lb[0], lb[1], lb[2], lb[3] = byte(l), byte(l>>8), byte(l>>16), byte(l>>24)
	r.writeWrapped(head, lb[:])
	r.writeWrapped(head+4, rec)
	u64(r.data, r.base+rHead).Store(head + need)
	return 0
}

func (r *ring) push(rec []byte, timeoutMs int) bool {
	start := time.Now()
	for {
		r.lock()
		rc := r.pushLocked(rec)
		if rc == 0 {
			r.unlock()
			u32(r.data, r.base+rDataSeq).Add(1)
			futexWake(r.data, r.base+rDataSeq, 1)
			return true
		}
		if rc < 0 {
			r.unlock()
			return false
		}
		seen := u32(r.data, r.base+rSpaceSeq).Load()
		r.unlock()
		if timeoutMs >= 0 {
			elapsed := int(time.Since(start).Milliseconds())
			if elapsed >= timeoutMs {
				return false
			}
			futexWait(r.data, r.base+rSpaceSeq, seen, timeoutMs-elapsed)
		} else {
			futexWait(r.data, r.base+rSpaceSeq, seen, 50)
		}
	}
}

func (r *ring) tryPop() []byte {
	tail := u64(r.data, r.base+rTail).Load()
	head := u64(r.data, r.base+rHead).Load()
	if head == tail {
		return nil
	}
	lb := r.readWrapped(tail, 4)
	n := int(lb[0]) | int(lb[1])<<8 | int(lb[2])<<16 | int(lb[3])<<24
	payload := r.readWrapped(tail+4, n)
	u64(r.data, r.base+rTail).Store(tail + 4 + uint64(n))
	u32(r.data, r.base+rSpaceSeq).Add(1)
	futexWake(r.data, r.base+rSpaceSeq, maxWakeAll)
	return payload
}

func (r *ring) popBlocking(timeoutMs int) []byte {
	start := time.Now()
	for {
		for i := 0; i < spinLimit; i++ {
			if v := r.tryPop(); v != nil {
				return v
			}
		}
		seen := u32(r.data, r.base+rDataSeq).Load()
		if v := r.tryPop(); v != nil {
			return v
		}
		if timeoutMs >= 0 {
			elapsed := int(time.Since(start).Milliseconds())
			if elapsed >= timeoutMs {
				return nil
			}
			futexWait(r.data, r.base+rDataSeq, seen, timeoutMs-elapsed)
		} else {
			futexWait(r.data, r.base+rDataSeq, seen, 100)
		}
	}
}
