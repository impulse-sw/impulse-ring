package impulsering

import (
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	futexWaitOp = 0
	futexWakeOp = 1
)

// u32 / u64 return atomic accessors for a word inside the mmap at byte offset
// off. The offsets used by the ring are naturally aligned and the mmap base is
// page-aligned, so these are safe.
func u32(b []byte, off int) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Pointer(&b[off]))
}

func u64(b []byte, off int) *atomic.Uint64 {
	return (*atomic.Uint64)(unsafe.Pointer(&b[off]))
}

// futexWait blocks while the word at off equals expected, up to timeoutMs
// (negative = no timeout). Uses the shared (non-private) futex since the word
// lives in a cross-process segment.
func futexWait(b []byte, off int, expected uint32, timeoutMs int) {
	addr := unsafe.Pointer(&b[off])
	var tsp unsafe.Pointer
	if timeoutMs >= 0 {
		ts := syscall.NsecToTimespec(int64(timeoutMs) * 1_000_000)
		tsp = unsafe.Pointer(&ts)
	}
	syscall.Syscall6(syscall.SYS_FUTEX, uintptr(addr), futexWaitOp,
		uintptr(expected), uintptr(tsp), 0, 0)
}

// futexWake wakes up to n waiters on the word at off.
func futexWake(b []byte, off, n int) {
	addr := unsafe.Pointer(&b[off])
	syscall.Syscall6(syscall.SYS_FUTEX, uintptr(addr), futexWakeOp,
		uintptr(n), 0, 0, 0)
}
