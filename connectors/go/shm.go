// Package impulsering is a native Go connector for Ring (by Impulse): a
// socket-free, shared-memory IPC bus with Apache Avro payloads.
//
// It implements the Ring wire protocol from spec/ directly — shared memory,
// ring buffers, futex wakeup, frames, Avro datums and the control plane. It is
// pure Go (no cgo): cross-process atomics use sync/atomic on the mmap'd memory
// and blocking uses the Linux futex syscall.
//
// Linux only (Tier 0: arm64/amd64). Requires the impulsed broker to be running.
// Per project policy, schema fingerprints are computed by the broker; the
// connector sends schema JSON and uses the fingerprints the broker returns.
package impulsering

import (
	"os"
	"syscall"
)

const shmDir = "/dev/shm"

// segment is an mmap'd POSIX shared-memory segment (a file under /dev/shm).
type segment struct {
	name       string
	data       []byte
	ownsUnlink bool
}

func segCreate(name string, size int) (*segment, error) {
	f, err := os.OpenFile(shmDir+name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := f.Truncate(int64(size)); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &segment{name: name, data: data, ownsUnlink: true}, nil
}

func segOpen(name string) (*segment, error) {
	f, err := os.OpenFile(shmDir+name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(st.Size())
	if size == 0 {
		return nil, os.ErrInvalid
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &segment{name: name, data: data, ownsUnlink: false}, nil
}

func (s *segment) close() {
	if s == nil || s.data == nil {
		return
	}
	syscall.Munmap(s.data)
	s.data = nil
	if s.ownsUnlink {
		os.Remove(shmDir + s.name)
	}
}
