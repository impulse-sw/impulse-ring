package impulsering

import (
	"encoding/binary"
	"math"
)

// Encoder builds an Apache Avro binary datum (the subset Ring needs).
type Encoder struct{ Buf []byte }

func NewEncoder() *Encoder { return &Encoder{} }

func (e *Encoder) putVarint(zz uint64) {
	for zz&^uint64(0x7f) != 0 {
		e.Buf = append(e.Buf, byte((zz&0x7f)|0x80))
		zz >>= 7
	}
	e.Buf = append(e.Buf, byte(zz))
}

// PutLong writes a zig-zag varint long (Avro int and long share this encoding).
func (e *Encoder) PutLong(v int64) { e.putVarint(uint64((v << 1) ^ (v >> 63))) }

func (e *Encoder) PutInt(v int32) { e.PutLong(int64(v)) }

func (e *Encoder) PutBool(v bool) {
	if v {
		e.Buf = append(e.Buf, 1)
	} else {
		e.Buf = append(e.Buf, 0)
	}
}

func (e *Encoder) PutDouble(v float64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	e.Buf = append(e.Buf, b[:]...)
}

func (e *Encoder) PutBytes(b []byte) {
	e.PutLong(int64(len(b)))
	e.Buf = append(e.Buf, b...)
}

func (e *Encoder) PutString(s string) { e.PutBytes([]byte(s)) }

func (e *Encoder) Bytes() []byte { return e.Buf }

// Decoder reads an Avro binary datum.
type Decoder struct {
	data []byte
	pos  int
}

func NewDecoder(data []byte) *Decoder { return &Decoder{data: data} }

func (d *Decoder) varint() uint64 {
	var result uint64
	var shift uint
	for d.pos < len(d.data) {
		b := d.data[d.pos]
		d.pos++
		result |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result
}

func (d *Decoder) Long() int64 {
	n := d.varint()
	return int64(n>>1) ^ -int64(n&1)
}

func (d *Decoder) Int() int32 { return int32(d.Long()) }

func (d *Decoder) Bool() bool {
	if d.pos >= len(d.data) {
		return false
	}
	b := d.data[d.pos]
	d.pos++
	return b != 0
}

func (d *Decoder) Double() float64 {
	if d.pos+8 > len(d.data) {
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(d.data[d.pos:]))
	d.pos += 8
	return v
}

func (d *Decoder) Bytes() []byte {
	n := int(d.Long())
	if n < 0 || d.pos+n > len(d.data) {
		n = 0
	}
	out := make([]byte, n)
	copy(out, d.data[d.pos:d.pos+n])
	d.pos += n
	return out
}

func (d *Decoder) String() string { return string(d.Bytes()) }

// ArrayCount reads a block count; 0 ends the array.
func (d *Decoder) ArrayCount() int64 { return d.Long() }

// peekLong reads the first Avro long of a body without consuming a Decoder.
func peekLong(body []byte) int64 { return NewDecoder(body).Long() }
