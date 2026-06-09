package impulsering

import "encoding/binary"

const (
	frameMagic  = 0x5249 // "IR"
	wireVersion = 1
	frameHeader = 16
)

func encodeFrame(schemaFP uint64, body []byte) []byte {
	out := make([]byte, frameHeader+len(body))
	binary.LittleEndian.PutUint16(out[0:], frameMagic)
	out[2] = wireVersion
	out[3] = 0
	binary.LittleEndian.PutUint64(out[4:], schemaFP)
	binary.LittleEndian.PutUint32(out[12:], uint32(len(body)))
	copy(out[frameHeader:], body)
	return out
}

func decodeFrame(buf []byte) (schemaFP uint64, body []byte, err error) {
	if len(buf) < frameHeader {
		return 0, nil, errf("frame shorter than header")
	}
	if binary.LittleEndian.Uint16(buf[0:]) != frameMagic {
		return 0, nil, errf("bad frame magic")
	}
	if buf[2] != wireVersion {
		return 0, nil, errf("unsupported wire version %d", buf[2])
	}
	schemaFP = binary.LittleEndian.Uint64(buf[4:])
	bodyLen := int(binary.LittleEndian.Uint32(buf[12:]))
	if frameHeader+bodyLen > len(buf) {
		return 0, nil, errf("frame body truncated")
	}
	return schemaFP, buf[frameHeader : frameHeader+bodyLen], nil
}
