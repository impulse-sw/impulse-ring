package impulsering

import "fmt"

// Control-segment + protocol constants (see spec/wire-format.md and
// spec/schemas/FINGERPRINTS.md). Control schemas are fixed, so a frame's
// fingerprint identifies the message type.
const (
	controlName    = "/impulse-ring.ctl.v1"
	submissionBase = 64
	replyCap       = 1 << 16
)

var ctlMagic = []byte("IMPRING\x00")

const (
	fpRegister      = 0xFC723BD7AFCFFC02
	fpRegisterReply = 0x3AF224883F4DEC77
	fpUnregister    = 0x82CDC2B7EBD36A6A
	fpPublish       = 0xF271A4C7A09FCDD7
	fpPublishReply  = 0x055439D7C0240142
	fpList          = 0xA5035910E842F4E6
	fpChannelList   = 0xA1048915E5931DA2
	fpSubscribe     = 0x8EBC74E247531CFF
	fpSubscribeRply = 0x83B6F56EF3D10C31
	fpExpose        = 0xA1ACEC8ABC87F374
	fpExposeReply   = 0x94BB3FE569F8ABF8
	fpLookup        = 0xE732B44C32D796FC
	fpLookupReply   = 0x5C9C6CBC1F3D26B2
	fpHeartbeat     = 0xA5073570FC81A3EA
	fpRPCRequest    = 0xE88F548E4F540CA4
	fpRPCResponse   = 0x5C4E149239AD5D24
)

// Broker status codes (proto::status).
const (
	stOK       = 0
	stNotFound = 1
	stDenied   = 2
	stMismatch = 3
	stInternal = 4
	stExists   = 5
)

func errf(format string, a ...any) error { return fmt.Errorf(format, a...) }
