"""Wire-protocol constants: control-segment layout, schema fingerprints, and
broker status codes. Fingerprints are taken from SPEC/schemas/FINGERPRINTS.md;
since control schemas are fixed, the frame's fingerprint identifies the message.
"""

CONTROL_NAME = "/impulse-ring.ctl.v1"
CTL_MAGIC = b"IMPRING\x00"
SUBMISSION_BASE = 64
REPLY_CAP = 1 << 16
# Offset of the broker epoch (8 bytes) in the control superblock. It changes on
# every broker run, so a different value on a freshly opened control segment
# means impulsed restarted (see spec/bootstrap.md).
CTL_OFF_EPOCH = 16

# Control / RPC schema fingerprints (CRC-64-AVRO Rabin).
FP_REGISTER = 0x879B416683EF2068
FP_REGISTER_REPLY = 0x3AF224883F4DEC77
FP_UNREGISTER = 0x82CDC2B7EBD36A6A
FP_PUBLISH = 0xF271A4C7A09FCDD7
FP_PUBLISH_REPLY = 0x055439D7C0240142
FP_LIST = 0xA5035910E842F4E6
FP_CHANNEL_LIST = 0xA1048915E5931DA2
FP_SUBSCRIBE = 0x8EBC74E247531CFF
FP_SUBSCRIBE_REPLY = 0x83B6F56EF3D10C31
FP_EXPOSE = 0x642A6C8E5B677E6A
FP_EXPOSE_REPLY = 0x94BB3FE569F8ABF8
FP_LOOKUP = 0xE732B44C32D796FC
FP_LOOKUP_REPLY = 0x5C9C6CBC1F3D26B2
FP_HEARTBEAT = 0xA5073570FC81A3EA
FP_RPC_REQUEST = 0xE88F548E4F540CA4
FP_RPC_RESPONSE = 0x5C4E149239AD5D24

# Broker status codes (proto::status).
ST_OK = 0
ST_NOT_FOUND = 1
ST_DENIED = 2
ST_MISMATCH = 3
ST_INTERNAL = 4
ST_EXISTS = 5
