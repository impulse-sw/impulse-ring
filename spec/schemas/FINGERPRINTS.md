# Control schema fingerprints (CRC-64-AVRO Rabin)

| Message | Fingerprint (u64, hex) |
|---------|------------------------|
| `Register` | `0x879b416683ef2068` |
| `RegisterReply` | `0x3af224883f4dec77` |
| `Unregister` | `0x82cdc2b7ebd36a6a` |
| `PublishChannel` | `0xf271a4c7a09fcdd7` |
| `PublishReply` | `0x055439d7c0240142` |
| `ListChannels` | `0xa5035910e842f4e6` |
| `ChannelList` | `0xa1048915e5931da2` |
| `Subscribe` | `0x8ebc74e247531cff` |
| `SubscribeReply` | `0x83b6f56ef3d10c31` |
| `ExposeFunction` | `0x642a6c8e5b677e6a` |
| `ExposeReply` | `0x94bb3fe569f8abf8` |
| `LookupFunction` | `0xe732b44c32d796fc` |
| `LookupReply` | `0x5c9c6cbc1f3d26b2` |
| `Heartbeat` | `0xa5073570fc81a3ea` |
| `RpcRequest` | `0xe88f548e4f540ca4` |
| `RpcResponse` | `0x5c4e149239ad5d24` |

## Legacy fingerprints (accepted for backward compatibility)

The broker still recognizes these superseded fingerprints and decodes such
frames with their original schema (newer fields default), so a newer broker can
serve not-yet-upgraded connectors.

| Message | Legacy fingerprint | Superseded by |
|---------|--------------------|---------------|
| `Register` (pre-`pid`) | `0xfc723bd7afcffc02` | added `pid` (`long`, default `0`) |
| `ExposeFunction` (pre-`req_arena_cap`) | `0xa1acec8abc87f374` | added `req_arena_cap` (`long`, default `0`) |
