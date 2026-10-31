# Wire protocol v1

Status: **normative for Phases 1+**. Constants live in `include/feed/protocol.hpp`;
the hex examples below are pinned by unit tests (`tests/test_protocol.cpp`), so
this document and the code cannot silently drift apart.

## 1. Conventions

- **Byte order:** big-endian (network order) for every multi-byte field.
- **Integers:** fixed width, no implicit padding. Prices are *signed 64-bit integer ticks*; no floating point on the wire.
- **One message per datagram/frame.** A frame is exactly one header plus its payload. No batching in v1.
- **Serialization is explicit.** Fields are written/read one at a time (`feed::ByteWriter` / `feed::ByteReader`). Network bytes are never reinterpreted as a C++ struct, so layout, alignment and host endianness cannot cause undefined behaviour.

## 2. Header (16 bytes, all messages)

| Offset | Size | Field         | Notes |
|-------:|-----:|---------------|-------|
| 0      | 1    | `version`     | Must be `1`. |
| 1      | 1    | `msg_type`    | 1 Add, 2 Cancel, 3 Trade, 4 Heartbeat. |
| 2      | 2    | `payload_len` | Bytes following the header. Must equal the fixed size for `msg_type` **and** `frame_len - 16`. |
| 4      | 4    | `stream_id`   | Independent sequence space per stream. |
| 8      | 8    | `seq`         | Per-stream transport sequence number, starts at 1, +1 per message. **Not** an order ID. |

## 3. Payloads

**Add order (type 1), 25 bytes**

| Offset | Size | Field           | Valid range |
|-------:|-----:|-----------------|-------------|
| 0 | 8 | `order_id`      | non-zero |
| 8 | 8 | `price`         | signed ticks, > 0 |
| 16 | 4 | `instrument_id` | any |
| 20 | 4 | `qty`           | >= 1 |
| 24 | 1 | `side`          | 0 Buy, 1 Sell |

**Cancel (type 2), 12 bytes:** `order_id` u64 (non-zero), `instrument_id` u32.

**Trade (type 3), 25 bytes:** `trade_id` u64 (non-zero), `price` i64 (> 0), `instrument_id` u32, `qty` u32 (>= 1), `aggressor` u8 (0 Buy, 1 Sell).

**Heartbeat (type 4), 0 bytes.** Carries a `seq`, which lets a receiver detect loss during quiet periods.

## 4. Hex examples

Add order: stream 1, seq 1, order 1000, price 10050, instrument 7, qty 100, Buy

```
01 01 00 19            version=1 type=Add payload_len=25
00 00 00 01            stream_id=1
00 00 00 00 00 00 00 01  seq=1
00 00 00 00 00 00 03 E8  order_id=1000
00 00 00 00 00 00 27 42  price=10050
00 00 00 07            instrument_id=7
00 00 00 64            qty=100
00                     side=Buy
```

Heartbeat: stream 1, seq 2 (16 bytes, no payload)

```
01 04 00 00 00 00 00 01 00 00 00 00 00 00 00 02
```

## 5. Replay file format ("FDRPLAY1")

A test-only container, so replay is deterministic and needs no sockets.

```
magic[8]   = "FDRPLAY1"
record*    = u64 offset_ns | u16 frame_len | frame[frame_len]     (big-endian)
```

- `offset_ns`: scheduled emission time relative to replay start. Must be non-decreasing; a decrease makes the file invalid.
- `frame_len` is independent of the frame's internal `payload_len`, so a file can carry malformed frames (truncated, oversize, lying `payload_len`) on purpose.
- A file that ends mid-record is rejected (`truncated_record`), never silently accepted.

## 6. Invalid-message behaviour (implemented by `feed::decode`, see `decoder.md`)

The decoder returns an explicit error and **never** crashes, reads out of bounds, or throws on hostile input. One reason per rejected frame, checked in this order:

| Reason | Condition |
|--------|-----------|
| `too_short`        | `frame_len < 16` |
| `too_large`        | `frame_len > 80` (`kMaxMessageSize`) |
| `bad_version`      | `version != 1` |
| `unknown_type`     | `msg_type` not in 1..4 |
| `length_mismatch`  | `payload_len != fixed size for type` or `payload_len != frame_len - 16` |
| `bad_field`        | any range in section 3 violated (`seq == 0` also counts) |

A rejected frame is counted as `Rejected` (see `metrics.md`) and is **not** forwarded. It never reaches the sequencer: its sequence number is untrusted, so the number appears as a gap when the next valid message arrives (`sequencing.md` §2).

## 7. Limits and versioning

- Max payload 64 bytes, max message 80 bytes (`kMaxPayloadSize`, `kMaxMessageSize`); well under a typical 1200-byte safe UDP payload.
- A change to any field layout or meaning bumps `version`. v1 receivers reject other versions rather than guessing.
