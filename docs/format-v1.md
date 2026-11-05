# On-disk format v1 (volume files)

**Status: APPROVED 2026-11-04 (decisions in §11 confirmed). Permanent once released**: any change is a new format version, and every version stays readable forever. Rationale lives in `design-notes.md` §7, §10, §11; this file is the normative definition. Sections marked **[REVIEW]** are choices the design notes left open.

This document is also written, verbatim, onto every disk so a rescued disk can be read without the software.

## 1. Conventions

- All integers are **little-endian** and unsigned unless stated. **[REVIEW]** (the shard function's big-endian u64 is a hash-derivation rule, not a stored field.)
- **CRC** = CRC-32C (Castagnoli), as in RFC 3720: reflected, init `0xFFFFFFFF`, final XOR `0xFFFFFFFF`; stored as u32.
- **SHA-256** = FIPS 180-4.
- All records and the header region are **8-byte aligned**; padding bytes are zero and not covered by any CRC.
- "Region as stored" means the bytes on disk: ciphertext when encrypted.
- Limits: bucket name ≤ 128 bytes, file name ≤ 512 bytes, metadata ≤ 16 KiB, payload ≤ 100 MiB, volume file ≤ 32 GiB. Limits may be raised in a later version, never lowered.

## 2. Volume file

```
+----------------------------+
| prelude (16 bytes)         |  never changes across versions
| volume header              |  header_len bytes, ends with its CRC
| record 0                   |  8-byte aligned
| record 1                   |
| ...                        |
| seal record (sealed only)  |  last record; file ends right after it
+----------------------------+
```

File name (informational, not relied upon): `<volume-id as 16 lower-case hex digits>.vol`. Identity always comes from the header.

## 3. Prelude (16 bytes, fixed forever)

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 8 | magic | `89 53 34 56 4F 4C 0D 0A` (`\x89 S4VOL \r\n`; the high bit and CR LF catch text-mode mangling) |
| 8 | 2 | format_major | `1` |
| 10 | 2 | format_minor | `0` |
| 12 | 4 | header_len | bytes after the prelude up to the first record; multiple of 8 |

- Readers **refuse an unknown `format_major`**. A higher `format_minor` of a known major is readable: skip fields you do not know using `header_len`.
- Anything that identifies or locates a file's version must only ever depend on these 16 bytes.

## 4. Volume header

Starts at offset 16. Fixed part (64 bytes):

| Offset (from file start) | Size | Field | Notes |
|---|---|---|---|
| 16 | 8 | volume_id | `node_id(16) \| node-local counter(48)` |
| 24 | 16 | cluster_id | opaque 16 bytes (UUID) |
| 40 | 4 | shard | `0 ≤ shard < shard_count` |
| 44 | 4 | shard_count (S) | fixed per bucket |
| 48 | 8 | ordinal | per-(bucket, shard) volume ordinal from the catalogue |
| 56 | 8 | created_unix_nano | i64, informational only; never used for ordering |
| 64 | 4 | epoch | epoch at creation (see seal record for the final epoch) |
| 68 | 4 | key_generation | 0 when unencrypted |
| 72 | 2 | cipher_id | 0 = none; 1 = AES-256-GCM (reserved, see §9); others: readers refuse |
| 74 | 2 | tier | 0 in the MVP |
| 76 | 2 | bucket_len | 1..128 |
| 78 | 2 | reserved | zero |
| 80 | bucket_len | bucket | bucket name bytes (see name rules in `api.md`) |

Then zero padding, then `header_CRC` (u32) as the **last 4 bytes** of the header region. `header_len = roundup8(64 + bucket_len + 4)`. `header_CRC` covers the prelude and every header byte before the CRC field (including padding). Fields added by later minor versions go **after the bucket name**, before the padding and CRC.

A header with a bad CRC, `shard ≥ shard_count`, a zero `shard_count`, or an invalid bucket name makes the volume unreadable (report, do not guess).

## 5. Record

A record has a fixed header, two regions A and B, and two CRCs:

```
 offset  size
 0       4   magic          "S4R1" = 53 34 52 31
 4       1   type
 5       1   flags          must be 0 in v1
 6       2   name_len       bytes
 8       4   seq            ≥ 1; 0 is never a valid seq
 12      4   check          random per record
 16      4   meta_len       bytes; 0 = no metadata
 20      4   payload_len    bytes (plaintext length)
 24      A   region A       = name ‖ sha256 (32) ‖ metadata ‖ tag_A
 24+A    4   header_CRC     CRC-32C over bytes [0, 24+A) as stored
 28+A    B   region B       = payload ‖ tag_B
 28+A+B  4   payload_CRC    CRC-32C over region B as stored
 ...     0-7 zero padding to a multiple of 8
```

- `A = name_len + 32 + meta_len + tag_A` and `B = payload_len + tag_B`. The tag sizes are `0` for cipher 0 and `16` for cipher 1, taken from the volume header. The length fields are **plaintext** lengths.
- Total record length = `roundup8(32 + A + B)`. **A record's length never changes after it is written.**
- `sha256` is the SHA-256 of the plaintext payload (the content identity; the HTTP ETag). It is always present in region A; for types other than `file` it is 32 zero bytes.
- `name` is the file name bytes, case-sensitive, no terminator. `metadata` is opaque and never parsed.
- `seq` values are **strictly increasing in file order**, but may have gaps (a failed append consumes its seq). A seq is **never reused** within a volume. The volume's latest-version order for a name is (ordinal, seq).
- `check` has no meaning beyond being stored; it lets a reader detect a locator that points at the wrong record.

### 5.1 Types

| Value | Name | Notes |
|---|---|---|
| 0 | invalid | never written |
| 1 | file | a stored version of a name |
| 2 | erased-file | see §7 |
| 3 | seal | see §6 |
| 4 | metadata-footer | reserved |
| 5 | chunk | reserved (objects larger than the cap) |

Readers that meet another `type` (or a reserved one they do not implement) in a record whose CRCs are valid **skip it by length** and keep scanning; they never fail the volume for it.

### 5.2 Validity and scanning

A record is **valid** iff: it lies wholly inside the file; `magic` matches; `flags = 0`; `header_CRC` verifies; and (unless `type = erased-file`) `payload_CRC` verifies.

- **Sequential scan** starts at the first record offset and proceeds by record length. The first invalid record ends the valid prefix of an unsealed volume; the remainder is torn or orphaned data.
- **Resync** after damage: look for `magic` at 8-byte-aligned offsets and accept the first candidate whose `header_CRC` verifies and whose `seq` is greater than the last good seq.
- Decoders must never panic on arbitrary bytes; lengths are checked against the file size and the limits in §1 before any allocation.

## 6. Seal record

The last record of a sealed volume. `type = 3`, `name_len = 0`, `meta_len = 0`, `sha256` all zero, no tags (cipher-independent). Region B (`payload_len = 24`):

| Offset in B | Size | Field |
|---|---|---|
| 0 | 8 | committed_length: byte offset at which the seal record starts |
| 8 | 4 | epoch: final epoch of the volume |
| 12 | 4 | record_count: number of `file` and `erased-file` records before it |
| 16 | 4 | last_seq: highest seq of any record before it (0 if none) |
| 20 | 4 | reserved, zero |

- The seal record takes the next seq like any other record.
- A volume is **sealed** iff its last valid record is a seal record, its `committed_length` equals that record's start offset, and the file ends exactly at the end of that record. Otherwise it is **open/unsealed**.
- Bytes after `committed_length` in an unsealed volume carry no durability promise (unacknowledged writes).
- Sealing never rewrites earlier bytes.

## 7. Erasure

To erase a `file` record **in place**, a replica holder:
1. reads the record's header, region A and `header_CRC` and verifies them (header CRC, and `seq`, `check` and `name` against the locator);
2. writes, in **one contiguous write** over those same bytes: `type = 2` (erased-file), region A (name, sha256, metadata, tag) as zeros, and the rewritten `header_CRC`;
3. overwrites all of region B (payload, tag) with zeros, in chunks and in any order;
4. fsyncs.

`name_len`, `meta_len`, `payload_len`, `seq` and `check` stay unchanged, so the record length and every later offset are unchanged. `payload_CRC` is **not** rewritten and is ignored for erased-file records. Erasure is idempotent.

Order matters for crash safety: once step 2 has landed, the record is a valid erased-file record whatever happens to step 3, so a crash during the (possibly 100 MiB) zeroing never invalidates the record and never ends the valid prefix of an unsealed volume. A crash *inside* step 2 may leave an invalid record; erasure is then retried from the erasure list, and the retry may rely on the 24-byte fixed header alone (it differs from the original only in the type byte). A replica that differs from another only in erased-file records, or whose erased-file record still has non-zero region B bytes, has an unfinished erasure, not corruption.

## 8. Index files (not part of this format)

`.idx` (sequence index) and `.nidx` (name index) are rebuildable caches with their own prelude and version, defined outside this document. They carry no content hashes and, once encrypted volumes exist, no plaintext names. A reader must never trust them without verifying the record they point to: magic, header CRC, seq, check and **name**.

## 9. Encryption (cipher 1, reserved)

Cipher 0 stores everything in plaintext. Cipher 1 (AES-256-GCM) uses **the same framing**: regions A and B are each encrypted independently, `tag_A` and `tag_B` are the 16-byte GCM tags, nonce = volume ID ‖ seq, CRCs cover ciphertext. Key derivation and associated data are specified when cipher 1 ships, as an addendum that does not change anything above. Until then readers refuse `cipher_id ≠ 0`.

## 10. Test vectors

Golden files (one per record type and a sealed volume, plus corrupted variants) are generated by `format/` in milestone 1, stored in `format/testdata/`, and appended here as hex. They are frozen once released.

Frozen golden files in `format/testdata/` (cipher 0), as hex. Generated by `format/golden_test.go` from fixed inputs; never edited, a new version adds new files.

**`bad-header-crc.bin`** (80 bytes): `record-file` with one name byte flipped: header CRC fails
```
533452310100070001000000040302010000000005000000602f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b68656c6c6f4cbb719a00000000
```

**`bad-payload-crc.bin`** (80 bytes): `record-file` with one payload byte flipped: payload CRC fails
```
533452310100070001000000040302010000000005000000612f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b69656c6c6f4cbb719a00000000
```

**`garbage-then-record.bin`** (296 bytes): header + seq 1 + 40 bytes of 0xFF + `file` seq 3 `after`: resync finds the last record
```
895334564f4c0d0a01000000500000000100000000000100a0a1a2a3a4a5a6a7
a8a9aaabacadaeaf0300000010000000070000000000000000006d85d5310218
010000000000000000000000060000006576656e7473000000000000889df36c
533452310100070001000000040302010000000005000000612f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b68656c6c6f4cbb719a00000000ffffffffffffffffffffffffffffffff
ffffffffffffffffffffffffffffffffffffffffffffffff5334523101000500
0300000077000000000000000800000061667465720c110011327ebd18b51d85
17e12e434132283dcbee98f5b33a4f02874fc11aaddeb997fd726573796e6365
6406b7a1df000000
```

**`header.bin`** (96 bytes): volume header: bucket `events`, volume ID 0x0001000000000001, shard 3 of 16, ordinal 7, epoch 1
```
895334564f4c0d0a01000000500000000100000000000100a0a1a2a3a4a5a6a7
a8a9aaabacadaeaf0300000010000000070000000000000000006d85d5310218
010000000000000000000000060000006576656e7473000000000000889df36c
```

**`record-erased.bin`** (96 bytes): `erased-file` (name `gone.bin`, seq 4, 6-byte metadata, 16-byte payload before erasure)
```
5334523102000800040000004433221106000000100000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
000000000000532660bd000000000000000000000000000000003128d24d0000
```

**`record-file-meta.bin`** (88 bytes): `file` with metadata `{"k":1}`, name `dir/x:y.json`, seq 2, payload `{}\n`
```
5334523101000c00020000000d0c0b0a07000000030000006469722f783a792e
6a736f6eca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e
9e4423567b226b223a317d42f758cf7b7d0a279d1ff00000
```

**`record-file.bin`** (80 bytes): `file`, name `a/b.txt`, seq 1, check 0x01020304, payload `hello`
```
533452310100070001000000040302010000000005000000612f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b68656c6c6f4cbb719a00000000
```

**`record-seal.bin`** (88 bytes): `seal`, seq 5, committed_length 1000, epoch 1, record_count 3, last_seq 4
```
5334523103000000050000008877665500000000180000000000000000000000
0000000000000000000000000000000000000000000000004d48bc90e8030000
000000000100000003000000040000000000000035cc0873
```

**`record-unknown-type.bin`** (72 bytes): type 9, seq 1: valid CRCs, skipped by length
```
5334523109000000010000009900000000000000060000000000000000000000
00000000000000000000000000000000000000000000000042b83f3c66757475
7265f851de810000
```

**`torn-tail.bin`** (443 bytes): `volume-sealed` minus its last 5 bytes: unsealed, valid prefix of 3 records
```
895334564f4c0d0a01000000500000000100000000000100a0a1a2a3a4a5a6a7
a8a9aaabacadaeaf0300000010000000070000000000000000006d85d5310218
010000000000000000000000060000006576656e7473000000000000889df36c
533452310100070001000000040302010000000005000000612f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b68656c6c6f4cbb719a000000005334523101000c00020000000d0c0b0a
07000000030000006469722f783a792e6a736f6eca3d163bab05538182722614
0568f3bef7eaac187cebd76878e0b63e9e4423567b226b223a317d42f758cf7b
7d0a279d1ff00000533452310200080004000000443322110600000010000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000532660bd0000000000000000000000000000
00003128d24d0000533452310300000005000000887766550000000018000000
0000000000000000000000000000000000000000000000000000000000000000
4d48bc906801000000000000010000000300000004000000000000
```

**`volume-sealed.bin`** (448 bytes): header + seq 1 + seq 2 + erased seq 4 (seq 3 is a gap) + seal seq 5
```
895334564f4c0d0a01000000500000000100000000000100a0a1a2a3a4a5a6a7
a8a9aaabacadaeaf0300000010000000070000000000000000006d85d5310218
010000000000000000000000060000006576656e7473000000000000889df36c
533452310100070001000000040302010000000005000000612f622e7478742c
f24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98247d
198a7b68656c6c6f4cbb719a000000005334523101000c00020000000d0c0b0a
07000000030000006469722f783a792e6a736f6eca3d163bab05538182722614
0568f3bef7eaac187cebd76878e0b63e9e4423567b226b223a317d42f758cf7b
7d0a279d1ff00000533452310200080004000000443322110600000010000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000532660bd0000000000000000000000000000
00003128d24d0000533452310300000005000000887766550000000018000000
0000000000000000000000000000000000000000000000000000000000000000
4d48bc9068010000000000000100000003000000040000000000000059d446a2
```

## 11. Decisions (confirmed 2026-11-04)

1. Little-endian for all stored integers.
2. Field widths: `ordinal` u64, `seq` u32 starting at 1, `name_len` u16, `meta_len`/`payload_len` u32, `check` u32, `tier` u16, `key_generation` u32.
3. Fixed record header is 24 bytes; `header_CRC` covers it plus region A; records are padded to 8 bytes.
4. Region A always carries a 32-byte sha256 (zero for non-file types).
5. Seal is an ordinary record with its own seq, and the file must end right after it.
6. `payload_CRC` ignored (stale) for erased-file records, so erasure rewrites the header CRC only.
7. Unknown record types are skipped by length; unknown `format_major`, `cipher_id` and nonzero `flags` are refused.
8. Header CRC covers the prelude and header; minor-version fields go after the bucket name.
9. `created_unix_nano` and header `epoch` are informational; the seal record's epoch is authoritative.
