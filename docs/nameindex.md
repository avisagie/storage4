# Name index (`nameindex/`)

Status: milestone 5, implemented 2026-11-05. The name index answers "which record in this volume holds the latest version of this name?". It is a **rebuildable cache** (design-notes §12: changeable class), not part of the permanent format. Design rationale: design-notes §7; layout on disk: `docs/disk.md`.

## Rules

- A `.nidx` is never trusted. A fingerprint match is only a **candidate**; the record at the candidate's offset is read and verified (magic, header CRC, seq, **name**) before anything is returned. A collision costs one read, never a wrong answer.
- Unknown version, wrong volume ID, bad size, bad header/footer, bad bucket CRC, or a candidate that points at a record that is not what the index claims: discard and rebuild from the volume, once, then retry. A bad index is never a permanent failure. Real I/O errors are not "bad index" and are returned.
- Index files contain **no content hashes and no names**: only a fingerprint (bytes 8–16 of SHA-256(name), big endian; deliberately not the bytes the shard function uses), a seq and an offset. With encryption (post-MVP) the fingerprint becomes an HMAC with an index sub-key; `Fingerprint` is the only place that changes.
- Only sealed volumes have a `.nidx`. Open volumes use the in-memory `OpenMap`.
- Erased records have no name, so rebuilds omit them. A `.nidx` built before an erasure still points at the erased record: such a candidate is **skipped** (not an answer). The node filters erased names through the erasure list.

## File layout

All integers little-endian. Strict decoder: nothing but the exact layout is accepted.

```
 offset      size   field
 0           8      magic        89 53 34 4E 49 58 0D 0A
 8           2      version      1
 10          2      reserved     0
 12          4      header_len   4096
 16          8      volume_id
 24          4      entry_count  distinct names in the volume
 28          4      bucket_count >= 1
 32          4      bucket_size  4096
 36          8      reserved     0
 44          4      header_CRC   CRC-32C over [0, 44)
 48          4048   padding      zero
 4096        bucket_count * 4096   table
 ...         16     footer       "S4NF" | entry_count (u32) | table_CRC (u32, CRC-32C over the whole table) | reserved 0
```

Exact file size is `4096 + 4096*bucket_count + 16`. The header is padded to 4096 so that every bucket is one aligned page.

**Bucket** (4096 bytes): 255 slots of 16 bytes, then a 16-byte trailer.

```
 slot     fingerprint (u64) | seq (u32) | offset/8 (u32)       all zero = empty; seq >= 1 and offset >= 1 when used
 4080     count (u32)       slots in use; they are slots 0..count-1, the rest are zero
 4084     bucket_CRC (u32)  CRC-32C over bytes [0, 4084)
 4088     8 reserved zero
```

The per-bucket CRC (an addition to design-notes §7) lets a lookup detect a damaged bucket on the one read it makes. Without it a flipped bit could turn a hit into a silent miss, which would serve an older version.

**Placement.** `bucket_count = max(1, ceil(entry_count / 178))` (load ≤ 0.7 of 255 slots). Entries are sorted by (fingerprint, seq) and each goes to the first non-full bucket starting at `fingerprint mod bucket_count`, wrapping, into the next free slot. The file is therefore a pure function of the volume ID and the entry set, so a scrub can rebuild in memory and compare byte for byte. `DecodeTable` rejects any file that is not exactly canonical.

**Lookup.** Read bucket `fingerprint mod bucket_count`; collect slots with the fingerprint; if the bucket is full (count 255) continue with the next bucket, else stop (a bucket that is not full never overflowed). One name appears once per volume: the builder keeps the highest seq per name, so the candidate is the volume's latest version of it. Distinct names with equal fingerprints are separate entries, tried highest seq first.

## API

| | |
|---|---|
| `Fingerprint(name)` | The fingerprint function. |
| `Builder`, `Builder.Add(name, seq, off)`, `Encode(volumeID, entries)` | Build in memory (highest seq per name), encode once. Deterministic. |
| `DecodeTable(b, volumeID)` | Full strict decode (scrub, fuzz). |
| `OpenReader(f, volumeID)`, `Reader.Candidates(fp)`, `Reader.Verify()` | Positioned reads through `disk.File`: header and footer at open, one bucket per lookup; `Verify` checks the table CRC with a sequential pass. No mmap, no block cache (the OS page cache does that). |
| `OpenMap` | Open volume: name to (locator, sha256), highest seq wins. The sha256 stays in memory (dedup reads it here); it never reaches a file. `Builder()` snapshots it for sealing. |
| `Store` | `Lookup(bucket, volumeID, name)` returns a verified `*Hit` (release it) or `ErrNotFound`; `Write` at seal; `Rebuild`; `Ensure`; `CloseIdle`, `Close`. |
| handle cache | Bounded LRU of open files (index and volume files), refcounted, closed when idle longer than `IdleTimeout` (`CloseIdle` is called by the owner's ticker; there is no goroutine in this package). When every handle is in use the cache may exceed its bound temporarily rather than close a file under a reader. The clock is injected. |

## Tests

- L0: encode/decode round trip, determinism, canonical-form rejection, every byte flip and truncation rejected, `FuzzDecodeTable` (no panic, decodes imply re-encode identical), `FuzzReaderCandidates`.
- L1: lookup against real sealed volumes, fingerprint collisions (injected fingerprint function), erased candidates, stale and corrupt index files rebuilt, wrong-volume file, missing file, I/O error not treated as corruption, crash sweep of the `.nidx` write, handle LRU bound and idle close with a fake clock, no names or hashes in the file bytes.
