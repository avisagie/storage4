# Per-file overhead: storage4 vs ext4 vs SeaweedFS

Informational. Not a spec. The storage4 numbers are derived from `format-v1.md` §5 and
`design-notes.md` (index files). The ext4 and SeaweedFS numbers are estimates from
default configurations and public format knowledge, not measurements. Verify before
relying on them for capacity planning.

Assumptions: cipher 0 (no encryption), no metadata unless stated, replicas not counted
(multiply everything by the replica count).

## 1. storage4

Record length = `roundup8(64 + name_len + meta_len + payload_len)`.

| Part | Bytes |
|---|---|
| Fixed record header | 24 |
| `sha256` | 32 |
| `header_CRC` + `payload_CRC` | 8 |
| **Fixed total** | **64** |
| Padding to 8 | 0–7 (avg 3.5) |
| Name | `name_len` (stored in the record, so it counts as data) |
| Metadata | `meta_len` (0–16 KiB, only if supplied) |

Index caches (rebuildable): `.idx` 8 B per seq (gaps from failed appends cost 8 B each),
`.nidx` 16 B per distinct name at load factor ≤ 0.7, about 23 B plus bucket rounding.
About 30 B per file together.

Per volume (amortised away): 16 B prelude, header `roundup8(64 + bucket_len + 4)`, one
seal record of about 88 B.

Encrypted volumes (post-MVP): +32 B (one 16 B GCM tag for each of regions A and B).

Dedup: an identical re-upload to the latest version stores nothing. A changed payload
appends a full new record. Erased records keep their size.

PostgreSQL: no per-file rows.

**Total: about 100 B per file** plus name and metadata.

## 2. ext4, one file per record, metadata folded into the file

| Part | Bytes |
|---|---|
| Inode | 256 (preallocated table, default one inode per 16 KiB of disk) |
| Directory entry | `8 + name_len` rounded to 4 (40-byte name: 48), plus htree/dir-block overhead |
| Folded-in header | about 4 (length) + metadata, +32 if the sha256 is kept |
| Block rounding | payload rounded up to 4096: about 2 KiB wasted on average, 4 KiB minimum |

Notes:
- `inline_data` (files under about 150 B in the inode) is off by default and rarely used.
- Default inode ratio exhausts inodes at about 1 per 16 KiB of disk; small-file use needs
  `mkfs -i 4096` or lower.
- Each version needs its own file (e.g. `name.v<n>`), multiplying inode and block cost.
- `/`-separated names become directories, each costing at least a 4 KiB block.
- Durability needs fsync of file and directory per file; no group commit.
- No data checksums: a header and CRC must be added (8–40 B).
- With encryption, names stay visible in directory entries.

## 3. SeaweedFS volumes plus a filer layer for names

Needle (v3): 16 B (cookie, id, size) + 4 B data size + 1 B flags + 4 B CRC32 + 8 B append
timestamp + padding to 8 (avg 3.5): **about 37 B**. Optional name/mime/pairs fields are
left unused when a filer owns the path. Volume `.idx`: 16 B per needle, and about the
same in RAM on the volume server. Volume layer: **about 53 B**. Only a CRC32, no SHA-256.

Filer entry (path → fid): protobuf with mtime, crtime, mode, uid, gid and a chunk list
(fid, offset, size, mtime, etag): 200–400 B. In PostgreSQL add about 24 B tuple header and
a primary-key index entry: **about 300–600 B per file**. With LevelDB or another embedded
store it is smaller, but that is not a replicated source of truth.

Notes:
- One filer row per file: the store grows with the file count. storage4 forbids this
  (invariant: no per-file rows in PostgreSQL).
- Dedup by hash, `If-None-Match: *` / `If-Match`, `201` vs `200` and latest-version
  ordering would have to be built on top. A failure between filer and volume writes can
  leave orphans or dangling fids.
- Deletes are tombstones until compaction. No in-place zeroing of name, hash and payload;
  the path remains in the filer store and its logs.
- Replication is synchronous across a replica set, but the default is none. Whether an ack
  waits for every replica's fsync needs verifying.
- Stored data is only useful together with the filer database, so long-term readability
  depends on both.
- The filer can inline small contents (`saveToFilerLimit`), which makes the row cost worse.

**Total: about 350–650 B per file.**

## 4. Side by side

| Payload (40 B name, no metadata) | storage4 | ext4 file-per-record | SeaweedFS + PostgreSQL filer |
|---|---|---|---|
| 100 B | about 130 B overhead | about 4.3 KiB | about 350–650 B |
| 1 KiB | about 100 B (about 10%) | about 3.4 KiB (about 330%) | about 350–650 B (about 35–65%) |
| 100 KiB | about 100 B (0.1%) | about 2 KiB (about 2%) | about 350–650 B (0.5%) |

Conclusions:
- Blob layer: SeaweedFS (about 53 B) and storage4 (about 67 B plus about 30 B of index) are
  comparable.
- The difference is the name-tracking layer. A filer row costs 3–6× the whole storage4
  overhead and turns the database into a store the size of the file count.
- ext4 only competes for files above about 64 KiB. For the many-small-files target it is
  20–40× worse in overhead per file.

## 5. What else differs (beyond bytes)

| Concern | storage4 | ext4 file-per-record | SeaweedFS + filer |
|---|---|---|---|
| Source of truth | PostgreSQL holds buckets, leases, volumes, erasure list only; file data and names are in self-describing volumes | The filesystem | Volumes plus the filer database; data is useful only with both |
| Per-file database rows | None (invariant) | None (inodes and dirents instead) | One filer row per file; the store grows with the file count (backup, vacuum, failover) |
| Durability / ack | Group commit; ack only after all replicas fsync | fsync of file and directory per file; no group commit | Synchronous replica set, but default is no replication; whether an ack waits for every replica's fsync must be verified |
| Content identity | SHA-256 in every record; dedup against the latest version (`200` vs `201`) | None unless added | CRC32 only; hash dedup, `If-None-Match: *` / `If-Match` must be built on top |
| Versions | Highest (volume number, seq); no clocks | One file per version (`name.v<n>`), multiplying inode and block cost | No equivalent ordering; version rows or S3 versioning needed |
| Consistency across layers | One append, one record | File plus directory entry | Filer write and volume write are separate; failures can leave orphans or dangling fids |
| Integrity | Header and payload CRC-32C; headers verified on every read | No data checksums; add a header and CRC (8–40 B) | CRC32 per needle |
| Erasure | In place: name, sha256, metadata and payload zeroed, record size unchanged | Zeroing possible, but inode and dirent remain | Tombstone until compaction; path remains in the filer store and its logs |
| Name privacy with encryption | Names are inside the encrypted region; index files hold fingerprints or HMACs | Names visible in directory entries | Paths visible in the filer store |
| Name rules | `/`-separated names are just bytes in a record | `/` creates directories, each at least a 4 KiB block | Directory tree in the filer |
| Replica comparison and scrub | Sequential volume streams | Per-file random IO | Per volume, but the filer must be reconciled separately |
| Small-file limits | Bounded by volume size (32 GiB) | Inode exhaustion at about 1 per 16 KiB by default; needs `mkfs -i` tuning | Filer inlining (`saveToFilerLimit`) moves content into the database and worsens row cost |
| Long-term readability (15–20 years) | Versioned format, old versions readable forever, golden files | Filesystem and your own header format | Needle format plus filer schema, both must be preserved |
