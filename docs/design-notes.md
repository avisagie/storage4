# File Storage Design Notes

Context: air-gapped, long-lived (hardware replaced every 5–8 years, sometimes expanded) storage for a vast number of small files (20 KB – 3 MB, roughly exponential, mean ~100 KB; hard cap 100 MiB per file). Oldest data deleted unless the client buys hardware to keep it. Requirements: simple operations, current data on SSD / older on HDD, disk health monitoring, redundancy, heterogeneous hardware, files addressed by (bucket, name) with idempotent writes, best-effort single-file erasure, efficient erasure by bucket, encryption at rest (required; null cipher in the MVP, §11).

---

## 1. Scale

Volume range: **1M unique files/day (small deployment) to 100M unique files/day (large)**, kept for years. Almost every file is written at least twice (updates within minutes, up to two weeks on error); duplicates with identical content are not stored again (§9).

| | 1M/day | 100M/day |
|---|---|---|
| Data per day | 100 GB | 10 TB |
| Per year | 36.5 TB | 3.65 PB |
| 5 years logical | ~180 TB | ~18 PB / **~183 B files** |
| SSD, ~35–40 days, 2 copies | ~8 TB | ~700–800 TB |
| HDD, 5 yr, EC 10+4 (1.4×) | ~250 TB | ~26 PB (~1,100–1,300 × 24 TB drives) |
| Avg write rate (unique) | trivial | ~115 MB/s (~350 MB/s with replicas); requests ≥ 2× that |

At the large end, **metadata (file count), failure handling and migration time are the constraints — not bandwidth.** Expect a drive failure every 2–3 weeks. One file per filesystem file does not work at this scale (inode counts, fsck and rebuild times on large RAID6/XFS volumes); files are packed into volumes.

## 2. Decisions / direction

- **Files are addressed by (bucket, name).** Clients never see internal locators. Writes are idempotent: same name + identical content → nothing stored, existing version returned; same name + different content → new version, last write wins (pointer move).
- **Why names live in storage (requirement, decided Nov 2026):** the application needs idempotent, name-addressed writes, and keeping the name layer inside the volumes (rebuildable from the disks, colocated with the data) avoids a second large database of per-file rows. Locator-returning designs (Haystack/SeaweedFS style, application stores the locator) were considered and rejected for this reason (§5).
- **The bucket is under the application's control**: a filename-safe name in the request path; storage gives it no meaning beyond "unit of deletion, migration and placement". Created implicitly on first write (or explicitly with options); once deleted, the name never accepts writes again.
- **Recommended application convention** (not enforced): bucket = `<retention-class>-<UTC event-time day>`, e.g. `docs-2026-10-29`; name = `<timestamp>/<uuid>/<role>`. All versions of a document share its timestamp, so they stay in one bucket.
- **Names are sharded within a bucket**: `shard = SHA-256(name) mod S`, S per bucket (deployment default). Each volume belongs to exactly one (bucket, shard), and each (bucket, shard) has one owner node at a time, granted by a catalogue lease (§6).
- **Name index per volume**, travelling with the volume's replicas: an in-memory map while the volume is open and, at seal, one **immutable hash table of 64-bit name fingerprints** (own format, built in memory, written once, read with positioned reads — no Pebble, no Bloom filter). Rebuildable from the volume's records, scrubbable. No central per-file store (§7).
- **SHA-256 of the content in every record**; returned as `ETag` / `Repr-Digest`; optional client `Content-Digest` verified (§6).
- **Metadata is optional, opaque pass-through** (≤ 16 KiB): stored with each version, never parsed by storage.
- **Delete by dropping whole buckets**, never file-by-file for retention.
- **Any node accepts any request and forwards it to the shard owner** — no fat client libraries. The extra hop is accepted (§9).
- **Hardware refresh = generational, not migration.** New hardware takes all new writes; old generation becomes write-free and drains via retention, then is decommissioned.
- **Writes to old buckets always go to current hardware**, in a new volume of that (bucket, shard).
- **Disks are self-describing** (§7): structure always recoverable from volume files; content of encrypted volumes needs key material.
- **Every volume file starts with a fixed prelude carrying its format version** (§7).
- **Max file size 100 MiB**, buffered whole in memory on the receiving node (§9). Splitting larger objects across records is deferred until much later (record type reserved).
- **Single-file erasure is best effort: zero in place**, offsets unchanged, no space reclaimed (§11).
- **Encryption at rest is required but staged**: the v1 format carries a cipher ID and a two-region record layout (name ‖ sha256 ‖ metadata / payload, each with a tag field that is empty when unencrypted). The MVP ships the **null cipher (0)**, so a rescued disk is readable without key tooling, and platform-level disk encryption (LUKS, another team) can land on its own schedule. A later cipher (AES-256-GCM, shorthand "v2" below) adds record encryption for **bucket-granular crypto-shredding** with **no framing change**: one key per bucket in the catalogue, per-volume sub-keys derived (§7, §11).
- **One Go binary, PostgreSQL as the only source of truth, no separate catalogue server** (§8). PostgreSQL holds buckets, shard leases and volumes — never per-file rows.
- **Catalogue uses synchronous replication** — leases, epochs, ordinals, bucket state and (later) keys must never go back in time (§8). **PostgreSQL down → writes fail** (reads continue from caches).
- **Catalogue schema managed by explicit `store db` commands** — idempotent, transactional, expand/contract; nodes never run DDL or auto-initialise (§13).
- **Write throughput is not the constraint**; node count is driven by capacity and failure domains (§9).
- **Balance comes only from placement at volume open** — data is never moved. **No space reservation** (§9).
- **MVP: flat disk pool, 3× replication, no coordinator, format v1 with the null cipher** (§10).

## 3. Lifecycle (target design; MVP uses a flat pool — §10)

Transitions are triggered by the application (or an operator policy on its behalf) via `migrate_bucket` / `delete_bucket`; storage does not know a bucket's age in business terms. With the recommended day-bucket convention:

| Bucket (day) age | Placement | Protection |
|---|---|---|
| 0 → ~35–40 days | SSD | Replicated (2 copies); absorbs nearly all updates and late writes |
| End of SSD window | Move to HDD (`migrate_bucket`) | Seal + erasure code in one step |
| After (rare stragglers) | HDD | Small replicated straggler volumes in the same (bucket, shard) |
| End of retention | — | `delete_bucket` + application drops ES index (+ destroy the bucket key once encrypted) |

**Erasure by bucket (major use case).** MVP: delete the bucket's volume files on every replica and mark the bucket deleted in the catalogue — no rewrite, pull-based GC catches up offline disks (§6). Once records are encrypted, destroying the bucket key additionally covers offline disks, tape copies and SSD remnants (§11).

## 4. Things not to forget

- **Scrubbing**: periodic full read + checksum verification, including rebuilding and comparing index files (§7). Works without keys (CRCs cover ciphertext).
- **Redundancy ≠ backup**: single air-gapped site; bad deploys and deletion bugs replicate faithfully. Consider tape (LTO) for cold / extended-retention periods. Tape copies are not reached by single-file erasure; v2 volumes on tape are shredded in bulk by destroying their keys.
- **One redundancy layer**: JBOD + distributed replication/EC, not RAID underneath as well. EC 10+4 needs ≥14 failure domains.
- **Rebuild time**: 24 TB drive ≥ ~1.5 days; tolerate several concurrent degraded drives.
- **Correlated failures**: old data sits on oldest, same-batch drives.
- **Air gap**: offline mirrors and patch process, local NTP, on-site spares, drive firmware, in-gap alerting path.
- **Decommissioning**: drives often can't leave the site — budget for keep-your-drive support / destruction. Record-level key destruction does **not** help here: a volume's other replicas live on other drives and would become unreadable too. Decommissioning relies on disk-level encryption (LUKS, platform team) or physical destruction.
- **Writes to deleted buckets**: reject (410) and log. Validating that a bucket makes sense (expired day, future day) is the application's job.
- **MVP hardware is the first generation** and lives 5–8 years: buy enough servers for the eventual EC failure-domain count, and budget 3× replication capacity until EC lands.

## 5. Options assessment (Oct 2026)

- **Home-grown** (preferred direction, see §6–14).
- **SeaweedFS** (Apache-2.0, Haystack/f4-derived): best off-the-shelf fit if used via raw volume API with own index. Largely one main maintainer — long-horizon risk. Open-core: custom EC ratios and automated EC repair are Enterprise-only; fast release cadence (4.13 Feb 2026 → 4.39 Jul 2026) and a Rust volume-server rewrite in progress — behind an air gap this means owning a pinned fork. Its Raft master, automatic vacuum/EC/balancing and heartbeat-driven behaviour differ from this design's PostgreSQL-only state, manual "gone" and no-move placement. Name-addressed idempotent writes would need its filer (separate metadata store per file). Suggested test: ~2 weeks of the raw volume API at realistic file counts with failure drills; if the list of things to disable/patch includes failure semantics or on-disk format, build home-grown. **Decision (Nov 2026): not used for this workload.** Name-addressed idempotent writes are a requirement, and at the high end SeaweedFS's name layer (the filer's per-file metadata store — a second large database) and its wider operational surface are the blocker. It will run anyway as the S3 target for Elasticsearch snapshots, and that deployment doubles as the evaluation: fsync-before-ack behaviour (unconfirmed), `kill -9` and replica-loss drills, EC plus delete semantics, master failover, master load at ~600k volumes, memory vs leveldb needle index. Facts checked Nov 2026: the in-memory needle index costs roughly 20 B per file (a leveldb index mode exists); the client stores the returned file id; deletes in EC volumes are tombstones in the `.ecx`/`.ecj` index files, not zeroed data; a Rust volume server reading the same on-disk formats exists.
- **Garage** (AGPL-3.0): replication only, no tiering, per-object metadata — not suited to the high end.
- **Ceph**: capable but operationally heavy.
- **MinIO CE**: repository archived Feb 2026, "no longer maintained" — out.
- **Rejected alternative — dedup in the Kafka Streams writer** (state store keyed on (bucket, name) → (locator, hash), two-week retention): keeps names out of storage but puts a blocking storage call inside `process()` and the dedup logic in the pipeline.

## 6. Home-grown design sketch

**Interface:** `put(bucket, name, [metadata], bytes, [digest], [condition]) → (etag, stored?)`, `get(bucket, name)` (latest version: payload and/or metadata), `head(bucket, name)`, `erase(bucket, name)` (best effort, all versions), `create_bucket(bucket, [options])`, `delete_bucket(bucket)`, `migrate_bucket(bucket, tier)`. Replication/EC policy is a property of the tier. Old buckets remain logically open.

**HTTP API (MVP, plain stdlib HTTP, any node):**

| Request | Body / headers | Result |
|---|---|---|
| `PUT /b/{bucket}/{name...}` | Raw file (`application/octet-stream`), or `multipart/form-data` with optional `metadata` part **first**, then exactly one `file` part. Optional `Content-Digest: sha-256=:…:`, `If-None-Match: *`, `If-Match: "<etag>"` | `201` new version stored; `200` identical to latest — nothing stored. Both return `ETag` (SHA-256) and `Repr-Digest` |
| `GET /b/{bucket}/{name...}` | Optional `If-None-Match: "<etag>"` | Latest version's bytes + `ETag`, `Repr-Digest`; `304` if unchanged; `404` unknown; `410` erased or bucket deleted |
| `HEAD /b/{bucket}/{name...}` | — | Headers only (existence, size, `ETag`) |
| `GET /m/{bucket}/{name...}` | — | Latest version's metadata bytes; `204` if none |
| `DELETE /b/{bucket}/{name...}` | — | Best-effort erase of **all versions** (§11); `202` |
| `PUT /b/{bucket}` | Optional JSON options, e.g. `{"shards": 16}` | Create bucket explicitly; idempotent; `409` if it exists with different options |
| `DELETE /b/{bucket}` | — | Delete bucket (pull-based GC); `202` |

- **Bucket name:** 1–128 characters from the POSIX portable filename set `[A-Za-z0-9._-]`, case-sensitive, not starting with `.` or `-`, not `.`/`..`. Filename-safe so it can be used as-is in on-disk paths and tooling (e.g. `/data/<disk>/<bucket>/<volume-id>.vol`). It can't contain `/`, so `/b/{bucket}/{name...}` splits unambiguously.
- **File name:** 1–512 bytes; `/`-separated segments, each from `[A-Za-z0-9._:-]`; no empty segments, no `.` or `..` segments, no leading or trailing `/`. Case-sensitive, compared byte-for-byte. Names are never used as filesystem paths, so `:` (ISO timestamps) is allowed.
- **Digest and conditions:** `Content-Digest` (RFC 9530, `sha-256`) mismatch → `400`, nothing stored. `If-None-Match: *` → `412` if the name has a version; `If-Match` → `412` unless the latest version's ETag matches. Upstream guarantees no concurrent writers of the same name, so no stronger locking is needed.
- **Errors:** `400` invalid bucket/name or malformed multipart (unknown parts, metadata after file, more than one file), `410` bucket deleted, `413` metadata > 16 KiB or file > 100 MiB, `503` + `Retry-After` when a node's memory budget is full or the shard is in handover.
- **Metadata goes in the body**, never in the URL or headers — URLs are logged by proxies and tools, which would copy metadata outside the store and defeat erasure. Rejected alternatives: query parameters (logged, ~8 KB URL limits, flat strings only) and S3-style `Meta-*` headers (flat, case-insensitive ASCII map as the permanent model).
- Only two write encodings; multipart if and only if there is metadata; metadata part first so size checks happen before the file is buffered (stdlib `mime/multipart`).

```
curl -T scan.pdf http://node:8080/b/docs-2026-10-29/2026-10-29T12:00:00Z/6f1c…/page
curl -F 'metadata=@meta.json' -F 'file=@scan.pdf' http://node:8080/b/docs-2026-10-29/2026-10-29T12:00:00Z/6f1c…/page
curl -H 'If-None-Match: *' -T scan.pdf http://node:8080/b/docs-2026-10-29/…/page
curl http://node:8080/b/docs-2026-10-29/2026-10-29T12:00:00Z/6f1c…/page -o scan.pdf
```

**Application responsibilities (not storage concerns):** choosing bucket and file names and keeping names unique per document/role; never writing the same name concurrently; calling `delete_bucket` at end of retention; storing (bucket, name) in ES; optional metadata.

**Structure:**
- Bucket → **S shards** (`shard = first 8 bytes of SHA-256(name) as big-endian u64 mod S`) → per (bucket, shard) a sequence of **volumes** (append-only pack files, a few GB, ≤ 32 GiB) → N replicas on distinct disks/servers. Replicas are byte-identical, except for erasures in progress (§11).
- **S** is chosen when the bucket is created (explicit option or deployment default) and fixed for the bucket's life; recorded in the catalogue and in every volume header. Small or rarely written buckets use small S (fewer, fuller volumes); the busiest buckets use roughly the node count.
- The **catalogue** maps volume → (bucket, shard, ordinal, replica locations); nodes cache and refresh on "not here".

**Shard ownership:**
- Each (bucket, shard) has at most one **owner**, holding a **shard lease** in the catalogue. Only the lease holder opens volumes in that shard and appends to them; **a node starts writing a shard only after the catalogue has granted it the lease.**
- The candidate owner is chosen by **rendezvous hashing** of (bucket, shard) over live, eligible nodes. Any node computes it locally and forwards; a node that isn't the lease holder answers "not owner, try X".
- **Handover** (node failure, node added/removed): the previous lease is released (graceful: old owner seals its open volumes and builds their index files) or expires. The new owner acquires the lease, seals any of the shard's volumes still open (epoch bump fences the old owner), rebuilds missing index files by a full pass over those volumes (§7, *Restart and index recovery*), then serves. While this runs, the shard answers `503` + `Retry-After` — seconds to a minute, growing with open-volume size.
- Shard leases exist only for (bucket, shard) pairs that are being written; idle ones are released.

**Per-shard volume ordinal:** the catalogue assigns each new volume of a (bucket, shard) the next ordinal, in the same transaction that opens it under the shard lease; the volume header records it. **Latest version of a name = highest (ordinal, seq)** — a strict order from the single writer per shard, no clocks involved. Synchronous catalogue replication means ordinals never go back.

**Internal record locator** (never exposed): `volume ID | seq | check`. Used in index entries, the erasure list and logs.
- **Volume ID (64 bit) = `node_id (16) | node-local counter (48)`**: each node has a node ID (assigned once by admin command, never reused) and a counter persisted on the node (allocated in blocks, fsync'd before use). A node that loses its counter state gets a **new node ID**. Startup refuses if another live node reports the same node ID. No PostgreSQL sequences, so no rewind after failover.
- **seq (32 bit):** record sequence within a volume; **never reused**, even after a failed append.
- **check (32 bit):** random per-record value stored in the record header; catches wrong-locator bugs.
- **Cluster ID** in the volume header and node-local state detects volumes from another installation and nodes pointed at the wrong catalogue (§13).

**Catalogue: plain PostgreSQL**, simple documented schema, no ORM — rescuable by hand-written SQL. Rows for buckets, shard leases and volumes only (a few million volume rows per year at the high end). Once records are encrypted it also holds the bucket keys, so **catalogue loss = data loss** for encrypted volumes (§8, §11). Schema lifecycle in §13.

- **Volume ownership:** a volume is written only by the owner of its (bucket, shard), under a volume lease tied to the shard lease — assigns seqs, writes to all replicas, acks after all fsync (group commit). The owner **does not have to hold a replica**; a local disk is used only if it wins placement on the same terms as any other (§9). Ownership ends at seal (full, idle timeout, failure/slow replica, ENOSPC, tier migration, lease loss, catalogue incarnation change). Sealed volumes have no owner; read from any replica.
- **Epoch fencing:** a per-volume monotonic epoch, bumped on seal; replicas reject appends with a stale epoch.

**Disk failure on an open volume — seal, don't repair:**
1. Append to the failed replica errors/times out; client has no acknowledgement.
2. Owner seals the volume at its committed length, bumping the epoch, and builds its index file.
3. Owner opens a new volume for the (bucket, shard) on healthy disks and retries — client sees latency only.
4. Surviving replicas truncated to committed length.
5. Sealed volume is now under-replicated → failure policy below.
- Owner also dead: the next owner (after handover) seals at the **minimum** surviving replica length (acked ⇒ on all replicas). Unacked-but-written records become orphans; the index file includes them, but no client was told they exist — harmless, since a later write of the same name gets a higher (ordinal, seq). Caveat: if the unacked record is a new version of an existing name and the client never retries, it becomes the latest version — a `503` after a mid-write failure is an indeterminate outcome (document in `docs/api.md`).
- Never ack on quorum; seal-on-failure keeps consensus confined to the catalogue.

**Failure policy — separate "down" from "gone":**

| Step | Automation | Why |
|---|---|---|
| **Down**: stop writing, seal open volumes, serve reads from other replicas, hand over shard leases | Automatic, immediate | Non-destructive; needed for availability |
| **Re-replicate**: copy surviving replica (volume + index files) to a new disk | Manual by default; optional automatic safety net only for volumes at last copy / last EC parity, single-disk scope only | Non-destructive; costs bandwidth only |
| **Gone**: forget the disk, discard its contents | **Manual commit point** | The only irreversible step |

- Never auto-rebuild for failures larger than one server — mostly transient; avoid rebuild storms.
- Reference points: Ceph auto mark-out ~10 min (`noout`, subtree limit), HDFS dead-node ~10 min, ES delayed reallocation 1 min, Kafka/Cassandra manual.
- Manual delay = time at reduced redundancy. Needs an operator SLA and escalating alerts.
- Re-replication copies bytes verbatim (ciphertext for v2) — never re-encrypts.

**Tier migration of an open bucket:** seal open volumes → new writes to target tier → copy / EC-encode sealed volumes (with their index files) → update catalogue → delete old copies. Names and ordinals unchanged.

**Edge cases:**
- Returning disk after "gone" committed: wipe and rejoin. Before commit: reconcile against catalogue (epoch, committed length, checksums, pending erasures).
- Bucket deleted while a disk is offline: mark deleted in catalogue first; nodes reconcile against the catalogue and GC volumes of deleted buckets (pull-based), so offline disks catch up when they return.
- **GC never deletes volumes that are merely unknown to the catalogue** — only volumes of buckets recorded as deleted. Unknown volumes are reported (input for `store scan rebuild`).
- Restore from tape/old disks: bucket enters an explicit **restored** state (operator action); accepts volume imports, no new writes; expiry set by operator. Erasure list re-applied on import. Never silent resurrection.

## 7. Self-describing volumes, indexes and recovery

**Goal:** an old set of disks can be recovered from the volume files plus, for encrypted (v2) volumes, key material from the catalogue or a key export. Structure (headers, record framing, seqs, sizes, CRCs) is always readable without keys. Every index is a rebuildable cache.

**Format versioning:**
- Every volume file — and every future EC shard file — starts with a **fixed prelude that never changes across versions**: `magic | format_major | format_minor | header_len`. Any tool can identify any file's version from its first bytes.
- `format_major` changes framing (readers refuse unknown majors); `format_minor` adds header fields older readers may skip using `header_len`.
- The catalogue also records format version per volume.
- **v1** = MVP. The volume header carries a **cipher ID** (0 = none, the MVP). Encrypted volumes (shorthand **"v2"** in these notes = cipher ID ≠ 0) use the **same framing** — the record layout below already has the tag fields, empty for cipher 0 — so enabling encryption needs no framing change; readers refuse unknown cipher IDs. Readers support every version and cipher that may still exist on disk — potentially 15–20 years with extended retention — so keep each spec small.

**On-disk format (v1):**
- **Volume header:** volume ID, cluster ID, bucket name (length-prefixed, ≤ 128 bytes), shard, shard count S, per-shard ordinal, tier, creation time, epoch; **cipher ID** (0 = none) and **key generation** (0 when unencrypted).
- **Record** (proposed layout — fix when writing `docs/format-v1.md`; this is the permanent part to review): fixed header, then two regions:
  `magic | type | flags | seq | check | name_len (u16) | meta_len (u32) | payload_len (u32) | A | header_CRC | B | payload_CRC`
  - **A** = `name ‖ sha256 (32 B) ‖ metadata ‖ tag_A`, **B** = `payload ‖ tag_B`. Tags are 0 bytes for cipher 0 and 16 bytes (GCM) otherwise. Each region is its own encryption unit with its own sub-key (§11), so dedup checks, `HEAD` and metadata reads read and decrypt only the small region A (metadata ≤ 16 KiB), never the payload.
  - Length fields in the fixed header are **plaintext field lengths**; the on-disk size of each region is the sum of its fields plus the cipher's tag size (known from the volume header). `meta_len = 0` means no metadata. u32 is ample for the 100 MiB cap; larger objects will be split across records (reserved type) rather than widened.
  - `header_CRC` covers the fixed header **and region A** as stored (ciphertext when encrypted), so a scanner can resync after corruption and index rebuilds can trust names and metadata. `payload_CRC` covers region B as stored, so scrub and replica comparison need no keys.
  - `sha256` is over the plaintext payload — the content identity used for dedup and returned as ETag. It lives in region A, so it is encrypted together with the name once a cipher is active and never appears in index files.
  - `type`: file / erased-file / seal / metadata-footer / chunk (reserved, much later) / future types.
  - 8-byte alignment. **Record size never changes after writing** — erasure zeroes in place (§11), so every later record keeps its offset.
- **Seal record/footer:** committed length, epoch, record count; optionally a copy of all records' metadata for fast metadata-only scans (erasure must zero that copy too).
- **Plain-text format spec (all versions) written onto every disk.**

**Seq index (`<volume-id>.idx`) — rebuildable cache:**
- **Own prelude:** `idx_magic | idx_format_version | header_len`, then volume ID and entry size. Independent of the volume format version; unknown version or volume-ID mismatch → discard and rebuild.
- **Entries:** dense array indexed by seq, 8 bytes each: `offset / 8 (u32) | record length (u32)`; all-zero = no record. u32 offsets in 8-byte units cover volumes up to **32 GiB** (volume size cap).
- **Open volumes:** appended without a separate fsync; after a crash, extended by a tail scan from the last indexed record to the committed end.
- **At seal:** index footer (record count, CRC32C over entries), fsync. Missing or failing footer → rebuild.
- Used for crash recovery, scrub and locator-based internal reads; name lookups use the name index below.

**Name index (`<volume-id>.nidx`) — rebuildable cache, replicated with the volume:**
- **Open volume:** in-memory map on the owner, name → (seq, offset, length, sha256). Lost on a crash; rebuilt by a full pass over the volume (*Restart and index recovery*, below).
- **At seal:** the sealer builds the table **in memory** and writes it once as an **immutable open-addressing hash table** (own format, no dependency):
  - Prelude `nidx_magic | nidx_format_version | header_len`, then volume ID, entry count, bucket count, bucket size (4 KiB). Unknown version, volume-ID mismatch or bad footer → discard and rebuild.
  - One entry per distinct name in the volume (the builder has the names, so it keeps only the highest seq per name): `fingerprint (u64) | seq (u32) | offset/8 (u32)` = 16 B. The fingerprint is bytes 8–16 of SHA-256(name) — deliberately not the bytes the shard function uses. Encrypted volumes: HMAC with an index sub-key instead.
  - Entries are placed by `fingerprint mod bucket_count` with linear probing across 4 KiB buckets at load factor ≤ 0.7, so a lookup reads one bucket (usually page-cached).
  - Footer: entry count, CRC32C over the table; fsync; copied to every replica.
- **No sha256 or plaintext name in the index.** Dedup reads the latest record's region A from the volume (open volumes: in-memory map). Index files never contain content hashes.
- **Size:** ~23 B per name including slack → ~2 MB per 8 GB volume at 100 KB mean file size, ~9 MB at 20 KB files.
- **Lookup:** every fingerprint match is only a candidate, verified against the record header (name). A fingerprint collision costs one extra read, never a wrong answer. No Bloom filter: the table is its own filter.
- **Rebuild:** a full sequential pass over the volume (skipping bodies would mean seeks at ~100 KB records) — ~40 s per 8 GB at ~200 MB/s on HDD, a few seconds on SSD. Erased records have no name and are omitted.
- **Scrub (`scan verify`):** rebuild in memory and compare with every replica's `.nidx` and `.idx`.
- **Readers opened on demand:** an owner opens a volume's `.nidx` when a lookup needs it (from a local replica, or a cached local copy fetched from a replica) and reads buckets with positioned reads through the injectable FS — no mmap, no block cache; the OS page cache does the caching. Only file handles are managed: a bounded LRU, closed on idle timeout. Access is heavily biased to recent data, so cold buckets cost nothing until touched.

**Lookup of (bucket, name)** on the shard owner: in-memory maps of the shard's open volumes first, then the shard's sealed volumes **newest ordinal first**; per volume, a miss costs one (usually cached) hash-table bucket read; the first verified hit is the latest version. The erasure list (cached on the owner) filters erased versions. Per lookup this touches the shard's few volumes of that bucket (usually 1–3, more with late-data straggler volumes), not the whole bucket.

**Every read verifies the record against the index entry:** magic, header CRC, seq, check, **name**, payload CRC. A wrong or stale index entry is detected, never served as another file; on mismatch the node rebuilds the index file.

**Restart and index recovery** (rules, so this is not left to the implementation):
1. **A restarted owner never resumes an open volume.** For each volume it had open, the node seals it (epoch bump, at the minimum committed length across replicas), extends `.idx` by a tail scan and rebuilds the name index by a full pass, then opens fresh volumes. This is the only unconditional startup scan. Its cost is bounded by the open-volume cap and the **open-volume size cap** (smaller open volumes = faster restart and handover; a periodic checkpoint of the name map is the alternative lever).
2. **Everything else is checked cheaply, not scanned:** for each local sealed volume, read the seal record and the `.idx` / `.nidx` footers (CRC). Failure → queue a **low-priority background rebuild**; startup does not wait. A lookup that hits an unusable index rebuilds it on demand (that request waits).
3. **Volumes sealed because something went wrong** (replica failure, owner dead before the sealer finished) take the same path: seal record present, index footer missing → background or on-demand rebuild. Handover applies rule 1 to the dead owner's open volumes, under `503`.
4. **Scrub** (`scan verify`) stays the periodic safety net and compares every replica's index files with a fresh rebuild.

**Encrypted volumes ("v2", post-MVP):** cipher ID ≠ 0 and key generation in the volume header (plaintext). Regions A and B are each encrypted independently with AES-256-GCM (§11). CRC32C stays **over ciphertext**, so scrubbing and replica comparison need no keys; index rebuild needs the key. The `.nidx` fingerprint becomes an HMAC (index sub-key) of the name, so index files reveal nothing. Record framing (lengths, type, seq) stays plaintext — sizes leak, accepted.

**Metadata:** optional, opaque bytes, stored per version and returned as-is; never parsed or validated by storage. Size limit 16 KiB (limits can be raised later, never lowered). JSON is the recommended application convention.

**Resurrection prevention:**
- Catalogue keeps deleted-bucket records **forever**; a deleted bucket name never accepts writes again; restore is an explicit state (§6).
- Deletion list **and erasure list** also exported to a separately backed-up append-only log.
- Scan/rebuild refuses volumes of buckets on the deletion list (unless an operator restores them) and re-applies the erasure list before importing.

**Scan realities:**
- Same volume on several disks: dedupe by volume ID, prefer valid seal + highest epoch.
- Replica differs only in records of type erased-file: an erasure not yet applied to the other copy — finish it, don't report corruption.
- Unsealed volumes: accept CRC-valid records; trailing unacked writes become orphans that a later write of the same name supersedes.
- Catalogue rebuild recovers buckets, shards, ordinals and replica locations from volume headers; "latest version" per name falls out of (ordinal, seq).
- v2 content needs keys: the scanner reads them from the catalogue or a key export.

**One tool, two modes:** *verify* (scan + diff against catalogue and index files = scheduled scrub; no keys needed for v1 structure) and *rebuild* (insert findings, rewrite index files; after `store db init --cluster-id` on an empty database, §13).

**EC shards (post-MVP, decide layout before shipping EC):** **contiguous-range sharding** — each data shard is a contiguous slice of the volume — so records remain scannable within an EC shard (resync on magic + header CRC) and small reads hit one disk. EC shard files carry the same prelude plus volume ID, EC shard index, k, m, range. Index files are replicated whole (they are small). In-place erasure on an EC volume = zero the record's range in its data shard(s) and recompute parity for the affected stripes.

## 8. Process architecture

**One static Go binary (no CGO, pgx driver; no other third-party dependency in the data path), several modes:**

```
store node    – accepts and forwards requests, owns shard leases and their open volumes, serves reads, stores replicas on its disks
store coord   – coordinator duties (any node may run it; one active at a time) — not in MVP
store scan    – verify / rebuild / recover tool (reads catalogue or key export)
store admin   – operator commands: mark disk gone/read-only, repair, delete bucket, migrate bucket, erase, assign node ID
store db      – catalogue bootstrap, init, status, migrate, verify (§13)
```

**No separate catalogue server.** The catalogue package inside the binary is the API; PostgreSQL is the only source of truth.

**Who talks to PostgreSQL:** storage nodes and tools directly (short transactions: shard leases, open/seal volume with ordinal, cached location lookups, key fetch, disk-stat heartbeats, erasure list). Never per file. Only `store db` runs DDL. Application clients never — they talk HTTP to any storage node.

**Coordinator (post-MVP) = singleton elected via PostgreSQL advisory lock.** Duties: placement policy, repair queue, down→re-replicate transitions (gone stays manual), retention deletes, erasure application, scrub scheduling, failure detection, planned shard rebalancing.
- Every decision is a DB transaction on the lock-holding session → database fences split-brain.
- Actions on disks via a task table (`SELECT … FOR UPDATE SKIP LOCKED`), executed idempotently.

**Replication and failover — the catalogue must never go back in time:**
- **Synchronous replication** (e.g. `synchronous_standby_names = ANY 1 (s1, s2)`). Write rate is tiny (leases, volume opens/seals), so the cost is negligible. Leases, epochs, ordinals, bucket state, erasure list — and from v2, keys — cannot be lost.
- v2: a bucket key is committed (synchronously replicated) before any data is encrypted with it.
- **Failover runbook:** promote standby → set a new **catalogue incarnation** (e.g. a timestamp) → nodes that see a changed incarnation drop all shard and volume leases and seal open volumes with epoch = max(catalogue, on-disk) + 1 → shard leases are re-acquired → verify node-ID uniqueness.
- v2: **backups include keys.** Backup and key-export retention = the key-material retention bound N, which also bounds crypto-shredding lag (§11).

**PostgreSQL outage behaviour:** **writes fail** (shard and volume leases can't be renewed or granted; idempotency and ordering depend on them). Reads continue for buckets whose volume locations are cached (and in-memory cached keys for v2 — never written to disk).

**15–20 year operation:** schema migrations embedded in the binary with explicit compatibility checks (§13); PostgreSQL major version pinned in the offline mirror, upgraded by a separate runbook (`pg_upgrade`) independent of schema migrations.

## 9. Write path, read path and scaling

**Write path:**
1. Any node receives `PUT /b/{bucket}/{name}`; validates bucket and name; **buffers the whole file in memory** (≤ 100 MiB) under the node's memory budget; computes SHA-256; checks `Content-Digest` if sent (`400` on mismatch).
2. Computes `shard = SHA-256(name) mod S` and the rendezvous owner of (bucket, shard); forwards metadata, body and hash. "Not owner, try X" → retry there.
3. Owner (holding the shard lease) looks up the latest version (§7).
4. Conditions: `If-None-Match: *` and a version exists → `412`; `If-Match` and the latest ETag differs → `412`.
5. Latest version's sha256 equals the new hash → **`200`, nothing stored**.
6. Otherwise append to the shard's open volume (all replicas fsync, group commit), add to the in-memory name map → `201`.
- Dedup compares **only with the latest version**: if content flips A → B → A (e.g. non-deterministic lossy compression), the second A is stored again. That keeps "latest" rebuildable from disk alone.
- **Memory budget per node** (e.g. a few GB) guarded by a semaphore sized in bytes; when full → `503` + `Retry-After`. The body crosses the network twice (receiver → owner → replicas); at ≤ ~1 GB/s peak including duplicates this is acceptable.
- **Retries:** replica failure → the owner seals and retries on a new volume internally. Owner failure mid-write → the forwarder returns `503`; the client retries, the new owner (after handover) finds the name if the write landed and returns `200` — retries don't waste space.

**Read path:** any node receives `GET /b/{bucket}/{name}` → forwards to the shard owner (it alone sees the open volumes' latest writes) → lookup → the owner reads the record from a replica (local if it has one, otherwise the least-loaded replica holder) → verifies → streams back through the receiving node. Cost: two network hops + usually one disk read (index buckets mostly cached). Readers are biased to recent data, which is mostly in page cache and in open volumes' memory maps.

**Open volumes and late data:** each owner keeps one (or a few, for busy buckets) open volume per (bucket, shard) it writes. A late write to an old bucket opens a small **straggler volume** for that (bucket, shard).
- Buckets without recent writes (judged by last-write time, not business meaning) get a **long idle timeout** (hours) so stragglers are reused; worst case ≈ S × days-with-late-data straggler volumes per idle-timeout period — choose small S for classes with lots of late data.
- Open-volume caps per node/disk, sealing the least recently used volume when hit.
- Idle shard leases are released, so lease rows track only shards being written.

**Placement and free-space balancing (at volume open, not per write):**
- Data is never moved, so **placement at volume open is the only balancing mechanism**.
- **Power of two choices**: for each replica, sample two random eligible disks on distinct servers (respecting failure domains); pick the lower **fill ratio** (used ÷ capacity), tie-break on fewer open volumes. Near-even balance without herding; a newly added empty disk fills faster but doesn't absorb everything. Fill ratio normalises heterogeneous disk sizes.
- **Owner locality is optional** — prefer a local disk only if it passes the same test.
- **No space reservation.** Below the placement cutoff a 24 TB disk has ~2.4 TB slack (~300 full volumes); if a disk does fill, the append fails with ENOSPC and seal-on-failure handles it. Optional node-local `fallocate` in small chunks (e.g. 256 MiB) for HDD contiguity.
- **Repair headroom**: stop placing on a disk at ~85–90% fill; cap open volumes per disk; keep at least one large server's worth of free capacity across the pool.
- **Inputs**: nodes heartbeat per-disk capacity, used and open-volume count into the catalogue; owners read it (seconds-stale is fine).
- **Steady state**: each bucket's volumes are spread pool-wide, so dropping a bucket frees space roughly evenly everywhere. The only deliberate imbalance is generational.
- **Later — tier-aware ownership:** the rendezvous candidate set per bucket can be restricted to nodes eligible for its tier.

**Why it scales:**
- **No per-file coordination in PostgreSQL** — touched only for shard leases and volume open/seal (~1,250 opens/day at the high end with 8 GB volumes, plus stragglers).
- **Unit of parallelism = (bucket, shard)**, spread over nodes by rendezvous hashing; each owner's lookups are local memory / hash-table checks.
- **New hardware is immediately useful** — rendezvous hashing moves ~1/N of shards to it; data stays where it is.

**High-end numbers vs capacity:**

| Load | Avg | Peak (~4×) | Reference |
|---|---|---|---|
| Unique files/s | ~1,200 | ~5,000 | One volume with group commit: ~10k–50k small appends/s |
| Requests/s (incl. duplicates) | ≥ ~2,400 | ≥ ~10,000 | Spread over shard owners; lookups are in-memory / hash-table |
| Ingest (unique) | ~115 MB/s | ~500 MB/s | One NVMe sustains 2–3 GB/s |
| SHA-256 | | ~1 GB/s incl. duplicates | Go stdlib with SHA-NI: ~1–2 GB/s per core |
| SSD writes incl. 2 replicas | ~20 TB/day | | ~0.025 DWPD over 800 TB — endurance irrelevant |
| SSD→HDD migration | ~14 TB/day post-EC | | Sequential, spread over 1,000+ HDDs |
| EC CPU | | | Go Reed-Solomon (klauspost/reedsolomon): GB/s per core |
| AES-GCM (v2) | | | Several GB/s per core with AES-NI — negligible |

→ **Node count is driven by capacity and failure domains, not write throughput.**

**What needs design:**
1. **Group commit** — batch appends for a few ms, one fsync. Essential on HDD (5–10 ms fsync). A 100 MiB file shouldn't hold small writes hostage: commit it in its own group, or on another open volume of the shard. Whether the MVP needs batching at all is an open question (open threads).
2. **Shard handover** — lease expiry/grace times, sealing and index rebuild (a full pass) of the old owner's volumes, `503` while in progress.
3. **Seal on slow** (post-MVP) — persistently slow replica treated as failed; bounds tail latency.
4. **Open-volume control** — per (bucket, shard), idle timeout by write recency, LRU sealing at the cap.
5. **Index file-handle cache** — lazy open of `.nidx`, bounded LRU of open handles, idle-timeout close (no block cache; the OS page cache does the rest).
6. **Placement** — see above.
7. **Memory budget and backpressure** for buffered requests.

**Scaling down:** at 1M/day throughput is irrelevant; failure domains are the limit. Redundancy scheme is per tier, chosen per deployment (e.g. EC 4+2 across disks for small sites, 10+4 across servers for large). Default S small (e.g. 4–8) on small sites.

## 10. MVP

**Principle:** anything that ends up **on disk, in an ID space or in the API** is permanent — get it right now. Process behaviour can be simplified now and added later. Limits start tight; they can be loosened, never tightened.

**Non-negotiable in the MVP (permanent or uncorrectable later):**
- Fixed **prelude with format version** on every volume file; v1 format with typed records, seq, check, **name, sha256**, metadata, header CRC (covering the fixed header and region A), payload CRC, seal record. Volume header with bucket, **shard, S, ordinal, cipher ID (0 = none), key generation**. Record layout with **two regions (name ‖ sha256 ‖ metadata, payload) and their (empty) tag fields** (§7). Reserve record types (erased-file, metadata footer, chunk).
- **Shard function** `SHA-256(name) mod S`, S fixed per bucket and recorded in every volume header.
- **Volume IDs = node ID + node-local counter**; seqs never reused within a volume (required for v2 nonces); cluster ID in volume header and node state.
- **Shard leases**: only the catalogue-granted owner writes a (bucket, shard); handover seals the previous owner's volumes before writing. **Per-shard ordinals** from the catalogue; latest = highest (ordinal, seq).
- **Write correctness:** all-replica fsync before ack, volume lease + epoch fencing, seal-on-failure (incl. ENOSPC). Dedup only against the latest version.
- **Read correctness:** every read verifies the record header (magic, CRC, seq, check, name) against the index entry — indexes are only caches.
- Volume size cap ≤ 32 GiB.
- Deleted buckets kept forever; restore as an explicit state; GC never deletes unknown volumes.
- **HTTP API as in §6** — name-addressed, digest headers, conditional writes, metadata in the body only, no exposed locators.
- **Synchronous catalogue replication + failover runbook** (incarnation bump, seal-all, lease re-acquisition).
- Group commit (flat pool is likely HDD).
- **`store db` init/migrate from release one** (§13).
- `store scan verify` — scrub and safety net, including index files.

**Simplifications:**

| Area | MVP | Later |
|---|---|---|
| Tiering | **Flat pool**, one disk type per deployment; `tier` column with single default value | Tiers + `migrate_bucket`, tier-aware owners |
| Redundancy | **3× replication** (2× too thin with manual repair) | EC for sealed volumes (contiguous-range shards) |
| Encryption | **Null cipher (0) in format v1**; cipher ID, key generation and tag fields already in the layout; disk-level encryption is the platform team's (LUKS) | Cipher 1 (AES-256-GCM): bucket key + derived volume sub-keys, HMAC name fingerprints, bulk crypto-shredding |
| Erasure | `DELETE /b/{bucket}/{name}` / `store admin erase`: erasure-list rows + in-place zeroing of all versions on all replicas | Coordinator-driven |
| Large objects | Hard cap 100 MiB, buffered in memory | Splitting across records (much later) |
| Coordinator | **None** — rendezvous + catalogue leases, admin CLI + cron | Elected coordinator + task table, planned rebalancing |
| Deletes / GC | Application calls `DELETE /b/{bucket}`; **pull-based reconcile**: nodes diff local volumes vs catalogue, delete volumes of deleted buckets, apply pending erasures | Likely permanent |
| Repair | `store admin repair --disk X` (copy sealed volumes + index files from survivors); "gone" manual | Last-copy auto safety net |
| Failure detection | Heartbeat timestamps in catalogue + alerting query; lease expiry drives handover | Automatic "down" marking |
| Routing | **Forward to shard owner** (rendezvous hashing, "not owner, try X"); no client library | Tier-aware candidate sets |
| Placement | **Power of two choices on fill ratio**, distinct servers, no reservation, ~85–90% cutoff, open-volume cap, owner locality optional | Richer failure-domain awareness |
| Generations | Per-disk read-only flag in catalogue | — |
| Sealing | On failure, full, idle timeout (by write recency), LRU at open-volume cap, lease loss | Seal-on-slow |
| Shard handover | Lease expiry or graceful release; `503` while sealing/indexing | Warm standby owner if handover is too slow |
| Seq index | `.idx` per volume: own version, `offset/8 \| length`, tail scan after crash, CRC footer at seal | — |
| Name index | In-memory per open volume; `.nidx` immutable hash table built in memory at seal (own format, no dependency), replicated with the volume, opened on demand, file handles LRU-closed | — |
| Metadata footer | Deferred (type reserved) | Additive |
| Schema | `store db` commands, SQL migrations, expand/contract with compat level | — |
| PostgreSQL | Primary + 2 standbys, synchronous (ANY 1), backups, manual failover via runbook | — |
| Security | Shared token inside the air gap | TLS/mTLS |

**MVP foot-guns to avoid:**
- Exposing internal locators to clients.
- Writing a (bucket, shard) before the catalogue grants the shard lease, or skipping the seal of the previous owner's volumes on handover.
- Comparing dedup against anything but the latest version, or ordering versions by clocks.
- Trusting an index entry without verifying seq, check and name in the record header.
- Putting content hashes (or, once encrypted, plaintext names) into index files.
- Resuming an open volume after a restart instead of sealing it and opening a new one.
- Volume IDs or ordinals from anything that can go back in time (PostgreSQL sequences without sync replication, local clocks).
- Reusing a seq within a volume (breaks v2 nonce uniqueness).
- Async catalogue replication.
- Skipping CRCs or epochs.
- Push-only deletes or erasures (offline disks miss them → resurrection).
- GC treating "unknown to catalogue" as garbage.
- Nodes auto-initialising or auto-migrating the catalogue.
- Parsing or validating metadata, or accepting it in URLs/headers.
- Building business meaning (event time, retention rules, IDs) into storage.
- Per-file rows in PostgreSQL.
- Unbounded request buffering or unbounded numbers of open index file handles.
- Short idle timeouts on old buckets (one new volume per late write).
- Mixing SSD and HDD in the flat pool (writes run at slowest-disk speed).
- Running the pool near full — repair needs headroom.

**MVP surface:** name-addressed write/read/head/erase path with forwarding, shard leases and handover, scan tool, admin commands (delete bucket, repair, erase, mark disk read-only/gone, assign node ID), `store db` commands.

**Prior art:** Windows Azure Storage stream layer (SOSP 2011) — extent sealing, partition layer over streams; Facebook Haystack and f4.

## 11. Erasure and encryption at rest

### Single-file erasure — best effort, in place

**Requirement:** rare; remove a file's name, content and metadata. Freeing space is **not** required. `DELETE /b/{bucket}/{name}` erases **all versions** of the name.

**Mechanism:**
1. The shard owner looks up every version of the name (all of the shard's volumes) and inserts their locators into the catalogue's **erasure list** → reads return `410` immediately (the owner caches the list). The list is permanent and exported with the deletion log.
2. Volumes still open are sealed first (keeps erasure off the append path).
3. Each replica holder, idempotently: overwrite the record's name, sha256, metadata and payload bytes with **zeros**, set `type = erased-file`, rewrite the header CRC, fsync. Record lengths unchanged → every later record keeps its offset; `.idx` unchanged.
4. Zero the record's entry in the seal-footer metadata copy, if present. `.nidx` files are immutable: lookups filter erased versions through the erasure list; scrub/rebuild produce index files without them.
5. The catalogue tracks which replicas have applied it; offline disks apply pending erasures on reconcile; scan/rebuild re-applies the list.
6. A later `PUT` of the same name stores a new version normally.

**Best effort, by design:**
- Not reached: tape copies, drives that already left the site, SSD remapped blocks (the FTL may keep old data in flash; HDDs mostly overwrite in place).
- No space reclaimed; the zeroed bytes are freed when the bucket is dropped.
- With encrypted volumes, bulk crypto-shredding (below) covers what in-place zeroing can't — at bucket granularity.

### Encryption at rest (required; null cipher in the MVP, AES-256-GCM later)

**Staging (decided Nov 2026):** the MVP uses the null cipher (cipher 0) so that rescued volumes are readable by the scan tool without catalogue keys, and so the platform team's disk encryption (LUKS) can land on its own schedule. The cipher is per volume, so an installation can switch new buckets to AES-GCM at any time; existing plaintext volumes stay plaintext (converting means copying into new volumes with new IDs — with 5-year retention most age out first). The framing already contains everything encryption needs (§7), so no format change is required.

**Purpose:** encryption at rest, and **crypto-shredding in bulk** (bucket expiry, tape sets) — not per-file erasure. Decommissioned drives are covered by disk-level encryption (LUKS), not by record keys: a volume's other replicas live elsewhere, so destroying its key would destroy them too.

**Constraint:** zeroing one record in the middle of a volume must not affect any other record. So **no chaining across records**: each record is encrypted independently with its own nonce.

**Scheme: AES-256-GCM, per record** (Go stdlib):
- One 256-bit **bucket key** per bucket (with a key generation number), generated at bucket creation and stored in the catalogue (committed and synchronously replicated before first use). **Per-volume sub-keys are derived, never stored** (HKDF-SHA256 over the bucket key, volume ID as context), so there is no extra catalogue write at volume open.
- Per-volume, per-region sub-keys (stdlib `crypto/hkdf`): `k_a` (region A: name + sha256 + metadata), `k_b` (payload), `k_index` (HMAC fingerprints for `.nidx`). Regions are **separate GCM messages**, so dedup checks, `HEAD` and metadata reads never read or decrypt the payload.
- **Nonce (96 bits) = volume ID (64) ‖ seq (32)** — deterministic and unique because seqs are never reused and records are never re-encrypted. Sub-keys are already per volume, so the volume ID in the nonce is belt and braces.
- Header fields (type, seq, lengths) can be bound as GCM associated data. Erasure changes `type`, but an erased record is never decrypted.
- On disk: `ciphertext ‖ 16-byte tag`; lengths in the record header are on-disk lengths. CRC32C over ciphertext for keyless scrub.
- Erasure writes plain zeros; type `erased-file` tells readers not to decrypt.
- Re-replication, EC and tier migration copy ciphertext verbatim.

**Why GCM over plain CTR:** both are position-independent per record (CTR is what GCM uses internally). GCM adds authentication — a wrong key, wrong volume or tampered record is detected, not silently returned as garbage. The cost is that a record must be read whole before its plaintext is trusted; with the 100 MiB cap and whole-file buffering that is fine.

**Rules:**
- Never encrypt two different plaintexts under the same (key, nonce): seqs never reused; no in-place re-encryption. A future "re-key" means writing a new volume with a new volume ID.
- Keys are cached in node memory only, never written to node disks.
- Optionally wrap keys with a site key-encryption key held by nodes/tools. Key handling lives entirely in the catalogue, so the key hierarchy can evolve later **without touching disks**. A KEK per **retention cohort** (e.g. class × month) would make expiry shredding immediate even for PG backups and key exports: destroy the cohort KEK.

**Crypto-shredding in bulk:** destroy a bucket's key (retention expiry). This covers every replica, offline disk, SSD remnant and tape copy of its volumes. Shredding completes once the destroyed key has also aged out of PG backups, WAL archives and key exports — the key-material retention bound N — or immediately with cohort KEKs.

**Consequences to accept (v2):**
- **Catalogue loss = loss of all encrypted data.** Synchronous replication, backups and a key export (separate media) are mandatory. The export is small — one key per bucket.
- Residual key copies in freed PostgreSQL pages: mitigate with periodic table rewrite of the keys table; a KEK hierarchy can remove the risk later without format change.
- Recovery from disks alone gives structure but not content.

## 12. Permanence classes (review, Nov 2026)

| Class | What | Implication |
|---|---|---|
| Forever | HTTP API contract (paths, digest/condition semantics), bucket-name and file-name rules, shard function, ID spaces (node, volume, cluster), seq-never-reused rule | Get right before the first client integrates |
| Application's choice | Bucket and name conventions (e.g. `<class>-<UTC day>`, `<timestamp>/<uuid>/<role>`), per-bucket S | Changeable for new buckets at any time |
| Life of the data (5 years, up to 15–20 with extended retention) | On-disk format v1 (all ciphers), EC shards; record framing incl. the two-region layout, name and sha256; CRC choice; metadata size limit; nonce construction | Readers support every version and cipher ever written; prelude identifies version |
| Life of the deployment | Migration mechanism (`schema_migrations`, `catalogue_meta`, compat checks); erasure and deletion lists; per-shard ordinals | Must exist in release one |
| Changeable | `.idx` / `.nidx` formats (rebuildable caches), internal locator encoding, routing details, placement, group-commit window, memory budget, idle timeouts, index cache policy, coordinator, repair automation, key hierarchy, PG topology | Simplify freely in MVP |

## 13. Catalogue initialisation and schema migration

**Goals:** simple command-line steps; every step **idempotent** (safe to rerun after any failure or interruption) and **transactional** (all or nothing); works offline in the air gap; supports rolling upgrades over 15–20 years.

**Commands:**

```
store db bootstrap-sql              # print CREATE ROLE / CREATE DATABASE SQL for the DBA (superuser, once)
store db init --cluster-name NAME   # create schema + cluster identity; no-op if already initialised as NAME
         [--cluster-id ID]          #   rebuild case: reuse the cluster ID found in volume headers
store db status                     # schema version, pending migrations, min compat, live nodes' versions, blockers
store db migrate [--dry-run]        # apply pending migrations in order; no-op if none pending
store db verify                     # applied-migration checksums + live schema vs expected snapshot
```

Nodes and tools **never run DDL**; they only check compatibility.

**Mechanics:**
- PostgreSQL DDL is transactional → each migration runs in **one transaction together with the insert of its `schema_migrations` row**. A crash mid-migration leaves nothing; rerunning resumes at the first unapplied migration.
- `init` runs all migrations and inserts the `catalogue_meta` row (cluster ID, name, created_at, initial incarnation) in **one transaction** — a database is either fully initialised or untouched. `init` on an initialised database: same name (and ID, if given) → success, no-op; different → error.
- `migrate` on an uninitialised database refuses ("run `store db init`") — pointing at the wrong host never silently creates a catalogue.
- **Serialisation:** every `store db` command first takes `pg_advisory_xact_lock(<fixed key>)`, then re-reads state. Concurrent operators or retried scripts can't interleave.
- **Dry run:** apply for real inside the transaction, print the result, `ROLLBACK`.
- Each migration sets `SET LOCAL lock_timeout` (e.g. 5 s) and `statement_timeout`; on timeout the transaction aborts cleanly and the operator reruns. Node transactions are short, so this rarely triggers.
- **Transactional statements only.** The catalogue is small (millions of rows), so plain `CREATE INDEX` and in-transaction backfills are fine — no `CREATE INDEX CONCURRENTLY`, no `VACUUM` in migrations. Prefer `text` + `CHECK` constraints over enum types.

**Migrations:**
- Plain SQL files embedded in the binary (`go:embed`), numbered `0001_initial.sql`, `0002_…`; no external migration tool (air gap, single binary, readable during a rescue). PL/pgSQL `DO` blocks allowed for data fixes.
- Header comments declare `-- kind: expand` or `-- kind: contract`, and for contract `-- min-compat: N`.
- **Immutable once released:** `schema_migrations` stores the SHA-256 of each applied file; `migrate` and `verify` refuse on mismatch.
- **No down migrations.** Binary rollback works because expand migrations are backward compatible; undoing a contract migration = restore the pre-upgrade backup.
- `verify` also compares the live schema (from `pg_catalog`) with an expected-schema snapshot embedded in the binary — catches hand-made changes after a manual rescue.

**Tables:**
- `schema_migrations(version, name, kind, sha256, applied_at, applied_by)` — `applied_by` = binary version + host.
- `catalogue_meta` (single row): `cluster_id`, `cluster_name`, `schema_version`, `min_compat`, `incarnation`.

**Compatibility and rolling upgrades:**
- Each binary carries `requires_schema` (lowest schema version it needs) and `compat_level`.
- At startup **and on every heartbeat**, nodes and tools check: `schema_version ≥ requires_schema` (else "run `store db migrate` first") and `min_compat ≤ compat_level` (else "binary too old"). A running node that fails the check seals its open volumes, releases its shard leases and exits.
- Nodes record binary version and compat level in their heartbeat row.
- **Expand** migrations (new table / column / index, nullable or defaulted columns) are safe for old binaries → applied any time.
- **Contract** migrations (drop, rename, tighten constraints) raise `min_compat` in the same transaction. `migrate` applies one only if every **live** node's compat level ≥ the new `min_compat`; otherwise it stops before it and `status` lists the blocking nodes. Offline nodes don't block — an old binary refuses to start afterwards.
- The contract half of a change ships at least one release after its expand half.

**Upgrade runbook (every step rerunnable):**
1. Catalogue backup (base backup or dump) — mandatory before any contract migration.
2. `store db migrate` with the new binary — expands apply; contracts wait while old nodes are live.
3. Rolling restart of nodes onto the new binary (each releases its shard leases gracefully).
4. `store db migrate` again — remaining contracts apply.
5. `store db verify`.

**New installation:** `store db bootstrap-sql` (DBA runs it) → `store db init --cluster-name X` → `store admin assign-node-id` per node → start nodes.

**Catalogue rebuild from disks:** empty database → `store db init --cluster-name X --cluster-id <from volume headers>` → `store scan rebuild` (buckets, shards, ordinals and locations from volume headers; re-applies the exported erasure list; v2 also needs the key export).

**Wrong-database protection:**
- Nodes and tools never auto-initialise; an empty or uninitialised database → refuse.
- Nodes keep the cluster ID in local state; mismatch with `catalogue_meta.cluster_id` → refuse. Combined with GC never deleting unknown volumes (§6), a node pointed at a fresh or wrong catalogue can't destroy data.

**Testing (CI):** apply all migrations from empty and from every released schema snapshot; run `migrate` twice (second run must be a no-op); run `verify` against the snapshot; run the previous release's binary against the post-expand schema.

## 14. Implementation and testing approach

**Principle:** permanent rules live in files the agent reads and in checks it can run; every dependency (clock, randomness, disks, network, catalogue) is injected so each layer is testable alone.

**Foundations before code:**
- Design notes, the **on-disk format spec (`docs/format-v1.md`)** and the **HTTP API spec (`docs/api.md`)** in the repo — the specs are the source of truth; code implements them.
- `CLAUDE.md` with the invariants and foot-guns (§10).
- **One definition of done:** `make check` = gofmt, `go vet`, staticcheck, `go test -race ./...`.
- **Mechanical protection of permanent artefacts:** CI fails if a released migration or a format golden file changes.
- Small tasks: one package / behaviour per task, tests in the same change. Review closely whenever a test is changed to make it pass.

**Package layering:**

```
format/     pure encode/decode of prelude, headers, records, seal   (no I/O)
names/      pure bucket/name validation, shard function
disk/       FS interface: real, in-memory, fault-injecting
catalogue/  Catalogue interface: PostgreSQL impl + in-memory fake
volume/     single-writer append, group commit, seal, erase, .idx   (uses disk/, clock)
nameindex/  open-volume map, .nidx build (in memory, write once) / read (positioned reads), file-handle LRU
transport/  node-to-node RPC (forwarding, replica I/O): HTTP impl + in-memory
node/       API handlers, routing, shard leases and handover, lookup, reconcile/GC
scan/, admin/, db/   tools
sim/        deterministic multi-node simulator
cmd/store/  wiring only
```

`Clock` and random source injected everywhere — no `time.Now()`, real sleeps or global rand in tested code.

**Test layers:**

| Layer | What | How | Speed |
|---|---|---|---|
| L0 pure | format, names, shard function, CRC, nonce construction, `.nidx` table encode/decode | Table tests, round-trip properties, Go fuzzing (decoder never panics on corrupt bytes; scanner resyncs), golden files per format version kept forever | ms |
| L1 component | volume writer, erasure, `.idx`, `.nidx` | In-memory and fault-injecting FS: short writes, fsync errors, ENOSPC, torn writes, crash at any write → reopen and check committed state; corrupted / truncated / wrong-version index → detected on read and rebuilt; fingerprint collisions handled (extra read, never a wrong answer); no content hash or plaintext name in any index file | ms |
| L2 catalogue | queries, shard/volume leases, ordinals, epochs, migrations | PostgreSQL in a container (testcontainers-go), DB per test package. Migrations: empty → latest, each released snapshot → latest, run twice, `verify`. Competing sessions for leases and the advisory lock | s |
| Contract | fake vs real catalogue | Same suite runs against the in-memory fake and PostgreSQL — keeps the fake honest for the simulator | s |
| L3 simulation | multi-node orchestration | N nodes in one process, in-memory disks/transport/catalogue, seeded deterministic scheduler, virtual clock. Faults: crashes, disk loss, partitions, slow replicas, shard handover, catalogue failover. Invariants checked after every step; failing seeds replay exactly | ms/run, thousands of seeds in CI |
| L4 end-to-end | real binaries | Docker Compose: PostgreSQL primary + standby, 3–5 nodes, small loop/tmpfs disks. `kill -9`, disk full, failover runbook, shard handover, `scan rebuild` drill | min, nightly |

**Simulation invariants (write first):**
- Every acknowledged `PUT` stays readable by name while at least one replica survives, until a newer version or an erase.
- `GET` by name always returns the latest acknowledged version (highest ordinal, seq).
- A `PUT` with content identical to the latest version never stores anything.
- At most one node holds a (bucket, shard) lease, and only it appends to that shard's volumes.
- No volume ever has two active writers; no seq reused within a volume.
- A restarted node never appends to a volume it had open before the restart (it seals it and opens a new one).
- Replicas agree up to committed length (modulo in-progress erasures).
- Deleted buckets never come back; GC never deletes a volume unknown to the catalogue.
- Erased names stay erased after reconcile, handover and rebuild.

**Build order:**
1. Format spec + golden files → `format/` with fuzzing; `names/`.
2. `db/` migrations + `catalogue/` against PostgreSQL container; in-memory fake + contract suite.
3. `disk/` fault FS → `volume/` (append, group commit, seal, crash recovery, `.idx`).
4. `nameindex/` (open-volume map, `.nidx` hash-table build/read, file-handle LRU).
5. Single-node name-addressed write/read over HTTP with dedup and conditions.
6. Simulator + invariants → multi-node forwarding, shard leases, handover, replication, seal-on-failure, reconcile.
7. `scan verify` / `scan rebuild`, erasure.
8. L4 Compose drills; rehearse failover runbook.

**Agents vs human review:**
- Agents: scaffolding, table tests, fuzz harnesses, fakes, simulator fault types, CLI plumbing, turning failing seeds into regression tests.
- Human review: format code, shard function, lease/handover/epoch logic, dedup ordering, GC deletion conditions, migrations, any weakened assertion.
- Each agent task gets: the relevant notes section, the invariant it must not break, the test layer to extend.
- Fast loop (L0–L1 + a few hundred seeds) under ~30 s so it runs on every change; larger seed sweeps and L4 in CI.

## 15. Embedding in a host service

The store must later run inside a standard host service: trivial to start from `main` (or any function) and to stop again. `cmd/store` is only a thin host of this API.

```go
type Deps struct {
    Clock Clock; Rand io.Reader; FS disk.FS
    Catalogue catalogue.Catalogue; Transport transport.Transport
    Alarms Alarms          // optional; defaults to a no-op sink
    Logger *slog.Logger
}
func Start(ctx context.Context, cfg Config, d Deps) (*Node, error) // returns once ready, or fails
func (n *Node) Handler() http.Handler    // host may mount it, or cfg may name a listener
func (n *Node) Metrics() Metrics         // getter interface
func (n *Node) Stop(ctx context.Context) error // drain, seal open volumes, release leases, close handles
```

**Alarms** — interface implemented by the host, called by the node: `Raise(id AlarmID, severity Severity, detail string)` and `Clear(id AlarmID)`. Raise is idempotent per ID (a persistent condition does not flood the host); calls are non-blocking and goroutine-safe. Details never contain names, metadata or keys. Candidates: catalogue unreachable, replica unavailable / seal-on-failure, disk full, index rebuild, shard lease lost, memory budget exhausted, erasure-list backlog.

**Metrics** — interface implemented by the node, polled by the host: typed getters plus a `Snapshot()` of named counters (monotonic) and gauges (point in time). No metrics library dependency; the host adapts values to its own exposition. Candidates: puts stored / deduplicated / rejected, gets, bytes in / out, fsync latency sum and count, open and sealed volumes, in-flight buffer bytes vs budget, index-handle cache size, forwarded requests, leases held.

**Constraints**
- `Start`/`Stop` are restartable in-process (several nodes per test process; `sim/` relies on it).
- No package-level state, `os.Exit` or `log.Fatal` outside `cmd/`.
- Stop joins all goroutines (leak test). A clean Stop and a crash recover identically: a restarted owner seals, never resumes.
- Alarm details and metric labels never carry names, metadata or keys.

**Open:** package name for the embedding API; whether the node owns the HTTP listener, the host mounts `Handler()`, or both (node-to-node transport likely needs its own listener).

## Open threads

- Node/drive sizing for small / medium / large deployments; server count vs eventual EC failure domains.
- Default S per deployment; guidance for per-bucket S (busy vs late-data-heavy classes).
- Shard lease duration and grace; acceptable handover time; whether a warm standby owner is ever needed.
- `.nidx` bucket size and load factor; index file-handle LRU size and idle timeout.
- Application side: retention as legal maximum vs minimum; list of retention classes in its bucket convention; final name convention.
- **Legal hold on a subset** of a bucket (e.g. one case): needs copying to another bucket or keeping the whole bucket.
- Whether best-effort erasure (no tape, SSD FTL remnants) satisfies the legal requirement — confirm with whoever owns compliance.
- Encryption: key-material retention bound N (= bulk shredding lag); KEK per retention cohort (immediate expiry shredding incl. backups) vs a single KEK; bucket key with derived volume sub-keys (current direction) vs per-volume keys; timing of the platform team's disk encryption (LUKS) and whether record encryption is then needed for anything beyond bulk shredding.
- Whether late-arriving data needs a short SSD stay.
- Volume size choice (≤ 32 GiB).
- Per-node memory budget for buffered requests; group-commit handling of large files.
- Metadata size limit; whether to duplicate metadata into the seal footer.
- Operator SLA for manual re-replication; whether to enable the last-copy safety net.
- Whether the MVP needs group commit at all (≈12 files/s small, a few appends/s per disk large — measure one fsync per append first); if yes, the window. Seal-on-slow threshold.
- Idle timeouts by write recency; open-volume caps per node/disk (sized for straggler volumes); actual late-data rate and spread.
- Placement cutoff (85% vs 90%) and repair-headroom sizing per deployment.
- Migration lock/statement timeouts; definition of "live" node for contract-migration checks (heartbeat age).
- Open-volume size cap vs restart/handover time on HDD (full-volume pass ≈ 40 s per 8 GB); optional periodic checkpoint of the name map.
- Zombie-owner reads: a paused old owner can answer `GET` or dedup from stale state after handover (epoch fencing only stops appends) — bound with a lease-validity margin, or accept and document.
- PostgreSQL vs a small Raft store (etcd) for leases/epochs — revisit before committing to the PG failover runbook.
- Indeterminate outcomes: after a `503` mid-write an unacked record can later surface as the latest version if the client never retries — document in `docs/api.md`.
