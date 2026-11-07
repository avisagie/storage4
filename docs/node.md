# Node (`node/`), single-node

Status: milestone 6, design approved 2026-11-05 (human gate: "Dedup and version ordering", §5); **implemented 2026-11-05, awaiting code review** (see "Implementation notes" at the end). Sources: design-notes §6, §9, §15; `docs/api.md`; `docs/volume.md`; `docs/nameindex.md`; `docs/catalogue.md`.

Scope of milestone 6: one node, several local disks, real catalogue (mem, SQLite or PostgreSQL), HTTP API, no transport (no forwarding, no remote replicas). The node's shape (`Deps`, replica seam, goroutine ownership) is what `sim/` will drive in milestone 7, so it is fixed here.

## 1. Invariants touched

- Only the shard-lease holder opens or appends to a shard's volumes; the lease is held **before** the first write.
- Latest version = highest (volume number, seq). Dedup compares only with the latest version.
- Ack only after **all** replicas fsync. Replica failure: seal, don't repair, retry on a new volume.
- A restarted owner never resumes an open volume: it seals it and opens a new one. Clean `Stop` and crash recover identically.
- Every read verifies the record header, including the name. Indexes are caches.
- Metadata is opaque, never parsed, never logged. Alarms and metrics never carry names, metadata or keys.
- No `time.Now`, `time.Sleep`, global rand or direct `os` use. Nodes never run DDL; refuse to start on an uninitialised catalogue or cluster-ID mismatch.

Tests: L1 (component, with `disk.Mem`/faulty FS and `catalogue/mem`) and in-process e2e over HTTP against mem, SQLite and PostgreSQL (the same suite).

## 2. Embedding API

```go
type Config struct {
    NodeName     string        // operator-chosen; AssignNodeID(name) is idempotent
    ListenAddr   string        // "" = host mounts Handler()
    Token        string        // shared bearer token (api.md §4)
    DefaultShards int          // implicit bucket creation (default 4)
    MemoryBudget int64         // bytes of buffered request bodies (default 256 MiB, must be >= 100 MiB + metadata)
    LeaseTTL     time.Duration // 30 s; renewed every TTL/3
    VolumeMaxSize int64        // open-volume size cap (default 1 GiB)
    VolumeIdle   time.Duration // seal after this long without a write (default 1 h)
    MaxOpenVolumes int         // 64; LRU seal beyond it
    NidxHandles  int; NidxIdle time.Duration
}
type Deps struct {
    Clock clock.Clock; Rand io.Reader
    Disks []disk.FS            // one FS per local disk (each has its identity file)
    Catalogue catalogue.Catalogue
    Alarms Alarms; Logger *slog.Logger
    // Transport: added in milestone 7
}
```

Deviation from design-notes §15: `FS` is `Disks []disk.FS` because a node manages many disks and every disk is its own filesystem root (identity file per disk). `Start` validates the config, checks the catalogue (initialised, cluster ID equals the disks' identity files), assigns/loads the node ID, registers the disks, seals leftovers (§7), starts goroutines and returns when ready. `Handler()`, `Metrics()`, `Stop(ctx)` as in §15. No package-level state; several nodes per process.

**Clock.** Background work needs waiting. `clock.Clock` gets `After(d) <-chan time.Time` (and `Fake` fires it from `Advance`). No other timer API; no `time.Sleep` anywhere.

**Listener.** `Handler()` always exists; `ListenAddr` is optional and, if set, the node runs an `http.Server` on it and joins it in `Stop`. Node-to-node transport gets its own listener in milestone 7.

## 3. Disks, identity, volume IDs

- At `Start` each disk's identity file is read (`disk.ReadIdentity`); cluster ID must match the catalogue; disks missing from the catalogue are registered; a disk whose catalogue state is `gone`, `readonly` or unhealthy takes no new volumes. A disk that fails to read its identity is excluded and an alarm is raised; fewer than 3 usable disks means writes return `503` (replication factor is 3, no degraded mode).
- Node ID: `AssignNodeID(NodeName)`.
- **Volume ID counter** (48 bit, never goes back): a small file `node-counter` is written atomically (`WriteFileAtomic`) to every disk, **best effort**; the value is the highest ID block reserved. Blocks of 256 are reserved by writing `max over readable disks + 256`; the reserve is usable once it is on at least one disk, and only a write failing on every disk refuses an ID (`503`). Startup takes the maximum over all readable disks and the catalogue's highest volume ID for this node, so an ID handed out but never registered (no file exists for it) may be reused harmlessly (decided 2026-11-06). It is lost only if all disks lose it (then the node must get a new node ID: operator action, documented). 

## 4. Replica seam

`volume.Writer` is one replica of one volume on one disk. The node wraps replicas behind an internal interface so milestone 7 can add remote ones:

```go
type replica interface {            // one replica of one volume
    Append(entries []volume.Entry) ([]volume.Locator, error) // durable on return
    Committed() int64
    Seal(epoch uint32) error         // at its own committed length, or SealFile at a given one
    ReadHead(loc volume.Locator, name string) (*volume.Head, error)
    Close() error
}
```

Milestone 6 has only `localReplica` (a `volume.Writer` plus the disk's `FS`). An open volume holds three replicas on three distinct disks. Placement with exactly three usable disks is "all three"; power-of-two choices arrives with more disks in milestone 7.

## 5. Lookup and dedup (human gate)

### What "latest" is and how it is found

For (bucket, name) the owner computes `shard = names.Shard(name, S)`, then searches **newest first**:

1. The shard's open volume's `OpenMap`, and the `OpenMap`s of **pending** volumes (closed for writes, seal not completed; see "Seals that cannot complete").
2. The shard's sealed volumes (from the catalogue, cached), through `nameindex.Store.Lookup`, which verifies magic, CRC, seq and name in the record.

Open, pending and sealed volumes are visited as one list ordered by volume number descending. A volume that no replica can read makes the lookup *uncertain* (see "Unreadable volumes"); it never falls through to an older version.

The first verified hit is the latest version: volume numbers only grow and `.nidx` keeps only the highest seq per name per volume. Clocks are never consulted.

The result is `{locator, sha256, volume number}`; metadata is read from the record only when needed (below).

### The PUT sequence on the owner

1. `lockName(bucket, name)`: striped mutex, 256 stripes keyed by a hash of (bucket, name). Held until the append is acked and the `OpenMap` is updated, or until the request returns without writing. Different names proceed in parallel and still share group commits. Needed because lookup-then-append is not atomic: a client retry can overlap its first request, and two identical PUTs (or two `If-Match` writers) could otherwise both pass the check.
2. Look up the latest version. A name on the erasure list (§9) counts as not existing.
3. Conditions, against the latest version: `If-None-Match: *` and a version exists → `412`; `If-Match` and the ETag differs (or no version) → `412`.
4. **Dedup**: if the latest version's sha256 equals the new file's sha256, read region A of that record (`ReadHead`) and compare the metadata bytes (absent = empty). Both equal → `200`, nothing stored. Otherwise continue. (Metadata is compared only after a sha256 match; decided 2026-11-05: a metadata change with identical content is a new version.)
5. Append to the shard's open volume (§6), wait for all replicas to fsync.
6. Only **after** the ack: add the record to the volume's `OpenMap`; unlock; respond `201` with `ETag` and `Repr-Digest`.

### Rules the code must keep

- The `OpenMap` is updated after the ack, never before. A phantom entry for a record that was not durably stored would make a retry dedup to `200` for data that does not exist.
- A failed append disables the volume: the node seals it (§6) and **discards its `OpenMap` entries that are beyond the sealed committed length**; the retry goes to a new volume. Since any entry is added only after the ack, the map never holds unacked records.
- A → B → A, and metadata M1 → M2 → M1, store the second A / M1 again. Intended: "latest" must be rebuildable from disk alone.
- After handover or restart an unacked record can be the latest version (design-notes §6, "indeterminate outcomes"); a retry dedups against it and returns `200`. Intended.
- Version order is the pair (volume number, seq), never a timestamp.

## 6. Open volumes

- One open volume per (bucket, shard) that is being written; `OpenVolume` (catalogue, lease-checked) assigns the volume number and volume ID, three replicas are created on three distinct disks (`volume.Create`), the volume gets a committer goroutine.
- **Group commit without a timer**: the committer takes every append queued since it last started, writes them as one `Append` to each of the three replicas in parallel, waits for all three, acks each caller. Batching happens naturally while the previous fsync runs. No window, so no timers in the hot path; a window can be added later with `Clock.After` if measurements ask for it. (Deviation from the default list in the review conversation: that was "2 ms window".)
- Seal triggers: size cap, idle timeout, replica failure or ENOSPC, lease loss, `Stop`, the open-volume cap (least recently written first).
- **Seal procedure** (epoch bump, minimum committed length): `BeginSeal` (catalogue, epoch bump, state `sealing`) → `SealFile` on every replica at the minimum committed length → `.idx` and `.nidx` written on every replica from the in-memory data → `FinishSeal`. A failure part-way leaves the volume `sealing` and the volume **pending**: `tick` retries, and so do `Stop`, a restart and the next lease acquisition. The volume is finished only if at least one replica really sealed; with none sealed it is left unsealed (never recorded as 0 records), and replicas are marked `failed` only after another replica sealed. All replicas sealed from their files share one seal check, so they stay byte-identical. Details: "Seals that cannot complete". `ErrShortReplica` (a replica is shorter than the minimum we computed) marks that replica lost in the catalogue; the volume is under-replicated, which is a milestone-7/8 concern (alarm only here).
- **Replica failure on append**: the volume's writers are dead (`ErrFailed`); the node seals at the minimum committed length, raises `replica-unavailable`, opens a new volume and retries (up to three attempts per request, each on a fresh volume). Unacked entries in the failed batch were never acked, so retrying them is safe. If every attempt fails the request returns `503`.
- No healthy 3-disk set for a new volume → `503` + `Retry-After`.

## 7. Shard leases, restart, Stop

- First write to (bucket, shard): `AcquireShard` (TTL 30 s); a renewal loop renews at TTL/3. A renewal failure raises `shard-lease-lost`, stops accepting writes for the shard (`503`), seals its open volumes as far as the fence allows, and drops the lease. Before each append batch the committer checks that the lease has at least TTL/3 left on the injected clock; otherwise `503`. Idle shards (no open and no pending volume) release their lease.
- **Start**: for each local disk, list the catalogue's volumes on it that this node still has open or sealing, acquire the shard lease, seal as in §6 (minimum committed length over the local replicas; with local replicas only this is straightforward), write indexes, `FinishSeal`, release leases. Only then does `Start` return. If the replicas are readable but none can be sealed, `Start` fails with `errNoReplicaSealed` instead of recording the volume as empty. Cost is bounded by the open-volume cap and size cap. Leftover empty files from a crash before registration are ignored.
- **Stop(ctx)**: refuse new requests, drain in-flight ones, seal all open volumes (same procedure, same as a crash recovery would do), release leases, close handles, join every goroutine. A leak test checks it.
- Reads of sealed volumes need no lease. Zombie-owner reads after a lost lease stay an accepted open thread.

## 8. HTTP handlers

Per `docs/api.md`. Order of checks for a PUT: auth (`401`) → query string (`400`) → bucket/name validation (`400`) → bucket state (`410` deleted; implicit create with `DefaultShards`) → `Content-Length` over cap (`413`) → memory budget reservation (`503` + `Retry-After` immediately, never queues) → multipart parse (metadata part first, size checks before the file is buffered) → buffer and sha256 → `Content-Digest` (`400`) → §5. `GET` streams from the verified head and aborts the connection if the payload CRC fails after bytes were sent. `GET /m/` returns the latest version's metadata (`204` if none). `HEAD` needs only region A plus lengths. `DELETE /b/{bucket}` marks the bucket deleted in the catalogue (`DeleteBucket`) and returns `202`; GC of the volume files is milestone 8. `DELETE /b/{bucket}/{name}` returns `501` until milestone 8 (decided 2026-11-05). Errors are one-line `text/plain` without names or internals.

**Memory budget**: a byte-counting semaphore (`TryAcquire(n)`). A request reserves `Content-Length` (or the 100 MiB cap + 16 KiB when unknown, shrinking after the body is read), releases when the response is complete. `MemoryBudget` must be at least that cap, enforced in `Start`.

## 9. Erasure list (wired, empty in milestone 6)

The owner loads `ShardErasures` when it takes a shard lease and consults it on every hit. Rule for milestone 8 to keep: a hit whose locator is on the list means the **name is erased** (stop searching; do not fall through to an older volume). Open item: `nameindex.Store.Lookup` currently skips an erased-in-place candidate and goes on to older volumes; while an erasure is half applied that could surface an older live version. Milestone 8 changes `Lookup` to report "erased candidate" so the node can stop. Not reachable in milestone 6 (no erasures).

## 10. Alarms and metrics

Alarms (idempotent per ID, no names): `catalogue-unavailable`, `replica-unavailable`, `disk-excluded`, `shard-lease-lost`, `index-rebuilt`, `memory-budget-exhausted`, `seal-pending`. Metrics: counters (puts stored, deduplicated, rejected, gets, bytes in/out, seals, fsync count and nanoseconds, index rebuilds) and gauges (open and sealed volumes, buffered bytes vs budget, index handles, leases held, pending seals), with `Snapshot()`. The catalogue calls a heartbeat per TTL/3 from the same loop as lease renewal.

## 11. Goroutines (all joined in `Stop`)

Per open volume: one committer. Per node: one maintenance loop (lease renewals, heartbeat, `nameindex.CloseIdle`, idle-seal checks, via `Clock.After`), the optional `http.Server`. Request goroutines belong to `net/http`; `Stop` waits for in-flight requests.

## 12. Tests

- L1: PUT/GET/HEAD/metadata round trips; dedup (same bytes, same metadata → `200`; same bytes, new metadata → `201`; A→B→A); conditions; concurrent identical PUTs of one name store one version; concurrent different names share a group commit; replica failure (faulty FS: fsync error, ENOSPC, torn write) seals and retries on a new volume, no ack without all three fsyncs, no seq reuse; crash at any write (`Mem.Crash`) then `Start` seals leftovers and every acked PUT is readable; memory budget `503`; `Stop`/`Start` in one process, several nodes in one process, goroutine leak check; reads verify names (collision via injected fingerprint); `DELETE` bucket then `410`.
- In-process e2e: the same HTTP suite against `catalogue/mem`, SQLite and PostgreSQL (`STORAGE4_TEST_PG_DSN` or testcontainers once added).

## Open items for review

1. `node-counter` is best effort: one disk suffices, `503` only if none takes it (§3, decided 2026-11-06).
2. `Clock.After` added (§2); group commit without a window (§6).
3. Fewer than three usable disks → `503`, no degraded mode (§3).
4. `Lookup` change for erased candidates, scheduled in milestone 8 (§9).

## Implementation notes (2026-11-05)

Where the code settled details this document left open:

- **Replicas are byte-identical.** Each replica's `volume.Writer` draws record `check` values from its own PCG, all seeded identically per volume from the node's random source, so every replica gets the same check and one locator serves all three. `commit` verifies that the replicas return equal locators. The seal record's check is covered too: replicas sealed by their writers share the sequence, and replicas sealed from their files (recovery, retry, a replica that is not clean) all get one check drawn per seal (2026-11-06).
- **Registration order.** `OpenVolume` (catalogue) runs before the replica files are created, because the header needs the volume number the catalogue assigns. A crash in between leaves a registered volume with no files; recovery marks the replicas lost and finishes the seal with length 0. A sealed volume with zero records is skipped by lookups; one with records but no usable replica is *unreadable* (see GET below).
- **Suspect disks.** A replica that fails an append marks its disk suspect for 10 minutes (injected clock): no new volume is placed there. Operators decide "gone"; the catalogue's replica state `failed` records the replica.
- **Bucket cache.** Bucket state is cached for 5 s (injected clock). `DELETE /b/{bucket}` clears this node's cache and abandons the bucket's open volumes at once (writers closed, no seal: the catalogue cleared the leases; pull-based GC removes the files).
- **Idle and lease ticks.** One maintenance goroutine ticks every TTL/3: renews leases that are due, seals idle volumes, retries pending seals, releases the lease of a shard with no open and no pending volume, closes idle index handles, refreshes disk states and heartbeats. A changed catalogue incarnation abandons all leases and raises `shard-lease-lost`.
- **GET.** The first 64 KiB are read and CRC-verified before headers go out, so small files are fully verified before the first byte. Larger payloads stream with a one-chunk lag: a chunk is sent only after the read that follows it succeeded, so the last chunk leaves only after the payload CRC verified and an abort leaves the client with a short body it can detect (an abort after the full `Content-Length` was written would look like success; fixed 2026-11-06). The record is released even when the transfer aborts. `docs/api.md` §2.2 says a corrupt replica is skipped in favour of another; the node does not do that yet (lookup uses the first replica that serves the record). Open item.
- **`Config.AllowSmallVolumes`** exists for tests only.
- **Tests run on three catalogues**: `catalogue/mem`, SQLite and PostgreSQL (`db/dbtest.Catalogue`, shared with the contract suite). The PostgreSQL runs skip without `STORAGE4_TEST_PG_DSN`.

Resolved the same day:

- **Restart within `NodeTTL`**: `Cluster.Goodbye(node, instance)` added to the catalogue (interface, mem, SQL, contract test). `Stop` calls it last, so a restart does not wait out the heartbeat TTL. A crash still holds the node ID until `NodeTTL` has passed, by design.
- **Fake vs SQL**: the only CHECK reachable through the interface that the fake did not mirror was the heartbeat's binary version (1..128 bytes); both now return `ErrInvalidArgument`, and the contract suite tests it. Rule going forward: every CHECK reachable through the interface gets a contract test.
- **Replica fallback on a corrupt read**: the owner reads from its replicas (all local in milestone 6, remote through the transport in milestone 7). If a copy fails verification before any byte is sent (first 64 KiB, including the payload CRC), `GET` retries with another replica and raises a warning alarm; if none is intact it returns `500`, never wrong bytes. A failure after streaming has started aborts the connection; since each chunk is sent only after the next read succeeded, the final chunk never leaves before the payload CRC verified, so the client sees a short body. Forwarding adds no second fallback: the receiving node simply relays what the owner returns.

Still open:

1. **testcontainers fallback** for `db/dbtest.OpenPostgres`: deferred (decided 2026-11-05); the local DSN is the default.
2. **Single-node load tests** before milestone 6 closes (mvp-plan.md).
3. **Zombie-owner reads** stay an accepted open thread (design-notes).

## Future: disk management

Today a failed append suspects the disk for 10 minutes (no new volumes there) and the replica is marked `failed`. Intended later, not built: a disk that fails an append or a verified read goes read-only in the catalogue automatically and raises an alarm to the operator; recovery and "gone" stay manual commands (design-notes §6, failure policy).

## Unreadable volumes (decided 2026-11-06)

Write availability comes first and reads tolerate errors, but an older version never stands in for the latest. If the newest volume that may hold a name has no replica any lookup can use (all failed, lost or unreadable), the lookup is *uncertain* and the node raises `replica-unavailable` (critical) and goes on:

- `GET`, `HEAD`, `GET /m/`: `503` + `Retry-After` for names the volume may hold (any name not found in a newer volume).
- `PUT` without conditions: proceeds and stores a new version (dedup is skipped; a new version is always the highest).
- `PUT` with `If-Match` / `If-None-Match`: `503` (the condition cannot be judged).
- Names found in the open volume or a newer sealed volume answer normally.

Recovery is operator-level (milestone 8: repair from another replica, or declare the volume lost). Corrupt copies found by a GET are excluded per (disk, volume), not per disk.

## Seals that cannot complete (decided 2026-11-06)

The owner is the only writer, and replicas never need to agree on anything to be sealed: everything acknowledged is on all replicas' stable storage, so the prefix up to the minimum committed length is byte-identical everywhere, and `SealFile` is idempotent (same length: no-op that makes the seal durable; other length: `ErrSealMismatch`). Anyone holding the lease can therefore finish a seal later, from the files, and nothing is copied between replicas except by repair (milestone 8).

- **Catalogue unreachable (one node or all)**: `BeginSeal` or `FinishSeal` fails. The volume stops being the open volume at once (slots freed, writes go on to a new volume) and becomes **pending**: its name map stays in the shard and lookups merge pending, open and sealed volumes by volume number, so acknowledged records stay readable and dedupable. `tick` retries the seal (from files, one seal check for all replicas), the shard keeps its lease while anything is pending, alarm `seal-pending`, gauge `pending_seals`. Stop retries once; a restart seals leftovers from the catalogue as always. Lease loss and bucket deletion are not covered: the pending list is dropped with a deleted bucket; after a lost lease the next acquisition seals the volume.
- **Replica IO error while sealing**: the other replicas seal, the volume is finished with its real length and count, and only the failing replicas are marked `failed` (after the fact, never before we know someone sealed).
- **No replica can be sealed but they are readable**: the volume is *not* recorded as empty (0/0); it stays unsealed and pending, no replica is marked failed, the seal is retried. At restart (`recoverVolume`) the same condition fails `Start` with `errNoReplicaSealed` rather than hiding the volume. A volume recorded with 0 records is only written when no replica is readable at all.
- **Byte-identical replicas**: one seal check is drawn per seal and used for every replica sealed from its file (replicas sealed by their writers already share the random sequence); `SealFile` on an already-sealed replica now fsyncs before returning.
