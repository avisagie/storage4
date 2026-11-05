# Node (`node/`), single-node

Status: milestone 6, **design for review (2026-11-05); no code yet.** Human gate: "Dedup and version ordering" (§5). Sources: design-notes §6, §9, §15; `docs/api.md`; `docs/volume.md`; `docs/nameindex.md`; `docs/catalogue.md`.

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
- **Volume ID counter** (48 bit, never goes back): a small file `node-counter` is written atomically (`WriteFileAtomic`) to **every** disk; the value is the highest ID block reserved. Blocks of 256 are reserved by writing `max over readable disks + 256` to all disks before use. Startup takes the maximum over all readable disks. It is lost only if all disks lose it (then the node must get a new node ID: operator action, documented). *Review: confirm this instead of "first disk only".*

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

1. The shard's open volumes' `OpenMap`s, ordered by volume number descending.
2. The shard's sealed volumes, volume number descending (from the catalogue, cached), through `nameindex.Store.Lookup`, which verifies magic, CRC, seq and name in the record.

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
- **Seal procedure** (epoch bump, minimum committed length): `BeginSeal` (catalogue, epoch bump, state `sealing`) → `SealFile` on every replica at the minimum committed length → `.idx` and `.nidx` written on every replica from the in-memory data → `FinishSeal`. A failure part-way leaves the volume `sealing`; the next seal pass (restart, lease acquire) finishes it. `ErrShortReplica` (a replica is shorter than the minimum we computed) marks that replica lost in the catalogue; the volume is under-replicated, which is a milestone-7/8 concern (alarm only here).
- **Replica failure on append**: the volume's writers are dead (`ErrFailed`); the node seals at the minimum committed length, raises `replica-unavailable`, opens a new volume and retries the batch once. Unacked entries in the failed batch were never acked, so retrying them is safe. A second failure returns `503`.
- No healthy 3-disk set for a new volume → `503` + `Retry-After`.

## 7. Shard leases, restart, Stop

- First write to (bucket, shard): `AcquireShard` (TTL 30 s); a renewal loop renews at TTL/3. A renewal failure raises `shard-lease-lost`, stops accepting writes for the shard (`503`), seals its open volumes as far as the fence allows, and drops the lease. Before each append batch the committer checks that the lease has at least TTL/3 left on the injected clock; otherwise `503`. Idle shards (no open volume) release their lease.
- **Start**: for each local disk, list the catalogue's volumes on it that this node still has open or sealing, acquire the shard lease, seal as in §6 (minimum committed length over the local replicas; with local replicas only this is straightforward), write indexes, `FinishSeal`, release leases. Only then does `Start` return. Cost is bounded by the open-volume cap and size cap. Leftover empty files from a crash before registration are ignored.
- **Stop(ctx)**: refuse new requests, drain in-flight ones, seal all open volumes (same procedure, same as a crash recovery would do), release leases, close handles, join every goroutine. A leak test checks it.
- Reads of sealed volumes need no lease. Zombie-owner reads after a lost lease stay an accepted open thread.

## 8. HTTP handlers

Per `docs/api.md`. Order of checks for a PUT: auth (`401`) → query string (`400`) → bucket/name validation (`400`) → bucket state (`410` deleted; implicit create with `DefaultShards`) → `Content-Length` over cap (`413`) → memory budget reservation (`503` + `Retry-After` immediately, never queues) → multipart parse (metadata part first, size checks before the file is buffered) → buffer and sha256 → `Content-Digest` (`400`) → §5. `GET` streams from the verified head and aborts the connection if the payload CRC fails after bytes were sent. `GET /m/` returns the latest version's metadata (`204` if none). `HEAD` needs only region A plus lengths. `DELETE /b/{bucket}` marks the bucket deleted in the catalogue (`DeleteBucket`) and returns `202`; GC of the volume files is milestone 8. `DELETE /b/{bucket}/{name}` returns `501` until milestone 8 (decided 2026-11-05). Errors are one-line `text/plain` without names or internals.

**Memory budget**: a byte-counting semaphore (`TryAcquire(n)`). A request reserves `Content-Length` (or the 100 MiB cap + 16 KiB when unknown, shrinking after the body is read), releases when the response is complete. `MemoryBudget` must be at least that cap, enforced in `Start`.

## 9. Erasure list (wired, empty in milestone 6)

The owner loads `ShardErasures` when it takes a shard lease and consults it on every hit. Rule for milestone 8 to keep: a hit whose locator is on the list means the **name is erased** (stop searching; do not fall through to an older volume). Open item: `nameindex.Store.Lookup` currently skips an erased-in-place candidate and goes on to older volumes; while an erasure is half applied that could surface an older live version. Milestone 8 changes `Lookup` to report "erased candidate" so the node can stop. Not reachable in milestone 6 (no erasures).

## 10. Alarms and metrics

Alarms (idempotent per ID, no names): `catalogue-unavailable`, `replica-unavailable`, `disk-excluded`, `shard-lease-lost`, `index-rebuilt`, `memory-budget-exhausted`. Metrics: counters (puts stored, deduplicated, rejected, gets, bytes in/out, seals, fsync count and nanoseconds, index rebuilds) and gauges (open and sealed volumes, buffered bytes vs budget, index handles, leases held), with `Snapshot()`. The catalogue calls a heartbeat per TTL/3 from the same loop as lease renewal.

## 11. Goroutines (all joined in `Stop`)

Per open volume: one committer. Per node: one maintenance loop (lease renewals, heartbeat, `nameindex.CloseIdle`, idle-seal checks, via `Clock.After`), the optional `http.Server`. Request goroutines belong to `net/http`; `Stop` waits for in-flight requests.

## 12. Tests

- L1: PUT/GET/HEAD/metadata round trips; dedup (same bytes, same metadata → `200`; same bytes, new metadata → `201`; A→B→A); conditions; concurrent identical PUTs of one name store one version; concurrent different names share a group commit; replica failure (faulty FS: fsync error, ENOSPC, torn write) seals and retries on a new volume, no ack without all three fsyncs, no seq reuse; crash at any write (`Mem.Crash`) then `Start` seals leftovers and every acked PUT is readable; memory budget `503`; `Stop`/`Start` in one process, several nodes in one process, goroutine leak check; reads verify names (collision via injected fingerprint); `DELETE` bucket then `410`.
- In-process e2e: the same HTTP suite against `catalogue/mem`, SQLite and PostgreSQL (`STORAGE4_TEST_PG_DSN` or testcontainers once added).

## Open items for review

1. `node-counter` on every disk, max on startup (§3).
2. `Clock.After` added (§2); group commit without a window (§6).
3. Fewer than three usable disks → `503`, no degraded mode (§3).
4. `Lookup` change for erased candidates, scheduled in milestone 8 (§9).
