# Catalogue (`catalogue/`, `db/`)

Status: implemented 2026-11-05 (`catalogue/`, `catalogue/mem`, `catalogue/sqlcat`, `catalogue/cattest`, `db/`); design decisions 1–8 below confirmed by the reviewer 2026-11-05 (times in milliseconds, not microseconds). Migration 0001 approved 2026-11-05 (frozen); the human gate on the interface and the lease/ordinal/epoch SQL stays open until the code is reviewed. Rationale: design-notes §6, §8, §13.

## Scope

- `catalogue.Catalogue` is the **runtime API for nodes and tools**: buckets, shard leases and ordinals, volumes and replicas, the erasure list, nodes and disks. It never runs DDL and never initialises anything.
- `db/` is the **DDL side** (`store db init|migrate|status|verify`, embedded migrations). It is not part of the `Catalogue` interface, so no node can reach it.
- Three implementations behind one contract suite: PostgreSQL (`pgx`, production), SQLite (`modernc.org/sqlite`, tests/dev only), in-memory fake (simulator). All three are constructed with an **injected `Clock`**.
- Small, per-concern interfaces (`Buckets`, `Leases`, `Volumes`, `Erasures`, `Cluster`) composed into `Catalogue`, so tests and the simulator can fake or wrap one concern.
- No per-file rows, ever. Every method is one short transaction.

## Versions: three different things

- **Schema version**: an integer, the number of the last applied migration (`catalogue_meta.schema_version`). A binary carries `requires_schema` and refuses a lower schema.
- **Compat level**: an integer per binary, compared with `catalogue_meta.min_compat` (raised by contract migrations). Bumped by hand in code, never derived from the version string.
- **Binary version**: SemVer text `major.minor.build+hash[.dirty]`, built by `scripts/version.sh` from the latest git tag `vMAJOR.MINOR`, the commits since it (`build`), the first 8 hex digits of the commit, and `.dirty` for uncommitted changes (`make build` passes it as `-ldflags -X main.version=...`). **Informational only**: stored as `node_beats.binary_version` and `schema_migrations.applied_by` (`version@host`), shown by `store db status`, never compared.

## Rules the interface bakes in

1. **Fencing, not clocks, protects data.** A shard lease carries a monotonic `Generation`. Every catalogue write by an owner (open volume, begin/finish seal, add erasures) is a single compare-and-set on `(node, generation, not expired)`. Replicas independently reject stale volume epochs. A skewed clock can therefore delay or hasten a handover but can never let two owners ack writes.
2. **Time comes from the injected `Clock`** (milliseconds stored as `bigint`), never from the database's `now()`. This keeps PostgreSQL, SQLite and the fake identical, and lets `sim/` use virtual time. Cost: lease expiry depends on node clock skew (seconds, against 30 s leases); acceptable because of rule 1.
3. **Generation, ordinal and epoch only move forward.** The shard row is permanent (never deleted on release), so they cannot rewind. `ReleaseShard` clears the holder, keeps the row.
4. **Leases are plain transactions** (single-statement compare-and-set with `RETURNING`): no advisory locks, no `SKIP LOCKED`, so all three implementations pass the same suite.
5. **A catalogue outage fails writes** (`ErrUnavailable`): no lease can be granted or renewed. Reads of cached data are the node's business.
6. Errors are sentinels (`errors.Is`), listed below; every method returns `ctx` errors and database errors wrapped.

## Interface

The Go interface in `catalogue/catalogue.go` is the source of truth (every method is documented there); `catalogue/errors.go` lists the sentinel errors. Shape: `Catalogue` = `Buckets` + `Leases` + `Volumes` + `Erasures` + `Cluster` + `Meta` + `Close`.

| Concern | Methods |
|---|---|
| `Buckets` | `CreateBucket` (idempotent; conflict on different S; deleted refuses), `GetBucket`, `DeleteBucket` (also clears the bucket's shard leases), `ListBuckets` |
| `Leases` | `AcquireShard`, `RenewShard`, `ReleaseShard`, `ShardOwner` |
| `Volumes` | `OpenVolume(lease, OpenParams)`, `BeginSeal`, `FinishSeal`, `GetVolume`, `ListShardVolumes` (newest ordinal first), `ListDiskVolumes`, `SetReplicaState`, `AddReplica` |
| `Erasures` | `AddErasures`, `ShardErasures`, `PendingErasures`, `MarkErasureApplied`, `ListErasures` (paged export) |
| `Cluster` | `AssignNodeID`, `RegisterDisk`, `SetDiskState`, `Heartbeat`, `LiveNodes`, `Disks` |

Details settled while writing the contract suite (all enforced by `cattest`):
- `OpenVolume` requires the volume ID's node part to equal the lease holder's node ID, distinct registered **active** disks, and `CipherID` equal to the bucket's. Refusals consume no ordinal.
- `AddReplica` only for **sealed** volumes and active disks; the new replica gets a pending erasure target for every erasure of the volume, because it was copied from a replica that may not have applied them yet. `AddErasures` creates targets only for non-lost replicas.
- `FinishSeal` is a no-op when repeated with the same values, `ErrSealMismatch` with different ones, `ErrEpochMismatch` if another `BeginSeal` intervened.
- `RenewShard` of an expired but untaken lease succeeds; of a superseded or cleared one, `ErrLeaseLost`.
- `SetDiskState`: `gone` is terminal (`ErrDiskGone`). `Heartbeat` refuses unknown nodes (`ErrNodeNotFound`), a second live instance of a node ID (`ErrNodeIDInUse`) and disks not registered to that node.
- Ordering contracts: `ListShardVolumes` by ordinal descending, `ListDiskVolumes` by volume ID, `Disks` by disk ID, erasure lists by (volume, seq).

Errors: `ErrNotInitialised`, `ErrUnavailable`, `ErrInvalidArgument`, `ErrBucketNotFound`, `ErrBucketConflict`, `ErrBucketDeleted`, `ErrLeaseHeld` (as `*HeldError{Node, Expires}`), `ErrLeaseLost`, `ErrVolumeNotFound`, `ErrVolumeExists`, `ErrVolumeSealed`, `ErrVolumeNotSealed`, `ErrEpochMismatch`, `ErrSealMismatch`, `ErrReplicaNotFound`, `ErrErasureNotFound`, `ErrNodeNotFound`, `ErrNodeIDInUse`, `ErrNodeIDsExhausted`, `ErrUnknownDisk`, `ErrDiskConflict`, `ErrDiskUnavailable`, `ErrDiskGone`.

## Seal and handover sequence (what the interface is shaped for)

1. New owner: `AcquireShard` (generation g+1; the old owner's catalogue writes now fail).
2. `ListShardVolumes` → for each `open`/`sealing` volume: `BeginSeal` (epoch bump fences the old writer at the replicas) → replicas `SealFile` at the minimum committed length → `FinishSeal`. Crash anywhere: the next owner repeats; every step is repeatable.
3. Normal seal (full, idle, replica failure): the same three calls by the current owner.
4. `OpenVolume` for the replacement volume assigns the next ordinal in the same transaction as the insert.

## Lease SQL (portable; `$now` is the injected clock in Unix milliseconds)

```sql
-- AcquireShard: one statement; zero rows => held by another (read the row for the error)
UPDATE shards
   SET lease_node = $node, lease_generation = lease_generation + 1, lease_expires_ms = $now + $ttl_ms
 WHERE bucket = $bucket AND shard = $shard
   AND (lease_node IS NULL OR lease_node = $node OR lease_expires_ms <= $now)
RETURNING lease_generation;
-- (first use of a shard: INSERT ... ON CONFLICT DO NOTHING of the shards row, then the UPDATE)

-- RenewShard: nobody took over <=> generation unchanged
UPDATE shards SET lease_expires_ms = $now + $ttl_ms
 WHERE bucket = $bucket AND shard = $shard AND lease_node = $node AND lease_generation = $gen;

-- OpenVolume: ordinal and lease check in one statement, same transaction as the INSERTs
UPDATE shards SET next_ordinal = next_ordinal + 1
 WHERE bucket = $bucket AND shard = $shard AND lease_node = $node AND lease_generation = $gen
   AND lease_expires_ms > $now
RETURNING next_ordinal - 1;

-- BeginSeal
UPDATE volumes SET epoch = epoch + 1, state = 'sealing'
 WHERE volume_id = $id AND bucket = $bucket AND shard = $shard AND state <> 'sealed'
   AND EXISTS (SELECT 1 FROM shards s WHERE s.bucket = $bucket AND s.shard = $shard
               AND s.lease_node = $node AND s.lease_generation = $gen AND s.lease_expires_ms > $now)
RETURNING epoch;
```

## Schema sketch for migration 0001 (portable PostgreSQL/SQLite subset)

Times are `bigint` Unix milliseconds from the injected clock; states are `text` + `CHECK`; IDs are lower-case hex text so a rescue `psql` session can match them with file names.

```sql
schema_migrations(version int PK, name text, kind text CHECK (kind IN ('expand','contract')),
                  sha256 text, applied_ms bigint, applied_by text)
catalogue_meta(singleton int PK CHECK (singleton = 1), cluster_id text, cluster_name text,
               schema_version int, min_compat int, incarnation bigint, created_ms bigint)
nodes(node_id int PK CHECK (node_id BETWEEN 1 AND 65535), name text UNIQUE, created_ms bigint)
node_beats(node_id int PK REFERENCES nodes, instance text, addr text, binary_version text,
           compat int, seen_ms bigint)
disks(disk_id text PK, node_id int REFERENCES nodes, state text CHECK (state IN ('active','readonly','gone')),
      capacity_bytes bigint, used_bytes bigint, open_volumes int, seen_ms bigint, created_ms bigint)
buckets(name text PK, shards int CHECK (shards BETWEEN 1 AND 1024),
        state text CHECK (state IN ('active','deleted','restored')), cipher_id int DEFAULT 0 CHECK (cipher_id IN (0,1)),
        created_ms bigint, deleted_ms bigint)
shards(bucket text REFERENCES buckets, shard int, next_ordinal bigint DEFAULT 1,
       lease_node int, lease_generation bigint DEFAULT 0, lease_expires_ms bigint, PRIMARY KEY (bucket, shard))
volumes(volume_id text PK, bucket text, shard int, ordinal bigint,
        state text CHECK (state IN ('open','sealing','sealed')), epoch int, format_major int, format_minor int,
        cipher_id int, tier int, committed_length bigint, record_count bigint, created_ms bigint, sealed_ms bigint,
        UNIQUE (bucket, shard, ordinal), FOREIGN KEY (bucket, shard) REFERENCES shards)
volume_replicas(volume_id text REFERENCES volumes, disk_id text REFERENCES disks,
                state text CHECK (state IN ('ok','failed','lost')), PRIMARY KEY (volume_id, disk_id))
erasures(volume_id text REFERENCES volumes, seq bigint, chk bigint, created_ms bigint, PRIMARY KEY (volume_id, seq))
erasure_targets(volume_id text, seq bigint, disk_id text REFERENCES disks, applied_ms bigint,
                PRIMARY KEY (volume_id, seq, disk_id), FOREIGN KEY (volume_id, seq) REFERENCES erasures)
```

Indexes: `volumes(bucket, shard, ordinal DESC)`, `volume_replicas(disk_id)`, `erasure_targets(disk_id) WHERE applied_ms IS NULL` (PostgreSQL partial; SQLite supports it too).

Not in 0001 (added by later expand migrations when needed): bucket keys (encryption), retention/tier tables, the task table for the post-MVP coordinator.

## Decisions (confirmed 2026-11-05)

1. **Injected clock for leases** (rule 2), not database time.
2. **Volume and disk IDs as hex text**, times as `bigint` Unix milliseconds (matches `time.UnixMilli` and Java; rescue-friendly, portable) rather than `uuid`/`timestamptz`.
3. **One portable migration set** for PostgreSQL and SQLite, restricted to the common subset (no `~` regex checks, no enums, no `timestamptz`), instead of a directory per dialect. The PostgreSQL `verify` snapshot still checks the live PostgreSQL schema.
4. **Two-phase seal** (`BeginSeal` then `FinishSeal`) so the epoch fence is committed before replicas are told to seal.
5. **Permanent shard rows** hold lease, generation and `next_ordinal`; released leases keep the row (rows = shards ever written).
6. **`DeleteBucket` of a never-created bucket returns `ErrBucketNotFound`** (the node answers `404`, not `202`), so a typo cannot block a name forever. `api.md` §2.6 says "idempotent" without covering this case.
7. **`SetReplicaState`/`AddReplica` are not lease-gated** (owner and admin both need them); `AddErasures` is (owner-only; an admin erase acquires the shard lease like a node).
8. **Node ID range 1..65535**, volume ID node part = node ID; `AssignNodeID(name)` takes the operator-chosen node name (e.g. the host name) and is idempotent per name: a retried admin script gets the same ID back and never burns a second one.
