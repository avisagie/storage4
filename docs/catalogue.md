# Catalogue (`catalogue/`, `db/`)

Status: design decisions 1–8 below confirmed by the reviewer 2026-11-05 (times in milliseconds, not microseconds). Nothing is implemented yet; the human gate on the final interface, migration 0001 and the lease/ordinal/epoch SQL stays open until the code is reviewed. Rationale: design-notes §6, §8, §13.

## Scope

- `catalogue.Catalogue` is the **runtime API for nodes and tools**: buckets, shard leases and ordinals, volumes and replicas, the erasure list, nodes and disks. It never runs DDL and never initialises anything.
- `db/` is the **DDL side** (`store db init|migrate|status|verify`, embedded migrations). It is not part of the `Catalogue` interface, so no node can reach it.
- Three implementations behind one contract suite: PostgreSQL (`pgx`, production), SQLite (`modernc.org/sqlite`, tests/dev only), in-memory fake (simulator). All three are constructed with an **injected `Clock`**.
- Small, per-concern interfaces (`Buckets`, `Leases`, `Volumes`, `Erasures`, `Cluster`) composed into `Catalogue`, so tests and the simulator can fake or wrap one concern.
- No per-file rows, ever. Every method is one short transaction.

## Rules the interface bakes in

1. **Fencing, not clocks, protects data.** A shard lease carries a monotonic `Generation`. Every catalogue write by an owner (open volume, begin/finish seal, add erasures) is a single compare-and-set on `(node, generation, not expired)`. Replicas independently reject stale volume epochs. A skewed clock can therefore delay or hasten a handover but can never let two owners ack writes.
2. **Time comes from the injected `Clock`** (milliseconds stored as `bigint`), never from the database's `now()`. This keeps PostgreSQL, SQLite and the fake identical, and lets `sim/` use virtual time. Cost: lease expiry depends on node clock skew (seconds, against 30 s leases); acceptable because of rule 1.
3. **Generation, ordinal and epoch only move forward.** The shard row is permanent (never deleted on release), so they cannot rewind. `ReleaseShard` clears the holder, keeps the row.
4. **Leases are plain transactions** (single-statement compare-and-set with `RETURNING`): no advisory locks, no `SKIP LOCKED`, so all three implementations pass the same suite.
5. **A catalogue outage fails writes** (`ErrUnavailable`): no lease can be granted or renewed. Reads of cached data are the node's business.
6. Errors are sentinels (`errors.Is`), listed below; every method returns `ctx` errors and database errors wrapped.

## Interface sketch

```go
package catalogue

type NodeID uint16      // 1..65535, never reused; 0 invalid
type DiskID [16]byte
type VolumeID uint64    // node_id(16) | node-local counter(48); stored as 16 lower-case hex digits

type Meta struct {
    ClusterID     [16]byte
    ClusterName   string
    SchemaVersion int
    MinCompat     int
    Incarnation   int64 // opaque; compared for equality; changed by the failover runbook
}

type Catalogue interface {
    Buckets
    Leases
    Volumes
    Erasures
    Cluster
    // Meta checks nothing itself: the node compares ClusterID with its local state,
    // SchemaVersion with its requires_schema and MinCompat with its compat_level.
    Meta(ctx context.Context) (Meta, error) // ErrNotInitialised on an empty database
    Close() error
}

// ---- buckets ----
type BucketState string // "active", "deleted", "restored"
type Bucket struct {
    Name      string
    Shards    int // S, fixed for life
    State     BucketState
    CipherID  uint16 // 0 in the MVP
    CreatedAt time.Time
    DeletedAt time.Time // zero unless deleted
}
type Buckets interface {
    // Idempotent. Same options: created=false. Different Shards: ErrBucketConflict.
    // A deleted bucket: ErrBucketDeleted (never accepts writes again).
    CreateBucket(ctx context.Context, name string, shards int) (b Bucket, created bool, err error)
    GetBucket(ctx context.Context, name string) (Bucket, error) // ErrBucketNotFound
    // Marks deleted (volumes are removed later by pull-based GC). Idempotent for an
    // existing bucket; ErrBucketNotFound for a name that never existed.
    DeleteBucket(ctx context.Context, name string) error
    ListBuckets(ctx context.Context) ([]Bucket, error) // all states, for reconcile/GC/scan
}

// ---- shard leases ----
// Lease is the fencing token. Treat as opaque; pass back unchanged.
type Lease struct {
    Bucket     string
    Shard      int
    Node       NodeID
    Generation int64         // strictly increases on every grant for this shard
    TTL        time.Duration // as granted; the node renews at TTL/3
}
type Leases interface {
    // Grants the lease if the shard is free, expired, or already held by the same node
    // (a restarted owner re-acquires at once). Generation increases on every grant.
    // Held by another node and unexpired: *ErrLeaseHeld (with the holder, for "not owner, try X").
    AcquireShard(ctx context.Context, bucket string, shard int, node NodeID, ttl time.Duration) (Lease, error)
    // Extends expiry. ErrLeaseLost if the generation is no longer current (someone else
    // took over, or the bucket is gone). Renewing an expired but untaken lease succeeds.
    RenewShard(ctx context.Context, l Lease) error
    // Clears the holder; keeps the row and counters. Idempotent; a stale lease is a no-op.
    ReleaseShard(ctx context.Context, l Lease) error
    // Current holder, for forwarding. ok=false: nobody holds an unexpired lease.
    ShardOwner(ctx context.Context, bucket string, shard int) (o Owner, ok bool, err error)
}
type Owner struct { Node NodeID; Generation int64; Expires time.Time }

// ---- volumes ----
type VolumeState string // "open", "sealing", "sealed"
type ReplicaState string // "ok", "failed", "lost"
type Replica struct { Disk DiskID; State ReplicaState }
type Volume struct {
    ID                     VolumeID
    Bucket                 string
    Shard                  int
    Ordinal                int64 // per (bucket, shard); latest version = highest (ordinal, seq)
    State                  VolumeState
    Epoch                  uint32 // 1 at creation, +1 per BeginSeal
    FormatMajor, FormatMinor uint16
    CipherID, Tier         uint16
    CommittedLength        int64  // valid when sealed
    RecordCount            uint32 // valid when sealed (seal record's count)
    CreatedAt, SealedAt    time.Time
    Replicas               []Replica
}
type Volumes interface {
    // One transaction, under the lease: assigns the next ordinal of the shard, inserts the
    // volume (state open, epoch 1) and its replica rows. ErrLeaseLost, ErrVolumeExists.
    OpenVolume(ctx context.Context, l Lease, id VolumeID, disks []DiskID, tier uint16) (Volume, error)
    // Fences the old writer: bumps the epoch, state "sealing", returns the new epoch.
    // Call before telling replicas to seal. Repeatable (each call bumps again). Under the lease;
    // the volume must belong to the lease's shard. Sealed volume: ErrVolumeSealed.
    BeginSeal(ctx context.Context, l Lease, id VolumeID) (epoch uint32, err error)
    // Records the outcome once replicas sealed at `committed`. Needs epoch == current
    // (ErrEpochMismatch if someone bumped since). Same values again: no-op. Other values: ErrSealMismatch.
    FinishSeal(ctx context.Context, l Lease, id VolumeID, epoch uint32, committed int64, records uint32) error
    GetVolume(ctx context.Context, id VolumeID) (Volume, error) // ErrVolumeNotFound
    // All volumes of a shard, newest ordinal first (lookup order). Includes open/sealing ones.
    ListShardVolumes(ctx context.Context, bucket string, shard int) ([]Volume, error)
    // Volumes with a replica on the disk, any bucket state (reconcile, GC, scan verify).
    ListDiskVolumes(ctx context.Context, disk DiskID) ([]Volume, error)
    // Not lease-gated: the owner (seal-on-failure) and admin (repair) both use them.
    SetReplicaState(ctx context.Context, id VolumeID, disk DiskID, s ReplicaState) error
    AddReplica(ctx context.Context, id VolumeID, disk DiskID) error // re-replication finished
}

// ---- erasure list ----
type Locator struct { Volume VolumeID; Seq, Check uint32 }
type Erasures interface {
    // Under the lease; every locator's volume must belong to the lease's shard.
    // Inserts the locators and one pending target per replica of each volume. Idempotent.
    AddErasures(ctx context.Context, l Lease, locs []Locator) error
    ShardErasures(ctx context.Context, bucket string, shard int) ([]Locator, error) // owner cache
    PendingErasures(ctx context.Context, disk DiskID, limit int) ([]Locator, error)
    MarkErasureApplied(ctx context.Context, disk DiskID, locs []Locator) error
    ListErasures(ctx context.Context, after Locator, limit int) ([]Locator, error) // export, ordered
}

// ---- nodes and disks ----
type DiskState string // "active", "readonly", "gone"; "down" is derived from a stale heartbeat
type NodeBeat struct { ID NodeID; Instance [16]byte; Addr, Version string; Compat int }
type DiskBeat struct { ID DiskID; CapacityBytes, UsedBytes int64; OpenVolumes int }
type NodeInfo struct { ID NodeID; Name, Addr, Version string; Compat int; LastSeen time.Time }
type Disk struct {
    ID DiskID; Node NodeID; State DiskState
    CapacityBytes, UsedBytes int64; OpenVolumes int; LastSeen time.Time
}
type Cluster interface {
    // Admin: idempotent per name; never reuses an ID.
    AssignNodeID(ctx context.Context, name string) (NodeID, error)
    // Admin: add a disk. Idempotent for the same (disk, node); a disk on another node: ErrDiskConflict.
    RegisterDisk(ctx context.Context, id DiskID, node NodeID) error
    SetDiskState(ctx context.Context, id DiskID, s DiskState) error
    // Node: upserts the node row and its disks' stats; returns current Meta so the node sees an
    // incarnation or compat change on every beat. ErrNodeIDInUse if another instance beat within NodeTTL.
    // ErrUnknownDisk for a disk that was not registered (nodes never auto-register).
    Heartbeat(ctx context.Context, n NodeBeat, disks []DiskBeat) (Meta, error)
    LiveNodes(ctx context.Context) ([]NodeInfo, error) // beat within NodeTTL; rendezvous candidates
    Disks(ctx context.Context) ([]Disk, error)         // placement inputs
}
```

Errors: `ErrNotInitialised`, `ErrUnavailable`, `ErrBucketNotFound`, `ErrBucketConflict`, `ErrBucketDeleted`, `*ErrLeaseHeld{Node, Expires}`, `ErrLeaseLost`, `ErrVolumeNotFound`, `ErrVolumeExists`, `ErrVolumeSealed`, `ErrEpochMismatch`, `ErrSealMismatch`, `ErrDiskConflict`, `ErrUnknownDisk`, `ErrNodeIDInUse`.

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
