package catalogue

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"
)

// NodeID is the node part of volume IDs: 1..65535, assigned once, never reused.
type NodeID uint16

// DiskID is the disk_id of the disk's identity file.
type DiskID [16]byte

func (d DiskID) String() string { return hex.EncodeToString(d[:]) }

// ParseDiskID parses 32 lower-case hex digits.
func ParseDiskID(s string) (DiskID, error) {
	var d DiskID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(d) || s != hex.EncodeToString(b) {
		return d, fmt.Errorf("catalogue: invalid disk id %q", s)
	}
	copy(d[:], b)
	return d, nil
}

// VolumeID is node_id(16) | node-local counter(48). Its text form is 16
// lower-case hex digits, the same as the volume's file name.
type VolumeID uint64

func (v VolumeID) String() string { return fmt.Sprintf("%016x", uint64(v)) }

// Node returns the node part of the ID.
func (v VolumeID) Node() NodeID { return NodeID(v >> 48) }

// ParseVolumeID parses 16 lower-case hex digits.
func ParseVolumeID(s string) (VolumeID, error) {
	var v uint64
	if len(s) != 16 {
		return 0, fmt.Errorf("catalogue: invalid volume id %q", s)
	}
	for _, c := range []byte(s) {
		switch {
		case c >= '0' && c <= '9':
			v = v<<4 | uint64(c-'0')
		case c >= 'a' && c <= 'f':
			v = v<<4 | uint64(c-'a'+10)
		default:
			return 0, fmt.Errorf("catalogue: invalid volume id %q", s)
		}
	}
	return VolumeID(v), nil
}

// Options configure an implementation. Zero values get defaults.
type Options struct {
	// NodeTTL: a node whose last heartbeat is older is not live, and its
	// node ID may be taken over by a new instance. Default 30 s.
	NodeTTL time.Duration
}

// Meta is the catalogue's identity and compatibility state (catalogue_meta).
type Meta struct {
	ClusterID     [16]byte
	ClusterName   string
	SchemaVersion int
	MinCompat     int
	// Incarnation is opaque and compared for equality only. The failover
	// runbook changes it; nodes that see a change drop their leases and seal.
	Incarnation int64
}

// Catalogue is everything a node or tool may do to the catalogue.
type Catalogue interface {
	Buckets
	Leases
	Volumes
	Erasures
	Cluster
	// Meta reads the catalogue's identity. The caller compares ClusterID with
	// its local state, SchemaVersion with its requires_schema and MinCompat
	// with its compat level. ErrNotInitialised on an empty database.
	Meta(ctx context.Context) (Meta, error)
	Close() error
}

// ---- buckets ----

// BucketState is the lifecycle state of a bucket.
type BucketState string

const (
	BucketActive   BucketState = "active"
	BucketDeleted  BucketState = "deleted" // remembered forever; never accepts writes again
	BucketRestored BucketState = "restored"
)

// Bucket is a bucket's catalogue record.
type Bucket struct {
	Name      string
	Shards    int // S, fixed for the bucket's life
	State     BucketState
	CipherID  uint16 // 0 in the MVP
	CreatedAt time.Time
	DeletedAt time.Time // zero unless deleted
}

// Buckets manages buckets.
type Buckets interface {
	// CreateBucket is idempotent: the same shards returns the bucket with
	// created=false; different shards is ErrBucketConflict; a deleted bucket
	// is ErrBucketDeleted. Invalid name or shard count: ErrInvalidArgument.
	CreateBucket(ctx context.Context, name string, shards int) (b Bucket, created bool, err error)
	// GetBucket returns the bucket in any state; ErrBucketNotFound otherwise.
	GetBucket(ctx context.Context, name string) (Bucket, error)
	// DeleteBucket marks the bucket deleted and clears its shard leases, so
	// no owner can keep writing. Idempotent for an existing bucket;
	// ErrBucketNotFound for a name that never existed.
	DeleteBucket(ctx context.Context, name string) error
	// ListBuckets returns all buckets in every state, ordered by name.
	ListBuckets(ctx context.Context) ([]Bucket, error)
}

// ---- shard leases ----

// Lease is the fencing token of a shard lease. Pass it back unchanged.
type Lease struct {
	Bucket     string
	Shard      int
	Node       NodeID
	Generation int64         // strictly increases on every grant for this shard
	TTL        time.Duration // as granted; the holder renews at TTL/3
}

// Owner is the current holder of an unexpired shard lease.
type Owner struct {
	Node       NodeID
	Generation int64
	Expires    time.Time
}

// Leases manages shard leases. Time is the injected clock's; correctness
// does not depend on it (see docs/catalogue.md, rule 1).
type Leases interface {
	// AcquireShard grants the lease if the shard is free, expired, or held by
	// the same node (a restarted owner re-acquires at once). The generation
	// increases on every grant. Held by another node and unexpired:
	// *HeldError (errors.Is ErrLeaseHeld). Unknown bucket: ErrBucketNotFound;
	// deleted: ErrBucketDeleted; shard out of range or ttl <= 0:
	// ErrInvalidArgument.
	AcquireShard(ctx context.Context, bucket string, shard int, node NodeID, ttl time.Duration) (Lease, error)
	// RenewShard extends the expiry by the lease's TTL. ErrLeaseLost if the
	// generation is no longer current or the lease was cleared. An expired
	// but untaken lease can still be renewed.
	RenewShard(ctx context.Context, l Lease) error
	// ReleaseShard clears the holder and keeps the counters. Idempotent; a
	// stale lease is a no-op.
	ReleaseShard(ctx context.Context, l Lease) error
	// ShardOwner returns the holder of an unexpired lease; ok=false if none.
	ShardOwner(ctx context.Context, bucket string, shard int) (o Owner, ok bool, err error)
}

// ---- volumes ----

// VolumeState is where a volume is in its life.
type VolumeState string

const (
	VolumeOpen    VolumeState = "open"
	VolumeSealing VolumeState = "sealing" // epoch bumped, replicas being sealed
	VolumeSealed  VolumeState = "sealed"
)

// ReplicaState is the catalogue's view of one replica.
type ReplicaState string

const (
	ReplicaOK     ReplicaState = "ok"
	ReplicaFailed ReplicaState = "failed" // may come back; excluded from new reads
	ReplicaLost   ReplicaState = "lost"   // gone for good
)

// Replica is one copy of a volume on one disk.
type Replica struct {
	Disk  DiskID
	State ReplicaState
}

// Volume is a volume's catalogue record.
type Volume struct {
	ID           VolumeID
	Bucket       string
	Shard        int
	VolumeNumber int64 // per (bucket, shard); latest version = highest (volume number, seq)
	State        VolumeState
	Epoch        uint32 // 1 at creation, +1 per BeginSeal

	FormatMajor, FormatMinor uint16
	CipherID, Tier           uint16

	CommittedLength int64  // valid when sealed
	RecordCount     uint32 // valid when sealed: the seal record's count

	CreatedAt time.Time
	SealedAt  time.Time // zero unless sealed
	Replicas  []Replica // ordered by disk ID
}

// OpenParams describe a new volume. The ID is allocated by the node from its
// own counter; its node part must be the lease holder's node ID.
type OpenParams struct {
	ID                       VolumeID
	Disks                    []DiskID // replica disks: registered, active, distinct
	Tier                     uint16
	FormatMajor, FormatMinor uint16
	CipherID                 uint16 // must equal the bucket's cipher
}

// Volumes manages volumes and their replicas.
type Volumes interface {
	// OpenVolume, under the lease and in one transaction, assigns the shard's
	// next volume number and inserts the volume (open, epoch 1) with its replicas.
	// ErrLeaseLost, ErrVolumeExists, ErrUnknownDisk, ErrDiskUnavailable,
	// ErrInvalidArgument.
	OpenVolume(ctx context.Context, l Lease, p OpenParams) (Volume, error)
	// BeginSeal fences the old writer: it bumps the epoch, sets state
	// "sealing" and returns the new epoch. Call it before telling replicas to
	// seal. Repeatable (each call bumps again). The volume must belong to the
	// lease's shard (else ErrVolumeNotFound); a sealed volume is ErrVolumeSealed.
	BeginSeal(ctx context.Context, l Lease, id VolumeID) (epoch uint32, err error)
	// FinishSeal records that the replicas sealed at committed with records
	// records. epoch must still be current (ErrEpochMismatch if another
	// BeginSeal intervened). Repeating with the same values is a no-op;
	// different values on a sealed volume are ErrSealMismatch.
	FinishSeal(ctx context.Context, l Lease, id VolumeID, epoch uint32, committed int64, records uint32) error
	// GetVolume returns ErrVolumeNotFound for an unknown ID.
	GetVolume(ctx context.Context, id VolumeID) (Volume, error)
	// ListShardVolumes returns every volume of the shard, newest volume number
	// first (lookup order), including open and sealing ones.
	ListShardVolumes(ctx context.Context, bucket string, shard int) ([]Volume, error)
	// ListDiskVolumes returns the volumes with a replica on the disk, in any
	// bucket state, ordered by volume ID (reconcile, GC, scan verify).
	ListDiskVolumes(ctx context.Context, disk DiskID) ([]Volume, error)
	// SetReplicaState is not lease-gated: the owner (seal on failure) and
	// admin (repair) both use it. ErrVolumeNotFound, ErrReplicaNotFound.
	SetReplicaState(ctx context.Context, id VolumeID, disk DiskID, s ReplicaState) error
	// AddReplica records a finished re-replication of a sealed volume
	// (ErrVolumeNotSealed otherwise) to an active disk. Idempotent. The new
	// replica gets a pending target for every erasure of the volume, because
	// it was copied from a replica that may not have applied them yet.
	AddReplica(ctx context.Context, id VolumeID, disk DiskID) error
}

// ---- erasure list ----

// Locator names one record: the catalogue-side form of a record locator.
type Locator struct {
	Volume     VolumeID
	Seq, Check uint32
}

// Erasures is the permanent erasure list.
type Erasures interface {
	// AddErasures, under the lease, inserts the locators and one pending
	// target for every non-lost replica of each volume. Every volume must
	// belong to the lease's shard (else ErrVolumeNotFound). Idempotent; a
	// known (volume, seq) with a different check is ErrInvalidArgument.
	AddErasures(ctx context.Context, l Lease, locs []Locator) error
	// ShardErasures returns the shard's erasures ordered by (volume, seq):
	// the owner's cache.
	ShardErasures(ctx context.Context, bucket string, shard int) ([]Locator, error)
	// PendingErasures returns up to limit targets of the disk not yet applied,
	// ordered by (volume, seq).
	PendingErasures(ctx context.Context, disk DiskID, limit int) ([]Locator, error)
	// MarkErasureApplied records that the disk applied the erasures.
	// Idempotent; a locator with no target on the disk is ErrErasureNotFound.
	MarkErasureApplied(ctx context.Context, disk DiskID, locs []Locator) error
	// ListErasures exports up to limit erasures ordered by (volume, seq),
	// strictly after the given locator (the zero Locator starts at the
	// beginning).
	ListErasures(ctx context.Context, after Locator, limit int) ([]Locator, error)
}

// ---- nodes and disks ----

// DiskState is the operator-controlled state of a disk. "Down" is derived
// from a stale heartbeat, not stored.
type DiskState string

const (
	DiskActive   DiskState = "active"
	DiskReadOnly DiskState = "readonly" // no new volumes
	DiskGone     DiskState = "gone"     // terminal: the operator forgot the disk
)

// NodeBeat is what a node reports about itself.
type NodeBeat struct {
	ID       NodeID
	Instance [16]byte // random per process; a second live instance of the ID is refused
	Addr     string
	Version  string
	Compat   int // the binary's compat level
}

// DiskBeat is what a node reports about one of its disks.
type DiskBeat struct {
	ID            DiskID
	CapacityBytes int64
	UsedBytes     int64
	OpenVolumes   int
}

// NodeInfo is a live node as seen by others.
type NodeInfo struct {
	ID       NodeID
	Name     string
	Addr     string
	Version  string
	Compat   int
	LastSeen time.Time
}

// Disk is a disk's catalogue record: placement input.
type Disk struct {
	ID            DiskID
	Node          NodeID
	State         DiskState
	CapacityBytes int64
	UsedBytes     int64
	OpenVolumes   int
	LastSeen      time.Time // zero before the first heartbeat
}

// Cluster manages nodes and disks.
type Cluster interface {
	// AssignNodeID gives the node called name its ID. Idempotent per name;
	// IDs are never reused. name is operator-chosen, 1..128 bytes.
	AssignNodeID(ctx context.Context, name string) (NodeID, error)
	// RegisterDisk adds a disk (state active) to a node: ErrNodeNotFound if
	// the node has no ID, ErrDiskConflict if the disk belongs to another
	// node. Idempotent for the same pair.
	RegisterDisk(ctx context.Context, id DiskID, node NodeID) error
	// SetDiskState: ErrUnknownDisk; gone is terminal (ErrDiskGone).
	SetDiskState(ctx context.Context, id DiskID, s DiskState) error
	// Heartbeat upserts the node's liveness and its disks' stats and returns
	// the current Meta, so the node sees an incarnation or compat change on
	// every beat. ErrNodeNotFound (nodes never self-register),
	// ErrNodeIDInUse if another instance beat within NodeTTL, ErrUnknownDisk
	// for a disk that is not registered to this node.
	Heartbeat(ctx context.Context, n NodeBeat, disks []DiskBeat) (Meta, error)
	// LiveNodes returns nodes whose last heartbeat is within NodeTTL, by ID.
	LiveNodes(ctx context.Context) ([]NodeInfo, error)
	// Disks returns every disk, ordered by ID.
	Disks(ctx context.Context) ([]Disk, error)
}
