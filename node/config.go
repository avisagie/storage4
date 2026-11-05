package node

import (
	"errors"
	"fmt"
	"time"
)

// Limits shared with docs/api.md.
const (
	// MaxFileSize is the largest file a PUT may carry.
	MaxFileSize = 100 << 20
	// MaxMetadataSize is the largest metadata a PUT may carry.
	MaxMetadataSize = 16 << 10
	// ReplicaCount is the MVP replication factor: every volume lives on
	// exactly this many distinct disks.
	ReplicaCount = 3
)

// Config configures a Node. Zero values get the defaults noted per field.
type Config struct {
	// AllowSmallVolumes lets tests use a VolumeMaxSize below one maximal
	// record. Never set it in production.
	AllowSmallVolumes bool
	// Version is the informational binary version reported in heartbeats.
	Version string
	// NodeName is the operator-chosen name; the catalogue maps it to a node
	// ID with AssignNodeID. Required.
	NodeName string
	// ListenAddr, if set, makes the node serve HTTP itself; empty means the
	// host mounts Handler().
	ListenAddr string
	// Token is the shared bearer token. Empty disables authentication.
	Token string
	// DefaultShards is S for implicitly created buckets (default 4).
	DefaultShards int
	// MemoryBudget bounds the bytes of buffered request bodies (default
	// 256 MiB; at least MaxFileSize + MaxMetadataSize).
	MemoryBudget int64
	// LeaseTTL is the shard lease TTL (default 30 s); renewed every TTL/3.
	LeaseTTL time.Duration
	// VolumeMaxSize seals an open volume at this size (default 1 GiB, at
	// most volume.MaxVolumeSize).
	VolumeMaxSize int64
	// VolumeIdle seals an open volume after this long without a write
	// (default 1 h).
	VolumeIdle time.Duration
	// MaxOpenVolumes caps open volumes; the least recently written is sealed
	// beyond it (default 64).
	MaxOpenVolumes int
	// NidxHandles bounds cached index/volume file handles per disk (default
	// 64); NidxIdle closes handles unused this long (default 1 min).
	NidxHandles int
	NidxIdle    time.Duration
}

// ErrConfig means the configuration or dependencies are unusable.
var ErrConfig = errors.New("node: invalid configuration")

// withDefaults returns c with defaults filled in, or ErrConfig.
func (c Config) withDefaults() (Config, error) {
	if c.NodeName == "" {
		return c, fmt.Errorf("%w: NodeName is required", ErrConfig)
	}
	if c.Version == "" {
		c.Version = "unknown" // the catalogue requires a non-empty binary version
	}
	if c.DefaultShards == 0 {
		c.DefaultShards = 4
	}
	if c.DefaultShards < 1 || c.DefaultShards > 1024 {
		return c, fmt.Errorf("%w: DefaultShards %d not in 1..1024", ErrConfig, c.DefaultShards)
	}
	if c.MemoryBudget == 0 {
		c.MemoryBudget = 256 << 20
	}
	if c.MemoryBudget < MaxFileSize+MaxMetadataSize {
		return c, fmt.Errorf("%w: MemoryBudget %d is below one maximal request", ErrConfig, c.MemoryBudget)
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.LeaseTTL < 0 {
		return c, fmt.Errorf("%w: negative LeaseTTL", ErrConfig)
	}
	if c.VolumeMaxSize == 0 {
		c.VolumeMaxSize = 1 << 30
	}
	if c.VolumeMaxSize < 0 || (!c.AllowSmallVolumes && c.VolumeMaxSize < MaxFileSize+MaxMetadataSize+(1<<20)) {
		return c, fmt.Errorf("%w: VolumeMaxSize %d cannot hold one maximal record", ErrConfig, c.VolumeMaxSize)
	}
	if c.VolumeIdle == 0 {
		c.VolumeIdle = time.Hour
	}
	if c.MaxOpenVolumes == 0 {
		c.MaxOpenVolumes = 64
	}
	if c.VolumeIdle < 0 || c.MaxOpenVolumes < 0 || c.NidxHandles < 0 || c.NidxIdle < 0 {
		return c, fmt.Errorf("%w: negative limit", ErrConfig)
	}
	if c.NidxHandles == 0 {
		c.NidxHandles = 64
	}
	if c.NidxIdle == 0 {
		c.NidxIdle = time.Minute
	}
	return c, nil
}
