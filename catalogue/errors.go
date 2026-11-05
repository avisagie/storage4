package catalogue

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors; test with errors.Is.
var (
	ErrNotInitialised   = errors.New("catalogue: database is not initialised")
	ErrUnavailable      = errors.New("catalogue: unavailable")
	ErrInvalidArgument  = errors.New("catalogue: invalid argument")
	ErrBucketNotFound   = errors.New("catalogue: bucket not found")
	ErrBucketConflict   = errors.New("catalogue: bucket exists with different options")
	ErrBucketDeleted    = errors.New("catalogue: bucket deleted")
	ErrLeaseHeld        = errors.New("catalogue: shard lease held by another node")
	ErrLeaseLost        = errors.New("catalogue: shard lease lost")
	ErrVolumeNotFound   = errors.New("catalogue: volume not found")
	ErrVolumeExists     = errors.New("catalogue: volume exists")
	ErrVolumeSealed     = errors.New("catalogue: volume sealed")
	ErrVolumeNotSealed  = errors.New("catalogue: volume not sealed")
	ErrEpochMismatch    = errors.New("catalogue: volume epoch changed")
	ErrSealMismatch     = errors.New("catalogue: volume sealed with different values")
	ErrReplicaNotFound  = errors.New("catalogue: disk is not a replica of the volume")
	ErrErasureNotFound  = errors.New("catalogue: no such erasure target")
	ErrNodeNotFound     = errors.New("catalogue: node not found")
	ErrNodeIDInUse      = errors.New("catalogue: node ID in use by another live instance")
	ErrNodeIDsExhausted = errors.New("catalogue: no node IDs left")
	ErrUnknownDisk      = errors.New("catalogue: unknown disk")
	ErrDiskConflict     = errors.New("catalogue: disk belongs to another node")
	ErrDiskUnavailable  = errors.New("catalogue: disk is not active")
	ErrDiskGone         = errors.New("catalogue: disk is gone")
)

// HeldError reports who holds a shard lease (for "not owner, try X").
// errors.Is(err, ErrLeaseHeld) is true.
type HeldError struct {
	Node    NodeID
	Expires time.Time
}

func (e *HeldError) Error() string {
	return fmt.Sprintf("%v: node %d until %s", ErrLeaseHeld, e.Node, e.Expires.UTC().Format(time.RFC3339))
}

func (e *HeldError) Unwrap() error { return ErrLeaseHeld }
