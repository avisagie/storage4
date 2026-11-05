package node

import "sync/atomic"

// Metrics is polled by the host. Names carry no bucket, name or metadata.
type Metrics interface {
	// Snapshot returns named counters (monotonic) and gauges (point in time).
	Snapshot() Snapshot
}

// Snapshot is a point-in-time copy of all metrics.
type Snapshot struct {
	Counters map[string]uint64
	Gauges   map[string]int64
}

// counters are the node's monotonic counters.
type counters struct {
	putsStored, putsDeduplicated, putsRejected  atomic.Uint64
	gets, bytesIn, bytesOut                     atomic.Uint64
	volumesSealed, fsyncs, fsyncNanos           atomic.Uint64
	indexRebuilds, appendRetries, bucketDeletes atomic.Uint64
}

func (c *counters) snapshot() map[string]uint64 {
	return map[string]uint64{
		"puts_stored":       c.putsStored.Load(),
		"puts_deduplicated": c.putsDeduplicated.Load(),
		"puts_rejected":     c.putsRejected.Load(),
		"gets":              c.gets.Load(),
		"bytes_in":          c.bytesIn.Load(),
		"bytes_out":         c.bytesOut.Load(),
		"volumes_sealed":    c.volumesSealed.Load(),
		"fsyncs":            c.fsyncs.Load(),
		"fsync_nanos":       c.fsyncNanos.Load(),
		"index_rebuilds":    c.indexRebuilds.Load(),
		"append_retries":    c.appendRetries.Load(),
		"bucket_deletes":    c.bucketDeletes.Load(),
	}
}
