package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"storage4/catalogue"
	"storage4/nameindex"
	"storage4/names"
	"storage4/volume"
)

var (
	// errPrecondition: If-None-Match / If-Match failed (412).
	errPrecondition = errors.New("node: precondition failed")
	// errRetriesExhausted: the volume kept failing under this request (503).
	errRetriesExhausted = errors.New("node: append retries exhausted")
)

// putRequest is a validated PUT: buffered file, optional metadata, its
// SHA-256 and the conditions.
type putRequest struct {
	sha          [32]byte
	payload      []byte
	meta         []byte
	ifNoneMatchA bool   // If-None-Match: *
	ifMatch      string // If-Match: lower-case hex etag, "" if absent
	ifMatchAny   bool   // If-Match: *
}

// put stores a version of (bucket, name) on the shard owner, or reports that
// the latest version is identical (docs/node.md §5). stored is true for a new
// version (HTTP 201), false for a duplicate (200).
func (n *Node) put(ctx context.Context, b catalogue.Bucket, name string, r putRequest) (stored bool, err error) {
	idx, err := names.Shard(name, uint32(b.Shards))
	if err != nil {
		return false, err
	}
	sh := n.shardOf(b, int(idx))
	unlock := n.locks.lock(b.Name, name)
	defer unlock()

	if err := sh.leaseOnly(ctx); err != nil {
		return false, err
	}
	f, uncertain, err := sh.lookup(ctx, name)
	exists := err == nil && !uncertain
	switch {
	case err == nil:
		if uncertain && (r.ifNoneMatchA || r.ifMatch != "" || r.ifMatchAny) {
			// A condition cannot be judged against an unreadable volume.
			return false, errLookupUncertain
		}
		// Unconditional: skip dedup and store a new version, which is
		// always the highest. Write availability over a saved duplicate.
	case errors.Is(err, errNoSuchName), errors.Is(err, errNameErased):
		// Erased names store a new version normally (api.md §2.4).
	default:
		return false, err
	}
	if r.ifNoneMatchA && exists || (r.ifMatch != "" || r.ifMatchAny) && !exists ||
		r.ifMatch != "" && exists && hex.EncodeToString(f.head.SHA256[:]) != r.ifMatch {
		if exists {
			f.release()
		}
		return false, errPrecondition
	}
	if exists {
		same := f.head.SHA256 == r.sha && bytes.Equal(f.head.Metadata, r.meta)
		f.release()
		if same {
			return false, nil
		}
	}
	entry := volume.Entry{Name: name, SHA256: r.sha, Metadata: r.meta, Payload: r.payload}
	for attempt := 0; attempt < 3; attempt++ {
		n.makeRoom(ctx)
		ov, err := sh.writable(ctx)
		if err != nil {
			return false, err
		}
		loc, err := ov.submit(entry)
		switch {
		case err == nil:
			// Only now, after every replica fsynced, may the name map learn
			// the record: an entry for an unacked record would make a retry
			// dedup against data that is not durable.
			ov.omap.Put(name, nameindex.OpenEntry{Loc: loc, SHA256: r.sha})
			return true, nil
		case errors.Is(err, errVolumeFull):
			if err := sh.sealOpen(ctx, ov, "full"); err != nil {
				return false, err
			}
		case errors.Is(err, errReplicaFailed):
			n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "append failed on a replica; volume sealed")
			n.cnt.appendRetries.Add(1)
			for _, d := range ov.bad {
				d.markSuspect(n.d.Clock.Now())
			}
			if err := sh.sealOpen(ctx, ov, "replica failure"); err != nil {
				return false, err
			}
		case errors.Is(err, errVolumeClosed):
			// Another request is sealing it; take the next volume.
		default:
			return false, fmt.Errorf("append: %w", err)
		}
	}
	return false, errRetriesExhausted
}
