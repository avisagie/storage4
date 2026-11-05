package node

import (
	"hash/fnv"
	"sync"
)

// nameLocks is a striped mutex keyed by (bucket, name). The write path holds
// a name's lock from lookup until the append is acked, so a retry that
// overlaps its first request cannot pass the dedup or If-Match check before
// the first one has finished (docs/node.md §5). Distinct names that share a
// stripe merely wait for each other briefly.
type nameLocks struct {
	stripes [256]sync.Mutex
}

// lock takes the lock for (bucket, name) and returns its unlock function.
func (l *nameLocks) lock(bucket, name string) (unlock func()) {
	m := &l.stripes[stripeOf(bucket, name)]
	m.Lock()
	return m.Unlock
}

// stripeOf maps (bucket, name) to a stripe index.
func stripeOf(bucket, name string) int {
	h := fnv.New64a()
	h.Write([]byte(bucket))
	h.Write([]byte{0}) // bucket names cannot contain NUL, so (b, n) pairs stay distinct
	h.Write([]byte(name))
	return int(h.Sum64() % uint64(len(nameLocks{}.stripes)))
}
