package node

import "sync"

// budget is a byte-counting semaphore for buffered request bodies. It never
// blocks: a request that does not fit is refused at once (503 + Retry-After).
type budget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

func newBudget(limit int64) *budget { return &budget{limit: limit} }

// tryAcquire reserves n bytes, or reports false if they do not fit.
func (b *budget) tryAcquire(n int64) bool {
	if n < 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.limit {
		return false
	}
	b.used += n
	return true
}

// release returns n bytes. Releasing more than is held is a bug and panics.
func (b *budget) release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.used {
		panic("node: budget released more than acquired")
	}
	b.used -= n
}

// inUse returns the reserved bytes (a gauge for Metrics).
func (b *budget) inUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
