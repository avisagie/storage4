package nameindex

import (
	"sync"

	"storage4/volume"
)

// OpenEntry is what the owner remembers about the latest version of a name
// in an open volume. The sha256 is for dedup and lives in memory only; it
// never reaches an index file.
type OpenEntry struct {
	Loc    volume.Locator
	SHA256 [32]byte
}

// OpenMap is the in-memory name map of one open volume. Lost on a crash; a
// restarted owner seals the volume and rebuilds a .nidx by a full pass
// instead (design-notes §7, restart rule 1). Safe for concurrent use.
type OpenMap struct {
	mu sync.RWMutex
	m  map[string]OpenEntry
}

// NewOpenMap returns an empty map.
func NewOpenMap() *OpenMap { return &OpenMap{m: map[string]OpenEntry{}} }

// Put records an acknowledged append. The highest seq wins, so a late or
// repeated Put never moves a name back to an older version.
func (o *OpenMap) Put(name string, e OpenEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if old, ok := o.m[name]; !ok || e.Loc.Seq > old.Loc.Seq {
		o.m[name] = e
	}
}

// Get returns the latest version of name in this volume. The caller still
// verifies the record with volume.ReadHead.
func (o *OpenMap) Get(name string) (OpenEntry, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	e, ok := o.m[name]
	return e, ok
}

// Len returns the number of distinct names.
func (o *OpenMap) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.m)
}

// Builder snapshots the map for sealing.
func (o *OpenMap) Builder() *Builder { return o.builder(Fingerprint) }

func (o *OpenMap) builder(fp func([]byte) uint64) *Builder {
	o.mu.RLock()
	defer o.mu.RUnlock()
	b := newBuilder(fp)
	for name, e := range o.m {
		// Locators from the writer always satisfy Add's checks.
		_ = b.Add([]byte(name), e.Loc.Seq, e.Loc.Off)
	}
	return b
}
