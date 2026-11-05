package nameindex

import (
	"container/list"
	"sync"
	"time"

	"storage4/clock"
	"storage4/disk"
)

// Clock is the time source for idle closing; inject a fake in tests.
type Clock = clock.Clock

// pool is a bounded LRU of open files, refcounted so a file is never closed
// under a reader. Files are opened on demand and closed when evicted or idle.
// With every handle in use the pool exceeds its bound rather than close one.
type pool struct {
	fsys  disk.FS
	max   int
	idle  time.Duration
	clock Clock

	mu  sync.Mutex
	lru *list.List // front = most recently used; values are *handle
	m   map[string]*handle
}

type handle struct {
	path string
	f    disk.File
	val  any // per-file state made by init (a *Reader for .nidx)
	refs int
	last time.Time
	el   *list.Element
	dead bool // removed from the pool; close on last release
}

func newPool(fsys disk.FS, max int, idle time.Duration, clock Clock) *pool {
	return &pool{fsys: fsys, max: max, idle: idle, clock: clock, lru: list.New(), m: map[string]*handle{}}
}

// acquire returns the open handle for path, opening it (and running init on
// the new file) if needed. The caller must release it.
func (p *pool) acquire(path string, init func(disk.File) (any, error)) (*handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	if h, ok := p.m[path]; ok {
		h.refs++
		h.last = now
		p.lru.MoveToFront(h.el)
		return h, nil
	}
	f, err := p.fsys.OpenFile(path, 0)
	if err != nil {
		return nil, err
	}
	var val any
	if init != nil {
		if val, err = init(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	h := &handle{path: path, f: f, val: val, refs: 1, last: now}
	h.el = p.lru.PushFront(h)
	p.m[path] = h
	p.evict()
	return h, nil
}

// evict closes least recently used unused handles until within the bound.
// Callers hold p.mu.
func (p *pool) evict() {
	for el := p.lru.Back(); el != nil && len(p.m) > p.max; {
		h := el.Value.(*handle)
		el = el.Prev()
		if h.refs == 0 {
			p.remove(h)
		}
	}
}

func (p *pool) remove(h *handle) {
	p.lru.Remove(h.el)
	delete(p.m, h.path)
	h.dead = true
	if h.refs == 0 {
		h.f.Close()
	}
}

func (p *pool) release(h *handle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h.refs--
	if h.refs == 0 && h.dead {
		h.f.Close()
	}
}

// invalidate drops path from the pool (its file is being replaced); users
// that still hold it finish with the old file.
func (p *pool) invalidate(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h, ok := p.m[path]; ok {
		p.remove(h)
	}
}

// closeIdle closes unused handles not used for longer than the idle timeout.
func (p *pool) closeIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	for el := p.lru.Back(); el != nil; {
		h := el.Value.(*handle)
		el = el.Prev()
		if h.refs == 0 && now.Sub(h.last) > p.idle {
			p.remove(h)
		}
	}
}

// closeAll closes every unused handle and marks the rest to close on release.
func (p *pool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.m {
		p.remove(h)
	}
}

func (p *pool) open() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.m)
}
