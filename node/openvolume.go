package node

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"storage4/catalogue"
	"storage4/nameindex"
	"storage4/volume"
)

var (
	// errVolumeFull: the batch does not fit; seal the volume and use a new one.
	errVolumeFull = errors.New("node: open volume full")
	// errVolumeClosed: the volume stopped taking appends (sealing, failed).
	errVolumeClosed = errors.New("node: open volume closed")
	// errReplicaFailed: an append or fsync failed on some replica.
	errReplicaFailed = errors.New("node: replica append failed")
)

// replicaW is one replica of an open volume: the writer on one local disk.
type replicaW struct {
	d *localDisk
	w *volume.Writer
}

type appendReq struct {
	entry volume.Entry
	resp  chan appendResp
}

type appendResp struct {
	loc volume.Locator
	err error
}

// openVolume is an open volume with its replicas, its in-memory name map and
// the committer goroutine that turns queued appends into group commits.
type openVolume struct {
	vol  catalogue.Volume
	id   uint64
	reps []*replicaW
	omap *nameindex.OpenMap
	sh   *shard       // set by the shard that opened it
	bad  []*localDisk // disks whose replica failed an append (read after the committer stopped)

	mu     sync.Mutex
	queue  []*appendReq
	closed bool
	wake   chan struct{} // capacity 1
	stop   chan struct{}
	done   chan struct{}

	lastWrite atomic.Int64 // Unix nanoseconds on the injected clock
	fsyncs    *counters
	now       func() time.Time
}

func newOpenVolume(vol catalogue.Volume, reps []*replicaW, c *counters, now func() time.Time) *openVolume {
	ov := &openVolume{
		vol: vol, id: uint64(vol.ID), reps: reps, omap: nameindex.NewOpenMap(),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		fsyncs: c, now: now,
	}
	ov.lastWrite.Store(now().UnixNano())
	return ov
}

// start runs the committer. The caller joins it through close.
func (ov *openVolume) start() { go ov.run() }

// submit queues one entry and waits until it is durable on all replicas.
func (ov *openVolume) submit(e volume.Entry) (volume.Locator, error) {
	r := &appendReq{entry: e, resp: make(chan appendResp, 1)}
	ov.mu.Lock()
	if ov.closed {
		ov.mu.Unlock()
		return volume.Locator{}, errVolumeClosed
	}
	ov.queue = append(ov.queue, r)
	ov.mu.Unlock()
	select {
	case ov.wake <- struct{}{}:
	default:
	}
	resp := <-r.resp
	return resp.loc, resp.err
}

// run is the committer: it takes everything queued since it last looked as
// one batch (natural group commit, no timer) and commits it.
func (ov *openVolume) run() {
	defer close(ov.done)
	for {
		select {
		case <-ov.wake:
		case <-ov.stop:
			ov.shutdown(errVolumeClosed)
			return
		}
		for {
			ov.mu.Lock()
			batch := ov.queue
			ov.queue = nil
			ov.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			if err := ov.commit(batch); err != nil {
				ov.shutdown(errVolumeClosed)
				return
			}
		}
	}
}

// shutdown marks the volume closed and fails everything still queued.
func (ov *openVolume) shutdown(err error) {
	ov.mu.Lock()
	ov.closed = true
	rest := ov.queue
	ov.queue = nil
	ov.mu.Unlock()
	for _, r := range rest {
		r.resp <- appendResp{err: err}
	}
}

// close stops the committer and waits for it. Appends already in flight
// finish first; queued ones fail with errVolumeClosed.
func (ov *openVolume) close() {
	select {
	case <-ov.stop:
	default:
		close(ov.stop)
	}
	<-ov.done
}

// commit appends the batch to every replica in parallel (one fsync each) and
// answers every request. It returns a non-nil error if the volume must stop
// (replica failure); a full volume also stops it, without failing the writers.
func (ov *openVolume) commit(batch []*appendReq) error {
	entries := make([]volume.Entry, len(batch))
	for i, r := range batch {
		entries[i] = r.entry
	}
	type result struct {
		locs []volume.Locator
		err  error
	}
	res := make([]result, len(ov.reps))
	start := ov.now()
	var wg sync.WaitGroup
	for i, rep := range ov.reps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i].locs, res[i].err = rep.w.Append(entries)
		}()
	}
	wg.Wait()
	ov.fsyncs.fsyncs.Add(1)
	ov.fsyncs.fsyncNanos.Add(uint64(ov.now().Sub(start)))

	full, failed := 0, error(nil)
	for _, r := range res {
		switch {
		case r.err == nil:
		case errors.Is(r.err, volume.ErrFull):
			full++
		default:
			if failed == nil {
				failed = r.err
			}
		}
	}
	if failed == nil && full == 0 {
		// Replicas must agree on every locator: they are byte-identical.
		for _, r := range res[1:] {
			for i := range r.locs {
				if r.locs[i] != res[0].locs[i] {
					failed = errors.New("replicas disagree on a record locator")
				}
			}
		}
	}
	for i, r := range res {
		if r.err != nil && !errors.Is(r.err, volume.ErrFull) {
			ov.bad = append(ov.bad, ov.reps[i].d)
		}
	}
	var err error
	switch {
	case failed != nil:
		err = fmt.Errorf("%w: %w", errReplicaFailed, failed)
	case full > 0:
		// All replicas are in lockstep, so all report full; anything mixed
		// is a failure.
		if full == len(res) {
			err = errVolumeFull
		} else {
			err = fmt.Errorf("%w: replicas disagree about volume size", errReplicaFailed)
		}
	}
	if err != nil {
		for _, r := range batch {
			r.resp <- appendResp{err: err}
		}
		return err
	}
	ov.lastWrite.Store(ov.now().UnixNano())
	for i, r := range batch {
		r.resp <- appendResp{loc: res[0].locs[i]}
	}
	return nil
}

// committed returns the minimum acknowledged length over the replicas.
func (ov *openVolume) committed() int64 {
	min := int64(-1)
	for _, r := range ov.reps {
		if c := r.w.Committed(); min < 0 || c < min {
			min = c
		}
	}
	return min
}

// released gives back the open-volume slots the replicas held on their disks.
func (ov *openVolume) released() {
	for _, r := range ov.reps {
		r.d.mu.Lock()
		r.d.open--
		r.d.mu.Unlock()
	}
}
