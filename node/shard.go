package node

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"storage4/catalogue"
	"storage4/disk"
	"storage4/format"
	"storage4/nameindex"
	"storage4/volume"
)

var (
	// errNoSuchName: no version of the name exists (HTTP 404).
	errNoSuchName = errors.New("node: no such name")
	// errNameErased: the name's latest version is on the erasure list (410).
	errNameErased = errors.New("node: name erased")
	// errNoReplicaSet: fewer than ReplicaCount usable disks (503).
	errNoReplicaSet = errors.New("node: no healthy replica set")
	// errLeaseUnavailable: the shard lease cannot be held (503).
	errLeaseUnavailable = errors.New("node: shard lease unavailable")
)

type shardKey struct {
	bucket string
	shard  int
}

// shard is the node's state for one (bucket, shard): its lease, its one open
// volume and its sealed volumes, newest volume number first.
type shard struct {
	n      *Node
	bucket string
	idx    int
	shards int // S of the bucket

	mu        sync.Mutex
	loaded    bool
	leased    bool
	lease     catalogue.Lease
	renewedAt time.Time
	sealed    []catalogue.Volume
	open      *openVolume
	erased    map[catalogue.Locator]struct{}
}

// loadLocked reads the shard's volumes and erasure list once.
func (sh *shard) loadLocked(ctx context.Context) error {
	if sh.loaded {
		return nil
	}
	return sh.refreshLocked(ctx)
}

// refreshLocked re-reads the sealed volumes and the erasure list.
func (sh *shard) refreshLocked(ctx context.Context) error {
	vols, err := sh.n.d.Catalogue.ListShardVolumes(ctx, sh.bucket, sh.idx)
	if err != nil {
		return err
	}
	sh.sealed = sh.sealed[:0]
	for _, v := range vols {
		if v.State == catalogue.VolumeSealed {
			sh.sealed = append(sh.sealed, v)
		}
	}
	er, err := sh.n.d.Catalogue.ShardErasures(ctx, sh.bucket, sh.idx)
	if err != nil {
		return err
	}
	sh.erased = make(map[catalogue.Locator]struct{}, len(er))
	for _, l := range er {
		sh.erased[l] = struct{}{}
	}
	sh.loaded = true
	return nil
}

// acquireLocked makes sure the shard lease is held with enough validity left,
// recovering leftover volumes of this node when the lease is newly taken.
func (sh *shard) acquireLocked(ctx context.Context) error {
	n := sh.n
	now := n.d.Clock.Now()
	ttl := n.cfg.LeaseTTL
	if sh.leased {
		if now.Before(sh.renewedAt.Add(ttl - ttl/3)) {
			return nil
		}
		if err := n.d.Catalogue.RenewShard(ctx, sh.lease); err == nil {
			sh.renewedAt = now
			return nil
		} else if errors.Is(err, catalogue.ErrLeaseLost) || errors.Is(err, catalogue.ErrBucketDeleted) {
			sh.dropLeaseLocked()
		} else {
			return err
		}
	}
	l, err := n.d.Catalogue.AcquireShard(ctx, sh.bucket, sh.idx, n.nodeID, ttl)
	if err != nil {
		return err
	}
	sh.lease, sh.leased, sh.renewedAt = l, true, now
	if err := sh.refreshLocked(ctx); err != nil {
		return err
	}
	return sh.recoverLeftoversLocked(ctx)
}

// dropLeaseLocked forgets the lease after it was lost: the open volume's
// writers are closed without sealing (the catalogue fence is gone); the next
// lease holder, possibly this node again, seals it.
func (sh *shard) dropLeaseLocked() {
	sh.abandonLocked()
	sh.n.d.Alarms.Raise(AlarmShardLeaseLost, Warning, "shard lease lost; open volume abandoned until the next lease")
}

// abandonLocked closes the open volume's writers without sealing and forgets
// the lease.
func (sh *shard) abandonLocked() {
	if sh.open != nil {
		sh.open.close()
		for _, r := range sh.open.reps {
			r.w.Close()
		}
		sh.open.released()
		sh.n.volumeClosed(sh.open)
		sh.open = nil
	}
	sh.leased = false
}

// recoverLeftoversLocked seals every volume of this shard that is not sealed
// and has no live writer here (a previous run of this node, a failed seal).
func (sh *shard) recoverLeftoversLocked(ctx context.Context) error {
	vols, err := sh.n.d.Catalogue.ListShardVolumes(ctx, sh.bucket, sh.idx)
	if err != nil {
		return err
	}
	for _, v := range vols {
		if v.State == catalogue.VolumeSealed || (sh.open != nil && sh.open.vol.ID == v.ID) {
			continue
		}
		if err := sh.n.recoverVolume(ctx, sh.lease, v); err != nil {
			return fmt.Errorf("recover volume: %w", err)
		}
	}
	return sh.refreshLocked(ctx)
}

// leaseOnly makes sure the lease is held without opening a volume.
func (sh *shard) leaseOnly(ctx context.Context) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.acquireLocked(ctx)
}

// writable returns the shard's open volume, taking the lease and opening a
// volume if needed.
func (sh *shard) writable(ctx context.Context) (*openVolume, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if err := sh.acquireLocked(ctx); err != nil {
		return nil, err
	}
	if sh.open == nil {
		ov, err := sh.openNewLocked(ctx)
		if err != nil {
			return nil, err
		}
		sh.open = ov
	}
	return sh.open, nil
}

// openNewLocked picks three disks, registers the volume (which assigns its
// volume number under the lease), creates the three replicas and starts the
// committer.
func (sh *shard) openNewLocked(ctx context.Context) (*openVolume, error) {
	n := sh.n
	if len(sh.n.openVolumes()) >= n.cfg.MaxOpenVolumes {
		return nil, errTooManyOpen
	}
	disks := n.placement()
	if len(disks) < ReplicaCount {
		n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "fewer than 3 usable disks")
		return nil, errNoReplicaSet
	}
	id, err := n.ids.allocate()
	if err != nil {
		return nil, err
	}
	params := catalogue.OpenParams{ID: id, FormatMajor: format.Major, FormatMinor: format.Minor}
	for _, d := range disks {
		params.Disks = append(params.Disks, d.id)
	}
	vol, err := n.d.Catalogue.OpenVolume(ctx, sh.lease, params)
	if err != nil {
		return nil, err
	}
	hdr := format.VolumeHeader{
		VolumeID: uint64(vol.ID), ClusterID: n.meta.ClusterID, Shard: uint32(sh.idx),
		ShardCount: uint32(sh.shards), VolumeNumber: uint64(vol.VolumeNumber),
		CreatedUnixNano: n.d.Clock.Now().UnixNano(), Epoch: vol.Epoch, Bucket: sh.bucket,
	}
	seed1, seed2 := n.randPair()
	var reps []*replicaW
	var createErr error
	for _, d := range disks {
		// Every replica draws the same check sequence, so the replicas are
		// byte-identical and one locator serves them all.
		pcg := rand.New(rand.NewPCG(seed1, seed2))
		w, err := volume.Create(d.fs, hdr, volume.Options{MaxSize: n.cfg.VolumeMaxSize, Rand: pcg.Uint32})
		if err != nil {
			createErr = fmt.Errorf("create replica: %w", err)
			break
		}
		reps = append(reps, &replicaW{d: d, w: w})
	}
	if createErr != nil {
		for _, r := range reps {
			r.w.Close()
		}
		// The volume is registered but unusable: seal what exists and move on.
		if err := n.recoverVolume(ctx, sh.lease, vol); err != nil {
			return nil, fmt.Errorf("%w (and recovery: %w)", createErr, err)
		}
		n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "creating a volume replica failed")
		_ = sh.refreshLocked(ctx)
		return nil, createErr
	}
	ov := newOpenVolume(vol, reps, &n.cnt, n.d.Clock.Now)
	ov.sh = sh
	for _, r := range reps {
		r.d.mu.Lock()
		r.d.open++
		r.d.mu.Unlock()
	}
	n.volumeOpened(ov)
	ov.start()
	return ov, nil
}

// sealOpen seals ov if it is still the shard's open volume.
func (sh *shard) sealOpen(ctx context.Context, ov *openVolume, reason string) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.open != ov {
		return nil
	}
	return sh.sealOpenLocked(ctx, ov, reason)
}

func (sh *shard) sealOpenLocked(ctx context.Context, ov *openVolume, reason string) error {
	n := sh.n
	ov.close()
	defer func() {
		ov.released()
		n.volumeClosed(ov)
		sh.open = nil
	}()
	if !sh.leased {
		for _, r := range ov.reps {
			r.w.Close()
		}
		return errLeaseUnavailable
	}
	epoch, err := n.d.Catalogue.BeginSeal(ctx, sh.lease, ov.vol.ID)
	if err != nil {
		for _, r := range ov.reps {
			r.w.Close()
		}
		if errors.Is(err, catalogue.ErrLeaseLost) {
			sh.leased = false
		}
		return fmt.Errorf("seal (%s): %w", reason, err)
	}
	committed := ov.committed()
	var records uint32
	sealedAny := false
	builder := ov.omap.Builder()
	for _, r := range ov.reps {
		path := volume.Path(sh.bucket, ov.id)
		var seal format.Seal
		var idx *volume.Idx
		var err error
		if r.w.Err() == nil && r.w.Committed() == committed {
			if seal, err = r.w.Seal(epoch); err == nil {
				idx, err = r.w.Idx()
			}
		} else {
			r.w.Close()
			seal, err = volume.SealFile(r.d.fs, path, committed, epoch, n.randUint32)
		}
		if err != nil {
			n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "a replica could not be sealed")
			_ = n.d.Catalogue.SetReplicaState(ctx, ov.vol.ID, r.d.id, catalogue.ReplicaFailed)
			continue
		}
		sealedAny = true
		records = seal.RecordCount
		if idx != nil {
			err = volume.WriteIdx(r.d.fs, sh.bucket, idx)
		} else {
			_, _, err = ensureIdx(r.d.fs, sh.bucket, ov.id)
		}
		if err == nil {
			err = r.d.store.Write(sh.bucket, ov.id, builder)
		}
		if err != nil {
			// Index files are caches: rebuilt on demand. Not a failed seal.
			n.d.Alarms.Raise(AlarmIndexRebuilt, Info, "an index file could not be written at seal")
		}
	}
	if !sealedAny {
		committed, records = 0, 0
	}
	if err := n.d.Catalogue.FinishSeal(ctx, sh.lease, ov.vol.ID, epoch, committed, records); err != nil {
		return fmt.Errorf("seal (%s): %w", reason, err)
	}
	n.cnt.volumesSealed.Add(1)
	if v, err := n.d.Catalogue.GetVolume(ctx, ov.vol.ID); err == nil {
		sh.sealed = append([]catalogue.Volume{v}, sh.sealed...)
		sort.SliceStable(sh.sealed, func(i, j int) bool { return sh.sealed[i].VolumeNumber > sh.sealed[j].VolumeNumber })
	}
	return nil
}

func ensureIdx(fsys disk.FS, bucket string, id uint64) (*volume.Idx, bool, error) {
	return volume.EnsureIdx(fsys, bucket, id)
}

// found is the latest version of a name with its verified record head. The
// caller must call release.
type found struct {
	disk        *localDisk // the replica that served the record
	head        *volume.Head
	release     func()
	volumeNum   int64
	fromOpenVol bool
}

// lookup finds the latest version of name: the open volume first, then the
// sealed volumes newest first (docs/node.md §5). The first verified hit wins.
func (sh *shard) lookup(ctx context.Context, name string) (*found, error) {
	return sh.lookupExcluding(ctx, name, nil)
}

// lookupExcluding is lookup that skips the replicas on the excluded disks:
// a GET that found a corrupt copy asks again for another one.
func (sh *shard) lookupExcluding(ctx context.Context, name string, excl map[*localDisk]bool) (*found, error) {
	sh.mu.Lock()
	if err := sh.loadLocked(ctx); err != nil {
		sh.mu.Unlock()
		return nil, err
	}
	ov := sh.open
	sealed := append([]catalogue.Volume(nil), sh.sealed...)
	erased := sh.erased
	sh.mu.Unlock()

	if ov != nil {
		if e, ok := ov.omap.Get(name); ok {
			f, err := sh.readOpen(ov, e.Loc, name, excl)
			if err != nil {
				return nil, err
			}
			return sh.checkErased(f, ov.vol.ID, erased)
		}
	}
	for _, v := range sealed {
		f, err := sh.lookupSealed(v, name, excl)
		if err != nil {
			return nil, err
		}
		if f != nil {
			return sh.checkErased(f, v.ID, erased)
		}
	}
	return nil, errNoSuchName
}

func (sh *shard) checkErased(f *found, vid catalogue.VolumeID, erased map[catalogue.Locator]struct{}) (*found, error) {
	l := f.head.Locator()
	if _, gone := erased[catalogue.Locator{Volume: vid, Seq: l.Seq, Check: l.Check}]; gone {
		f.release()
		return nil, errNameErased
	}
	return f, nil
}

// readOpen reads the record of an open volume's name-map entry from the first
// replica that serves it.
func (sh *shard) readOpen(ov *openVolume, loc volume.Locator, name string, excl map[*localDisk]bool) (*found, error) {
	var lastErr error
	for _, r := range ov.reps {
		if excl[r.d] {
			continue
		}
		f, err := r.d.fs.OpenFile(volume.Path(sh.bucket, ov.id), 0)
		if err != nil {
			lastErr = err
			continue
		}
		h, err := volume.ReadHead(f, loc, name)
		if err != nil {
			f.Close()
			lastErr = err
			continue
		}
		return &found{disk: r.d, head: h, release: func() { f.Close() }, volumeNum: ov.vol.VolumeNumber, fromOpenVol: true}, nil
	}
	return nil, fmt.Errorf("read open volume: %w", lastErr)
}

// lookupSealed returns the name's record in a sealed volume, or nil if the
// volume has none. Replicas are tried in turn when one fails.
func (sh *shard) lookupSealed(v catalogue.Volume, name string, excl map[*localDisk]bool) (*found, error) {
	var lastErr error
	tried := 0
	for _, rep := range v.Replicas {
		if rep.State != catalogue.ReplicaOK {
			continue
		}
		d := sh.n.byID[rep.Disk]
		if d == nil || excl[d] {
			continue
		}
		tried++
		hit, err := d.store.Lookup(sh.bucket, uint64(v.ID), name)
		if errors.Is(err, nameindex.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			lastErr = err
			continue
		}
		return &found{disk: d, head: hit.Head, release: hit.Release, volumeNum: v.VolumeNumber}, nil
	}
	if tried == 0 {
		return nil, nil // a volume with no usable replica holds nothing readable
	}
	return nil, fmt.Errorf("lookup in sealed volume: %w", lastErr)
}
