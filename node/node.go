package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"storage4/catalogue"
	"storage4/clock"
	"storage4/disk"
	"storage4/nameindex"
	"storage4/volume"
)

// Compatibility of this binary with the catalogue schema (docs/catalogue.md,
// "Versions: three different things").
const (
	requiresSchema = 1
	compatLevel    = 1
)

// bucketCacheTTL bounds how stale this node's view of a bucket's state may be.
const bucketCacheTTL = 5 * time.Second

var errTooManyOpen = errors.New("node: too many open volumes")

// ErrClusterMismatch: no disk belongs to the catalogue's cluster and node.
var ErrClusterMismatch = errors.New("node: disks do not belong to this cluster and node")

// Deps are the injected dependencies (design-notes §15).
type Deps struct {
	Clock     clock.Clock
	Rand      io.Reader
	Disks     []disk.FS // one FS per local disk; each holds its identity file
	Catalogue catalogue.Catalogue
	Alarms    Alarms       // optional; defaults to a no-op sink
	Logger    *slog.Logger // optional; defaults to a discarding logger
}

type bucketEntry struct {
	b       catalogue.Bucket
	fetched time.Time
}

// Node is one storage node: HTTP API, shard leases, volumes on local disks.
type Node struct {
	cfg Config
	d   Deps

	nodeID   catalogue.NodeID
	meta     catalogue.Meta
	instance [16]byte
	disks    []*localDisk
	byID     map[catalogue.DiskID]*localDisk
	ids      *idCounter

	rngMu sync.Mutex
	rng   *rand.Rand

	budget *budget
	locks  nameLocks
	cnt    counters

	mu      sync.Mutex
	shards  map[shardKey]*shard
	buckets map[string]bucketEntry

	ovMu     sync.Mutex
	openVols map[*openVolume]struct{}

	reqMu    sync.RWMutex
	stopping bool
	inflight sync.WaitGroup

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	srv     *http.Server
	stopMu  sync.Mutex
	stopped bool
}

// Start validates the configuration, checks the catalogue and the disks,
// seals the volumes a previous run left open, starts the background loop and
// the optional HTTP listener, and returns when the node is ready.
func Start(ctx context.Context, cfg Config, d Deps) (*Node, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if d.Clock == nil || d.Rand == nil || d.Catalogue == nil || len(d.Disks) == 0 {
		return nil, fmt.Errorf("%w: Clock, Rand, Catalogue and at least one disk are required", ErrConfig)
	}
	if d.Alarms == nil {
		d.Alarms = NopAlarms{}
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	n := &Node{cfg: cfg, d: d, byID: map[catalogue.DiskID]*localDisk{},
		budget: newBudget(cfg.MemoryBudget), shards: map[shardKey]*shard{},
		buckets: map[string]bucketEntry{}, openVols: map[*openVolume]struct{}{}}
	var seed [32]byte
	if _, err := io.ReadFull(d.Rand, seed[:]); err != nil {
		return nil, fmt.Errorf("node: random source: %w", err)
	}
	n.rng = rand.New(rand.NewChaCha8(seed))
	if _, err := io.ReadFull(d.Rand, n.instance[:]); err != nil {
		return nil, fmt.Errorf("node: random source: %w", err)
	}
	if err := n.checkCatalogue(ctx); err != nil {
		return nil, err
	}
	if err := n.openDisks(ctx); err != nil {
		return nil, err
	}
	n.ids, err = loadCounter(ctx, n.nodeID, n.disks, d.Catalogue)
	if err != nil {
		n.closeStores()
		return nil, err
	}
	if err := n.recoverAll(ctx); err != nil {
		n.closeStores()
		return nil, fmt.Errorf("node start: %w", err)
	}
	n.ctx, n.cancel = context.WithCancel(context.WithoutCancel(ctx))
	if err := n.heartbeat(ctx); err != nil {
		n.cancel()
		n.closeStores()
		return nil, fmt.Errorf("node start: %w", err)
	}
	n.wg.Add(1)
	go n.maintain()
	if cfg.ListenAddr != "" {
		ln, err := net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			n.cancel()
			n.wg.Wait()
			n.closeStores()
			return nil, fmt.Errorf("node start: %w", err)
		}
		n.srv = &http.Server{Handler: n.Handler()}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			_ = n.srv.Serve(ln)
		}()
	}
	return n, nil
}

func (n *Node) checkCatalogue(ctx context.Context) error {
	m, err := n.d.Catalogue.Meta(ctx)
	if err != nil {
		return fmt.Errorf("node start: catalogue: %w", err)
	}
	if m.SchemaVersion < requiresSchema {
		return fmt.Errorf("node start: catalogue schema %d is older than required %d: run `store db migrate`", m.SchemaVersion, requiresSchema)
	}
	if m.MinCompat > compatLevel {
		return fmt.Errorf("node start: this binary (compat %d) is older than the catalogue requires (%d)", compatLevel, m.MinCompat)
	}
	n.meta = m
	n.nodeID, err = n.d.Catalogue.AssignNodeID(ctx, n.cfg.NodeName)
	if err != nil {
		return fmt.Errorf("node start: %w", err)
	}
	return nil
}

// openDisks reads every disk's identity, excludes the ones that do not
// belong to this cluster and node, and registers the rest.
func (n *Node) openDisks(ctx context.Context) error {
	for i, fsys := range n.d.Disks {
		id, err := disk.ReadIdentity(fsys)
		if err != nil || id.ClusterID != n.meta.ClusterID || catalogue.NodeID(id.NodeID) != n.nodeID {
			n.d.Alarms.Raise(AlarmDiskExcluded, Warning, fmt.Sprintf("disk %d has no valid identity for this cluster and node", i))
			continue
		}
		ld := &localDisk{fs: fsys, id: catalogue.DiskID(id.DiskID), state: catalogue.DiskActive}
		if err := n.d.Catalogue.RegisterDisk(ctx, ld.id, n.nodeID); err != nil {
			return fmt.Errorf("node start: register disk: %w", err)
		}
		ld.store = nameindex.NewStore(fsys, nameindex.Options{
			MaxHandles: n.cfg.NidxHandles, IdleTimeout: n.cfg.NidxIdle, Clock: n.d.Clock})
		n.disks = append(n.disks, ld)
		n.byID[ld.id] = ld
	}
	if len(n.disks) == 0 {
		return ErrClusterMismatch
	}
	return n.refreshDiskStates(ctx)
}

func (n *Node) refreshDiskStates(ctx context.Context) error {
	all, err := n.d.Catalogue.Disks(ctx)
	if err != nil {
		return err
	}
	for _, cd := range all {
		if ld := n.byID[cd.ID]; ld != nil {
			ld.mu.Lock()
			ld.state = cd.State
			ld.mu.Unlock()
		}
	}
	return nil
}

func (n *Node) closeStores() {
	for _, d := range n.disks {
		d.store.Close()
	}
}

// recoverAll seals every volume a previous run left open or sealing, so a
// restarted owner never resumes a volume (CLAUDE.md invariant).
func (n *Node) recoverAll(ctx context.Context) error {
	pending := map[shardKey]bool{}
	for _, d := range n.disks {
		vols, err := n.d.Catalogue.ListDiskVolumes(ctx, d.id)
		if err != nil {
			return err
		}
		for _, v := range vols {
			if v.State != catalogue.VolumeSealed && v.ID.Node() == n.nodeID {
				pending[shardKey{v.Bucket, v.Shard}] = true
			}
		}
	}
	keys := make([]shardKey, 0, len(pending))
	for k := range pending {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket != keys[j].bucket {
			return keys[i].bucket < keys[j].bucket
		}
		return keys[i].shard < keys[j].shard
	})
	for _, k := range keys {
		b, err := n.d.Catalogue.GetBucket(ctx, k.bucket)
		if err != nil {
			return err
		}
		if b.State == catalogue.BucketDeleted {
			continue // pull-based GC removes its volumes
		}
		sh := n.shardOf(b, k.shard)
		sh.mu.Lock()
		err = sh.acquireLocked(ctx)
		if err == nil {
			err = sh.releaseLocked(ctx)
		}
		sh.mu.Unlock()
		if err != nil {
			return fmt.Errorf("shard recovery: %w", err)
		}
	}
	return nil
}

// releaseLocked gives the lease back (idle shard, Stop).
func (sh *shard) releaseLocked(ctx context.Context) error {
	if !sh.leased {
		return nil
	}
	sh.leased = false
	return sh.n.d.Catalogue.ReleaseShard(ctx, sh.lease)
}

// shardOf returns the node's state for (bucket, shard index).
func (n *Node) shardOf(b catalogue.Bucket, idx int) *shard {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := shardKey{b.Name, idx}
	sh := n.shards[k]
	if sh == nil {
		sh = &shard{n: n, bucket: b.Name, idx: idx, shards: b.Shards}
		n.shards[k] = sh
	}
	return sh
}

// bucket returns the bucket, from a short-lived cache. With create, an
// unknown bucket is created with the default shard count.
func (n *Node) bucket(ctx context.Context, name string, create bool) (catalogue.Bucket, error) {
	now := n.d.Clock.Now()
	n.mu.Lock()
	e, ok := n.buckets[name]
	n.mu.Unlock()
	if ok && now.Sub(e.fetched) < bucketCacheTTL {
		return e.b, nil
	}
	b, err := n.d.Catalogue.GetBucket(ctx, name)
	if errors.Is(err, catalogue.ErrBucketNotFound) && create {
		b, _, err = n.d.Catalogue.CreateBucket(ctx, name, n.cfg.DefaultShards)
	}
	if err != nil {
		return catalogue.Bucket{}, err
	}
	n.mu.Lock()
	if b.State == catalogue.BucketDeleted {
		delete(n.buckets, name)
	} else {
		n.buckets[name] = bucketEntry{b: b, fetched: now}
	}
	n.mu.Unlock()
	return b, nil
}

// abandonBucket stops writing a deleted bucket: open volumes are closed
// without sealing (the catalogue cleared the leases; pull-based GC removes
// the files).
func (n *Node) abandonBucket(name string) {
	n.mu.Lock()
	var shards []*shard
	for k, sh := range n.shards {
		if k.bucket == name {
			shards = append(shards, sh)
		}
	}
	n.mu.Unlock()
	for _, sh := range shards {
		sh.mu.Lock()
		sh.abandonLocked()
		sh.mu.Unlock()
	}
}

func (n *Node) forgetBucket(name string) {
	n.mu.Lock()
	delete(n.buckets, name)
	n.mu.Unlock()
}

// placement picks the disks for a new volume: usable disks with the fewest
// open volumes first. With exactly three usable disks that is all of them.
func (n *Node) placement() []*localDisk {
	type cand struct {
		d    *localDisk
		open int
		i    int
	}
	var cs []cand
	for i, d := range n.disks {
		d.mu.Lock()
		open := d.open
		d.mu.Unlock()
		if d.placeable(n.d.Clock.Now()) {
			cs = append(cs, cand{d, open, i})
		}
	}
	sort.Slice(cs, func(a, b int) bool {
		if cs[a].open != cs[b].open {
			return cs[a].open < cs[b].open
		}
		return cs[a].i < cs[b].i
	})
	var out []*localDisk
	for _, c := range cs {
		if len(out) == ReplicaCount {
			break
		}
		out = append(out, c.d)
	}
	return out
}

func (n *Node) randPair() (uint64, uint64) {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	return n.rng.Uint64(), n.rng.Uint64()
}

func (n *Node) randUint32() uint32 {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	return n.rng.Uint32()
}

func (n *Node) volumeOpened(ov *openVolume) {
	n.ovMu.Lock()
	n.openVols[ov] = struct{}{}
	n.ovMu.Unlock()
}

func (n *Node) volumeClosed(ov *openVolume) {
	n.ovMu.Lock()
	delete(n.openVols, ov)
	n.ovMu.Unlock()
}

func (n *Node) openVolumes() []*openVolume {
	n.ovMu.Lock()
	defer n.ovMu.Unlock()
	out := make([]*openVolume, 0, len(n.openVols))
	for ov := range n.openVols {
		out = append(out, ov)
	}
	return out
}

// makeRoom seals the least recently written open volume when the cap is
// reached, before a shard opens another one.
func (n *Node) makeRoom(ctx context.Context) {
	ovs := n.openVolumes()
	if len(ovs) < n.cfg.MaxOpenVolumes {
		return
	}
	oldest := ovs[0]
	for _, ov := range ovs[1:] {
		if ov.lastWrite.Load() < oldest.lastWrite.Load() {
			oldest = ov
		}
	}
	if oldest.sh != nil {
		_ = oldest.sh.sealOpen(ctx, oldest, "open volume cap")
	}
}

// Metrics returns the node's metrics.
func (n *Node) Metrics() Metrics { return nodeMetrics{n} }

type nodeMetrics struct{ n *Node }

func (m nodeMetrics) Snapshot() Snapshot {
	n := m.n
	handles := 0
	for _, d := range n.disks {
		handles += d.store.OpenHandles()
	}
	n.mu.Lock()
	leases := 0
	for _, sh := range n.shards {
		sh.mu.Lock()
		if sh.leased {
			leases++
		}
		sh.mu.Unlock()
	}
	n.mu.Unlock()
	return Snapshot{
		Counters: n.cnt.snapshot(),
		Gauges: map[string]int64{
			"open_volumes":  int64(len(n.openVolumes())),
			"buffer_bytes":  n.budget.inUse(),
			"buffer_budget": n.cfg.MemoryBudget,
			"index_handles": int64(handles),
			"leases_held":   int64(leases),
			"disks_usable":  int64(len(n.placement())),
		},
	}
}

// maintain is the node's only background goroutine: lease renewal, idle
// seals, idle lease release, handle cleanup and heartbeats, every TTL/3.
func (n *Node) maintain() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.d.Clock.After(n.cfg.LeaseTTL / 3):
		}
		n.tick(n.ctx)
	}
}

func (n *Node) tick(ctx context.Context) {
	now := n.d.Clock.Now()
	n.mu.Lock()
	shards := make([]*shard, 0, len(n.shards))
	for _, sh := range n.shards {
		shards = append(shards, sh)
	}
	n.mu.Unlock()
	for _, sh := range shards {
		sh.mu.Lock()
		if sh.leased {
			if ov := sh.open; ov != nil && now.Sub(time.Unix(0, ov.lastWrite.Load())) >= n.cfg.VolumeIdle {
				_ = sh.sealOpenLocked(ctx, ov, "idle")
			}
			switch {
			case sh.leased && sh.open == nil:
				_ = sh.releaseLocked(ctx) // idle shards give their lease back
			case sh.leased && !now.Before(sh.renewedAt.Add(n.cfg.LeaseTTL/3)):
				if err := n.d.Catalogue.RenewShard(ctx, sh.lease); err == nil {
					sh.renewedAt = now
				} else if errors.Is(err, catalogue.ErrLeaseLost) || errors.Is(err, catalogue.ErrBucketDeleted) {
					sh.dropLeaseLocked()
				}
			}
		}
		sh.mu.Unlock()
	}
	for _, d := range n.disks {
		d.store.CloseIdle()
	}
	_ = n.refreshDiskStates(ctx)
	if err := n.heartbeat(ctx); err != nil {
		n.d.Logger.Warn("heartbeat failed", "err", err)
	}
}

func (n *Node) heartbeat(ctx context.Context) error {
	beats := make([]catalogue.DiskBeat, 0, len(n.disks))
	for _, d := range n.disks {
		d.mu.Lock()
		beats = append(beats, catalogue.DiskBeat{ID: d.id, OpenVolumes: d.open})
		d.mu.Unlock()
	}
	m, err := n.d.Catalogue.Heartbeat(ctx, catalogue.NodeBeat{
		ID: n.nodeID, Instance: n.instance, Addr: n.cfg.ListenAddr,
		Version: n.cfg.Version, Compat: compatLevel}, beats)
	if err != nil {
		if errors.Is(err, catalogue.ErrUnavailable) {
			n.d.Alarms.Raise(AlarmCatalogueUnavailable, Critical, "catalogue unreachable")
		}
		return err
	}
	n.d.Alarms.Clear(AlarmCatalogueUnavailable)
	if m.ClusterID != n.meta.ClusterID || m.Incarnation != n.meta.Incarnation {
		// A failover changed the catalogue's incarnation: every lease and open
		// volume is suspect. Abandon them; the next lease holder seals.
		n.d.Alarms.Raise(AlarmShardLeaseLost, Critical, "catalogue incarnation changed")
		n.meta = m
		n.mu.Lock()
		for _, sh := range n.shards {
			sh.mu.Lock()
			if sh.leased {
				sh.dropLeaseLocked()
			}
			sh.mu.Unlock()
		}
		n.mu.Unlock()
	}
	return nil
}

// Stop drains requests, seals every open volume, releases the leases, closes
// handles and joins every goroutine. Safe to call more than once.
func (n *Node) Stop(ctx context.Context) error {
	n.stopMu.Lock()
	defer n.stopMu.Unlock()
	if n.stopped {
		return nil
	}
	n.stopped = true
	n.reqMu.Lock()
	n.stopping = true
	n.reqMu.Unlock()
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	if n.srv != nil {
		keep(n.srv.Shutdown(ctx))
	}
	idle := make(chan struct{})
	go func() { n.inflight.Wait(); close(idle) }()
	select {
	case <-idle:
	case <-ctx.Done():
		keep(ctx.Err())
	}
	n.cancel()
	n.wg.Wait()
	n.mu.Lock()
	shards := make([]*shard, 0, len(n.shards))
	for _, sh := range n.shards {
		shards = append(shards, sh)
	}
	n.mu.Unlock()
	for _, sh := range shards {
		sh.mu.Lock()
		if sh.open != nil {
			// A lost lease or a deleted bucket leaves the volume for the
			// next lease holder (or GC) to seal; that is not a Stop failure.
			if err := sh.sealOpenLocked(ctx, sh.open, "stop"); err != nil && !errors.Is(err, catalogue.ErrLeaseLost) &&
				!errors.Is(err, catalogue.ErrBucketDeleted) && !errors.Is(err, errLeaseUnavailable) {
				keep(err)
			}
		}
		if err := sh.releaseLocked(ctx); err != nil && !errors.Is(err, catalogue.ErrBucketDeleted) {
			keep(err)
		}
		sh.mu.Unlock()
	}
	n.closeStores()
	// Last: tell the catalogue this instance is gone, so a restart does not
	// wait for NodeTTL.
	if err := n.d.Catalogue.Goodbye(ctx, n.nodeID, n.instance); err != nil {
		keep(err)
	}
	return first
}

// recoverVolume seals a volume that has no live writer here: a leftover of a
// previous run, or a volume whose creation failed. Replicas are sealed at the
// minimum valid length (everything beyond is unacknowledged and discarded).
// Replicas that are missing or unusable are marked in the catalogue.
func (n *Node) recoverVolume(ctx context.Context, l catalogue.Lease, v catalogue.Volume) error {
	epoch, err := n.d.Catalogue.BeginSeal(ctx, l, v.ID)
	if errors.Is(err, catalogue.ErrVolumeSealed) {
		return nil
	}
	if err != nil {
		return err
	}
	id := uint64(v.ID)
	path := volume.Path(v.Bucket, id)
	type rep struct {
		d *localDisk
		c int64
	}
	var good []rep
	for _, cr := range v.Replicas {
		d := n.byID[cr.Disk]
		if d == nil || cr.State != catalogue.ReplicaOK {
			continue
		}
		f, err := d.fs.OpenFile(path, 0)
		if err != nil {
			st := catalogue.ReplicaFailed
			if errors.Is(err, disk.ErrNotExist) {
				st = catalogue.ReplicaLost
			}
			_ = n.d.Catalogue.SetReplicaState(ctx, v.ID, d.id, st)
			n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "a volume replica is missing or unreadable")
			continue
		}
		res, err := volume.Scan(f, nil)
		f.Close()
		if err != nil || res.Header.VolumeID != id {
			_ = n.d.Catalogue.SetReplicaState(ctx, v.ID, d.id, catalogue.ReplicaFailed)
			n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "a volume replica is unreadable")
			continue
		}
		c := res.End
		if res.Sealed {
			c = int64(res.Seal.CommittedLength)
		}
		good = append(good, rep{d, c})
	}
	var committed int64
	var records uint32
	sealed := 0
	for i, r := range good {
		if i == 0 || r.c < committed {
			committed = r.c
		}
	}
	for _, r := range good {
		seal, err := volume.SealFile(r.d.fs, path, committed, epoch, n.randUint32)
		if err != nil {
			_ = n.d.Catalogue.SetReplicaState(ctx, v.ID, r.d.id, catalogue.ReplicaFailed)
			n.d.Alarms.Raise(AlarmReplicaUnavailable, Critical, "a volume replica could not be sealed")
			continue
		}
		sealed++
		records = seal.RecordCount
		if _, _, err := volume.EnsureIdx(r.d.fs, v.Bucket, id); err != nil {
			return fmt.Errorf("index: %w", err)
		}
		rebuilt, err := r.d.store.Ensure(v.Bucket, id)
		if err != nil {
			return fmt.Errorf("name index: %w", err)
		}
		if rebuilt {
			n.cnt.indexRebuilds.Add(1)
		}
	}
	if sealed == 0 {
		committed, records = 0, 0
	}
	if err := n.d.Catalogue.FinishSeal(ctx, l, v.ID, epoch, committed, records); err != nil {
		return err
	}
	n.cnt.volumesSealed.Add(1)
	return nil
}
