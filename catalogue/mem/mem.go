// Package mem is the in-memory Catalogue: the simulator's catalogue and the
// executable reference for the contract suite (catalogue/cattest). It has no
// durability; Close does nothing.
package mem

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"storage4/catalogue"
	"storage4/clock"
	"storage4/names"
)

type shardKey struct {
	bucket string
	shard  int
}

type shardRow struct {
	nextOrdinal  int64
	leaseNode    catalogue.NodeID // 0 = none
	generation   int64
	leaseExpires int64 // Unix ms
}

type volRow struct {
	v        catalogue.Volume
	created  int64
	sealedMs int64
	replicas map[catalogue.DiskID]catalogue.ReplicaState
}

type erasureKey struct {
	vol catalogue.VolumeID
	seq uint32
}

type targetKey struct {
	e    erasureKey
	disk catalogue.DiskID
}

type nodeRow struct {
	id       catalogue.NodeID
	name     string
	beat     catalogue.NodeBeat
	beatSeen int64 // Unix ms; 0 = never
}

type diskRow struct {
	d    catalogue.Disk
	seen int64
}

// Catalogue is the in-memory implementation. Safe for concurrent use.
type Catalogue struct {
	clk     clock.Clock
	nodeTTL time.Duration

	mu         sync.Mutex
	meta       catalogue.Meta
	buckets    map[string]*bucketRow
	shards     map[shardKey]*shardRow
	volumes    map[catalogue.VolumeID]*volRow
	erasures   map[erasureKey]uint32 // check
	targets    map[targetKey]int64   // applied ms; 0 = pending
	nodes      map[catalogue.NodeID]*nodeRow
	nodeByName map[string]catalogue.NodeID
	disks      map[catalogue.DiskID]*diskRow
}

type bucketRow struct {
	b       catalogue.Bucket
	created int64
}

// New returns an initialised in-memory catalogue.
func New(clk clock.Clock, meta catalogue.Meta, o catalogue.Options) *Catalogue {
	if o.NodeTTL <= 0 {
		o.NodeTTL = 30 * time.Second
	}
	return &Catalogue{
		clk: clk, nodeTTL: o.NodeTTL, meta: meta,
		buckets:    map[string]*bucketRow{},
		shards:     map[shardKey]*shardRow{},
		volumes:    map[catalogue.VolumeID]*volRow{},
		erasures:   map[erasureKey]uint32{},
		targets:    map[targetKey]int64{},
		nodes:      map[catalogue.NodeID]*nodeRow{},
		nodeByName: map[string]catalogue.NodeID{},
		disks:      map[catalogue.DiskID]*diskRow{},
	}
}

var _ catalogue.Catalogue = (*Catalogue)(nil)

// SetIncarnation simulates the failover runbook (tests and sim).
func (c *Catalogue) SetIncarnation(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.meta.Incarnation = n
}

func (c *Catalogue) nowMs() int64 { return c.clk.Now().UnixMilli() }

func tm(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func (c *Catalogue) Close() error { return nil }

func (c *Catalogue) Meta(ctx context.Context) (catalogue.Meta, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Meta{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.meta, nil
}

// ---- buckets ----

func (c *Catalogue) CreateBucket(ctx context.Context, name string, shards int) (catalogue.Bucket, bool, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Bucket{}, false, err
	}
	if names.ValidateBucket(name) != nil || shards < 1 || shards > 1<<31-1 || names.ValidateShardCount(uint32(shards)) != nil {
		return catalogue.Bucket{}, false, catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.buckets[name]; ok {
		switch {
		case r.b.State == catalogue.BucketDeleted:
			return catalogue.Bucket{}, false, catalogue.ErrBucketDeleted
		case r.b.Shards != shards:
			return catalogue.Bucket{}, false, catalogue.ErrBucketConflict
		}
		return r.b, false, nil
	}
	now := c.nowMs()
	b := catalogue.Bucket{Name: name, Shards: shards, State: catalogue.BucketActive, CreatedAt: tm(now)}
	c.buckets[name] = &bucketRow{b: b, created: now}
	return b, true, nil
}

func (c *Catalogue) GetBucket(ctx context.Context, name string) (catalogue.Bucket, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Bucket{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.buckets[name]
	if !ok {
		return catalogue.Bucket{}, catalogue.ErrBucketNotFound
	}
	return r.b, nil
}

func (c *Catalogue) DeleteBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.buckets[name]
	if !ok {
		return catalogue.ErrBucketNotFound
	}
	if r.b.State == catalogue.BucketDeleted {
		return nil
	}
	r.b.State = catalogue.BucketDeleted
	r.b.DeletedAt = tm(c.nowMs())
	for k, s := range c.shards {
		if k.bucket == name {
			s.leaseNode, s.leaseExpires = 0, 0
		}
	}
	return nil
}

func (c *Catalogue) ListBuckets(ctx context.Context) ([]catalogue.Bucket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]catalogue.Bucket, 0, len(c.buckets))
	for _, r := range c.buckets {
		out = append(out, r.b)
	}
	slices.SortFunc(out, func(a, b catalogue.Bucket) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

// ---- leases ----

func (c *Catalogue) AcquireShard(ctx context.Context, bucket string, shard int, node catalogue.NodeID, ttl time.Duration) (catalogue.Lease, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Lease{}, err
	}
	if node == 0 || ttl < time.Millisecond {
		return catalogue.Lease{}, catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.buckets[bucket]
	switch {
	case !ok:
		return catalogue.Lease{}, catalogue.ErrBucketNotFound
	case b.b.State == catalogue.BucketDeleted:
		return catalogue.Lease{}, catalogue.ErrBucketDeleted
	case shard < 0 || shard >= b.b.Shards:
		return catalogue.Lease{}, catalogue.ErrInvalidArgument
	}
	k := shardKey{bucket, shard}
	s := c.shards[k]
	if s == nil {
		s = &shardRow{nextOrdinal: 1}
		c.shards[k] = s
	}
	now := c.nowMs()
	if s.leaseNode != 0 && s.leaseNode != node && s.leaseExpires > now {
		return catalogue.Lease{}, &catalogue.HeldError{Node: s.leaseNode, Expires: tm(s.leaseExpires)}
	}
	s.leaseNode = node
	s.generation++
	s.leaseExpires = now + ttl.Milliseconds()
	return catalogue.Lease{Bucket: bucket, Shard: shard, Node: node, Generation: s.generation, TTL: ttl}, nil
}

// current returns the shard row if l is its current lease (holder and
// generation); withExpiry also requires the lease to be unexpired.
func (c *Catalogue) current(l catalogue.Lease, withExpiry bool) *shardRow {
	s := c.shards[shardKey{l.Bucket, l.Shard}]
	if s == nil || s.leaseNode != l.Node || s.generation != l.Generation || l.Node == 0 {
		return nil
	}
	if withExpiry && s.leaseExpires <= c.nowMs() {
		return nil
	}
	return s
}

func (c *Catalogue) RenewShard(ctx context.Context, l catalogue.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.current(l, false)
	if s == nil {
		return catalogue.ErrLeaseLost
	}
	s.leaseExpires = c.nowMs() + l.TTL.Milliseconds()
	return nil
}

func (c *Catalogue) ReleaseShard(ctx context.Context, l catalogue.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.current(l, false); s != nil {
		s.leaseNode, s.leaseExpires = 0, 0
	}
	return nil
}

func (c *Catalogue) ShardOwner(ctx context.Context, bucket string, shard int) (catalogue.Owner, bool, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Owner{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.shards[shardKey{bucket, shard}]
	if s == nil || s.leaseNode == 0 || s.leaseExpires <= c.nowMs() {
		return catalogue.Owner{}, false, nil
	}
	return catalogue.Owner{Node: s.leaseNode, Generation: s.generation, Expires: tm(s.leaseExpires)}, true, nil
}

// ---- volumes ----

func (c *Catalogue) snapshot(r *volRow) catalogue.Volume {
	v := r.v
	v.CreatedAt, v.SealedAt = tm(r.created), tm(r.sealedMs)
	v.Replicas = make([]catalogue.Replica, 0, len(r.replicas))
	for d, s := range r.replicas {
		v.Replicas = append(v.Replicas, catalogue.Replica{Disk: d, State: s})
	}
	slices.SortFunc(v.Replicas, func(a, b catalogue.Replica) int { return slices.Compare(a.Disk[:], b.Disk[:]) })
	return v
}

func (c *Catalogue) OpenVolume(ctx context.Context, l catalogue.Lease, p catalogue.OpenParams) (catalogue.Volume, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Volume{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.current(l, true)
	if s == nil {
		return catalogue.Volume{}, catalogue.ErrLeaseLost
	}
	if p.ID == 0 || p.ID.Node() != l.Node || len(p.Disks) == 0 {
		return catalogue.Volume{}, catalogue.ErrInvalidArgument
	}
	if p.CipherID != c.buckets[l.Bucket].b.CipherID {
		return catalogue.Volume{}, catalogue.ErrInvalidArgument
	}
	if _, dup := c.volumes[p.ID]; dup {
		return catalogue.Volume{}, catalogue.ErrVolumeExists
	}
	seen := map[catalogue.DiskID]bool{}
	for _, d := range p.Disks {
		if seen[d] {
			return catalogue.Volume{}, catalogue.ErrInvalidArgument
		}
		seen[d] = true
		dr := c.disks[d]
		switch {
		case dr == nil:
			return catalogue.Volume{}, catalogue.ErrUnknownDisk
		case dr.d.State != catalogue.DiskActive:
			return catalogue.Volume{}, catalogue.ErrDiskUnavailable
		}
	}
	r := &volRow{created: c.nowMs(), replicas: map[catalogue.DiskID]catalogue.ReplicaState{}}
	r.v = catalogue.Volume{ID: p.ID, Bucket: l.Bucket, Shard: l.Shard, Ordinal: s.nextOrdinal,
		State: catalogue.VolumeOpen, Epoch: 1, FormatMajor: p.FormatMajor, FormatMinor: p.FormatMinor,
		CipherID: p.CipherID, Tier: p.Tier}
	s.nextOrdinal++
	for _, d := range p.Disks {
		r.replicas[d] = catalogue.ReplicaOK
	}
	c.volumes[p.ID] = r
	return c.snapshot(r), nil
}

// owned returns the volume if it belongs to the lease's shard.
func (c *Catalogue) owned(l catalogue.Lease, id catalogue.VolumeID) (*volRow, error) {
	r := c.volumes[id]
	if r == nil || r.v.Bucket != l.Bucket || r.v.Shard != l.Shard {
		return nil, catalogue.ErrVolumeNotFound
	}
	return r, nil
}

func (c *Catalogue) BeginSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current(l, true) == nil {
		return 0, catalogue.ErrLeaseLost
	}
	r, err := c.owned(l, id)
	if err != nil {
		return 0, err
	}
	if r.v.State == catalogue.VolumeSealed {
		return 0, catalogue.ErrVolumeSealed
	}
	r.v.Epoch++
	r.v.State = catalogue.VolumeSealing
	return r.v.Epoch, nil
}

func (c *Catalogue) FinishSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID, epoch uint32, committed int64, records uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current(l, true) == nil {
		return catalogue.ErrLeaseLost
	}
	r, err := c.owned(l, id)
	if err != nil {
		return err
	}
	if committed < 0 {
		return catalogue.ErrInvalidArgument
	}
	if r.v.State == catalogue.VolumeSealed {
		if r.v.Epoch == epoch && r.v.CommittedLength == committed && r.v.RecordCount == records {
			return nil
		}
		return catalogue.ErrSealMismatch
	}
	if r.v.Epoch != epoch {
		return catalogue.ErrEpochMismatch
	}
	r.v.State, r.v.CommittedLength, r.v.RecordCount = catalogue.VolumeSealed, committed, records
	r.sealedMs = c.nowMs()
	return nil
}

func (c *Catalogue) GetVolume(ctx context.Context, id catalogue.VolumeID) (catalogue.Volume, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Volume{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.volumes[id]
	if r == nil {
		return catalogue.Volume{}, catalogue.ErrVolumeNotFound
	}
	return c.snapshot(r), nil
}

func (c *Catalogue) ListShardVolumes(ctx context.Context, bucket string, shard int) ([]catalogue.Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalogue.Volume
	for _, r := range c.volumes {
		if r.v.Bucket == bucket && r.v.Shard == shard {
			out = append(out, c.snapshot(r))
		}
	}
	slices.SortFunc(out, func(a, b catalogue.Volume) int { return cmp.Compare(b.Ordinal, a.Ordinal) })
	return out, nil
}

func (c *Catalogue) ListDiskVolumes(ctx context.Context, disk catalogue.DiskID) ([]catalogue.Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalogue.Volume
	for _, r := range c.volumes {
		if _, ok := r.replicas[disk]; ok {
			out = append(out, c.snapshot(r))
		}
	}
	slices.SortFunc(out, func(a, b catalogue.Volume) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

func (c *Catalogue) SetReplicaState(ctx context.Context, id catalogue.VolumeID, disk catalogue.DiskID, s catalogue.ReplicaState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s != catalogue.ReplicaOK && s != catalogue.ReplicaFailed && s != catalogue.ReplicaLost {
		return catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.volumes[id]
	if r == nil {
		return catalogue.ErrVolumeNotFound
	}
	if _, ok := r.replicas[disk]; !ok {
		return catalogue.ErrReplicaNotFound
	}
	r.replicas[disk] = s
	return nil
}

func (c *Catalogue) AddReplica(ctx context.Context, id catalogue.VolumeID, disk catalogue.DiskID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.volumes[id]
	if r == nil {
		return catalogue.ErrVolumeNotFound
	}
	if r.v.State != catalogue.VolumeSealed {
		return catalogue.ErrVolumeNotSealed
	}
	dr := c.disks[disk]
	switch {
	case dr == nil:
		return catalogue.ErrUnknownDisk
	case dr.d.State != catalogue.DiskActive:
		return catalogue.ErrDiskUnavailable
	}
	r.replicas[disk] = catalogue.ReplicaOK
	for e := range c.erasures {
		if e.vol == id {
			if _, ok := c.targets[targetKey{e, disk}]; !ok {
				c.targets[targetKey{e, disk}] = 0
			}
		}
	}
	return nil
}

// ---- erasures ----

func cmpLoc(a, b catalogue.Locator) int {
	if c := cmp.Compare(a.Volume, b.Volume); c != 0 {
		return c
	}
	return cmp.Compare(a.Seq, b.Seq)
}

func (c *Catalogue) AddErasures(ctx context.Context, l catalogue.Lease, locs []catalogue.Locator) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current(l, true) == nil {
		return catalogue.ErrLeaseLost
	}
	// Validate everything before changing anything.
	for _, loc := range locs {
		if loc.Seq == 0 {
			return catalogue.ErrInvalidArgument
		}
		if _, err := c.owned(l, loc.Volume); err != nil {
			return err
		}
		if chk, ok := c.erasures[erasureKey{loc.Volume, loc.Seq}]; ok && chk != loc.Check {
			return catalogue.ErrInvalidArgument
		}
	}
	for _, loc := range locs {
		e := erasureKey{loc.Volume, loc.Seq}
		if _, ok := c.erasures[e]; ok {
			continue
		}
		c.erasures[e] = loc.Check
		for d, st := range c.volumes[loc.Volume].replicas {
			if st != catalogue.ReplicaLost {
				c.targets[targetKey{e, d}] = 0
			}
		}
	}
	return nil
}

func (c *Catalogue) ShardErasures(ctx context.Context, bucket string, shard int) ([]catalogue.Locator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalogue.Locator
	for e, chk := range c.erasures {
		if v := c.volumes[e.vol]; v.v.Bucket == bucket && v.v.Shard == shard {
			out = append(out, catalogue.Locator{Volume: e.vol, Seq: e.seq, Check: chk})
		}
	}
	slices.SortFunc(out, cmpLoc)
	return out, nil
}

func (c *Catalogue) PendingErasures(ctx context.Context, disk catalogue.DiskID, limit int) ([]catalogue.Locator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalogue.Locator
	for t, applied := range c.targets {
		if t.disk == disk && applied == 0 {
			out = append(out, catalogue.Locator{Volume: t.e.vol, Seq: t.e.seq, Check: c.erasures[t.e]})
		}
	}
	slices.SortFunc(out, cmpLoc)
	return out[:min(len(out), limit)], nil
}

func (c *Catalogue) MarkErasureApplied(ctx context.Context, disk catalogue.DiskID, locs []catalogue.Locator) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, loc := range locs {
		if _, ok := c.targets[targetKey{erasureKey{loc.Volume, loc.Seq}, disk}]; !ok {
			return catalogue.ErrErasureNotFound
		}
	}
	now := c.nowMs()
	for _, loc := range locs {
		k := targetKey{erasureKey{loc.Volume, loc.Seq}, disk}
		if c.targets[k] == 0 {
			c.targets[k] = now
		}
	}
	return nil
}

func (c *Catalogue) ListErasures(ctx context.Context, after catalogue.Locator, limit int) ([]catalogue.Locator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalogue.Locator
	for e, chk := range c.erasures {
		loc := catalogue.Locator{Volume: e.vol, Seq: e.seq, Check: chk}
		if cmpLoc(loc, after) > 0 {
			out = append(out, loc)
		}
	}
	slices.SortFunc(out, cmpLoc)
	return out[:min(len(out), limit)], nil
}

// ---- nodes and disks ----

func (c *Catalogue) AssignNodeID(ctx context.Context, name string) (catalogue.NodeID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(name) < 1 || len(name) > 128 {
		return 0, catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, ok := c.nodeByName[name]; ok {
		return id, nil
	}
	var next catalogue.NodeID
	for id := range c.nodes {
		next = max(next, id)
	}
	if next == 65535 {
		return 0, catalogue.ErrNodeIDsExhausted
	}
	next++
	c.nodes[next] = &nodeRow{id: next, name: name}
	c.nodeByName[name] = next
	return next, nil
}

func (c *Catalogue) RegisterDisk(ctx context.Context, id catalogue.DiskID, node catalogue.NodeID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nodes[node]; !ok {
		return catalogue.ErrNodeNotFound
	}
	if d, ok := c.disks[id]; ok {
		if d.d.Node != node {
			return catalogue.ErrDiskConflict
		}
		return nil
	}
	c.disks[id] = &diskRow{d: catalogue.Disk{ID: id, Node: node, State: catalogue.DiskActive}}
	return nil
}

func (c *Catalogue) SetDiskState(ctx context.Context, id catalogue.DiskID, s catalogue.DiskState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s != catalogue.DiskActive && s != catalogue.DiskReadOnly && s != catalogue.DiskGone {
		return catalogue.ErrInvalidArgument
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.disks[id]
	if d == nil {
		return catalogue.ErrUnknownDisk
	}
	if d.d.State == catalogue.DiskGone && s != catalogue.DiskGone {
		return catalogue.ErrDiskGone
	}
	d.d.State = s
	return nil
}

func (c *Catalogue) Heartbeat(ctx context.Context, n catalogue.NodeBeat, disks []catalogue.DiskBeat) (catalogue.Meta, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.Meta{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	nr := c.nodes[n.ID]
	if nr == nil {
		return catalogue.Meta{}, catalogue.ErrNodeNotFound
	}
	now := c.nowMs()
	if nr.beatSeen != 0 && nr.beat.Instance != n.Instance && now-nr.beatSeen < c.nodeTTL.Milliseconds() {
		return catalogue.Meta{}, catalogue.ErrNodeIDInUse
	}
	for _, b := range disks {
		if d := c.disks[b.ID]; d == nil || d.d.Node != n.ID {
			return catalogue.Meta{}, catalogue.ErrUnknownDisk
		}
	}
	nr.beat, nr.beatSeen = n, now
	for _, b := range disks {
		d := c.disks[b.ID]
		d.d.CapacityBytes, d.d.UsedBytes, d.d.OpenVolumes, d.seen = b.CapacityBytes, b.UsedBytes, b.OpenVolumes, now
	}
	return c.meta, nil
}

func (c *Catalogue) LiveNodes(ctx context.Context) ([]catalogue.NodeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.nowMs()
	var out []catalogue.NodeInfo
	for _, n := range c.nodes {
		if n.beatSeen != 0 && now-n.beatSeen < c.nodeTTL.Milliseconds() {
			out = append(out, catalogue.NodeInfo{ID: n.id, Name: n.name, Addr: n.beat.Addr,
				Version: n.beat.Version, Compat: n.beat.Compat, LastSeen: tm(n.beatSeen)})
		}
	}
	slices.SortFunc(out, func(a, b catalogue.NodeInfo) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

func (c *Catalogue) Disks(ctx context.Context) ([]catalogue.Disk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]catalogue.Disk, 0, len(c.disks))
	for _, d := range c.disks {
		x := d.d
		x.LastSeen = tm(d.seen)
		out = append(out, x)
	}
	slices.SortFunc(out, func(a, b catalogue.Disk) int { return slices.Compare(a.ID[:], b.ID[:]) })
	return out, nil
}
