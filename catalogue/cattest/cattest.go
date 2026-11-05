// Package cattest is the contract suite every catalogue.Catalogue
// implementation must pass: the in-memory fake, SQLite and PostgreSQL run the
// same tests, which keeps the simulator's fake honest.
package cattest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"storage4/catalogue"
	"storage4/clock"
)

// Factory returns a fresh, initialised, empty catalogue using clk for all
// time, closed automatically when the test ends.
type Factory func(t *testing.T, clk clock.Clock, meta catalogue.Meta, o catalogue.Options) catalogue.Catalogue

var testMeta = catalogue.Meta{
	ClusterID:     [16]byte{1, 2, 3},
	ClusterName:   "test-cluster",
	SchemaVersion: 1,
	MinCompat:     1,
	Incarnation:   7,
}

const nodeTTL = 30 * time.Second

type env struct {
	t   *testing.T
	clk *clock.Fake
	c   catalogue.Catalogue
	ctx context.Context
}

func newEnv(t *testing.T, f Factory) *env {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	c := f(t, clk, testMeta, catalogue.Options{NodeTTL: nodeTTL})
	t.Cleanup(func() { c.Close() })
	return &env{t: t, clk: clk, c: c, ctx: context.Background()}
}

func (e *env) must(err error) {
	e.t.Helper()
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) is(err, want error) {
	e.t.Helper()
	if !errors.Is(err, want) {
		e.t.Fatalf("got %v, want %v", err, want)
	}
}

func disk(n byte) catalogue.DiskID { return catalogue.DiskID{0xD, n} }

func vol(node catalogue.NodeID, n uint64) catalogue.VolumeID {
	return catalogue.VolumeID(uint64(node)<<48 | n)
}

// cluster registers nodes "n1".."nN" (IDs 1..N) with two disks each:
// disk(10*node+1), disk(10*node+2).
func (e *env) cluster(nodes int) {
	e.t.Helper()
	for i := 1; i <= nodes; i++ {
		id, err := e.c.AssignNodeID(e.ctx, fmt.Sprintf("n%d", i))
		e.must(err)
		for d := 1; d <= 2; d++ {
			e.must(e.c.RegisterDisk(e.ctx, disk(byte(10*int(id)+d)), id))
		}
	}
}

func (e *env) bucket(name string, shards int) {
	e.t.Helper()
	_, _, err := e.c.CreateBucket(e.ctx, name, shards)
	e.must(err)
}

func (e *env) lease(bucket string, shard int, node catalogue.NodeID) catalogue.Lease {
	e.t.Helper()
	l, err := e.c.AcquireShard(e.ctx, bucket, shard, node, 30*time.Second)
	e.must(err)
	return l
}

func (e *env) open(l catalogue.Lease, n uint64, disks ...catalogue.DiskID) catalogue.Volume {
	e.t.Helper()
	v, err := e.c.OpenVolume(e.ctx, l, catalogue.OpenParams{ID: vol(l.Node, n), Disks: disks, FormatMajor: 1})
	e.must(err)
	return v
}

// Run runs the contract suite.
func Run(t *testing.T, f Factory) {
	for name, fn := range map[string]func(*testing.T, Factory){
		"Meta":             testMeta_,
		"Buckets":          testBuckets,
		"Leases":           testLeases,
		"LeaseExpiry":      testLeaseExpiry,
		"Volumes":          testVolumes,
		"Seal":             testSeal,
		"Fencing":          testFencing,
		"Replicas":         testReplicas,
		"Erasures":         testErasures,
		"Cluster":          testCluster,
		"Heartbeat":        testHeartbeat,
		"ConcurrentLeases": testConcurrentLeases,
		"ConcurrentOpen":   testConcurrentOpen,
	} {
		t.Run(name, func(t *testing.T) { fn(t, f) })
	}
}

func testMeta_(t *testing.T, f Factory) {
	e := newEnv(t, f)
	m, err := e.c.Meta(e.ctx)
	e.must(err)
	if m != testMeta {
		t.Fatalf("%+v", m)
	}
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()
	if _, err := e.c.Meta(ctx); err == nil {
		t.Fatal("cancelled context accepted")
	}
}

func testBuckets(t *testing.T, f Factory) {
	e := newEnv(t, f)
	b, created, err := e.c.CreateBucket(e.ctx, "events", 4)
	e.must(err)
	if !created || b.Name != "events" || b.Shards != 4 || b.State != catalogue.BucketActive ||
		b.CreatedAt != e.clk.Now() || !b.DeletedAt.IsZero() {
		t.Fatalf("%+v %v", b, created)
	}
	b2, created, err := e.c.CreateBucket(e.ctx, "events", 4)
	e.must(err)
	if created || b2 != b {
		t.Fatalf("not idempotent: %+v %v", b2, created)
	}
	_, _, err = e.c.CreateBucket(e.ctx, "events", 8)
	e.is(err, catalogue.ErrBucketConflict)
	for _, bad := range []struct {
		n string
		s int
	}{{"", 4}, {".x", 4}, {"a/b", 4}, {"ok", 0}, {"ok", -1}, {"ok", 1025}} {
		_, _, err = e.c.CreateBucket(e.ctx, bad.n, bad.s)
		e.is(err, catalogue.ErrInvalidArgument)
	}
	_, err = e.c.GetBucket(e.ctx, "nope")
	e.is(err, catalogue.ErrBucketNotFound)
	e.bucket("alpha", 1)
	e.clk.Advance(time.Hour)
	e.must(e.c.DeleteBucket(e.ctx, "alpha"))
	deletedAt := e.clk.Now()
	e.clk.Advance(time.Hour)
	e.must(e.c.DeleteBucket(e.ctx, "alpha")) // idempotent, keeps the first deletion time
	a, err := e.c.GetBucket(e.ctx, "alpha")
	e.must(err)
	if a.State != catalogue.BucketDeleted || a.DeletedAt != deletedAt {
		t.Fatalf("%+v", a)
	}
	_, _, err = e.c.CreateBucket(e.ctx, "alpha", 1)
	e.is(err, catalogue.ErrBucketDeleted)
	e.is(e.c.DeleteBucket(e.ctx, "never"), catalogue.ErrBucketNotFound)
	list, err := e.c.ListBuckets(e.ctx)
	e.must(err)
	if len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "events" {
		t.Fatalf("%+v", list)
	}
}

func testLeases(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 4)
	l1 := e.lease("b", 0, 1)
	if l1.Generation != 1 || l1.Node != 1 || l1.Bucket != "b" || l1.Shard != 0 || l1.TTL != 30*time.Second {
		t.Fatalf("%+v", l1)
	}
	o, ok, err := e.c.ShardOwner(e.ctx, "b", 0)
	e.must(err)
	if !ok || o.Node != 1 || o.Generation != 1 || !o.Expires.Equal(e.clk.Now().Add(30*time.Second)) {
		t.Fatalf("%+v %v", o, ok)
	}
	// Another node is refused and told who holds it.
	_, err = e.c.AcquireShard(e.ctx, "b", 0, 2, time.Minute)
	e.is(err, catalogue.ErrLeaseHeld)
	var held *catalogue.HeldError
	if !errors.As(err, &held) || held.Node != 1 {
		t.Fatalf("%v", err)
	}
	// Another shard is independent.
	if l := e.lease("b", 1, 2); l.Generation != 1 {
		t.Fatalf("%+v", l)
	}
	// The holder re-acquires at once (a restarted owner): new generation fences the old token.
	l1b := e.lease("b", 0, 1)
	if l1b.Generation != 2 {
		t.Fatalf("%+v", l1b)
	}
	e.is(e.c.RenewShard(e.ctx, l1), catalogue.ErrLeaseLost)
	e.must(e.c.RenewShard(e.ctx, l1b))
	// A stale release is a no-op; the current one clears the holder but not the counter.
	e.must(e.c.ReleaseShard(e.ctx, l1))
	if _, ok, _ := e.c.ShardOwner(e.ctx, "b", 0); !ok {
		t.Fatal("stale release cleared the lease")
	}
	e.must(e.c.ReleaseShard(e.ctx, l1b))
	e.must(e.c.ReleaseShard(e.ctx, l1b)) // idempotent
	if _, ok, _ := e.c.ShardOwner(e.ctx, "b", 0); ok {
		t.Fatal("released lease still owned")
	}
	e.is(e.c.RenewShard(e.ctx, l1b), catalogue.ErrLeaseLost)
	l2 := e.lease("b", 0, 2)
	if l2.Generation != 3 {
		t.Fatalf("generation went back: %+v", l2)
	}
	// Argument and state errors.
	_, err = e.c.AcquireShard(e.ctx, "b", 4, 1, time.Minute)
	e.is(err, catalogue.ErrInvalidArgument)
	_, err = e.c.AcquireShard(e.ctx, "b", -1, 1, time.Minute)
	e.is(err, catalogue.ErrInvalidArgument)
	_, err = e.c.AcquireShard(e.ctx, "b", 0, 0, time.Minute)
	e.is(err, catalogue.ErrInvalidArgument)
	_, err = e.c.AcquireShard(e.ctx, "b", 0, 1, 0)
	e.is(err, catalogue.ErrInvalidArgument)
	_, err = e.c.AcquireShard(e.ctx, "nope", 0, 1, time.Minute)
	e.is(err, catalogue.ErrBucketNotFound)
	// Deleting the bucket clears its leases and forbids new ones.
	e.must(e.c.DeleteBucket(e.ctx, "b"))
	e.is(e.c.RenewShard(e.ctx, l2), catalogue.ErrLeaseLost)
	_, err = e.c.AcquireShard(e.ctx, "b", 0, 2, time.Minute)
	e.is(err, catalogue.ErrBucketDeleted)
	if _, ok, _ := e.c.ShardOwner(e.ctx, "b", 1); ok {
		t.Fatal("deleted bucket still has an owner")
	}
}

func testLeaseExpiry(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 1)
	l1 := e.lease("b", 0, 1)
	e.clk.Advance(29 * time.Second)
	_, err := e.c.AcquireShard(e.ctx, "b", 0, 2, time.Minute)
	e.is(err, catalogue.ErrLeaseHeld)
	e.must(e.c.RenewShard(e.ctx, l1)) // extends from now
	e.clk.Advance(29 * time.Second)
	_, err = e.c.AcquireShard(e.ctx, "b", 0, 2, time.Minute)
	e.is(err, catalogue.ErrLeaseHeld)
	e.clk.Advance(time.Second) // exactly expired: free
	if _, ok, _ := e.c.ShardOwner(e.ctx, "b", 0); ok {
		t.Fatal("expired lease still reported")
	}
	// Expired but untaken: the holder may renew and keeps its generation.
	e.must(e.c.RenewShard(e.ctx, l1))
	if _, ok, _ := e.c.ShardOwner(e.ctx, "b", 0); !ok {
		t.Fatal("renewed lease not owned")
	}
	// Expired and taken: the old holder is fenced.
	e.clk.Advance(time.Minute)
	l2, err := e.c.AcquireShard(e.ctx, "b", 0, 2, time.Minute)
	e.must(err)
	if l2.Generation != l1.Generation+1 {
		t.Fatalf("%+v %+v", l1, l2)
	}
	e.is(e.c.RenewShard(e.ctx, l1), catalogue.ErrLeaseLost)
	// An expired lease cannot open volumes even if untaken.
	e.clk.Advance(2 * time.Minute)
	_, err = e.c.OpenVolume(e.ctx, l2, catalogue.OpenParams{ID: vol(2, 1), Disks: []catalogue.DiskID{disk(21)}})
	e.is(err, catalogue.ErrLeaseLost)
}

func testVolumes(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 2)
	l := e.lease("b", 0, 1)
	v1 := e.open(l, 1, disk(11), disk(21))
	if v1.Ordinal != 1 || v1.State != catalogue.VolumeOpen || v1.Epoch != 1 || v1.Bucket != "b" || v1.Shard != 0 ||
		v1.FormatMajor != 1 || !v1.CreatedAt.Equal(e.clk.Now()) || !v1.SealedAt.IsZero() ||
		len(v1.Replicas) != 2 || v1.Replicas[0].Disk != disk(11) || v1.Replicas[1].Disk != disk(21) ||
		v1.Replicas[0].State != catalogue.ReplicaOK {
		t.Fatalf("%+v", v1)
	}
	got, err := e.c.GetVolume(e.ctx, v1.ID)
	e.must(err)
	if fmt.Sprint(got) != fmt.Sprint(v1) {
		t.Fatalf("%+v vs %+v", got, v1)
	}
	_, err = e.c.GetVolume(e.ctx, vol(1, 99))
	e.is(err, catalogue.ErrVolumeNotFound)
	// Ordinals continue across releases and re-acquisition; they never go back.
	e.must(e.c.ReleaseShard(e.ctx, l))
	l = e.lease("b", 0, 2)
	v2 := e.open(l, 2, disk(12), disk(22))
	v3 := e.open(l, 3, disk(12), disk(22))
	if v2.Ordinal != 2 || v3.Ordinal != 3 {
		t.Fatalf("ordinals %d %d", v2.Ordinal, v3.Ordinal)
	}
	// Another shard counts on its own.
	l1 := e.lease("b", 1, 1)
	if v := e.open(l1, 4, disk(11)); v.Ordinal != 1 {
		t.Fatalf("%+v", v)
	}
	list, err := e.c.ListShardVolumes(e.ctx, "b", 0)
	e.must(err)
	if len(list) != 3 || list[0].ID != v3.ID || list[1].ID != v2.ID || list[2].ID != v1.ID {
		t.Fatalf("not newest first: %+v", list)
	}
	list, err = e.c.ListDiskVolumes(e.ctx, disk(12))
	e.must(err)
	if len(list) != 2 || list[0].ID != v2.ID || list[1].ID != v3.ID {
		t.Fatalf("%+v", list)
	}
	if list, _ = e.c.ListShardVolumes(e.ctx, "b", 1); len(list) != 1 {
		t.Fatalf("%+v", list)
	}
	// Refusals leave no trace and consume no ordinal.
	open := func(p catalogue.OpenParams) error { _, err := e.c.OpenVolume(e.ctx, l, p); return err }
	e.is(open(catalogue.OpenParams{ID: v2.ID, Disks: []catalogue.DiskID{disk(12)}}), catalogue.ErrVolumeExists)
	e.is(open(catalogue.OpenParams{ID: vol(1, 50), Disks: []catalogue.DiskID{disk(12)}}), catalogue.ErrInvalidArgument) // other node's ID
	e.is(open(catalogue.OpenParams{ID: 0, Disks: []catalogue.DiskID{disk(12)}}), catalogue.ErrInvalidArgument)
	e.is(open(catalogue.OpenParams{ID: vol(2, 50)}), catalogue.ErrInvalidArgument)
	e.is(open(catalogue.OpenParams{ID: vol(2, 50), Disks: []catalogue.DiskID{disk(12), disk(12)}}), catalogue.ErrInvalidArgument)
	e.is(open(catalogue.OpenParams{ID: vol(2, 50), Disks: []catalogue.DiskID{disk(99)}}), catalogue.ErrUnknownDisk)
	e.is(open(catalogue.OpenParams{ID: vol(2, 50), Disks: []catalogue.DiskID{disk(12)}, CipherID: 1}), catalogue.ErrInvalidArgument)
	e.must(e.c.SetDiskState(e.ctx, disk(22), catalogue.DiskReadOnly))
	e.is(open(catalogue.OpenParams{ID: vol(2, 50), Disks: []catalogue.DiskID{disk(22)}}), catalogue.ErrDiskUnavailable)
	if v := e.open(l, 5, disk(12)); v.Ordinal != 4 {
		t.Fatalf("a refusal consumed an ordinal: %+v", v)
	}
}

func testSeal(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(1)
	e.bucket("b", 2)
	l := e.lease("b", 0, 1)
	v := e.open(l, 1, disk(11))
	e2, err := e.c.BeginSeal(e.ctx, l, v.ID)
	e.must(err)
	if e2 != 2 {
		t.Fatalf("epoch %d", e2)
	}
	if g, _ := e.c.GetVolume(e.ctx, v.ID); g.State != catalogue.VolumeSealing || g.Epoch != 2 {
		t.Fatalf("%+v", g)
	}
	e3, err := e.c.BeginSeal(e.ctx, l, v.ID) // repeatable, bumps again
	e.must(err)
	if e3 != 3 {
		t.Fatalf("epoch %d", e3)
	}
	e.is(e.c.FinishSeal(e.ctx, l, v.ID, e2, 1000, 3), catalogue.ErrEpochMismatch)
	e.is(e.c.FinishSeal(e.ctx, l, v.ID, e3, -1, 3), catalogue.ErrInvalidArgument)
	e.clk.Advance(time.Second)
	e.must(e.c.FinishSeal(e.ctx, l, v.ID, e3, 1000, 3))
	g, err := e.c.GetVolume(e.ctx, v.ID)
	e.must(err)
	if g.State != catalogue.VolumeSealed || g.Epoch != 3 || g.CommittedLength != 1000 || g.RecordCount != 3 ||
		!g.SealedAt.Equal(e.clk.Now()) {
		t.Fatalf("%+v", g)
	}
	e.must(e.c.FinishSeal(e.ctx, l, v.ID, e3, 1000, 3)) // same values: no-op
	e.is(e.c.FinishSeal(e.ctx, l, v.ID, e3, 1001, 3), catalogue.ErrSealMismatch)
	e.is(e.c.FinishSeal(e.ctx, l, v.ID, e3, 1000, 4), catalogue.ErrSealMismatch)
	_, err = e.c.BeginSeal(e.ctx, l, v.ID)
	e.is(err, catalogue.ErrVolumeSealed)
	// A volume of another shard is invisible to this lease.
	l1 := e.lease("b", 1, 1)
	_, err = e.c.BeginSeal(e.ctx, l1, v.ID)
	e.is(err, catalogue.ErrVolumeNotFound)
	e.is(e.c.FinishSeal(e.ctx, l1, v.ID, 3, 1000, 3), catalogue.ErrVolumeNotFound)
	_, err = e.c.BeginSeal(e.ctx, l, vol(1, 77))
	e.is(err, catalogue.ErrVolumeNotFound)
}

// After a handover the old owner's token fails every write; the new owner
// can finish what the old one started.
func testFencing(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 1)
	old := e.lease("b", 0, 1)
	v := e.open(old, 1, disk(11), disk(21))
	e.clk.Advance(time.Minute)
	nl := e.lease("b", 0, 2)
	_, err := e.c.BeginSeal(e.ctx, old, v.ID)
	e.is(err, catalogue.ErrLeaseLost)
	e.is(e.c.FinishSeal(e.ctx, old, v.ID, 2, 10, 1), catalogue.ErrLeaseLost)
	_, err = e.c.OpenVolume(e.ctx, old, catalogue.OpenParams{ID: vol(1, 2), Disks: []catalogue.DiskID{disk(11)}})
	e.is(err, catalogue.ErrLeaseLost)
	e.is(e.c.AddErasures(e.ctx, old, []catalogue.Locator{{Volume: v.ID, Seq: 1, Check: 9}}), catalogue.ErrLeaseLost)
	// Handover: the new owner lists the shard's open volumes and seals them.
	vs, err := e.c.ListShardVolumes(e.ctx, "b", 0)
	e.must(err)
	if len(vs) != 1 || vs[0].State != catalogue.VolumeOpen {
		t.Fatalf("%+v", vs)
	}
	ep, err := e.c.BeginSeal(e.ctx, nl, vs[0].ID)
	e.must(err)
	e.must(e.c.FinishSeal(e.ctx, nl, vs[0].ID, ep, 4096, 5))
	nv := e.open(nl, 3, disk(12))
	if nv.Ordinal != 2 {
		t.Fatalf("%+v", nv)
	}
}

func testReplicas(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 1)
	l := e.lease("b", 0, 1)
	v := e.open(l, 1, disk(11), disk(21))
	e.must(e.c.SetDiskState(e.ctx, disk(12), catalogue.DiskActive))
	e.must(e.c.SetReplicaState(e.ctx, v.ID, disk(21), catalogue.ReplicaFailed))
	g, _ := e.c.GetVolume(e.ctx, v.ID)
	if g.Replicas[1].State != catalogue.ReplicaFailed || g.Replicas[0].State != catalogue.ReplicaOK {
		t.Fatalf("%+v", g.Replicas)
	}
	e.is(e.c.SetReplicaState(e.ctx, v.ID, disk(12), catalogue.ReplicaOK), catalogue.ErrReplicaNotFound)
	e.is(e.c.SetReplicaState(e.ctx, vol(1, 9), disk(11), catalogue.ReplicaOK), catalogue.ErrVolumeNotFound)
	e.is(e.c.SetReplicaState(e.ctx, v.ID, disk(11), "bogus"), catalogue.ErrInvalidArgument)
	// Re-replication only for sealed volumes, to active registered disks.
	e.is(e.c.AddReplica(e.ctx, v.ID, disk(12)), catalogue.ErrVolumeNotSealed)
	ep, err := e.c.BeginSeal(e.ctx, l, v.ID)
	e.must(err)
	e.must(e.c.FinishSeal(e.ctx, l, v.ID, ep, 100, 1))
	e.is(e.c.AddReplica(e.ctx, v.ID, disk(99)), catalogue.ErrUnknownDisk)
	e.is(e.c.AddReplica(e.ctx, vol(1, 9), disk(12)), catalogue.ErrVolumeNotFound)
	e.must(e.c.SetDiskState(e.ctx, disk(12), catalogue.DiskReadOnly))
	e.is(e.c.AddReplica(e.ctx, v.ID, disk(12)), catalogue.ErrDiskUnavailable)
	e.must(e.c.SetDiskState(e.ctx, disk(12), catalogue.DiskActive))
	e.must(e.c.AddReplica(e.ctx, v.ID, disk(12)))
	e.must(e.c.AddReplica(e.ctx, v.ID, disk(12))) // idempotent
	g, _ = e.c.GetVolume(e.ctx, v.ID)
	if len(g.Replicas) != 3 || g.Replicas[0].Disk != disk(11) || g.Replicas[1].Disk != disk(12) {
		t.Fatalf("%+v", g.Replicas)
	}
	// Re-adding a failed replica's disk makes it ok again.
	e.must(e.c.AddReplica(e.ctx, v.ID, disk(21)))
	g, _ = e.c.GetVolume(e.ctx, v.ID)
	if g.Replicas[2].State != catalogue.ReplicaOK {
		t.Fatalf("%+v", g.Replicas)
	}
}

func testErasures(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	e.bucket("b", 2)
	l := e.lease("b", 0, 1)
	v1 := e.open(l, 1, disk(11), disk(21))
	v2 := e.open(l, 2, disk(11), disk(22))
	other := e.lease("b", 1, 2)
	vo := e.open(other, 3, disk(12))

	locs := []catalogue.Locator{{Volume: v2.ID, Seq: 5, Check: 55}, {Volume: v1.ID, Seq: 9, Check: 99}, {Volume: v1.ID, Seq: 2, Check: 22}}
	e.must(e.c.AddErasures(e.ctx, l, locs))
	e.must(e.c.AddErasures(e.ctx, l, locs)) // idempotent
	e.must(e.c.AddErasures(e.ctx, l, nil))
	// Volume of another shard, seq 0 or a different check for a known record: refused, nothing added.
	e.is(e.c.AddErasures(e.ctx, l, []catalogue.Locator{{Volume: v1.ID, Seq: 3, Check: 1}, {Volume: vo.ID, Seq: 1, Check: 1}}), catalogue.ErrVolumeNotFound)
	e.is(e.c.AddErasures(e.ctx, l, []catalogue.Locator{{Volume: v1.ID, Seq: 0, Check: 1}}), catalogue.ErrInvalidArgument)
	e.is(e.c.AddErasures(e.ctx, l, []catalogue.Locator{{Volume: v1.ID, Seq: 2, Check: 23}}), catalogue.ErrInvalidArgument)
	want := []catalogue.Locator{{Volume: v1.ID, Seq: 2, Check: 22}, {Volume: v1.ID, Seq: 9, Check: 99}, {Volume: v2.ID, Seq: 5, Check: 55}}
	got, err := e.c.ShardErasures(e.ctx, "b", 0)
	e.must(err)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%v", got)
	}
	if got, _ = e.c.ShardErasures(e.ctx, "b", 1); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	// One pending target per replica disk.
	p, err := e.c.PendingErasures(e.ctx, disk(11), 10)
	e.must(err)
	if fmt.Sprint(p) != fmt.Sprint(want) {
		t.Fatalf("%v", p)
	}
	if p, _ = e.c.PendingErasures(e.ctx, disk(21), 10); fmt.Sprint(p) != fmt.Sprint(want[:2]) {
		t.Fatalf("%v", p)
	}
	if p, _ = e.c.PendingErasures(e.ctx, disk(22), 10); fmt.Sprint(p) != fmt.Sprint(want[2:]) {
		t.Fatalf("%v", p)
	}
	if p, _ = e.c.PendingErasures(e.ctx, disk(12), 10); len(p) != 0 {
		t.Fatalf("%v", p)
	}
	if p, _ = e.c.PendingErasures(e.ctx, disk(11), 2); len(p) != 2 {
		t.Fatalf("limit: %v", p)
	}
	_, err = e.c.PendingErasures(e.ctx, disk(11), 0)
	e.is(err, catalogue.ErrInvalidArgument)
	// Applying is per disk and idempotent; unknown targets are an error and apply nothing.
	e.must(e.c.MarkErasureApplied(e.ctx, disk(11), want[:2]))
	e.must(e.c.MarkErasureApplied(e.ctx, disk(11), want[:2]))
	e.is(e.c.MarkErasureApplied(e.ctx, disk(11), []catalogue.Locator{want[2], {Volume: v1.ID, Seq: 77}}), catalogue.ErrErasureNotFound)
	e.is(e.c.MarkErasureApplied(e.ctx, disk(21), want[2:]), catalogue.ErrErasureNotFound)
	if p, _ = e.c.PendingErasures(e.ctx, disk(11), 10); fmt.Sprint(p) != fmt.Sprint(want[2:]) {
		t.Fatalf("%v", p)
	}
	if p, _ = e.c.PendingErasures(e.ctx, disk(21), 10); len(p) != 2 {
		t.Fatalf("disk 21 affected by disk 11: %v", p)
	}
	// Export pages through the whole list in order.
	var all []catalogue.Locator
	var after catalogue.Locator
	for {
		page, err := e.c.ListErasures(e.ctx, after, 2)
		e.must(err)
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		after = page[len(page)-1]
	}
	if fmt.Sprint(all) != fmt.Sprint(want) {
		t.Fatalf("%v", all)
	}
	_, err = e.c.ListErasures(e.ctx, catalogue.Locator{}, 0)
	e.is(err, catalogue.ErrInvalidArgument)
	// Lost replicas get no target; a re-replicated disk gets one for every
	// erasure of the volume (the copy may predate them).
	e.must(e.c.SetReplicaState(e.ctx, v2.ID, disk(22), catalogue.ReplicaLost))
	e.must(e.c.AddErasures(e.ctx, l, []catalogue.Locator{{Volume: v2.ID, Seq: 6, Check: 66}}))
	if p, _ = e.c.PendingErasures(e.ctx, disk(22), 10); len(p) != 1 { // only the old (5), not 6
		t.Fatalf("%v", p)
	}
	ep, err := e.c.BeginSeal(e.ctx, l, v2.ID)
	e.must(err)
	e.must(e.c.FinishSeal(e.ctx, l, v2.ID, ep, 100, 9))
	e.must(e.c.AddReplica(e.ctx, v2.ID, disk(12)))
	p, _ = e.c.PendingErasures(e.ctx, disk(12), 10)
	if len(p) != 2 || p[0].Seq != 5 || p[1].Seq != 6 {
		t.Fatalf("%v", p)
	}
}

func testCluster(t *testing.T, f Factory) {
	e := newEnv(t, f)
	a, err := e.c.AssignNodeID(e.ctx, "store-a1")
	e.must(err)
	b, err := e.c.AssignNodeID(e.ctx, "store-b1")
	e.must(err)
	again, err := e.c.AssignNodeID(e.ctx, "store-a1")
	e.must(err)
	if a != 1 || b != 2 || again != a {
		t.Fatalf("%d %d %d", a, b, again)
	}
	for _, bad := range []string{"", string(make([]byte, 129))} {
		_, err = e.c.AssignNodeID(e.ctx, bad)
		e.is(err, catalogue.ErrInvalidArgument)
	}
	e.is(e.c.RegisterDisk(e.ctx, disk(1), 9), catalogue.ErrNodeNotFound)
	e.must(e.c.RegisterDisk(e.ctx, disk(1), a))
	e.must(e.c.RegisterDisk(e.ctx, disk(1), a)) // idempotent
	e.is(e.c.RegisterDisk(e.ctx, disk(1), b), catalogue.ErrDiskConflict)
	e.must(e.c.RegisterDisk(e.ctx, disk(2), b))
	ds, err := e.c.Disks(e.ctx)
	e.must(err)
	if len(ds) != 2 || ds[0].ID != disk(1) || ds[0].Node != a || ds[0].State != catalogue.DiskActive ||
		!ds[0].LastSeen.IsZero() || ds[1].ID != disk(2) || ds[1].Node != b {
		t.Fatalf("%+v", ds)
	}
	e.is(e.c.SetDiskState(e.ctx, disk(9), catalogue.DiskGone), catalogue.ErrUnknownDisk)
	e.is(e.c.SetDiskState(e.ctx, disk(1), "bogus"), catalogue.ErrInvalidArgument)
	e.must(e.c.SetDiskState(e.ctx, disk(1), catalogue.DiskReadOnly))
	e.must(e.c.SetDiskState(e.ctx, disk(1), catalogue.DiskActive))
	e.must(e.c.SetDiskState(e.ctx, disk(1), catalogue.DiskGone))
	e.must(e.c.SetDiskState(e.ctx, disk(1), catalogue.DiskGone))
	e.is(e.c.SetDiskState(e.ctx, disk(1), catalogue.DiskActive), catalogue.ErrDiskGone)
	if ds, _ = e.c.Disks(e.ctx); ds[0].State != catalogue.DiskGone {
		t.Fatalf("%+v", ds)
	}
}

func testHeartbeat(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(2)
	inst1, inst2 := [16]byte{1}, [16]byte{2}
	beat := func(node catalogue.NodeID, inst [16]byte, disks ...catalogue.DiskBeat) (catalogue.Meta, error) {
		return e.c.Heartbeat(e.ctx, catalogue.NodeBeat{ID: node, Instance: inst, Addr: fmt.Sprintf("10.0.0.%d:8080", node),
			Version: "1.0", Compat: 3}, disks)
	}
	_, err := beat(9, inst1)
	e.is(err, catalogue.ErrNodeNotFound)
	m, err := beat(1, inst1, catalogue.DiskBeat{ID: disk(11), CapacityBytes: 1000, UsedBytes: 100, OpenVolumes: 2})
	e.must(err)
	if m != testMeta {
		t.Fatalf("%+v", m)
	}
	// A disk of another node, or an unregistered one, is refused and nothing is recorded.
	_, err = beat(1, inst1, catalogue.DiskBeat{ID: disk(11), UsedBytes: 5}, catalogue.DiskBeat{ID: disk(21)})
	e.is(err, catalogue.ErrUnknownDisk)
	_, err = beat(1, inst1, catalogue.DiskBeat{ID: disk(99)})
	e.is(err, catalogue.ErrUnknownDisk)
	ds, _ := e.c.Disks(e.ctx)
	var d11 catalogue.Disk
	for _, d := range ds {
		if d.ID == disk(11) {
			d11 = d
		}
	}
	if d11.CapacityBytes != 1000 || d11.UsedBytes != 100 || d11.OpenVolumes != 2 || !d11.LastSeen.Equal(e.clk.Now()) {
		t.Fatalf("%+v", d11)
	}
	// Same instance may beat again; another instance is refused while the first is live.
	e.clk.Advance(10 * time.Second)
	_, err = beat(1, inst1)
	e.must(err)
	_, err = beat(1, inst2)
	e.is(err, catalogue.ErrNodeIDInUse)
	_, err = beat(2, inst2)
	e.must(err)
	live, err := e.c.LiveNodes(e.ctx)
	e.must(err)
	if len(live) != 2 || live[0].ID != 1 || live[0].Name != "n1" || live[0].Addr != "10.0.0.1:8080" ||
		live[0].Version != "1.0" || live[0].Compat != 3 || !live[0].LastSeen.Equal(e.clk.Now()) || live[1].ID != 2 {
		t.Fatalf("%+v", live)
	}
	// Node 1 goes quiet: not live after NodeTTL, and a new instance may take the ID.
	e.clk.Advance(nodeTTL - time.Second)
	if live, _ = e.c.LiveNodes(e.ctx); len(live) != 2 {
		t.Fatalf("%+v", live)
	}
	_, err = beat(2, inst2) // node 2 keeps beating
	e.must(err)
	e.clk.Advance(time.Second)
	if live, _ = e.c.LiveNodes(e.ctx); len(live) != 1 || live[0].ID != 2 {
		t.Fatalf("%+v", live)
	}
	_, err = beat(1, inst2)
	e.must(err)
	_, err = beat(1, inst1)
	e.is(err, catalogue.ErrNodeIDInUse) // the old instance is now the intruder
}

// Many nodes race for one shard: exactly one wins.
func testConcurrentLeases(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(8)
	e.bucket("b", 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var won []catalogue.Lease
	for n := 1; n <= 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := e.c.AcquireShard(e.ctx, "b", 0, catalogue.NodeID(n), time.Minute)
			if err == nil {
				mu.Lock()
				won = append(won, l)
				mu.Unlock()
			} else if !errors.Is(err, catalogue.ErrLeaseHeld) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(won) != 1 || won[0].Generation != 1 {
		t.Fatalf("%d winners: %+v", len(won), won)
	}
}

// Concurrent opens under one lease get distinct, dense ordinals.
func testConcurrentOpen(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.cluster(1)
	e.bucket("b", 1)
	l := e.lease("b", 0, 1)
	const n = 16
	ords := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := e.c.OpenVolume(e.ctx, l, catalogue.OpenParams{ID: vol(1, uint64(i+1)), Disks: []catalogue.DiskID{disk(11)}})
			if err != nil {
				t.Error(err)
				return
			}
			ords[i] = v.Ordinal
		}()
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, o := range ords {
		seen[o] = true
	}
	for o := int64(1); o <= n; o++ {
		if !seen[o] {
			t.Fatalf("ordinals not dense: %v", ords)
		}
	}
}
