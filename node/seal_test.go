package node

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"storage4/catalogue"
	"storage4/disk"
	"storage4/volume"
)

// flakyCat fails the seal calls on demand, as an unreachable catalogue would.
type flakyCat struct {
	catalogue.Catalogue
	failBegin, failFinish atomic.Bool
}

func (f *flakyCat) BeginSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID) (uint32, error) {
	if f.failBegin.Load() {
		return 0, catalogue.ErrUnavailable
	}
	return f.Catalogue.BeginSeal(ctx, l, id)
}

func (f *flakyCat) FinishSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID, epoch uint32, committed int64, records uint32) error {
	if f.failFinish.Load() {
		return catalogue.ErrUnavailable
	}
	return f.Catalogue.FinishSeal(ctx, l, id, epoch, committed, records)
}

// newSealRig starts a node with small volumes (they fill after about three
// 1.5 KB files) behind a flaky catalogue and optional disk faults.
func newSealRig(t *testing.T, wrap func(i int, m *disk.Mem) disk.FS) (*rig, *flakyCat) {
	t.Helper()
	r := newRigWith(t, 3, Config{VolumeMaxSize: 6 << 10, AllowSmallVolumes: true, DefaultShards: 1}, wrap)
	r.stop()
	fc := &flakyCat{Catalogue: r.cat}
	r.cat = fc
	r.start()
	return r, fc
}

func sealPayload(i int) []byte { return append([]byte{byte(i)}, bytes.Repeat([]byte("x"), 1500)...) }

// fillUntilSealFails writes files until the one that finds the volume full
// fails (503) because the seal cannot complete. It returns the acked names.
func fillUntilSealFails(t *testing.T, r *rig) (acked []int) {
	t.Helper()
	for i := range 10 {
		res := r.put(fmt.Sprintf("/b/c/f%d", i), sealPayload(i))
		if res.code == 503 {
			return acked
		}
		r.want(res, 201)
		acked = append(acked, i)
	}
	t.Fatal("the volume never filled up")
	return nil
}

func (r *rig) wantAllReadable(t *testing.T, acked []int) {
	t.Helper()
	for _, i := range acked {
		if g := r.do("GET", fmt.Sprintf("/b/c/f%d", i), nil); g.code != 200 || !bytes.Equal(g.body, sealPayload(i)) {
			t.Fatalf("f%d: %d", i, g.code)
		}
	}
}

func (r *rig) ticks(t *testing.T, n int) {
	t.Helper()
	for range n {
		eventually(t, "maintenance loop waiting", func() bool { return r.clk.Waiters() >= 1 })
		r.clk.Advance(10 * time.Second)
	}
}

func (r *rig) pendingSeals() int64 { return r.n.Metrics().Snapshot().Gauges["pending_seals"] }

func (r *rig) alarmActive(id AlarmID) bool {
	for _, a := range r.alarm.Active() {
		if a == id {
			return true
		}
	}
	return false
}

func (r *rig) volumes(t *testing.T) []catalogue.Volume {
	t.Helper()
	vs, err := r.cat.ListShardVolumes(context.Background(), "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

func (r *rig) sealedCount(t *testing.T) int {
	n := 0
	for _, v := range r.volumes(t) {
		if v.State == catalogue.VolumeSealed {
			n++
		}
	}
	return n
}

// Scenarios 1 and 2: the catalogue cannot be reached when the volume must be
// sealed. Acknowledged records stay readable and dedupable, writes go on to a
// new volume, the lease is kept, and the seal completes once the catalogue
// is back.
func testPendingSealWhenCatalogueDown(t *testing.T) {
	r, fc := newSealRig(t, nil)
	fc.failBegin.Store(true)
	acked := fillUntilSealFails(t, r)
	if len(acked) < 2 {
		t.Fatalf("only %d files fit", len(acked))
	}
	if r.pendingSeals() != 1 || !r.alarmActive(AlarmSealPending) {
		t.Fatalf("pending = %d, alarm = %v", r.pendingSeals(), r.alarmActive(AlarmSealPending))
	}
	r.wantAllReadable(t, acked)
	// Dedup still sees the pending volume's records.
	r.want(r.put("/b/c/f0", sealPayload(0)), 200)
	// Writes go on to a new volume.
	r.want(r.put("/b/c/new", []byte("fresh")), 201)
	// Ticks while the catalogue refuses the seal: the lease is kept.
	r.ticks(t, 6)
	if _, held, _ := r.cat.ShardOwner(context.Background(), "c", 0); !held {
		t.Fatal("the lease was released while a seal is pending")
	}
	r.wantAllReadable(t, acked)

	fc.failBegin.Store(false)
	r.ticks(t, 2)
	eventually(t, "pending seal to complete", func() bool { return r.pendingSeals() == 0 })
	if r.alarmActive(AlarmSealPending) {
		t.Fatal("alarm not cleared")
	}
	for _, v := range r.volumes(t) {
		if v.VolumeNumber == 1 && (v.State != catalogue.VolumeSealed || int(v.RecordCount) != len(acked)) {
			t.Fatalf("volume 1: state %v, %d records, want sealed with %d", v.State, v.RecordCount, len(acked))
		}
	}
	r.wantAllReadable(t, acked)
	if g := r.do("GET", "/b/c/new", nil); g.code != 200 {
		t.Fatalf("new: %d", g.code)
	}
}

func TestPendingSealWhenCatalogueDown(t *testing.T) { eachBackend(t, testPendingSealWhenCatalogueDown) }

// FinishSeal failing leaves the replicas sealed but the catalogue unaware.
func testPendingSealWhenFinishFails(t *testing.T) {
	r, fc := newSealRig(t, nil)
	fc.failFinish.Store(true)
	acked := fillUntilSealFails(t, r)
	r.wantAllReadable(t, acked)
	if r.pendingSeals() != 1 {
		t.Fatalf("pending = %d", r.pendingSeals())
	}
	fc.failFinish.Store(false)
	r.ticks(t, 2)
	eventually(t, "pending seal to complete", func() bool { return r.pendingSeals() == 0 })
	r.wantAllReadable(t, acked)
}

func TestPendingSealWhenFinishFails(t *testing.T) { eachBackend(t, testPendingSealWhenFinishFails) }

// A crash while a seal is pending: the restart seals the volume from its
// files and nothing acknowledged is lost.
func testPendingSealSurvivesCrash(t *testing.T) {
	r, fc := newSealRig(t, nil)
	fc.failBegin.Store(true)
	acked := fillUntilSealFails(t, r)
	fc.failBegin.Store(false)
	r.kill()
	r.clk.Advance(time.Minute) // the dead instance's node ID expires
	r.start()
	r.wantAllReadable(t, acked)
	if r.pendingSeals() != 0 {
		t.Fatal("pending after restart")
	}
	for _, v := range r.volumes(t) {
		if v.State != catalogue.VolumeSealed {
			t.Fatalf("volume %d left %v", v.VolumeNumber, v.State)
		}
	}
}

func TestPendingSealSurvivesCrash(t *testing.T) { eachBackend(t, testPendingSealSurvivesCrash) }

// syncFault fails the next armed fsyncs on each disk, then lets them through.
type syncFault struct{ next [3]atomic.Int32 }

func (f *syncFault) arm(disks ...int) {
	for _, i := range disks {
		f.next[i].Store(1)
	}
}

func (f *syncFault) wrap(i int, m *disk.Mem) disk.FS {
	return disk.NewFaulty(m, func() {}, func(op disk.Op) disk.Decision {
		if op.Kind == disk.OpSync {
			for {
				n := f.next[i].Load()
				if n <= 0 {
					break
				}
				if f.next[i].CompareAndSwap(n, n-1) {
					return disk.Decision{Err: disk.ErrIO}
				}
			}
		}
		return disk.Decision{}
	})
}

// Every replica is readable but none can be sealed (IO errors on the seal
// write). The volume must not be recorded as empty: its records stay visible,
// no replica is marked failed, and the seal completes later.
func testNoReplicaSealableIsNotRecordedEmpty(t *testing.T) {
	var sf syncFault
	r, _ := newSealRig(t, sf.wrap)
	r.want(r.put("/b/c/f0", sealPayload(0)), 201)
	r.want(r.put("/b/c/f1", sealPayload(1)), 201)
	r.want(r.put("/b/c/f2", sealPayload(2)), 201)
	sf.arm(0, 1, 2) // the seal's fsync fails on every replica
	r.want(r.put("/b/c/f3", sealPayload(3)), 503)
	if r.pendingSeals() != 1 {
		t.Fatalf("pending = %d", r.pendingSeals())
	}
	if r.sealedCount(t) != 0 {
		t.Fatal("the catalogue was told the volume is sealed")
	}
	for _, v := range r.volumes(t) {
		for _, rep := range v.Replicas {
			if rep.State != catalogue.ReplicaOK {
				t.Fatalf("replica marked %v although nothing was sealed", rep.State)
			}
		}
	}
	r.wantAllReadable(t, []int{0, 1, 2})
	r.ticks(t, 2)
	eventually(t, "pending seal to complete", func() bool { return r.pendingSeals() == 0 })
	for _, v := range r.volumes(t) {
		if v.VolumeNumber == 1 && (v.State != catalogue.VolumeSealed || v.RecordCount != 3 || v.CommittedLength == 0) {
			t.Fatalf("volume 1: %v, %d records, committed %d", v.State, v.RecordCount, v.CommittedLength)
		}
	}
	r.wantAllReadable(t, []int{0, 1, 2})
}

func TestNoReplicaSealableIsNotRecordedEmpty(t *testing.T) {
	eachBackend(t, testNoReplicaSealableIsNotRecordedEmpty)
}

// Scenario 3: one replica has an IO error while sealing. The others seal, the
// volume is recorded with its real length and count, and only the bad replica
// is marked failed. The sealed replicas are byte-identical.
func testOneReplicaSealFailure(t *testing.T) {
	var sf syncFault
	r, _ := newSealRig(t, sf.wrap)
	for i := range 3 {
		r.want(r.put(fmt.Sprintf("/b/c/f%d", i), sealPayload(i)), 201)
	}
	sf.arm(0)                                     // the seal's fsync fails on disk 0 only
	r.want(r.put("/b/c/f3", sealPayload(3)), 201) // seals volume 1, writes to volume 2
	var v1 catalogue.Volume
	for _, v := range r.volumes(t) {
		if v.VolumeNumber == 1 {
			v1 = v
		}
	}
	if v1.State != catalogue.VolumeSealed || v1.RecordCount != 3 {
		t.Fatalf("volume 1: %v, %d records", v1.State, v1.RecordCount)
	}
	for _, rep := range v1.Replicas {
		want := catalogue.ReplicaOK
		if rep.Disk == (catalogue.DiskID{1}) {
			want = catalogue.ReplicaFailed
		}
		if rep.State != want {
			t.Fatalf("replica on disk %x is %v, want %v", rep.Disk[0], rep.State, want)
		}
	}
	r.wantAllReadable(t, []int{0, 1, 2})
	a, _ := disk.ReadFile(r.fs[1], volume.Path("c", uint64(v1.ID)))
	b, _ := disk.ReadFile(r.fs[2], volume.Path("c", uint64(v1.ID)))
	if len(a) == 0 || !bytes.Equal(a, b) {
		t.Fatal("sealed replicas are not byte-identical")
	}
}

func TestOneReplicaSealFailure(t *testing.T) { eachBackend(t, testOneReplicaSealFailure) }

// Replicas sealed independently (restart recovery) carry the same seal check
// and are byte-identical, like replicas sealed by their writers.
func testReplicasByteIdenticalAfterSeal(t *testing.T) {
	for _, how := range []string{"stop", "crash"} {
		t.Run(how, func(t *testing.T) {
			r := newRigWith(t, 3, Config{DefaultShards: 1}, nil)
			r.want(r.put("/b/c/a", []byte("one")), 201)
			r.want(r.put("/b/c/b", []byte("two")), 201)
			if how == "stop" {
				r.restart()
			} else {
				r.kill()
				r.clk.Advance(time.Minute)
				r.start()
			}
			v := newestSealed(t, r, "c")
			var files [3][]byte
			for i := range 3 {
				files[i], _ = disk.ReadFile(r.fs[i], volume.Path("c", uint64(v.ID)))
			}
			if len(files[0]) == 0 || !bytes.Equal(files[0], files[1]) || !bytes.Equal(files[0], files[2]) {
				t.Fatal("replicas differ")
			}
		})
	}
}

func TestReplicasByteIdenticalAfterSeal(t *testing.T) {
	eachBackend(t, testReplicasByteIdenticalAfterSeal)
}
