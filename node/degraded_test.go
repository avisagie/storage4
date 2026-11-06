package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"testing"

	"storage4/catalogue"
	"storage4/catalogue/mem"
	"storage4/disk"
	"storage4/format"
	"storage4/volume"
)

// newestSealed returns the sealed volume of (bucket, shard 0) with the
// highest volume number.
func newestSealed(t *testing.T, r *rig, bucket string) catalogue.Volume {
	t.Helper()
	vs, err := r.cat.ListShardVolumes(context.Background(), bucket, 0)
	if err != nil {
		t.Fatal(err)
	}
	var best catalogue.Volume
	for _, v := range vs {
		if v.State == catalogue.VolumeSealed && v.VolumeNumber > best.VolumeNumber {
			best = v
		}
	}
	if best.ID == 0 {
		t.Fatal("no sealed volume")
	}
	return best
}

// A newest volume that no replica can serve must never let an older version
// of a name answer: reads say 503, unconditional writes go on (a new version
// is always the highest), conditional writes cannot be judged and say 503.
func testUnreadableNewestVolume(t *testing.T) {
	r := newRigWith(t, 3, Config{DefaultShards: 1}, nil)
	r.want(r.put("/b/c/a", []byte("v1")), 201)
	r.restart() // seals volume 1
	r.want(r.put("/b/c/a", []byte("v2")), 201)
	r.stop() // seals volume 2
	v2 := newestSealed(t, r, "c")
	if v2.RecordCount == 0 {
		t.Fatal("setup: the volume under test must not be empty")
	}
	for _, rep := range v2.Replicas {
		if err := r.cat.SetReplicaState(context.Background(), v2.ID, rep.Disk, catalogue.ReplicaFailed); err != nil {
			t.Fatal(err)
		}
	}
	r.start()

	for _, m := range []string{"GET", "HEAD"} {
		res := r.do(m, "/b/c/a", nil)
		r.want(res, 503)
		if res.h.Get("Retry-After") == "" {
			t.Fatalf("%s: 503 without Retry-After", m)
		}
	}
	r.want(r.do("GET", "/m/c/a", nil), 503)
	r.want(r.put("/b/c/a", []byte("v3"), "If-None-Match", "*"), 503)
	r.want(r.put("/b/c/a", []byte("v3"), "If-Match", etag([]byte("v1"))), 503)
	if len(r.alarm.Active()) == 0 {
		t.Fatal("no alarm for the unreadable volume")
	}
	// Writes keep going, and the new version is what reads return.
	r.want(r.put("/b/c/a", []byte("v1")), 201) // even bytes equal to an older version
	if g := r.do("GET", "/b/c/a", nil); g.code != 200 || string(g.body) != "v1" {
		t.Fatalf("after write: %d %q", g.code, g.body)
	}
	// A name the broken volume cannot hold is still unknowable, but names
	// written since are certain.
	r.want(r.put("/b/c/z", []byte("zz")), 201)
	if g := r.do("GET", "/b/c/z", nil); g.code != 200 || string(g.body) != "zz" {
		t.Fatalf("z: %d %q", g.code, g.body)
	}
	r.restart() // seals the new volume: it now answers before the broken one
	if g := r.do("GET", "/b/c/a", nil); g.code != 200 || string(g.body) != "v1" {
		t.Fatalf("after restart: %d %q", g.code, g.body)
	}
	r.want(r.do("GET", "/b/c/never", nil), 503) // might live in the broken volume
}

func TestUnreadableNewestVolume(t *testing.T) { eachBackend(t, testUnreadableNewestVolume) }

// A GET that aborts mid-transfer must still release its record (file handle,
// index reference).
func testAbortedGetReleasesHandle(t *testing.T) {
	r := newRigWith(t, 3, Config{DefaultShards: 1}, nil)
	data := make([]byte, 300<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	r.want(r.put("/b/c/big", data), 201)
	r.restart() // sealed: reads go through the name index and its handle cache
	v := newestSealed(t, r, "c")
	for i := range 3 { // corrupt the end of the payload on every replica
		f, err := r.fs[i].OpenFile(volume.Path("c", uint64(v.ID)), disk.Write)
		if err != nil {
			t.Fatal(err)
		}
		var off, ln int64
		if _, err := volume.Scan(f, func(rec volume.Rec) error {
			if rec.Type == format.TypeFile {
				off, ln = rec.Off, int64(rec.Len)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{0xFF, 0xFF}, off+ln-4096); err != nil { // inside the payload, past the first 64 KiB chunk
			t.Fatal(err)
		}
		f.Sync()
		f.Close()
	}
	res, err := http.Get(r.srv.URL + "/b/c/big")
	if err == nil {
		_, err = readAll(res)
	}
	if err == nil {
		t.Fatalf("a corrupt tail was delivered without an error (status %d)", res.StatusCode)
	}
	r.clk.Advance(2 * r.n.cfg.NidxIdle)
	for _, d := range r.n.disks {
		d.store.CloseIdle()
	}
	open := 0
	for _, d := range r.n.disks {
		open += d.store.OpenHandles()
	}
	if open != 0 {
		t.Fatalf("%d index handles still referenced after the aborted GET", open)
	}
}

func readAll(res *http.Response) (int, error) {
	defer res.Body.Close()
	var buf bytes.Buffer
	n, err := buf.ReadFrom(res.Body)
	return int(n), err
}

func TestAbortedGetReleasesHandle(t *testing.T) { eachBackend(t, testAbortedGetReleasesHandle) }

// Best-effort counter: a dead disk must not stop volume IDs; only failing on
// every disk does. On load the highest value wins.
func TestCounterBestEffort(t *testing.T) {
	mk := func(fail bool) *localDisk {
		m := disk.NewMem(disk.MemOptions{})
		var fs disk.FS = m
		if fail {
			fs = disk.NewFaulty(m, func() {}, func(op disk.Op) disk.Decision {
				if op.Kind.Mutating() {
					return disk.Decision{Err: disk.ErrIO}
				}
				return disk.Decision{}
			})
		}
		return &localDisk{fs: fs}
	}
	good1, good2, dead := mk(false), mk(false), mk(true)
	c := &idCounter{node: 3, next: 1, disks: []*localDisk{dead, good1, good2}}
	seen := map[catalogue.VolumeID]bool{}
	for range counterBlock + 5 { // crosses a reserve boundary
		id, err := c.allocate()
		if err != nil {
			t.Fatalf("allocate with one dead disk: %v", err)
		}
		if seen[id] || id.Node() != 3 {
			t.Fatalf("bad or repeated id %x", uint64(id))
		}
		seen[id] = true
	}
	// A restart reads the highest value over the disks it can read.
	var hi uint64
	for _, d := range []*localDisk{good1, good2} {
		b, err := disk.ReadFile(d.fs, counterFile)
		if err != nil {
			t.Fatal(err)
		}
		v, err := decodeCounter(b)
		if err != nil {
			t.Fatal(err)
		}
		hi = max(hi, v)
	}
	if hi < uint64(counterBlock+5) {
		t.Fatalf("reserve %d below the IDs handed out", hi)
	}
	// Every disk failing is the only failure.
	c2 := &idCounter{node: 3, next: 1, disks: []*localDisk{mk(true), mk(true)}}
	if _, err := c2.allocate(); !errors.Is(err, errCounterUnavailable) {
		t.Fatalf("all disks dead: %v", err)
	}
}

// The incarnation check in the heartbeat must not race with volume creation
// on request goroutines (run with -race).
func TestIncarnationChangeDoesNotRace(t *testing.T) {
	r := newRigWith(t, 3, Config{DefaultShards: 16}, nil)
	mc, ok := r.cat.(*mem.Catalogue)
	if !ok {
		t.Skip("mem catalogue only")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 40 {
			r.put(fmt.Sprintf("/b/c/n%d", i), []byte("x"))
		}
	}()
	for i := range 40 {
		mc.SetIncarnation(int64(100 + i))
		r.n.heartbeat(context.Background())
	}
	wg.Wait()
}

// A sealed volume with no records (a crashed creation, sealed at length 0 with
// every replica lost) holds nothing, so it must not make lookups uncertain.
func TestEmptySealedVolumeIsNotUncertain(t *testing.T) {
	r := newRigWith(t, 3, Config{DefaultShards: 1}, nil)
	r.want(r.put("/b/c/a", []byte("v1")), 201)
	sh := r.n.shardOf(catalogue.Bucket{Name: "c", Shards: 1}, 0)
	empty := catalogue.Volume{ID: 1, Bucket: "c", State: catalogue.VolumeSealed,
		Replicas: []catalogue.Replica{{Disk: catalogue.DiskID{1}, State: catalogue.ReplicaLost}}}
	f, unreadable, err := sh.lookupSealed(empty, "a", nil)
	if f != nil || unreadable || err != nil {
		t.Fatalf("empty volume: %v %v %v", f, unreadable, err)
	}
	empty.RecordCount = 3
	if _, unreadable, _ := sh.lookupSealed(empty, "a", nil); !unreadable {
		t.Fatal("a volume with records and no usable replica must be unreadable")
	}
}
