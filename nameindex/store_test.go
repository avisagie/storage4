package nameindex

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"storage4/disk"
	"storage4/format"
	"storage4/volume"
)

const bkt = "events"

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) After(time.Duration) <-chan time.Time { return nil }

func rnd() func() uint32 {
	var n uint32
	return func() uint32 { n += 0x9E3779B1; return n }
}

func payload(name string, ver int) []byte { return []byte(fmt.Sprintf("payload of %s v%d", name, ver)) }

// mkVolume writes and seals a volume holding the given names, one record
// each, in order (a name may repeat: later is the newer version). It
// returns the locators and the open map the owner would have kept.
func mkVolume(t *testing.T, fsys disk.FS, volumeId uint64, names ...string) ([]volume.Locator, *OpenMap) {
	t.Helper()
	w, err := volume.Create(fsys, format.VolumeHeader{VolumeID: volumeId, ShardCount: 4, Shard: 1, VolumeNumber: 3,
		Epoch: 1, Bucket: bkt}, volume.Options{Rand: rnd()})
	if err != nil {
		t.Fatal(err)
	}
	var es []volume.Entry
	for i, n := range names {
		p := payload(n, i)
		es = append(es, volume.Entry{Name: n, SHA256: sha256.Sum256(p), Payload: p})
	}
	locs, err := w.Append(es)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Seal(2); err != nil {
		t.Fatal(err)
	}
	om := NewOpenMap()
	for i, n := range names {
		om.Put(n, OpenEntry{Loc: locs[i], SHA256: es[i].SHA256})
	}
	return locs, om
}

func newStore(fsys disk.FS, clk Clock, max int, fp func([]byte) uint64) *Store {
	return NewStore(fsys, Options{MaxHandles: max, IdleTimeout: time.Minute, Clock: clk, Fingerprint: fp})
}

func lookupPayload(t *testing.T, s *Store, volumeId uint64, name string) ([]byte, uint32, error) {
	t.Helper()
	h, err := s.Lookup(bkt, volumeId, name)
	if err != nil {
		return nil, 0, err
	}
	defer h.Release()
	b, err := h.Head.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return b, h.Loc.Seq, nil
}

func TestLookupLatestVersion(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	locs, om := mkVolume(t, fsys, vid, "a", "b", "a", "c")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	got, seq, err := lookupPayload(t, s, vid, "a")
	if err != nil || seq != locs[2].Seq || !bytes.Equal(got, payload("a", 2)) {
		t.Fatalf("a: seq %d %q %v", seq, got, err)
	}
	for n, ver := range map[string]int{"b": 1, "c": 3} {
		got, _, err := lookupPayload(t, s, vid, n)
		if err != nil || !bytes.Equal(got, payload(n, ver)) {
			t.Fatalf("%s: %q %v", n, got, err)
		}
	}
	if _, _, err := lookupPayload(t, s, vid, "zzz"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing name: %v", err)
	}
}

func TestWriteFromOpenMapEqualsRebuild(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	_, om := mkVolume(t, fsys, vid, "a", "b", "a", "c/d:e")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	w1, _ := disk.ReadFile(fsys, volume.NidxPath(bkt, vid))
	if err := s.Rebuild(bkt, vid); err != nil {
		t.Fatal(err)
	}
	w2, _ := disk.ReadFile(fsys, volume.NidxPath(bkt, vid))
	if !bytes.Equal(w1, w2) {
		t.Fatal("writer-side index differs from a rebuild")
	}
	if _, err := DecodeTable(w2, vid); err != nil {
		t.Fatal(err)
	}
}

func TestNoNamesOrHashesInFile(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	_, om := mkVolume(t, fsys, vid, "secret-name-one", "secret-name-two")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	b, _ := disk.ReadFile(fsys, volume.NidxPath(bkt, vid))
	for i, n := range []string{"secret-name-one", "secret-name-two"} {
		sum := sha256.Sum256(payload(n, i))
		if bytes.Contains(b, []byte(n)) || bytes.Contains(b, sum[:]) {
			t.Fatalf("%s: name or content hash in index file", n)
		}
	}
}

func TestMissingStaleAndCorruptIndexRebuilt(t *testing.T) {
	flip := func(off int) func([]byte) []byte {
		return func(b []byte) []byte { c := bytes.Clone(b); c[off] ^= 0xff; return c }
	}
	cases := map[string]func([]byte) []byte{
		"missing":      nil,
		"bucket flip":  flip(headerLen + 3),
		"header flip":  flip(20),
		"truncated":    func(b []byte) []byte { return b[:len(b)-100] },
		"empty":        func([]byte) []byte { return nil },
		"garbage":      func(b []byte) []byte { return bytes.Repeat([]byte{0xAB}, len(b)) },
		"wrong volume": nil, // filled below
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fsys := disk.NewMem(disk.MemOptions{})
			_, om := mkVolume(t, fsys, vid, "a", "b", "c")
			mkVolume(t, fsys, vid+1, "x")
			s := newStore(fsys, &fakeClock{}, 8, nil)
			if err := s.Write(bkt, vid, om.Builder()); err != nil {
				t.Fatal(err)
			}
			path := volume.NidxPath(bkt, vid)
			good, _ := disk.ReadFile(fsys, path)
			switch name {
			case "missing":
				if err := fsys.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "wrong volume":
				other, _ := Encode(vid+1, []Entry{{FP: Fingerprint([]byte("x")), Seq: 1, Off: 64}})
				if err := disk.WriteFileAtomic(fsys, path, other); err != nil {
					t.Fatal(err)
				}
			default:
				if err := disk.WriteFileAtomic(fsys, path, mutate(good)); err != nil {
					t.Fatal(err)
				}
			}
			s.Close() // drop cached handles to the old file
			for _, n := range []string{"a", "b", "c"} {
				if _, _, err := lookupPayload(t, s, vid, n); err != nil {
					t.Fatalf("%s: %v", n, err)
				}
			}
			after, _ := disk.ReadFile(fsys, path)
			if !bytes.Equal(after, good) {
				t.Fatal("index was not rebuilt to the canonical bytes")
			}
		})
	}
}

// A valid index whose entry points into the middle of a record is stale, not
// a collision: rebuild and answer correctly.
func TestStaleOffsetRebuilt(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	locs, _ := mkVolume(t, fsys, vid, "a", "b")
	bad, _ := Encode(vid, []Entry{
		{FP: Fingerprint([]byte("a")), Seq: locs[0].Seq, Off: locs[0].Off + 8},
		{FP: Fingerprint([]byte("b")), Seq: locs[1].Seq, Off: locs[1].Off},
	})
	if err := disk.WriteFileAtomic(fsys, volume.NidxPath(bkt, vid), bad); err != nil {
		t.Fatal(err)
	}
	s := newStore(fsys, &fakeClock{}, 8, nil)
	got, _, err := lookupPayload(t, s, vid, "a")
	if err != nil || !bytes.Equal(got, payload("a", 0)) {
		t.Fatalf("%q %v", got, err)
	}
}

// Every name has the same fingerprint: lookups cost extra reads but never
// return another name's record, and a name that is absent is not found.
func TestFingerprintCollisions(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	var names []string
	for i := range 300 { // also overflows one bucket (255 slots)
		names = append(names, fmt.Sprintf("n%03d", i))
	}
	mkVolume(t, fsys, vid, names...)
	s := newStore(fsys, &fakeClock{}, 8, func([]byte) uint64 { return 7 })
	if err := s.Rebuild(bkt, vid); err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		got, _, err := lookupPayload(t, s, vid, n)
		if err != nil || !bytes.Equal(got, payload(n, i)) {
			t.Fatalf("%s: %q %v", n, got, err)
		}
	}
	if _, _, err := lookupPayload(t, s, vid, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("absent: %v", err)
	}
}

func TestErasedRecordIsSkipped(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	locs, om := mkVolume(t, fsys, vid, "a", "b")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	f, _ := fsys.OpenFile(volume.Path(bkt, vid), disk.Write)
	if err := volume.Erase(f, []volume.EraseItem{{Loc: locs[1], Name: "b"}}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, _, err := lookupPayload(t, s, vid, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("erased: %v", err)
	}
	if _, _, err := lookupPayload(t, s, vid, "a"); err != nil {
		t.Fatalf("a: %v", err)
	}
	// A rebuild omits the erased record.
	if err := s.Rebuild(bkt, vid); err != nil {
		t.Fatal(err)
	}
	b, _ := disk.ReadFile(fsys, volume.NidxPath(bkt, vid))
	tab, err := DecodeTable(b, vid)
	if err != nil || len(tab.Entries) != 1 {
		t.Fatalf("%v %+v", err, tab)
	}
}

// A disk read error is not a corrupt index: it is returned, and the index
// file is left alone.
func TestIOErrorIsNotCorruption(t *testing.T) {
	mem := disk.NewMem(disk.MemOptions{})
	_, om := mkVolume(t, mem, vid, "a")
	ff := disk.NewFaulty(mem, func() {}, nil)
	s := newStore(ff, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	before, _ := disk.ReadFile(mem, volume.NidxPath(bkt, vid))
	s.Close()
	ff.SetInjector(func(op disk.Op) disk.Decision {
		if op.Kind == disk.OpRead && op.Path == volume.NidxPath(bkt, vid) {
			return disk.Decision{Err: disk.ErrIO}
		}
		return disk.Decision{}
	})
	if _, _, err := lookupPayload(t, s, vid, "a"); !errors.Is(err, disk.ErrIO) {
		t.Fatalf("got %v", err)
	}
	ff.SetInjector(nil)
	after, _ := disk.ReadFile(mem, volume.NidxPath(bkt, vid))
	if !bytes.Equal(before, after) {
		t.Fatal("index rewritten after an I/O error")
	}
}

func TestMissingVolumeFileIsAnError(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	_, om := mkVolume(t, fsys, vid, "a")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Write(bkt, vid, om.Builder()); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Remove(volume.Path(bkt, vid)); err != nil {
		t.Fatal(err)
	}
	_, _, err := lookupPayload(t, s, vid, "a")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestEnsure(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	_, om := mkVolume(t, fsys, vid, "a", "b")
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if rebuilt, err := s.Ensure(bkt, vid); err != nil || !rebuilt {
		t.Fatalf("missing: %v %v", rebuilt, err)
	}
	if rebuilt, err := s.Ensure(bkt, vid); err != nil || rebuilt {
		t.Fatalf("present: %v %v", rebuilt, err)
	}
	_ = om
	path := volume.NidxPath(bkt, vid)
	b, _ := disk.ReadFile(fsys, path)
	b[len(b)-1] ^= 1 // footer
	if err := disk.WriteFileAtomic(fsys, path, b); err != nil {
		t.Fatal(err)
	}
	if rebuilt, err := s.Ensure(bkt, vid); err != nil || !rebuilt {
		t.Fatalf("bad footer: %v %v", rebuilt, err)
	}
}

func TestRebuildRefusesOpenVolume(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, err := volume.Create(fsys, format.VolumeHeader{VolumeID: vid, ShardCount: 4, Shard: 1, VolumeNumber: 3,
		Epoch: 1, Bucket: bkt}, volume.Options{Rand: rnd()})
	if err != nil {
		t.Fatal(err)
	}
	p := []byte("x")
	if _, err := w.Append([]volume.Entry{{Name: "a", SHA256: sha256.Sum256(p), Payload: p}}); err != nil {
		t.Fatal(err)
	}
	s := newStore(fsys, &fakeClock{}, 8, nil)
	if err := s.Rebuild(bkt, vid); !errors.Is(err, volume.ErrNotSealed) {
		t.Fatalf("got %v", err)
	}
	if _, err := fsys.Stat(volume.NidxPath(bkt, vid)); err == nil {
		t.Fatal("wrote an index for an open volume")
	}
}

func TestReaderVerify(t *testing.T) {
	mem := disk.NewMem(disk.MemOptions{})
	b, _ := Encode(vid, mkEntries(500))
	if err := disk.WriteFileAtomic(mem, "x.nidx", b); err != nil {
		t.Fatal(err)
	}
	f, _ := mem.OpenFile("x.nidx", disk.Write)
	r, err := OpenReader(f, vid)
	if err != nil || r.Count() != 500 {
		t.Fatal(err)
	}
	if err := r.Verify(); err != nil {
		t.Fatal(err)
	}
	// Damage a bucket the footer-only open cannot see.
	if _, err := f.WriteAt([]byte{0xFF}, headerLen+bucketSize+5); err != nil {
		t.Fatal(err)
	}
	if err := r.Verify(); !errors.Is(err, ErrBadNidx) {
		t.Fatalf("verify: %v", err)
	}
	// A lookup that touches the damaged bucket reports it; others still work.
	var bads int
	for _, e := range mkEntries(500) {
		if _, err := r.Candidates(e.FP); errors.Is(err, ErrBadNidx) {
			bads++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if bads == 0 {
		t.Fatal("damaged bucket never detected")
	}
}

func FuzzReaderCandidates(f *testing.F) {
	good, _ := Encode(vid, mkEntries(30))
	f.Add(good, uint64(1))
	f.Fuzz(func(t *testing.T, b []byte, fp uint64) {
		mem := disk.NewMem(disk.MemOptions{})
		if err := disk.WriteFileAtomic(mem, "x.nidx", b); err != nil {
			t.Skip()
		}
		file, _ := mem.OpenFile("x.nidx", 0)
		r, err := OpenReader(file, vid)
		if err != nil {
			return
		}
		_, _ = r.Candidates(fp)
		_ = r.Verify()
	})
}

func TestHandleCacheBoundAndIdleClose(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	clk := &fakeClock{t: time.Unix(1000, 0)}
	s := newStore(fsys, clk, 2, nil)
	for i := range 4 {
		volumeId := vid + uint64(i)
		mkVolume(t, fsys, volumeId, "a")
		if err := s.Rebuild(bkt, volumeId); err != nil {
			t.Fatal(err)
		}
		if _, _, err := lookupPayload(t, s, volumeId, "a"); err != nil {
			t.Fatal(err)
		}
		if n := s.OpenHandles(); n > 2 {
			t.Fatalf("after volume %d: %d handles, bound is 2", i, n)
		}
	}
	if s.OpenHandles() == 0 {
		t.Fatal("nothing cached")
	}
	clk.t = clk.t.Add(30 * time.Second)
	s.CloseIdle()
	if s.OpenHandles() == 0 {
		t.Fatal("closed before the idle timeout")
	}
	clk.t = clk.t.Add(time.Minute)
	s.CloseIdle()
	if n := s.OpenHandles(); n != 0 {
		t.Fatalf("%d handles after idle timeout", n)
	}
}

// A Hit in use is never closed under the reader, by idle close, eviction or
// Close; it is closed when released.
func TestHeldHitSurvivesCloseAndEviction(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	clk := &fakeClock{t: time.Unix(1000, 0)}
	s := newStore(fsys, clk, 1, nil)
	mkVolume(t, fsys, vid, "a")
	mkVolume(t, fsys, vid+1, "b")
	h, err := s.Lookup(bkt, vid, "a")
	if err != nil {
		t.Fatal(err)
	}
	clk.t = clk.t.Add(time.Hour)
	s.CloseIdle()
	if _, _, err := lookupPayload(t, s, vid+1, "b"); err != nil { // evicts around the held handle
		t.Fatal(err)
	}
	s.Close()
	if _, err := io.ReadAll(h.Head.Payload()); err != nil {
		t.Fatalf("read after close: %v", err)
	}
	h.Release()
	h.Release() // idempotent
	if n := s.OpenHandles(); n != 0 {
		t.Fatalf("%d handles left", n)
	}
}

func TestOpenMap(t *testing.T) {
	om := NewOpenMap()
	loc := func(seq uint32) OpenEntry {
		return OpenEntry{Loc: volume.Locator{Seq: seq, Off: int64(seq) * 8, Len: 64}}
	}
	om.Put("a", loc(2))
	om.Put("a", loc(1)) // older must not win
	om.Put("a", loc(3))
	om.Put("b", loc(4))
	if e, ok := om.Get("a"); !ok || e.Loc.Seq != 3 {
		t.Fatalf("%+v", e)
	}
	if _, ok := om.Get("zz"); ok || om.Len() != 2 {
		t.Fatal("len or miss")
	}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				om.Put(fmt.Sprintf("k%d", i%20), loc(uint32(g*200+i+10)))
				om.Get("a")
			}
		}()
	}
	wg.Wait()
	raw, err := om.Builder().Encode(vid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeTable(raw, vid); err != nil {
		t.Fatal(err)
	}
}

// Crash at every operation of publishing the index: afterwards there is no
// index or a complete valid one, and lookups work either way.
func TestCrashSweepWrite(t *testing.T) {
	for _, mode := range []disk.CrashMode{disk.CrashDropAll, disk.CrashAny, disk.CrashKeepAll} {
	sweep:
		for n := 1; ; n++ {
			mem := disk.NewMem(disk.MemOptions{})
			_, om := mkVolume(t, mem, vid, "a", "b", "c")
			rng := rand.New(rand.NewPCG(uint64(n), 1))
			ff := disk.NewFaulty(mem, func() { mem.Crash(rng, mode) }, disk.CrashAt(n, n%2 == 0, nil))
			s := newStore(ff, &fakeClock{}, 8, nil)
			_ = s.Write(bkt, vid, om.Builder())
			if !ff.Down() {
				break sweep
			}
			if b, err := disk.ReadFile(mem, volume.NidxPath(bkt, vid)); err == nil {
				if _, err := DecodeTable(b, vid); err != nil {
					t.Fatalf("mode %v n %d: torn index visible: %v", mode, n, err)
				}
			}
			s2 := newStore(mem, &fakeClock{}, 8, nil)
			for _, name := range []string{"a", "b", "c"} {
				if _, _, err := lookupPayload(t, s2, vid, name); err != nil {
					t.Fatalf("mode %v n %d: %s: %v", mode, n, name, err)
				}
			}
		}
	}
}
