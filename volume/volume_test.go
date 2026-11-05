package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"storage4/disk"
	"storage4/format"
)

func counter() func() uint32 {
	var n uint32
	return func() uint32 { n += 0x9E3779B1; return n }
}

func hdr() format.VolumeHeader {
	return format.VolumeHeader{VolumeID: 0x0001_0000_0000_0007, ShardCount: 4, Shard: 1, Ordinal: 3,
		Epoch: 1, Bucket: "events"}
}

func entry(name string, n int) Entry {
	p := bytes.Repeat([]byte{byte(len(name))}, n)
	return Entry{Name: name, SHA256: sha256.Sum256(p), Metadata: []byte(`{"a":1}`), Payload: p}
}

func open(t *testing.T, fsys disk.FS) disk.File {
	t.Helper()
	f, err := fsys.OpenFile(Path("events", hdr().VolumeID), disk.Write)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAppendScanRead(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, err := Create(fsys, hdr(), Options{Rand: counter()})
	if err != nil {
		t.Fatal(err)
	}
	es := []Entry{entry("a/one", 10), entry("b", 0), entry("big", smallPayload+5000), entry("c", 100)}
	locs, err := w.Append(es)
	if err != nil {
		t.Fatal(err)
	}
	more, err := w.Append([]Entry{entry("d", 1)})
	if err != nil {
		t.Fatal(err)
	}
	locs = append(locs, more...)
	es = append(es, entry("d", 1))

	f := open(t, fsys)
	var seen []Rec
	res, err := Scan(f, func(r Rec) error {
		r.Name = append([]byte(nil), r.Name...)
		seen = append(seen, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.End != w.Committed() || res.Size != res.End || res.Count != 5 || res.LastSeq != 5 || res.Sealed {
		t.Fatalf("scan result %+v, committed %d", res, w.Committed())
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d records", len(seen))
	}
	for i, e := range es {
		if seen[i].Locator != locs[i] || string(seen[i].Name) != e.Name {
			t.Fatalf("record %d: scan %+v name %q, append %+v", i, seen[i].Locator, seen[i].Name, locs[i])
		}
		h, err := ReadHead(f, locs[i], e.Name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.ReadAll()
		if err != nil || !bytes.Equal(got, e.Payload) || h.SHA256 != e.SHA256 || !bytes.Equal(h.Metadata, e.Metadata) {
			t.Fatalf("record %d read: %v", i, err)
		}
	}
}

func TestGroupCommitIsOneSync(t *testing.T) {
	mem := disk.NewMem(disk.MemOptions{})
	syncs := 0
	ff := disk.NewFaulty(mem, func() {}, func(op disk.Op) disk.Decision {
		if op.Kind == disk.OpSync && op.Path == Path("events", hdr().VolumeID) {
			syncs++
		}
		return disk.Decision{}
	})
	w, err := Create(ff, hdr(), Options{Rand: counter()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]Entry{entry("a", 5), entry("b", 5), entry("c", 5)}); err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("%d syncs for one batch", syncs)
	}
}

func TestInvalidAndFullConsumeNothing(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter(), MaxSize: 600})
	if _, err := w.Append([]Entry{entry("ok", 1), {Name: "bad//name"}}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("got %v", err)
	}
	if _, err := w.Append([]Entry{entry("big", 1000)}); !errors.Is(err, ErrFull) {
		t.Fatalf("got %v", err)
	}
	locs, err := w.Append([]Entry{entry("fits", 1)})
	if err != nil || locs[0].Seq != 1 {
		t.Fatalf("seq after refusals: %v %+v", err, locs)
	}
}

func TestCreateExistsAndAtomic(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	if _, err := Create(fsys, hdr(), Options{Rand: counter()}); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(fsys, hdr(), Options{Rand: counter()}); !errors.Is(err, disk.ErrExist) {
		t.Fatalf("got %v", err)
	}
}

// A failed append consumes its seqs and disables the writer; SealFile
// discards the unacknowledged tail and seals with a seq above every seq the
// file ever carried.
func TestFailedAppendThenSealFile(t *testing.T) {
	mem := disk.NewMem(disk.MemOptions{})
	fail := false
	ff := disk.NewFaulty(mem, func() {}, func(op disk.Op) disk.Decision {
		if fail && op.Kind == disk.OpSync {
			return disk.Decision{Err: disk.ErrIO}
		}
		return disk.Decision{}
	})
	w, err := Create(ff, hdr(), Options{Rand: counter()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]Entry{entry("a", 5), entry("b", 5)}); err != nil {
		t.Fatal(err)
	}
	acked := w.Committed()
	fail = true
	if _, err := w.Append([]Entry{entry("c", 5), entry("d", 5), entry("e", 5)}); !errors.Is(err, ErrFailed) {
		t.Fatalf("got %v", err)
	}
	fail = false
	if _, err := w.Append([]Entry{entry("f", 1)}); !errors.Is(err, ErrFailed) {
		t.Fatalf("writer must stay failed, got %v", err)
	}
	if _, err := w.Seal(2); !errors.Is(err, ErrFailed) {
		t.Fatalf("failed writer must not seal itself, got %v", err)
	}
	if w.Committed() != acked {
		t.Fatal("committed moved")
	}
	w.Close()

	s, err := SealFile(ff, Path("events", hdr().VolumeID), acked, 2, counter())
	if err != nil {
		t.Fatal(err)
	}
	if s.CommittedLength != uint64(acked) || s.RecordCount != 2 || s.LastSeq != 2 || s.Epoch != 2 {
		t.Fatalf("seal %+v", s)
	}
	f := open(t, mem)
	var seqs []uint32
	res, err := Scan(f, func(r Rec) error { seqs = append(seqs, r.Seq); return nil })
	if err != nil || !res.Sealed || res.LastSeq != 3 {
		t.Fatalf("res %+v err %v", res, err)
	}
	if fmt.Sprint(seqs) != "[1 2 3]" {
		t.Fatalf("seqs %v", seqs)
	}
	// Idempotent; different length refused.
	if _, err := SealFile(ff, Path("events", hdr().VolumeID), acked, 2, counter()); err != nil {
		t.Fatal(err)
	}
	if _, err := SealFile(ff, Path("events", hdr().VolumeID), acked-8, 2, counter()); !errors.Is(err, ErrSealMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestSealOwnWriter(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	w.Append([]Entry{entry("a", 5)})
	s, err := w.Seal(7)
	if err != nil || s.RecordCount != 1 || s.LastSeq != 1 || s.Epoch != 7 {
		t.Fatalf("%+v %v", s, err)
	}
	if _, err := w.Append([]Entry{entry("b", 1)}); !errors.Is(err, ErrSealed) {
		t.Fatalf("got %v", err)
	}
	res, err := Scan(open(t, fsys), nil)
	if err != nil || !res.Sealed || *res.Seal != s || res.LastSeq != 2 {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestSealFileShortReplica(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	w.Append([]Entry{entry("a", 5)})
	c := w.Committed()
	p := Path("events", hdr().VolumeID)
	for _, bad := range []int64{c + 8, c - 8, c - 1} {
		if _, err := SealFile(fsys, p, bad, 2, counter()); !errors.Is(err, ErrShortReplica) {
			t.Fatalf("committed %d: got %v", bad, err)
		}
	}
	// Nothing was changed by the refusals.
	if res, _ := Scan(open(t, fsys), nil); res.Sealed || res.End != c {
		t.Fatalf("%+v", res)
	}
}

// The valid prefix ends at the first invalid record even when valid records
// follow (docs/disk.md recovery rule).
func TestScanStopsAtFirstInvalid(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	locs, _ := w.Append([]Entry{entry("a", 20), entry("b", 20), entry("c", 20)})
	f := open(t, fsys)
	f.WriteAt([]byte{0xFF}, locs[1].Off+30) // inside b
	res, _ := Scan(f, nil)
	if res.End != locs[1].Off || res.Count != 1 {
		t.Fatalf("%+v", res)
	}
	// A seq that does not increase also ends the prefix.
	w2fs := disk.NewMem(disk.MemOptions{})
	w2, _ := Create(w2fs, hdr(), Options{Rand: counter()})
	l2, _ := w2.Append([]Entry{entry("a", 20), entry("b", 20)})
	f2 := open(t, w2fs)
	rec := make([]byte, l2[0].Len)
	f2.ReadAt(rec, l2[0].Off)
	f2.WriteAt(rec, l2[1].Off+int64(l2[1].Len)) // copy of seq 1 after seq 2
	res, _ = Scan(f2, nil)
	if res.Count != 2 || res.End != l2[1].Off+int64(l2[1].Len) {
		t.Fatalf("%+v", res)
	}
}

func TestReadHeadVerifies(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	locs, _ := w.Append([]Entry{entry("a", 20), entry("b", 20)})
	f := open(t, fsys)
	for name, tc := range map[string]struct {
		loc  Locator
		name string
	}{
		"wrong name":  {locs[0], "b"},
		"wrong seq":   {Locator{Seq: 9, Check: locs[0].Check, Off: locs[0].Off, Len: locs[0].Len}, "a"},
		"wrong check": {Locator{Seq: 1, Check: locs[0].Check + 1, Off: locs[0].Off, Len: locs[0].Len}, "a"},
		"wrong len":   {Locator{Seq: 1, Check: locs[0].Check, Off: locs[0].Off, Len: locs[0].Len + 8}, "a"},
		"wrong off":   {Locator{Seq: 1, Check: locs[0].Check, Off: locs[0].Off + 8, Len: locs[0].Len}, "a"},
		"past end":    {Locator{Seq: 1, Off: 1 << 20, Len: 64}, "a"},
	} {
		if _, err := ReadHead(f, tc.loc, tc.name); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The mismatch cases report ErrMismatch where the file has bytes.
	if _, err := ReadHead(f, locs[0], "b"); !errors.Is(err, ErrMismatch) {
		t.Errorf("got %v", err)
	}
}

func TestStreamingReadDetectsPayloadCorruption(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	e := entry("a", 5000)
	locs, _ := w.Append([]Entry{e})
	f := open(t, fsys)
	f.WriteAt([]byte{0x55}, locs[0].Off+int64(locs[0].Len)-40)
	h, err := ReadHead(f, locs[0], "a") // header is intact: head reads succeed
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, h.Payload())
	if !errors.Is(err, format.ErrCorrupt) || n != int64(len(e.Payload)) {
		t.Fatalf("copied %d, err %v", n, err)
	}
	if _, err := h.ReadAll(); !errors.Is(err, format.ErrCorrupt) {
		t.Fatalf("got %v", err)
	}
}

func TestEraseThenReadAndScan(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	locs, _ := w.Append([]Entry{entry("a", 300), entry("b", 70000), entry("c", 3)})
	w.Seal(2)
	f := open(t, fsys)
	before, _ := Scan(f, nil)
	if err := Erase(f, []EraseItem{{locs[0], "wrong"}}); !errors.Is(err, ErrMismatch) {
		t.Fatalf("wrong name accepted: %v", err)
	}
	if err := Erase(f, []EraseItem{{locs[0], "a"}, {locs[1], "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := Erase(f, []EraseItem{{locs[0], "a"}}); err != nil { // idempotent
		t.Fatal(err)
	}
	for i, n := range []string{"a", "b"} {
		if _, err := ReadHead(f, locs[i], n); !errors.Is(err, ErrErased) {
			t.Fatalf("%s: %v", n, err)
		}
		raw := make([]byte, locs[i].Len-32)
		f.ReadAt(raw, locs[i].Off+int64(format.RecordHeaderLen))
		if bytes.Count(raw[:len(raw)-8], []byte{0}) < len(raw)-8-8 {
			t.Fatalf("%s: record not zeroed", n)
		}
	}
	after, err := Scan(f, nil)
	if err != nil || after.End != before.End || after.Count != before.Count || !after.Sealed {
		t.Fatalf("before %+v after %+v err %v", before, after, err)
	}
	if h, err := ReadHead(f, locs[2], "c"); err != nil {
		t.Fatal(err)
	} else if p, err := h.ReadAll(); err != nil || len(p) != 3 {
		t.Fatal(err)
	}
}

// Crash at every operation of a sequence of acknowledged batches, in
// every crash mode: all acknowledged records are inside the valid prefix
// with their locators, and the volume can then be sealed at the
// acknowledged length.
func TestCrashSweepAppend(t *testing.T) {
	batches := [][]Entry{
		{entry("a", 10)},
		{entry("b", 200), entry("c", 70000)},
		{entry("d", 0), entry("e", 4000), entry("f", 1)},
	}
	for _, mode := range []disk.CrashMode{disk.CrashDropAll, disk.CrashPrefix, disk.CrashAny, disk.CrashKeepAll} {
	sweep:
		for n := 1; ; n++ {
			for seed := uint64(0); seed < 6; seed++ {
				mem := disk.NewMem(disk.MemOptions{})
				rng := rand.New(rand.NewPCG(uint64(n), seed))
				ff := disk.NewFaulty(mem, func() { mem.Crash(rng, mode) }, disk.CrashAt(n, seed%2 == 0, nil))
				var acked [][]Locator
				var ackedEnd int64
				w, err := Create(ff, hdr(), Options{Rand: counter()})
				crashed := err != nil
				if err == nil {
					for _, b := range batches {
						locs, err := w.Append(b)
						if err != nil {
							crashed = true
							break
						}
						acked = append(acked, locs)
						ackedEnd = w.Committed()
					}
				}
				if !ff.Down() {
					break sweep // n is beyond the last operation
				}
				_ = crashed
				p := Path("events", hdr().VolumeID)
				if _, err := mem.Stat(p); err != nil {
					if len(acked) != 0 {
						t.Fatalf("mode %v n %d: acked data but no file", mode, n)
					}
					continue
				}
				f, _ := mem.OpenFile(p, disk.Write)
				var got []Locator
				res, err := Scan(f, func(r Rec) error { got = append(got, r.Locator); return nil })
				if err != nil {
					t.Fatalf("mode %v n %d: scan: %v", mode, n, err)
				}
				i := 0
				for _, b := range acked {
					for _, l := range b {
						if i >= len(got) || got[i] != l {
							t.Fatalf("mode %v n %d seed %d: acked record %+v missing from prefix %v", mode, n, seed, l, got)
						}
						i++
					}
				}
				f.Close()
				if len(acked) == 0 {
					ackedEnd = res.FirstOff
				}
				s, err := SealFile(mem, p, ackedEnd, 2, counter())
				if err != nil {
					t.Fatalf("mode %v n %d seed %d: seal at %d: %v", mode, n, seed, ackedEnd, err)
				}
				f, _ = mem.OpenFile(p, disk.Write)
				res, err = Scan(f, nil)
				if err != nil || !res.Sealed || *res.Seal != s || res.Size != res.End {
					t.Fatalf("mode %v n %d: after seal %+v %v", mode, n, res, err)
				}
				f.Close()
			}
		}
	}
}

// Crash at every operation of the seal sequence: the volume is either still
// unsealed with the full acknowledged prefix, or sealed; a second SealFile
// always completes it.
func TestCrashSweepSealFile(t *testing.T) {
	for _, mode := range []disk.CrashMode{disk.CrashDropAll, disk.CrashAny, disk.CrashKeepAll} {
		for n := 1; ; n++ {
			mem := disk.NewMem(disk.MemOptions{})
			w, _ := Create(mem, hdr(), Options{Rand: counter()})
			w.Append([]Entry{entry("a", 100), entry("b", 100)})
			acked := w.Committed()
			w.Append([]Entry{entry("tail", 100)}) // acked here, but the caller seals shorter
			w.Close()
			rng := rand.New(rand.NewPCG(uint64(n), 1))
			ff := disk.NewFaulty(mem, func() { mem.Crash(rng, mode) }, disk.CrashAt(n, false, nil))
			p := Path("events", hdr().VolumeID)
			SealFile(ff, p, acked, 2, counter())
			if !ff.Down() {
				break
			}
			f, _ := mem.OpenFile(p, disk.Write)
			res, err := Scan(f, nil)
			if err != nil || res.Count < 2 || res.LastSeq < 2 {
				t.Fatalf("mode %v n %d: lost data: %+v %v", mode, n, res, err)
			}
			f.Close()
			s, err := SealFile(mem, p, acked, 2, counter())
			if err != nil {
				t.Fatalf("mode %v n %d: resume: %v", mode, n, err)
			}
			f, _ = mem.OpenFile(p, disk.Write)
			res, _ = Scan(f, nil)
			if !res.Sealed || *res.Seal != s || res.Count != 2 || res.LastSeq != 3 {
				t.Fatalf("mode %v n %d: %+v", mode, n, res)
			}
			f.Close()
		}
	}
}

// A crash during erasure never loses other records after a retry.
func TestCrashSweepErase(t *testing.T) {
	for _, mode := range []disk.CrashMode{disk.CrashDropAll, disk.CrashAny, disk.CrashKeepAll} {
		for n := 1; ; n++ {
			mem := disk.NewMem(disk.MemOptions{})
			w, _ := Create(mem, hdr(), Options{Rand: counter()})
			locs, _ := w.Append([]Entry{entry("a", 100), entry("b", 3000), entry("c", 100)})
			w.Seal(2)
			p := Path("events", hdr().VolumeID)
			rng := rand.New(rand.NewPCG(uint64(n), 2))
			ff := disk.NewFaulty(mem, func() { mem.Crash(rng, mode) }, disk.CrashAt(n, false, nil))
			if f, err := ff.OpenFile(p, disk.Write); err == nil {
				Erase(f, []EraseItem{{locs[1], "b"}})
			}
			if !ff.Down() {
				break
			}
			f, _ := mem.OpenFile(p, disk.Write)
			if err := Erase(f, []EraseItem{{locs[1], "b"}}); err != nil {
				t.Fatalf("mode %v n %d: retry: %v", mode, n, err)
			}
			res, err := Scan(f, nil)
			if err != nil || !res.Sealed || res.Count != 3 {
				t.Fatalf("mode %v n %d: %+v %v", mode, n, res, err)
			}
			if _, err := ReadHead(f, locs[1], "b"); !errors.Is(err, ErrErased) {
				t.Fatalf("mode %v n %d: %v", mode, n, err)
			}
			for _, i := range []int{0, 2} {
				h, err := ReadHead(f, locs[i], []string{"a", "", "c"}[i])
				if err != nil {
					t.Fatal(err)
				}
				if _, err := h.ReadAll(); err != nil {
					t.Fatal(err)
				}
			}
			f.Close()
		}
	}
}
