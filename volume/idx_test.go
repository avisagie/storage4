package volume

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"

	"storage4/disk"
)

func sealedVolume(t *testing.T, fsys disk.FS) (*Writer, []Locator) {
	t.Helper()
	w, err := Create(fsys, hdr(), Options{Rand: counter()})
	if err != nil {
		t.Fatal(err)
	}
	l1, err := w.Append([]Entry{entry("a", 10), entry("b", 70000)})
	if err != nil {
		t.Fatal(err)
	}
	l2, err := w.Append([]Entry{entry("c", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Seal(2); err != nil {
		t.Fatal(err)
	}
	return w, append(l1, l2...)
}

func TestIdxFromWriterEqualsRebuild(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, locs := sealedVolume(t, fsys)
	x, err := w.Idx()
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteIdx(fsys, "events", x); err != nil {
		t.Fatal(err)
	}
	y, rebuilt, err := EnsureIdx(fsys, "events", hdr().VolumeID)
	if err != nil || rebuilt || !reflect.DeepEqual(x, y) {
		t.Fatalf("ensure: rebuilt=%v err=%v", rebuilt, err)
	}
	z, err := RebuildIdx(fsys, "events", hdr().VolumeID)
	if err != nil || !reflect.DeepEqual(x, z) {
		t.Fatalf("rebuild differs: %v", err)
	}
	for _, l := range locs {
		if e, ok := x.Lookup(l.Seq); !ok || e.Off != l.Off || e.Len != l.Len {
			t.Fatalf("seq %d: %+v", l.Seq, e)
		}
	}
	if _, ok := x.Lookup(0); ok {
		t.Fatal("seq 0")
	}
	if _, ok := x.Lookup(99); ok {
		t.Fatal("seq 99")
	}
}

func TestIdxGapsAreEmptyEntries(t *testing.T) {
	x := &Idx{VolumeID: 5, Entries: []IdxEntry{{96, 64}, {}, {160, 72}}}
	b, err := x.Encode()
	if err != nil {
		t.Fatal(err)
	}
	y, err := DecodeIdx(b, 5)
	if err != nil || !reflect.DeepEqual(x, y) {
		t.Fatal(err)
	}
	if _, ok := y.Lookup(2); ok {
		t.Fatal("gap present")
	}
}

func TestIdxEveryByteFlipAndTruncationRejected(t *testing.T) {
	x := &Idx{VolumeID: 5, Entries: []IdxEntry{{96, 64}, {}, {160, 72}}}
	b, _ := x.Encode()
	for i := range b {
		c := bytes.Clone(b)
		c[i] ^= 0x01
		if _, err := DecodeIdx(c, 5); !errors.Is(err, ErrBadIdx) {
			t.Fatalf("flip byte %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, err := DecodeIdx(b[:n], 5); !errors.Is(err, ErrBadIdx) {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
	if _, err := DecodeIdx(append(bytes.Clone(b), 0), 5); !errors.Is(err, ErrBadIdx) {
		t.Fatal("trailing byte accepted")
	}
	if _, err := DecodeIdx(b, 6); !errors.Is(err, ErrBadIdx) {
		t.Fatal("wrong volume id accepted")
	}
}

func FuzzDecodeIdx(f *testing.F) {
	x := &Idx{VolumeID: 5, Entries: []IdxEntry{{96, 64}, {}, {160, 72}}}
	b, _ := x.Encode()
	f.Add(b)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		y, err := DecodeIdx(b, 5)
		if err != nil {
			return
		}
		again, err := y.Encode()
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded index does not re-encode identically: %v", err)
		}
	})
}

func TestEnsureIdxRebuildsBadOrStale(t *testing.T) {
	id := hdr().VolumeID
	for name, damage := range map[string]func(fsys *disk.Mem){
		"missing": func(m *disk.Mem) { m.Remove(IdxPath("events", id)) },
		"garbage": func(m *disk.Mem) { disk.WriteFileAtomic(m, IdxPath("events", id), []byte("junk")) },
		"stale: from an older volume state": func(m *disk.Mem) {
			old := &Idx{VolumeID: id, Entries: []IdxEntry{{96, 64}}} // valid file, wrong content
			b, _ := old.Encode()
			disk.WriteFileAtomic(m, IdxPath("events", id), b)
		},
		"other volume's index": func(m *disk.Mem) {
			b, _ := (&Idx{VolumeID: id + 1}).Encode()
			disk.WriteFileAtomic(m, IdxPath("events", id), b)
		},
	} {
		fsys := disk.NewMem(disk.MemOptions{})
		w, _ := sealedVolume(t, fsys)
		good, _ := w.Idx()
		WriteIdx(fsys, "events", good)
		damage(fsys)
		x, rebuilt, err := EnsureIdx(fsys, "events", id)
		if err != nil || !rebuilt || !reflect.DeepEqual(x, good) {
			t.Fatalf("%s: rebuilt=%v err=%v", name, rebuilt, err)
		}
		if _, rebuilt, _ := EnsureIdx(fsys, "events", id); rebuilt {
			t.Fatalf("%s: rewritten index not accepted", name)
		}
	}
}

func TestBuildIdxRefusesOpenVolume(t *testing.T) {
	fsys := disk.NewMem(disk.MemOptions{})
	w, _ := Create(fsys, hdr(), Options{Rand: counter()})
	w.Append([]Entry{entry("a", 1)})
	if _, err := RebuildIdx(fsys, "events", hdr().VolumeID); !errors.Is(err, ErrNotSealed) {
		t.Fatalf("got %v", err)
	}
	if _, err := w.Idx(); !errors.Is(err, ErrNotSealed) {
		t.Fatalf("got %v", err)
	}
}

// WriteIdx is atomic: after a crash at any operation the file is absent or
// complete, and EnsureIdx always ends with a correct index.
func TestCrashSweepWriteIdx(t *testing.T) {
	for _, mode := range []disk.CrashMode{disk.CrashDropAll, disk.CrashAny, disk.CrashKeepAll} {
		for n := 1; ; n++ {
			mem := disk.NewMem(disk.MemOptions{})
			w, _ := sealedVolume(t, mem)
			x, _ := w.Idx()
			rng := rand.New(rand.NewPCG(uint64(n), 3))
			ff := disk.NewFaulty(mem, func() { mem.Crash(rng, mode) }, disk.CrashAt(n, false, nil))
			WriteIdx(ff, "events", x)
			if !ff.Down() {
				break
			}
			if b, err := disk.ReadFile(mem, IdxPath("events", hdr().VolumeID)); err == nil {
				if _, err := DecodeIdx(b, hdr().VolumeID); err != nil {
					t.Fatalf("mode %v n %d: torn index file: %v", mode, n, err)
				}
			}
			y, _, err := EnsureIdx(mem, "events", hdr().VolumeID)
			if err != nil || !reflect.DeepEqual(x, y) {
				t.Fatalf("mode %v n %d: %v", mode, n, err)
			}
		}
	}
}
