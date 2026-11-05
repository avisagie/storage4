package disk

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
)

func rng(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 0x5eed)) }

// sectors returns n sectors of 512 bytes, sector i filled with byte i+1.
func sectors(n int) []byte {
	b := make([]byte, n*512)
	for i := range b {
		b[i] = byte(i/512 + 1)
	}
	return b
}

// survivors lists which sectors of a file of n filled sectors still hold
// their data (as opposed to zeros / missing).
func survivors(t *testing.T, fsys FS, name string, n int) []bool {
	t.Helper()
	got, err := ReadFile(fsys, name)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]bool, n)
	for i := 0; i < n; i++ {
		lo, hi := i*512, (i+1)*512
		if hi > len(got) {
			continue
		}
		sec := got[lo:hi]
		switch {
		case bytes.Equal(sec, bytes.Repeat([]byte{byte(i + 1)}, 512)):
			out[i] = true
		case bytes.Equal(sec, make([]byte, 512)):
		default:
			t.Fatalf("sector %d is neither written nor zero (not sector-atomic)", i)
		}
	}
	return out
}

func newFileWith(t *testing.T, m *Mem, name string, data []byte, sync bool) File {
	t.Helper()
	f, err := m.OpenFile(name, Create)
	must(t, err)
	_, err = f.WriteAt(data, 0)
	must(t, err)
	if sync {
		must(t, f.Sync())
		must(t, m.SyncDir("."))
	}
	return f
}

func TestCrashDropAllKeepsOnlySynced(t *testing.T) {
	m := NewMem(MemOptions{})
	f := newFileWith(t, m, "a", sectors(2), true)
	_, err := f.WriteAt(bytes.Repeat([]byte{9}, 512), 512) // unsynced overwrite
	must(t, err)
	_, err = f.WriteAt(sectors(1), 2048) // unsynced extension
	must(t, err)
	newFileWith(t, m, "b", []byte("never synced"), false)

	m.Crash(nil, CrashDropAll)

	if got := readAll(t, m, "a"); !bytes.Equal(got, sectors(2)) {
		t.Fatalf("a changed: len %d", len(got))
	}
	if _, err := m.Stat("b"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("unsynced create survived: %v", err)
	}
}

func TestCrashKeepAllKeepsEverything(t *testing.T) {
	m := NewMem(MemOptions{})
	f := newFileWith(t, m, "a", sectors(2), false)
	_, err := f.WriteAt([]byte("zz"), 3)
	must(t, err)
	m.Crash(nil, CrashKeepAll)
	got := readAll(t, m, "a")
	want := sectors(2)
	copy(want[3:], "zz")
	if !bytes.Equal(got, want) {
		t.Fatal("keep-all lost data")
	}
}

func TestCrashInvalidatesHandles(t *testing.T) {
	m := NewMem(MemOptions{})
	f := newFileWith(t, m, "a", []byte("x"), true)
	m.Crash(rng(1), CrashAny)
	if _, err := f.WriteAt([]byte("y"), 0); !errors.Is(err, ErrCrashed) {
		t.Fatal(err)
	}
	if err := f.Sync(); !errors.Is(err, ErrCrashed) {
		t.Fatal(err)
	}
	g, err := m.OpenFile("a", 0)
	must(t, err)
	g.Close()
}

// One synced file, then 6 sequential sector writes without sync.
func crashSectors(t *testing.T, seed uint64, mode CrashMode) []bool {
	m := NewMem(MemOptions{})
	f := newFileWith(t, m, "a", nil, true)
	for i := 0; i < 6; i++ {
		_, err := f.WriteAt(bytes.Repeat([]byte{byte(i + 1)}, 512), int64(i*512))
		must(t, err)
	}
	m.Crash(rng(seed), mode)
	return survivors(t, m, "a", 6)
}

func TestCrashPrefixKeepsAPrefixOnly(t *testing.T) {
	sawPartial := false
	for seed := uint64(0); seed < 200; seed++ {
		s := crashSectors(t, seed, CrashPrefix)
		k := 0
		for k < len(s) && s[k] {
			k++
		}
		for _, alive := range s[k:] {
			if alive {
				t.Fatalf("seed %d: not a prefix: %v", seed, s)
			}
		}
		if k > 0 && k < len(s) {
			sawPartial = true
		}
	}
	if !sawPartial {
		t.Fatal("never saw a partial prefix")
	}
}

func TestCrashAnyCanLoseEarlierKeepLater(t *testing.T) {
	hole := false
	for seed := uint64(0); seed < 200 && !hole; seed++ {
		s := crashSectors(t, seed, CrashAny)
		for i := 1; i < len(s); i++ {
			if s[i] && !s[i-1] {
				hole = true
			}
		}
	}
	if !hole {
		t.Fatal("CrashAny never produced a later write surviving without an earlier one")
	}
}

func TestCrashTearsAtSectorBoundaries(t *testing.T) {
	// One 4-sector write at an unaligned file offset 0; pieces must be
	// sector-atomic (survivors fails the test on a half sector).
	torn := false
	for seed := uint64(0); seed < 100; seed++ {
		m := NewMem(MemOptions{})
		f := newFileWith(t, m, "a", nil, true)
		_, err := f.WriteAt(sectors(4), 0)
		must(t, err)
		m.Crash(rng(seed), CrashAny)
		s := survivors(t, m, "a", 4)
		n := 0
		for _, a := range s {
			if a {
				n++
			}
		}
		if n > 0 && n < 4 {
			torn = true
		}
	}
	if !torn {
		t.Fatal("a single write was never torn")
	}
}

func TestCrashTearUsesAbsoluteSectorBoundaries(t *testing.T) {
	// A 24-byte record header at offset 496 straddles sectors 0 and 1 and
	// can be torn in the middle.
	var split bool
	for seed := uint64(0); seed < 100 && !split; seed++ {
		m := NewMem(MemOptions{})
		f := newFileWith(t, m, "a", nil, true)
		_, err := f.WriteAt(bytes.Repeat([]byte{7}, 24), 496)
		must(t, err)
		m.Crash(rng(seed), CrashAny)
		got, _ := ReadFile(m, "a")
		first := len(got) >= 512 && got[496] == 7
		second := len(got) >= 520 && got[512] == 7
		if first != second && (first || second) {
			split = true
		}
	}
	if !split {
		t.Fatal("straddling write never torn at the sector boundary")
	}
}

func TestCrashSyncedSurvivesEveryMode(t *testing.T) {
	for _, mode := range []CrashMode{CrashDropAll, CrashPrefix, CrashAny, CrashKeepAll} {
		for seed := uint64(0); seed < 50; seed++ {
			m := NewMem(MemOptions{})
			f := newFileWith(t, m, "a", sectors(3), true)
			_, err := f.WriteAt(bytes.Repeat([]byte{0xEE}, 700), 100) // unsynced overwrite
			must(t, err)
			must(t, f.Truncate(1)) // unsynced truncate
			m.Crash(rng(seed), mode)
			got := readAll(t, m, "a")
			// Whatever survived, the file is the synced content with some of
			// the unsynced changes applied: never shorter than 1 byte and
			// never containing bytes from nowhere.
			for i, b := range got {
				ok := b == sectors(3)[min(i, 1535)] || b == 0xEE || b == 0
				if !ok {
					t.Fatalf("%v seed %d: byte %d = %#x", mode, seed, i, b)
				}
			}
		}
	}
}

func TestCrashNamesNeedSyncDir(t *testing.T) {
	// Data synced but directory entry not: the file may vanish.
	lost, kept := false, false
	for seed := uint64(0); seed < 50; seed++ {
		m := NewMem(MemOptions{})
		f, err := m.OpenFile("a", Create)
		must(t, err)
		_, err = f.WriteAt([]byte("x"), 0)
		must(t, err)
		must(t, f.Sync())
		m.Crash(rng(seed), CrashAny)
		if _, err := m.Stat("a"); err != nil {
			lost = true
		} else {
			kept = true
		}
	}
	if !lost || !kept {
		t.Fatalf("lost=%v kept=%v; want both outcomes possible", lost, kept)
	}
	// With SyncDir it always survives.
	m := NewMem(MemOptions{})
	newFileWith(t, m, "a", []byte("x"), true)
	m.Crash(rng(1), CrashDropAll)
	if got := readAll(t, m, "a"); string(got) != "x" {
		t.Fatal("synced file lost")
	}
}

func TestSyncDirIsPerDirectory(t *testing.T) {
	m := NewMem(MemOptions{})
	must(t, m.MkdirAll("A"))
	must(t, m.MkdirAll("B"))
	must(t, m.SyncDir("."))
	f1, _ := m.OpenFile("A/x", Create)
	f2, _ := m.OpenFile("B/y", Create)
	f1.Close()
	f2.Close()
	must(t, m.SyncDir("B")) // only B
	m.Crash(nil, CrashDropAll)
	if _, err := m.Stat("B/y"); err != nil {
		t.Fatalf("B/y lost though B was synced: %v", err)
	}
	if _, err := m.Stat("A/x"); err == nil {
		t.Fatal("A/x survived without SyncDir(A)")
	}
}

func TestCrashRenameReplaceIsAtomicOrNotApplied(t *testing.T) {
	// Replacing via rename without syncing the directory: after a crash the
	// name holds the old or the new content, whole.
	for seed := uint64(0); seed < 100; seed++ {
		m := NewMem(MemOptions{})
		newFileWith(t, m, "a", []byte("old"), true)
		f := newFileWith(t, m, "a.tmp", []byte("new"), false)
		must(t, f.Sync())
		must(t, m.Rename("a.tmp", "a"))
		m.Crash(rng(seed), CrashAny)
		got := readAll(t, m, "a")
		if s := string(got); s != "old" && s != "new" {
			t.Fatalf("seed %d: a = %q", seed, s)
		}
	}
}

func TestCapacityENOSPC(t *testing.T) {
	m := NewMem(MemOptions{Capacity: 1000})
	f, err := m.OpenFile("a", Create)
	must(t, err)
	n, err := f.WriteAt(make([]byte, 600), 0)
	if n != 600 || err != nil {
		t.Fatal(n, err)
	}
	n, err = f.WriteAt(make([]byte, 600), 600) // only 400 fit
	if n != 400 || !errors.Is(err, ErrNoSpace) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if sz, _ := f.Size(); sz != 1000 {
		t.Fatal(sz)
	}
	// Overwrite in place needs no space.
	if n, err := f.WriteAt(make([]byte, 100), 0); n != 100 || err != nil {
		t.Fatal(n, err)
	}
	if err := f.Truncate(2000); !errors.Is(err, ErrNoSpace) {
		t.Fatal(err)
	}
	must(t, f.Truncate(100))
	if n, err := f.WriteAt(make([]byte, 500), 100); n != 500 || err != nil {
		t.Fatal("space not reclaimed", n, err)
	}
}
