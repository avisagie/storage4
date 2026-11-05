package disk

import (
	"errors"
	"fmt"
	"testing"
)

func newFaultyMem(seed uint64, mode CrashMode) (*Faulty, *Mem) {
	m := NewMem(MemOptions{})
	r := rng(seed)
	return NewFaulty(m, func() { m.Crash(r, mode) }, nil), m
}

func onKind(k OpKind) func(Op) bool { return func(op Op) bool { return op.Kind == k } }

func TestFaultyShortWrite(t *testing.T) {
	f, m := newFaultyMem(1, CrashAny)
	file, err := f.OpenFile("a", Create)
	must(t, err)
	f.SetInjector(func(op Op) Decision {
		if op.Kind == OpWrite {
			return Decision{Err: ErrIO, Short: 3}
		}
		return Decision{}
	})
	n, err := file.WriteAt([]byte("abcdef"), 0)
	if n != 3 || !errors.Is(err, ErrIO) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := readAll(t, m, "a"); string(got) != "abc" {
		t.Fatalf("got %q", got)
	}
}

func TestFaultyENOSPC(t *testing.T) {
	f, _ := newFaultyMem(1, CrashAny)
	file, err := f.OpenFile("a", Create)
	must(t, err)
	f.SetInjector(func(op Op) Decision {
		if op.Kind == OpWrite {
			return Decision{Err: ErrNoSpace}
		}
		return Decision{}
	})
	n, err := file.WriteAt([]byte("abcdef"), 0)
	if n != 0 || !errors.Is(err, ErrNoSpace) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestFaultySyncErrorCanDropData(t *testing.T) {
	f, m := newFaultyMem(1, CrashAny)
	file, err := f.OpenFile("a", Create)
	must(t, err)
	_, err = file.WriteAt([]byte("durable"), 0)
	must(t, err)
	must(t, file.Sync())
	_, err = file.WriteAt([]byte("LOST!!!"), 0)
	must(t, err)
	f.SetInjector(func(op Op) Decision {
		if op.Kind == OpSync {
			return Decision{Err: ErrIO, DropUnsynced: true}
		}
		return Decision{}
	})
	if err := file.Sync(); !errors.Is(err, ErrIO) {
		t.Fatal(err)
	}
	// Even without a crash, the dirty data is gone: a retried fsync that
	// "succeeds" would be a lie. This is why callers must not retry.
	f.SetInjector(nil)
	must(t, file.Sync())
	if got := readAll(t, m, "a"); string(got) != "durable" {
		t.Fatalf("got %q", got)
	}
}

func TestFaultyCrashPoints(t *testing.T) {
	for _, before := range []bool{true, false} {
		f, m := newFaultyMem(1, CrashKeepAll)
		file, err := f.OpenFile("a", Create) // op 1
		must(t, err)
		f.SetInjector(CrashAt(2, before, onKind(OpWrite)))
		_, err = file.WriteAt([]byte("1"), 0) // write 1: passes
		must(t, err)
		_, err = file.WriteAt([]byte("22"), 1) // write 2: crashes
		if !errors.Is(err, ErrCrashed) {
			t.Fatal(err)
		}
		if !f.Down() {
			t.Fatal("not down")
		}
		if _, err := f.Stat("a"); !errors.Is(err, ErrCrashed) {
			t.Fatal("op ran while down:", err)
		}
		f.Restart()
		// KeepAll: the creation was never synced, so keep-all keeps it too.
		got := readAll(t, m, "a")
		want := "122"
		if before {
			want = "1"
		}
		if string(got) != want {
			t.Fatalf("before=%v: got %q want %q", before, got, want)
		}
		if _, err := file.WriteAt([]byte("x"), 0); !errors.Is(err, ErrCrashed) {
			t.Fatal("old handle usable after crash:", err)
		}
	}
}

// atomicWriteSweep crashes WriteFileAtomic at every operation, with every
// crash mode and many seeds, and checks the all-or-nothing property.
func TestWriteFileAtomicCrashSweep(t *testing.T) {
	old, neu := []byte("old content"), sectors(5) // new spans several sectors
	// Count ops in an uninterrupted run.
	probe, m := newFaultyMem(0, CrashAny)
	must(t, WriteFileAtomic(m, "a", old))
	base := probe.Ops()
	must(t, WriteFileAtomic(probe, "a", neu))
	total := probe.Ops() - base

	for _, mode := range []CrashMode{CrashDropAll, CrashPrefix, CrashAny, CrashKeepAll} {
		for n := 1; n <= total; n++ {
			for _, before := range []bool{true, false} {
				for seed := uint64(0); seed < 20; seed++ {
					t.Run(fmt.Sprintf("%v/op%d/before=%v/seed%d", mode, n, before, seed), func(t *testing.T) {
						f, m := newFaultyMem(seed, mode)
						must(t, WriteFileAtomic(m, "a", old))
						f.SetInjector(CrashAt(n, before, nil))
						err := WriteFileAtomic(f, "a", neu)
						if !errors.Is(err, ErrCrashed) {
							t.Fatalf("expected crash, got %v", err)
						}
						f.Restart()
						got, err := ReadFile(m, "a")
						if err != nil {
							t.Fatalf("file lost: %v", err)
						}
						if string(got) != string(old) && string(got) != string(neu) {
							t.Fatalf("mixed content: %d bytes", len(got))
						}
					})
				}
			}
		}
	}
}
