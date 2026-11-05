package disk

import (
	"fmt"
	"sync"
)

// OpKind identifies a file system operation for fault injection.
type OpKind int

const (
	OpOpen OpKind = iota
	OpStat
	OpReadDir
	OpMkdir
	OpRename
	OpRemove
	OpSyncDir
	OpRead
	OpWrite
	OpTruncate
	OpSync
)

var opNames = [...]string{"open", "stat", "readdir", "mkdir", "rename", "remove", "syncdir", "read", "write", "truncate", "sync"}

func (k OpKind) String() string {
	if int(k) < len(opNames) {
		return opNames[k]
	}
	return fmt.Sprintf("OpKind(%d)", int(k))
}

// Mutating reports whether the operation changes disk state.
func (k OpKind) Mutating() bool {
	switch k {
	case OpMkdir, OpRename, OpRemove, OpSyncDir, OpWrite, OpTruncate, OpSync:
		return true
	}
	return false
}

// Op describes one call about to run.
type Op struct {
	Kind OpKind
	Path string // for rename: the old name
	Off  int64  // reads and writes
	Len  int    // reads and writes
	// Seq counts every operation through this Faulty, from 1, across
	// restarts. It is deterministic for a deterministic workload, so a
	// crash point can be named by Seq.
	Seq int
}

// Decision says what Faulty does with an operation. The zero value lets
// it through.
type Decision struct {
	// Err fails the operation with this error. For a write, Short bytes
	// are written first (a short write); Short is clamped to the length.
	Err   error
	Short int
	// DropUnsynced, on a failed OpSync, also discards the file's unsynced
	// data (the kernel dropped the dirty pages). Needs a Mem underneath.
	DropUnsynced bool
	// CrashBefore crashes the disk instead of running the operation.
	CrashBefore bool
	// CrashAfter runs the operation, then crashes the disk. The caller
	// still sees ErrCrashed, never the result.
	CrashAfter bool
}

// Injector decides per operation. It is called with Faulty's lock held, so
// it must not call back into the FS.
type Injector func(Op) Decision

// CrashAt crashes before (or after) the nth operation that satisfies
// match; match may be nil for "any operation". It sweeps crash points
// over a workload: run once to count operations, then once per n.
func CrashAt(n int, before bool, match func(Op) bool) Injector {
	count := 0
	return func(op Op) Decision {
		if match != nil && !match(op) {
			return Decision{}
		}
		count++
		if count != n {
			return Decision{}
		}
		return Decision{CrashBefore: before, CrashAfter: !before}
	}
}

// Faulty wraps an FS and injects the faults an Injector asks for.
//
// A crash decision calls the crash function given to NewFaulty (normally
// Mem.Crash with a seeded rng) and switches Faulty to the down state: all
// operations return ErrCrashed until Restart, which models the process
// starting again on the surviving disk.
type Faulty struct {
	inner FS
	crash func()

	mu     sync.Mutex
	inject Injector
	seq    int
	down   bool
}

// NewFaulty wraps inner. crash is called when an Injector decides to crash;
// it should apply the crash model to the underlying disk. inject may be nil
// and replaced later with SetInjector.
func NewFaulty(inner FS, crash func(), inject Injector) *Faulty {
	return &Faulty{inner: inner, crash: crash, inject: inject}
}

// SetInjector replaces the injector.
func (f *Faulty) SetInjector(inject Injector) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inject = inject
}

// Restart brings a crashed Faulty back up. Handles from before the crash
// stay invalid.
func (f *Faulty) Restart() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = false
}

// Down reports whether a crash has happened and Restart has not.
func (f *Faulty) Down() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.down
}

// Ops returns the number of operations seen so far.
func (f *Faulty) Ops() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seq
}

// run applies the injector, then runs do unless the decision stops it.
func (f *Faulty) run(op Op, do func(Decision) (int, error)) (int, error) {
	f.mu.Lock()
	if f.down {
		f.mu.Unlock()
		return 0, ErrCrashed
	}
	f.seq++
	op.Seq = f.seq
	var d Decision
	if f.inject != nil {
		d = f.inject(op)
	}
	if d.CrashBefore {
		f.down = true
		f.mu.Unlock()
		f.crash()
		return 0, ErrCrashed
	}
	f.mu.Unlock()

	n, err := do(d)

	if d.CrashAfter {
		f.mu.Lock()
		f.down = true
		f.mu.Unlock()
		f.crash()
		return 0, ErrCrashed
	}
	return n, err
}

func (f *Faulty) simple(op Op, do func() error) error {
	_, err := f.run(op, func(d Decision) (int, error) {
		if d.Err != nil {
			return 0, d.Err
		}
		return 0, do()
	})
	return err
}

func (f *Faulty) OpenFile(name string, flag Flag) (File, error) {
	var file File
	err := f.simple(Op{Kind: OpOpen, Path: name}, func() (err error) {
		file, err = f.inner.OpenFile(name, flag)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &faultyFile{f: f, in: file, name: name}, nil
}

func (f *Faulty) Stat(name string) (info Info, err error) {
	err = f.simple(Op{Kind: OpStat, Path: name}, func() (err error) {
		info, err = f.inner.Stat(name)
		return err
	})
	return info, err
}

func (f *Faulty) ReadDir(dir string) (names []string, err error) {
	err = f.simple(Op{Kind: OpReadDir, Path: dir}, func() (err error) {
		names, err = f.inner.ReadDir(dir)
		return err
	})
	return names, err
}

func (f *Faulty) MkdirAll(dir string) error {
	return f.simple(Op{Kind: OpMkdir, Path: dir}, func() error { return f.inner.MkdirAll(dir) })
}

func (f *Faulty) Rename(oldName, newName string) error {
	return f.simple(Op{Kind: OpRename, Path: oldName}, func() error { return f.inner.Rename(oldName, newName) })
}

func (f *Faulty) Remove(name string) error {
	return f.simple(Op{Kind: OpRemove, Path: name}, func() error { return f.inner.Remove(name) })
}

func (f *Faulty) SyncDir(dir string) error {
	return f.simple(Op{Kind: OpSyncDir, Path: dir}, func() error { return f.inner.SyncDir(dir) })
}

type faultyFile struct {
	f    *Faulty
	in   File
	name string
}

func (ff *faultyFile) ReadAt(p []byte, off int64) (int, error) {
	return ff.f.run(Op{Kind: OpRead, Path: ff.name, Off: off, Len: len(p)}, func(d Decision) (int, error) {
		if d.Err != nil {
			return 0, d.Err
		}
		return ff.in.ReadAt(p, off)
	})
}

func (ff *faultyFile) WriteAt(p []byte, off int64) (int, error) {
	return ff.f.run(Op{Kind: OpWrite, Path: ff.name, Off: off, Len: len(p)}, func(d Decision) (int, error) {
		if d.Err == nil {
			return ff.in.WriteAt(p, off)
		}
		short := max(0, min(d.Short, len(p)))
		if short == 0 {
			return 0, d.Err
		}
		n, err := ff.in.WriteAt(p[:short], off)
		if err != nil {
			return n, err
		}
		return n, d.Err
	})
}

func (ff *faultyFile) Truncate(size int64) error {
	return ff.f.simple(Op{Kind: OpTruncate, Path: ff.name, Off: size}, func() error { return ff.in.Truncate(size) })
}

func (ff *faultyFile) Size() (int64, error) {
	// Size is metadata of the open handle; not an injection point.
	return ff.in.Size()
}

func (ff *faultyFile) Sync() error {
	_, err := ff.f.run(Op{Kind: OpSync, Path: ff.name}, func(d Decision) (int, error) {
		if d.Err != nil {
			if d.DropUnsynced {
				if l, ok := ff.f.inner.(interface{ DropUnsynced(string) }); ok {
					l.DropUnsynced(ff.name)
				}
			}
			return 0, d.Err
		}
		return 0, ff.in.Sync()
	})
	return err
}

func (ff *faultyFile) Close() error { return ff.in.Close() }
