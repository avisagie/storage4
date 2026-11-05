package disk

import (
	"fmt"
	"io"
	"math/rand/v2"
	"path"
	"sort"
	"strings"
	"sync"
)

// DefaultSectorSize is the granularity at which Mem tears writes on a
// crash. 512 is the worst case across HDD, SSD and NVMe; every 4 KiB tear
// is also a valid 512-byte tear.
const DefaultSectorSize = 512

// MemOptions configures Mem.
type MemOptions struct {
	// SectorSize is the crash tear granularity (default 512).
	SectorSize int
	// Capacity limits the total size of all files; 0 means unlimited.
	// A write that does not fit is applied as far as it fits and returns
	// ErrNoSpace, like a full disk.
	Capacity int64
}

// CrashMode says which unsynced changes survive Mem.Crash.
type CrashMode int

const (
	// CrashDropAll loses every unsynced change.
	CrashDropAll CrashMode = iota
	// CrashPrefix keeps a random prefix of each file's unsynced changes
	// (in write order, torn at a sector boundary) and a random prefix of
	// the unsynced name operations.
	CrashPrefix
	// CrashAny keeps a random subset of the unsynced changes: sectors of
	// pending writes land independently, so later writes may survive
	// while earlier ones vanish. This is the realistic model and the
	// default for tests.
	CrashAny
	// CrashKeepAll keeps everything (the OS wrote it all back in time).
	CrashKeepAll
)

func (m CrashMode) String() string {
	switch m {
	case CrashDropAll:
		return "drop-all"
	case CrashPrefix:
		return "prefix"
	case CrashAny:
		return "any"
	case CrashKeepAll:
		return "keep-all"
	}
	return fmt.Sprintf("CrashMode(%d)", int(m))
}

// Mem is an in-memory FS with a crash model.
//
// Every file has a live content (what reads see) and a durable content
// (what a crash falls back to) plus the list of writes and truncates since
// the last successful Sync. Names work the same way: the live namespace is
// what lookups see, the durable namespace is what a crash falls back to,
// and name operations stay pending until SyncDir covers their directories.
//
// Crash rolls the disk back to the durable state plus whichever pending
// changes the CrashMode lets through. All open handles are invalidated.
type Mem struct {
	mu     sync.Mutex
	opts   MemOptions
	gen    int
	live   map[string]*memNode
	dur    map[string]*memNode
	nsPend []*nsOp
}

type memNode struct {
	isDir bool
	live  []byte
	dur   []byte
	pend  []wop
}

type wop struct {
	trunc bool
	size  int64 // for truncate
	off   int64 // for write
	data  []byte
}

type nsOp struct {
	kind     int // nsCreate, nsRemove, nsRename
	a, b     string
	node     *memNode
	dirsLeft map[string]bool
}

const (
	nsCreate = iota
	nsRemove
	nsRename
)

// NewMem returns an empty file system containing only the root directory.
func NewMem(opts MemOptions) *Mem {
	if opts.SectorSize <= 0 {
		opts.SectorSize = DefaultSectorSize
	}
	root := &memNode{isDir: true}
	return &Mem{
		opts: opts,
		live: map[string]*memNode{".": root},
		dur:  map[string]*memNode{".": root},
	}
}

func (m *Mem) lookup(name string) (*memNode, error) {
	if err := checkPath(name); err != nil {
		return nil, err
	}
	n, ok := m.live[name]
	if !ok {
		return nil, fmt.Errorf("%s: %w", name, ErrNotExist)
	}
	return n, nil
}

func (m *Mem) parentDir(name string) (string, error) {
	dir := path.Dir(name)
	p, ok := m.live[dir]
	if !ok {
		return "", fmt.Errorf("%s: %w", dir, ErrNotExist)
	}
	if !p.isDir {
		return "", fmt.Errorf("%s: not a directory", dir)
	}
	return dir, nil
}

func (m *Mem) used() int64 {
	var t int64
	for _, n := range m.live {
		t += int64(len(n.live))
	}
	return t
}

func (m *Mem) addNS(op *nsOp, dirs ...string) {
	op.dirsLeft = map[string]bool{}
	for _, d := range dirs {
		op.dirsLeft[d] = true
	}
	m.nsPend = append(m.nsPend, op)
}

// OpenFile implements FS.
func (m *Mem) OpenFile(name string, flag Flag) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkPath(name); err != nil {
		return nil, err
	}
	n, ok := m.live[name]
	if ok {
		if n.isDir {
			return nil, fmt.Errorf("%s: is a directory", name)
		}
		if flag&Create != 0 && flag&Excl != 0 {
			return nil, fmt.Errorf("%s: %w", name, ErrExist)
		}
	} else {
		if flag&Create == 0 {
			return nil, fmt.Errorf("%s: %w", name, ErrNotExist)
		}
		dir, err := m.parentDir(name)
		if err != nil {
			return nil, err
		}
		n = &memNode{}
		m.live[name] = n
		m.addNS(&nsOp{kind: nsCreate, a: name, node: n}, dir)
	}
	return &memFile{m: m, n: n, gen: m.gen, write: flag&(Write|Create) != 0}, nil
}

// Stat implements FS.
func (m *Mem) Stat(name string) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err := m.lookup(name)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: int64(len(n.live)), IsDir: n.isDir}, nil
}

func (m *Mem) children(dir string) []string {
	var names []string
	for p := range m.live {
		if p != "." && path.Dir(p) == dir {
			names = append(names, path.Base(p))
		}
	}
	sort.Strings(names)
	return names
}

// ReadDir implements FS.
func (m *Mem) ReadDir(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err := m.lookup(dir)
	if err != nil {
		return nil, err
	}
	if !n.isDir {
		return nil, fmt.Errorf("%s: not a directory", dir)
	}
	return m.children(dir), nil
}

// MkdirAll implements FS.
func (m *Mem) MkdirAll(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkPath(dir); err != nil {
		return err
	}
	if dir == "." {
		return nil
	}
	parts := strings.Split(dir, "/")
	cur := "."
	for _, p := range parts {
		next := p
		if cur != "." {
			next = cur + "/" + p
		}
		if n, ok := m.live[next]; ok {
			if !n.isDir {
				return fmt.Errorf("%s: not a directory", next)
			}
		} else {
			n := &memNode{isDir: true}
			m.live[next] = n
			m.addNS(&nsOp{kind: nsCreate, a: next, node: n}, cur)
		}
		cur = next
	}
	return nil
}

// Rename implements FS.
func (m *Mem) Rename(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err := m.lookup(oldName)
	if err != nil {
		return err
	}
	if err := checkPath(newName); err != nil {
		return err
	}
	if n.isDir {
		return fmt.Errorf("rename %s: directories cannot be renamed", oldName)
	}
	if t, ok := m.live[newName]; ok && t.isDir {
		return fmt.Errorf("rename to %s: is a directory", newName)
	}
	nd, err := m.parentDir(newName)
	if err != nil {
		return err
	}
	m.live[newName] = n
	if newName != oldName {
		delete(m.live, oldName)
	}
	m.addNS(&nsOp{kind: nsRename, a: oldName, b: newName}, path.Dir(oldName), nd)
	return nil
}

// Remove implements FS.
func (m *Mem) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err := m.lookup(name)
	if err != nil {
		return err
	}
	if name == "." {
		return fmt.Errorf("remove root: %w", ErrInvalidPath)
	}
	if n.isDir && len(m.children(name)) > 0 {
		return fmt.Errorf("remove %s: directory not empty", name)
	}
	delete(m.live, name)
	m.addNS(&nsOp{kind: nsRemove, a: name}, path.Dir(name))
	return nil
}

// SyncDir implements FS: pending name operations in dir become durable.
func (m *Mem) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err := m.lookup(dir)
	if err != nil {
		return err
	}
	if !n.isDir {
		return fmt.Errorf("%s: not a directory", dir)
	}
	// An operation becomes durable once all its directories are synced.
	// Directories are independent of each other, so a ready operation does
	// not wait for an earlier one in an unsynced directory.
	var rest []*nsOp
	for _, op := range m.nsPend {
		delete(op.dirsLeft, dir)
		if len(op.dirsLeft) == 0 {
			m.applyNS(m.dur, op)
		} else {
			rest = append(rest, op)
		}
	}
	m.nsPend = rest
	return nil
}

func (m *Mem) applyNS(ns map[string]*memNode, op *nsOp) {
	switch op.kind {
	case nsCreate:
		ns[op.a] = op.node
	case nsRemove:
		delete(ns, op.a)
	case nsRename:
		if n, ok := ns[op.a]; ok {
			ns[op.b] = n
			if op.a != op.b {
				delete(ns, op.a)
			}
		}
	}
}

// DropUnsynced discards the unsynced changes to name's content, as if the
// kernel gave up on dirty pages after a failed fsync. Used by Faulty.
func (m *Mem) DropUnsynced(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.live[name]; ok && !n.isDir {
		n.live = append([]byte(nil), n.dur...)
		n.pend = nil
	}
}

// Crash simulates power loss followed by a restart. The durable state is
// restored, plus the unsynced changes selected by mode (using rng). All
// handles opened before the crash fail with ErrCrashed. rng may be nil for
// CrashDropAll and CrashKeepAll.
//
// Torn writes: a pending write is split at absolute SectorSize boundaries
// and each piece lands or not independently (CrashAny), as a prefix
// (CrashPrefix), or not at all (CrashDropAll).
func (m *Mem) Crash(rng *rand.Rand, mode CrashMode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gen++

	// Namespace: the unsettled operations in order.
	keep := selectKept(len(m.nsPend), rng, mode)
	for i, op := range m.nsPend {
		if keep[i] {
			m.applyNS(m.dur, op)
		}
	}
	m.nsPend = nil

	// Contents of everything reachable in the durable namespace. Nodes
	// reachable only through dropped names are gone.
	seen := map[*memNode]bool{}
	for _, n := range m.dur {
		if n.isDir || seen[n] {
			continue
		}
		seen[n] = true
		pieces := m.pieces(n)
		keep := selectKept(len(pieces), rng, mode)
		for i, p := range pieces {
			if keep[i] {
				n.dur = applyWop(n.dur, p)
			}
		}
	}
	for _, n := range m.dur {
		n.pend = nil
		n.live = append([]byte(nil), n.dur...)
	}
	m.live = make(map[string]*memNode, len(m.dur))
	for p, n := range m.dur {
		m.live[p] = n
	}
}

// pieces splits a node's pending writes into sector-aligned pieces.
func (m *Mem) pieces(n *memNode) []wop {
	ss := int64(m.opts.SectorSize)
	var out []wop
	for _, w := range n.pend {
		if w.trunc {
			out = append(out, w)
			continue
		}
		off, data := w.off, w.data
		for len(data) > 0 {
			end := (off/ss + 1) * ss
			k := int(end - off)
			if k > len(data) {
				k = len(data)
			}
			out = append(out, wop{off: off, data: data[:k]})
			off += int64(k)
			data = data[k:]
		}
	}
	return out
}

func selectKept(n int, rng *rand.Rand, mode CrashMode) []bool {
	keep := make([]bool, n)
	switch mode {
	case CrashKeepAll:
		for i := range keep {
			keep[i] = true
		}
	case CrashPrefix:
		k := rng.IntN(n + 1)
		for i := 0; i < k; i++ {
			keep[i] = true
		}
	case CrashAny:
		for i := range keep {
			keep[i] = rng.IntN(2) == 0
		}
	}
	return keep
}

func applyWop(b []byte, w wop) []byte {
	if w.trunc {
		return resize(b, w.size)
	}
	end := w.off + int64(len(w.data))
	if end > int64(len(b)) {
		b = resize(b, end)
	}
	copy(b[w.off:], w.data)
	return b
}

func resize(b []byte, size int64) []byte {
	if size <= int64(len(b)) {
		return b[:size]
	}
	nb := make([]byte, size)
	copy(nb, b)
	return nb
}

type memFile struct {
	m      *Mem
	n      *memNode
	gen    int
	write  bool
	closed bool
}

func (f *memFile) check() error {
	if f.closed {
		return ErrClosed
	}
	if f.gen != f.m.gen {
		return ErrCrashed
	}
	return nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %w", ErrInvalidPath)
	}
	if off >= int64(len(f.n.live)) {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, f.n.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if !f.write {
		return 0, fmt.Errorf("file not open for writing")
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %w", ErrInvalidPath)
	}
	var space error
	if c := f.m.opts.Capacity; c > 0 {
		grow := off + int64(len(p)) - int64(len(f.n.live))
		if grow < 0 {
			grow = 0
		}
		if free := c - f.m.used(); grow > free {
			// Fit what fits: bytes inside the file plus the free space.
			fit := int64(len(f.n.live)) + free - off
			if fit < 0 {
				fit = 0
			}
			p = p[:min(int64(len(p)), fit)]
			space = ErrNoSpace
		}
	}
	if len(p) > 0 {
		w := wop{off: off, data: append([]byte(nil), p...)}
		f.n.live = applyWop(f.n.live, w)
		f.n.pend = append(f.n.pend, w)
	}
	return len(p), space
}

func (f *memFile) Truncate(size int64) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if !f.write {
		return fmt.Errorf("file not open for writing")
	}
	if size < 0 {
		return fmt.Errorf("negative size: %w", ErrInvalidPath)
	}
	if c := f.m.opts.Capacity; c > 0 && size > int64(len(f.n.live)) &&
		size-int64(len(f.n.live)) > c-f.m.used() {
		return ErrNoSpace
	}
	w := wop{trunc: true, size: size}
	f.n.live = applyWop(f.n.live, w)
	f.n.pend = append(f.n.pend, w)
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return int64(len(f.n.live)), nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	for _, w := range f.n.pend {
		f.n.dur = applyWop(f.n.dur, w)
	}
	f.n.pend = nil
	return nil
}

func (f *memFile) Close() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	f.closed = true
	return nil
}
