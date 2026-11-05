package disk

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"
)

// implementations returns every FS implementation that must satisfy the
// same contract.
func implementations() map[string]func(t *testing.T) FS {
	return map[string]func(t *testing.T) FS{
		"os": func(t *testing.T) FS {
			o, err := NewOS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return o
		},
		"mem": func(t *testing.T) FS { return NewMem(MemOptions{}) },
		"faulty-passthrough": func(t *testing.T) FS {
			return NewFaulty(NewMem(MemOptions{}), func() {}, nil)
		},
	}
}

func TestContract(t *testing.T) {
	for name, mk := range implementations() {
		t.Run(name, func(t *testing.T) {
			for _, tc := range contractTests {
				t.Run(tc.name, func(t *testing.T) { tc.fn(t, mk(t)) })
			}
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, fsys FS, name string) []byte {
	t.Helper()
	b, err := ReadFile(fsys, name)
	must(t, err)
	return b
}

var contractTests = []struct {
	name string
	fn   func(*testing.T, FS)
}{
	{"write-read", func(t *testing.T, fsys FS) {
		f, err := fsys.OpenFile("a", Create)
		must(t, err)
		n, err := f.WriteAt([]byte("hello"), 0)
		if n != 5 || err != nil {
			t.Fatal(n, err)
		}
		_, err = f.WriteAt([]byte("HE"), 0)
		must(t, err)
		must(t, f.Close())
		if got := readAll(t, fsys, "a"); string(got) != "HEllo" {
			t.Fatalf("got %q", got)
		}
	}},
	{"readat-eof", func(t *testing.T, fsys FS) {
		must(t, WriteFileAtomic(fsys, "a", []byte("abcd")))
		f, err := fsys.OpenFile("a", 0)
		must(t, err)
		defer f.Close()
		p := make([]byte, 8)
		n, err := f.ReadAt(p, 2)
		if n != 2 || err != io.EOF || string(p[:n]) != "cd" {
			t.Fatalf("n=%d err=%v p=%q", n, err, p[:n])
		}
		n, err = f.ReadAt(p, 4)
		if n != 0 || err != io.EOF {
			t.Fatalf("at end: n=%d err=%v", n, err)
		}
		n, err = f.ReadAt(p[:4], 0)
		if n != 4 || err != nil {
			t.Fatalf("exact: n=%d err=%v", n, err)
		}
	}},
	{"sparse-write-zero-fills", func(t *testing.T, fsys FS) {
		f, err := fsys.OpenFile("a", Create)
		must(t, err)
		_, err = f.WriteAt([]byte("x"), 10)
		must(t, err)
		f.Close()
		want := append(make([]byte, 10), 'x')
		if got := readAll(t, fsys, "a"); !bytes.Equal(got, want) {
			t.Fatalf("got %v", got)
		}
	}},
	{"truncate", func(t *testing.T, fsys FS) {
		f, err := fsys.OpenFile("a", Create)
		must(t, err)
		defer f.Close()
		_, err = f.WriteAt([]byte("abcdef"), 0)
		must(t, err)
		must(t, f.Truncate(3))
		if sz, _ := f.Size(); sz != 3 {
			t.Fatal(sz)
		}
		must(t, f.Truncate(5))
		got := readAll(t, fsys, "a")
		if !bytes.Equal(got, []byte{'a', 'b', 'c', 0, 0}) {
			t.Fatalf("got %v", got)
		}
	}},
	{"open-missing", func(t *testing.T, fsys FS) {
		_, err := fsys.OpenFile("nope", 0)
		if !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
		_, err = fsys.OpenFile("nodir/x", Create)
		if !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
		_, err = fsys.Stat("nope")
		if !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
	}},
	{"excl", func(t *testing.T, fsys FS) {
		f, err := fsys.OpenFile("a", Create|Excl)
		must(t, err)
		f.Close()
		_, err = fsys.OpenFile("a", Create|Excl)
		if !errors.Is(err, ErrExist) {
			t.Fatal(err)
		}
		f, err = fsys.OpenFile("a", Create) // plain Create opens it
		must(t, err)
		f.Close()
	}},
	{"readonly-handle-cannot-write", func(t *testing.T, fsys FS) {
		must(t, WriteFileAtomic(fsys, "a", []byte("x")))
		f, err := fsys.OpenFile("a", 0)
		must(t, err)
		defer f.Close()
		if _, err := f.WriteAt([]byte("y"), 0); err == nil {
			t.Fatal("write on read-only handle succeeded")
		}
		if err := f.Truncate(0); err == nil {
			t.Fatal("truncate on read-only handle succeeded")
		}
	}},
	{"closed-handle", func(t *testing.T, fsys FS) {
		f, err := fsys.OpenFile("a", Create)
		must(t, err)
		must(t, f.Close())
		if _, err := f.WriteAt([]byte("x"), 0); err == nil {
			t.Fatal("write after close succeeded")
		}
	}},
	{"dirs", func(t *testing.T, fsys FS) {
		must(t, fsys.MkdirAll("d/e/f"))
		must(t, fsys.MkdirAll("d/e/f")) // idempotent
		info, err := fsys.Stat("d/e")
		if err != nil || !info.IsDir {
			t.Fatal(info, err)
		}
		must(t, WriteFileAtomic(fsys, "d/b", []byte("1")))
		must(t, WriteFileAtomic(fsys, "d/a", []byte("1")))
		names, err := fsys.ReadDir("d")
		must(t, err)
		if want := []string{"a", "b", "e"}; !slices.Equal(names, want) {
			t.Fatalf("got %v", names)
		}
		if _, err := fsys.OpenFile("d", 0); err == nil {
			t.Fatal("opened a directory as a file")
		}
		if err := fsys.Remove("d"); err == nil {
			t.Fatal("removed a non-empty directory")
		}
		must(t, fsys.SyncDir("d"))
		root, err := fsys.ReadDir(".")
		must(t, err)
		if !slices.Contains(root, "d") {
			t.Fatal(root)
		}
	}},
	{"rename", func(t *testing.T, fsys FS) {
		must(t, WriteFileAtomic(fsys, "a", []byte("new")))
		must(t, WriteFileAtomic(fsys, "b", []byte("old")))
		must(t, fsys.Rename("a", "b"))
		if got := readAll(t, fsys, "b"); string(got) != "new" {
			t.Fatalf("got %q", got)
		}
		if _, err := fsys.Stat("a"); !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
		must(t, fsys.MkdirAll("d"))
		if err := fsys.Rename("d", "e"); err == nil {
			t.Fatal("renamed a directory")
		}
		if err := fsys.Rename("nope", "x"); !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
	}},
	{"remove", func(t *testing.T, fsys FS) {
		must(t, WriteFileAtomic(fsys, "a", []byte("x")))
		must(t, fsys.Remove("a"))
		if err := fsys.Remove("a"); !errors.Is(err, ErrNotExist) {
			t.Fatal(err)
		}
		must(t, fsys.MkdirAll("d"))
		must(t, fsys.Remove("d"))
	}},
	{"invalid-paths", func(t *testing.T, fsys FS) {
		for _, p := range []string{"", "/abs", "../x", "a/../b", "a//b", "a/", "./a"} {
			if _, err := fsys.OpenFile(p, Create); !errors.Is(err, ErrInvalidPath) {
				t.Errorf("OpenFile(%q): %v", p, err)
			}
			if _, err := fsys.Stat(p); !errors.Is(err, ErrInvalidPath) {
				t.Errorf("Stat(%q): %v", p, err)
			}
		}
	}},
	{"atomic-write-replaces", func(t *testing.T, fsys FS) {
		must(t, WriteFileAtomic(fsys, "a", []byte("long content")))
		must(t, WriteFileAtomic(fsys, "a", []byte("short")))
		if got := readAll(t, fsys, "a"); string(got) != "short" {
			t.Fatalf("got %q", got)
		}
	}},
}
