package disk

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"syscall"
)

// Flag selects how OpenFile opens a file. The zero value opens an existing
// file read-only.
type Flag int

const (
	// Write opens the file for reading and writing.
	Write Flag = 1 << iota
	// Create creates the file if it does not exist (implies Write).
	Create
	// Excl with Create fails with ErrExist if the file exists.
	Excl
)

// Errors. Use errors.Is; implementations wrap these or the equivalent
// io/fs and syscall errors.
var (
	ErrNotExist = fs.ErrNotExist
	ErrExist    = fs.ErrExist
	ErrNoSpace  = syscall.ENOSPC
	// ErrCrashed is returned by handles and file systems that went through
	// a simulated crash: the process "died", reopen everything.
	ErrCrashed = errors.New("disk: crashed")
	// ErrClosed is returned when using a closed file.
	ErrClosed = fs.ErrClosed
	// ErrInvalidPath is returned for paths that are not fs.ValidPath.
	ErrInvalidPath = errors.New("disk: invalid path")
	// ErrIO is the generic injected I/O error.
	ErrIO = errors.New("disk: I/O error")
)

// Info describes a file or directory.
type Info struct {
	Size  int64
	IsDir bool
}

// FS is one disk's file system. Safe for concurrent use.
type FS interface {
	// OpenFile opens a regular file.
	OpenFile(name string, flag Flag) (File, error)
	// Stat returns information about name.
	Stat(name string) (Info, error)
	// ReadDir returns the names in dir, sorted.
	ReadDir(dir string) ([]string, error)
	// MkdirAll creates dir and its parents. Not durable until SyncDir on
	// the parent.
	MkdirAll(dir string) error
	// Rename atomically renames a file within the FS, replacing an existing
	// target file. Directories cannot be renamed. Not durable until SyncDir
	// on the directories involved.
	Rename(oldName, newName string) error
	// Remove removes a file or an empty directory. Not durable until
	// SyncDir.
	Remove(name string) error
	// SyncDir makes the names in dir (creates, renames, removes) durable.
	SyncDir(dir string) error
}

// File is an open regular file. Safe for concurrent use; concurrent writes
// to overlapping ranges have no defined order.
type File interface {
	// ReadAt follows io.ReaderAt: n < len(p) implies a non-nil error, io.EOF
	// at the end of the file.
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt follows io.WriterAt: n < len(p) implies a non-nil error.
	// Writing past the end extends the file; the gap reads as zeros.
	WriteAt(p []byte, off int64) (int, error)
	// Truncate sets the file size; growth reads as zeros.
	Truncate(size int64) error
	// Size returns the current size.
	Size() (int64, error)
	// Sync makes all completed writes and truncates of this file durable.
	Sync() error
	Close() error
}

func checkPath(name string) error {
	if !fs.ValidPath(name) {
		return fmt.Errorf("%w: %q", ErrInvalidPath, name)
	}
	return nil
}

// WriteFileAtomic publishes data at name so that, after a crash at any
// point, name holds either its previous content (or does not exist) or all
// of data, never a mixture: write name+".tmp", fsync it, rename over name,
// fsync the directory. A leftover ".tmp" file after a crash is harmless.
// The caller must treat any error as "state unknown".
func WriteFileAtomic(fsys FS, name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := fsys.OpenFile(tmp, Create)
	if err != nil {
		return fmt.Errorf("atomic write %s: %w", name, err)
	}
	fail := func(err error) error {
		f.Close()
		return fmt.Errorf("atomic write %s: %w", name, err)
	}
	if err := f.Truncate(0); err != nil {
		return fail(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("atomic write %s: %w", name, err)
	}
	if err := fsys.Rename(tmp, name); err != nil {
		return fmt.Errorf("atomic write %s: %w", name, err)
	}
	if err := fsys.SyncDir(path.Dir(name)); err != nil {
		return fmt.Errorf("atomic write %s: %w", name, err)
	}
	return nil
}

// ReadFile reads a whole file.
func ReadFile(fsys FS, name string) ([]byte, error) {
	f, err := fsys.OpenFile(name, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	n, err := f.ReadAt(b, 0)
	if n == len(b) {
		return b, nil
	}
	return nil, fmt.Errorf("read %s: %w", name, err)
}
