package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// OS is an FS on a real directory. It never reaches outside its root.
type OS struct{ root string }

// NewOS returns an FS rooted at dir, which must exist.
func NewOS(dir string) (*OS, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("disk root: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("disk root %s: not a directory", dir)
	}
	return &OS{root: dir}, nil
}

func (o *OS) path(name string) (string, error) {
	if err := checkPath(name); err != nil {
		return "", err
	}
	return filepath.Join(o.root, filepath.FromSlash(name)), nil
}

func (o *OS) OpenFile(name string, flag Flag) (File, error) {
	p, err := o.path(name)
	if err != nil {
		return nil, err
	}
	osFlag := os.O_RDONLY
	if flag&(Write|Create) != 0 {
		osFlag = os.O_RDWR
	}
	if flag&Create != 0 {
		osFlag |= os.O_CREATE
	}
	if flag&Excl != 0 {
		osFlag |= os.O_EXCL
	}
	f, err := os.OpenFile(p, osFlag, 0o644)
	if err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err == nil && st.IsDir() {
		f.Close()
		return nil, fmt.Errorf("%s: is a directory", name)
	}
	return osFile{f}, nil
}

func (o *OS) Stat(name string) (Info, error) {
	p, err := o.path(name)
	if err != nil {
		return Info{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: st.Size(), IsDir: st.IsDir()}, nil
}

func (o *OS) ReadDir(dir string) ([]string, error) {
	p, err := o.path(dir)
	if err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names, nil
}

func (o *OS) MkdirAll(dir string) error {
	p, err := o.path(dir)
	if err != nil {
		return err
	}
	return os.MkdirAll(p, 0o755)
}

func (o *OS) Rename(oldName, newName string) error {
	op, err := o.path(oldName)
	if err != nil {
		return err
	}
	np, err := o.path(newName)
	if err != nil {
		return err
	}
	if st, err := os.Stat(op); err == nil && st.IsDir() {
		return fmt.Errorf("rename %s: directories cannot be renamed", oldName)
	}
	return os.Rename(op, np)
}

func (o *OS) Remove(name string) error {
	p, err := o.path(name)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (o *OS) SyncDir(dir string) error {
	p, err := o.path(dir)
	if err != nil {
		return err
	}
	d, err := os.Open(p)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// *os.File's Size is not a method; adapt it.
var _ File = (*osFile)(nil)

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	st, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
