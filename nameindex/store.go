package nameindex

import (
	"errors"
	"fmt"
	"time"

	"storage4/disk"
	"storage4/format"
	"storage4/volume"
)

// ErrNotFound means the volume holds no (non-erased) record for the name.
var ErrNotFound = errors.New("nameindex: not found")

// Options configure a Store. Zero values get defaults.
type Options struct {
	MaxHandles  int           // open files kept (index and volume files); default 64
	IdleTimeout time.Duration // CloseIdle closes handles unused this long; default 1 min
	Clock       Clock         // required
	// Fingerprint replaces Fingerprint (tests inject collisions; later an HMAC).
	Fingerprint func(name []byte) uint64
}

// Store looks names up in the sealed volumes of one disk and keeps their
// .nidx files fresh. Safe for concurrent use.
type Store struct {
	fsys disk.FS
	fp   func([]byte) uint64
	pool *pool
}

// NewStore returns a Store for one disk.
func NewStore(fsys disk.FS, o Options) *Store {
	if o.MaxHandles <= 0 {
		o.MaxHandles = 64
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = time.Minute
	}
	if o.Fingerprint == nil {
		o.Fingerprint = Fingerprint
	}
	return &Store{fsys: fsys, fp: o.Fingerprint, pool: newPool(fsys, o.MaxHandles, o.IdleTimeout, o.Clock)}
}

// CloseIdle closes handles that have been unused longer than IdleTimeout.
// The owner calls it from its ticker.
func (s *Store) CloseIdle() { s.pool.closeIdle() }

// Close closes all cached handles; handles in use close when released.
func (s *Store) Close() { s.pool.closeAll() }

// OpenHandles reports how many files are cached (for metrics and tests).
func (s *Store) OpenHandles() int { return s.pool.open() }

// Hit is a verified record: the header matched the index entry and the name.
// Release it when done reading the payload.
type Hit struct {
	Head *volume.Head
	Loc  volume.Locator
	s    *Store
	h    *handle
}

// Release returns the volume file to the cache. Head is unusable afterwards.
func (h *Hit) Release() {
	if h.h != nil {
		h.s.pool.release(h.h)
		h.h = nil
	}
}

// Lookup finds the latest version of name in the sealed volume volumeId of bucket.
// It returns ErrNotFound if the volume has none. An unusable or stale index
// is rebuilt from the volume once, then the lookup is retried; the caller
// never sees that except as latency. Erased records are skipped (the erasure
// list is the node's concern).
func (s *Store) Lookup(bucket string, volumeId uint64, name string) (*Hit, error) {
	rebuilt := false
	for {
		hit, err := s.lookup(bucket, volumeId, name)
		if err == nil || errors.Is(err, ErrNotFound) {
			return hit, err
		}
		if rebuilt || !s.isStale(err) {
			return nil, err
		}
		if err := s.Rebuild(bucket, volumeId); err != nil {
			return nil, err
		}
		rebuilt = true
	}
}

// isStale reports whether err means "the index cannot be used, rebuild".
func (s *Store) isStale(err error) bool {
	return errors.Is(err, ErrBadNidx) || errors.Is(err, disk.ErrNotExist) && !errors.Is(err, errVolumeMissing) ||
		errors.Is(err, volume.ErrMismatch) && !errors.Is(err, volume.ErrNameMismatch)
}

// errVolumeMissing marks a missing .vol, which a rebuild cannot fix.
var errVolumeMissing = errors.New("nameindex: volume file missing")

func (s *Store) lookup(bucket string, volumeId uint64, name string) (*Hit, error) {
	nh, err := s.pool.acquire(volume.NidxPath(bucket, volumeId), func(f disk.File) (any, error) {
		return OpenReader(f, volumeId)
	})
	if err != nil {
		return nil, err
	}
	defer s.pool.release(nh)
	cands, err := nh.val.(*Reader).Candidates(s.fp([]byte(name)))
	if err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, ErrNotFound
	}
	vh, err := s.pool.acquire(volume.Path(bucket, volumeId), nil)
	if err != nil {
		if errors.Is(err, disk.ErrNotExist) {
			err = fmt.Errorf("%w: %w", errVolumeMissing, err)
		}
		return nil, err
	}
	for _, c := range cands {
		head, err := volume.ReadHeadAt(vh.f, c.Off, c.Seq, name)
		switch {
		case err == nil:
			return &Hit{Head: head, Loc: head.Locator(), s: s, h: vh}, nil
		case errors.Is(err, volume.ErrNameMismatch), errors.Is(err, volume.ErrErased):
			continue // fingerprint collision, or erased: not an answer
		default:
			s.pool.release(vh)
			return nil, err
		}
	}
	s.pool.release(vh)
	return nil, ErrNotFound
}

// Write publishes a .nidx for a sealed volume atomically (tmp, fsync, rename,
// SyncDir). b usually comes from OpenMap.Builder at seal time. Failure does
// not fail the seal: the index is rebuilt on demand.
func (s *Store) Write(bucket string, volumeId uint64, b *Builder) error {
	data, err := b.Encode(volumeId)
	if err != nil {
		return err
	}
	return s.writeBytes(bucket, volumeId, data)
}

func (s *Store) writeBytes(bucket string, volumeId uint64, data []byte) error {
	path := volume.NidxPath(bucket, volumeId)
	s.pool.invalidate(path)
	if err := disk.WriteFileAtomic(s.fsys, path, data); err != nil {
		return fmt.Errorf("nidx write: %w", err)
	}
	return nil
}

// Rebuild rebuilds the .nidx of a sealed volume with a full pass over the
// volume file and writes it. Erased records have no name and are omitted.
func (s *Store) Rebuild(bucket string, volumeId uint64) error {
	f, err := s.fsys.OpenFile(volume.Path(bucket, volumeId), 0)
	if err != nil {
		return fmt.Errorf("nidx rebuild: %w", err)
	}
	defer f.Close()
	b := newBuilder(s.fp)
	res, err := volume.Scan(f, func(r volume.Rec) error {
		if r.Type != format.TypeFile {
			return nil
		}
		return b.Add(r.Name, r.Seq, r.Off)
	})
	if err != nil {
		return fmt.Errorf("nidx rebuild: %w", err)
	}
	if res.Header.VolumeID != volumeId || res.Header.Bucket != bucket {
		return fmt.Errorf("nidx rebuild: header names volume %x in %q, not %x in %q: %w",
			res.Header.VolumeID, res.Header.Bucket, volumeId, bucket, volume.ErrMismatch)
	}
	if !res.Sealed {
		return fmt.Errorf("nidx rebuild: %w", volume.ErrNotSealed)
	}
	data, err := b.Encode(volumeId)
	if err != nil {
		return err
	}
	return s.writeBytes(bucket, volumeId, data)
}

// Ensure checks the cheap things startup checks (design-notes §7 rule 2):
// the .nidx exists and its header, size and footer are valid for the
// volume. Otherwise it rebuilds. A bad index is never an error.
func (s *Store) Ensure(bucket string, volumeId uint64) (rebuilt bool, err error) {
	f, err := s.fsys.OpenFile(volume.NidxPath(bucket, volumeId), 0)
	if err == nil {
		_, err = OpenReader(f, volumeId)
		f.Close()
		if err == nil {
			return false, nil
		}
	}
	if err != nil && !errors.Is(err, ErrBadNidx) && !errors.Is(err, disk.ErrNotExist) {
		return false, fmt.Errorf("nidx ensure: %w", err)
	}
	return true, s.Rebuild(bucket, volumeId)
}
