package volume

import (
	"fmt"

	"storage4/disk"
	"storage4/format"
	"storage4/names"
)

// MaxVolumeSize is the hard cap on a volume file: .idx entries hold
// offset/8 in a u32 (design-notes §7).
const MaxVolumeSize = 32 << 30

// Options configure a Writer.
type Options struct {
	// MaxSize caps the file size including the seal record; 0 means
	// MaxVolumeSize.
	MaxSize int64
	// Rand returns the per-record check value. Injected; required.
	Rand func() uint32
}

// Entry is one file version to append. Name must be a valid file name.
type Entry struct {
	Name     string
	SHA256   [32]byte
	Metadata []byte
	Payload  []byte
}

// Writer appends to one open volume replica. It is not safe for concurrent
// use: the single writer of an open volume is the invariant (one shard owner
// per open volume), and group commit is done by passing several entries to
// one Append.
//
// Sequence numbers: every record takes the next seq from a counter that only
// moves forward. A seq consumed by an append that then fails is never reused
// (AES-GCM nonce = volume ID ‖ seq). After any failure the Writer refuses
// all further work (ErrFailed); the owner seals the volume with SealFile and
// moves on to a new one.
type Writer struct {
	f         disk.File
	hdr       format.VolumeHeader
	maxSize   int64
	rnd       func() uint32
	committed int64      // end of the last record made durable by a successful Sync
	nextSeq   uint32     // next seq to hand out; 0 means exhausted
	records   uint32     // file and erased-file records committed
	idx       []IdxEntry // by seq-1; only durable records
	err       error
	sealed    bool
	hasSeal   bool // sealed by Seal (not merely closed)
}

// Create makes a new volume file for h on fsys and returns its Writer. The
// file appears atomically with a complete, durable header (written to a
// temporary name, synced, renamed, directory synced), so after a crash the
// path either does not exist or holds a valid empty volume. It fails with
// disk.ErrExist if the volume already exists. The caller registers the
// volume in the catalogue; a leftover empty file from a crash before that is
// harmless.
func Create(fsys disk.FS, h format.VolumeHeader, opts Options) (*Writer, error) {
	if opts.Rand == nil {
		return nil, fmt.Errorf("volume create: no random source")
	}
	hdr, err := format.AppendVolumeHeader(nil, h)
	if err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	p := Path(h.Bucket, h.VolumeID)
	if _, err := fsys.Stat(p); err == nil {
		return nil, fmt.Errorf("volume create %s: %w", p, disk.ErrExist)
	}
	dir := Dir(h.Bucket)
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	// A new bucket directory needs both the root and itself synced.
	if err := fsys.SyncDir("."); err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	if err := fsys.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	if err := disk.WriteFileAtomic(fsys, p, hdr); err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	f, err := fsys.OpenFile(p, disk.Write)
	if err != nil {
		return nil, fmt.Errorf("volume create: %w", err)
	}
	max := opts.MaxSize
	if max <= 0 || max > MaxVolumeSize {
		max = MaxVolumeSize
	}
	return &Writer{f: f, hdr: h, maxSize: max, rnd: opts.Rand,
		committed: int64(len(hdr)), nextSeq: 1}, nil
}

// Header returns the volume header.
func (w *Writer) Header() format.VolumeHeader { return w.hdr }

// Committed is the file offset up to which everything is durable: the
// acknowledged length of this replica.
func (w *Writer) Committed() int64 { return w.committed }

// Err returns the error that disabled the writer, or nil.
func (w *Writer) Err() error { return w.err }

func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return fmt.Errorf("%w: %w", ErrFailed, err)
}

// Append writes all entries as consecutive records and makes them durable
// with one Sync (group commit). On success the returned locators, one per
// entry, are final: the data is on disk. Validation errors and ErrFull
// consume nothing and leave the writer usable. Any I/O error disables the
// writer; seqs handed to the batch are lost for good, and Committed() still
// names the last acknowledged length.
func (w *Writer) Append(entries []Entry) ([]Locator, error) {
	if w.sealed {
		return nil, ErrSealed
	}
	if w.err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFailed, w.err)
	}
	if len(entries) == 0 {
		return nil, nil
	}
	var total int64
	for i := range entries {
		e := &entries[i]
		if err := names.ValidateName(e.Name); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidEntry, err)
		}
		if len(e.Metadata) > format.MaxMetaLen || len(e.Payload) > format.MaxPayloadLen {
			return nil, fmt.Errorf("%w: size over limit", ErrInvalidEntry)
		}
		total += int64(format.RecordLen(len(e.Name), len(e.Metadata), len(e.Payload), 0))
	}
	// Leave room for the seal record (max 64 bytes, rounded up).
	if w.committed+total+sealReserve > w.maxSize ||
		w.nextSeq == 0 || uint64(w.nextSeq)+uint64(len(entries))+1 > 1<<32-1 {
		return nil, ErrFull
	}

	locs := make([]Locator, len(entries))
	var buf []byte
	off := w.committed
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if _, err := w.f.WriteAt(buf, off); err != nil {
			return err
		}
		off += int64(len(buf))
		buf = buf[:0]
		return nil
	}
	for i := range entries {
		e := &entries[i]
		seq := w.nextSeq
		w.nextSeq++ // consumed from here on, whatever happens next
		rec := format.Record{Type: format.TypeFile, Seq: seq, Check: w.rnd(),
			Name: []byte(e.Name), SHA256: e.SHA256, Metadata: e.Metadata}
		start := len(buf)
		var err error
		buf, err = format.AppendRecordHead(buf, rec, len(e.Payload))
		if err != nil {
			return nil, w.fail(err)
		}
		headLen := len(buf) - start
		// buf[0] sits at file offset off, so this record starts at off+start.
		locs[i] = Locator{Seq: seq, Check: rec.Check, Off: off + int64(start),
			Len: uint32(format.RecordLen(len(e.Name), len(e.Metadata), len(e.Payload), 0))}
		crc := format.CRC(e.Payload)
		if len(e.Payload) <= smallPayload {
			buf = append(buf, e.Payload...)
		} else {
			// Big payload: flush the head, then write it straight from the
			// caller's buffer (no copy); only the tail goes back in buf.
			if err := flush(); err != nil {
				return nil, w.fail(err)
			}
			if _, err := w.f.WriteAt(e.Payload, off); err != nil {
				return nil, w.fail(err)
			}
			off += int64(len(e.Payload))
		}
		buf = format.AppendRecordTail(buf, crc, headLen, len(e.Payload))
	}
	if err := flush(); err != nil {
		return nil, w.fail(err)
	}
	if err := w.f.Sync(); err != nil {
		return nil, w.fail(err)
	}
	w.committed = off
	w.records += uint32(len(entries))
	for _, l := range locs {
		w.setIdx(l.Seq, l.Off, l.Len)
	}
	return locs, nil
}

const (
	smallPayload = 64 << 10
	sealReserve  = 64
)

func (w *Writer) setIdx(seq uint32, off int64, n uint32) {
	for uint32(len(w.idx)) < seq {
		w.idx = append(w.idx, IdxEntry{})
	}
	w.idx[seq-1] = IdxEntry{Off: off, Len: n}
}

// Idx returns the seq index of a sealed writer's volume, built from the
// locators it handed out (no re-read). Write it with WriteIdx; it is a cache,
// so a failure to write it is not a failure to seal.
func (w *Writer) Idx() (*Idx, error) {
	if !w.hasSeal {
		return nil, ErrNotSealed
	}
	return &Idx{VolumeID: w.hdr.VolumeID, Entries: w.idx}, nil
}
