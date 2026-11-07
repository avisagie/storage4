package volume

import (
	"fmt"

	"storage4/disk"
	"storage4/format"
)

// Seal ends the volume: it writes the seal record at Committed() and syncs.
// epoch is the volume's final epoch (chosen by the catalogue). The seal
// takes the next seq like any record. A writer that has failed cannot seal
// itself; use SealFile. After success, or after a failed seal attempt, the
// writer is closed.
func (w *Writer) Seal(epoch uint32) (format.Seal, error) {
	if w.sealed {
		return format.Seal{}, ErrSealed
	}
	if w.err != nil {
		return format.Seal{}, fmt.Errorf("%w: %w", ErrFailed, w.err)
	}
	if w.nextSeq == 0 {
		return format.Seal{}, w.fail(ErrFull)
	}
	s := format.Seal{CommittedLength: uint64(w.committed), Epoch: epoch,
		RecordCount: w.records, LastSeq: w.nextSeq - 1}
	rec, err := format.AppendSealRecord(nil, w.nextSeq, w.rnd(), s)
	if err != nil {
		return format.Seal{}, w.fail(err)
	}
	w.nextSeq++
	if _, err := w.f.WriteAt(rec, w.committed); err != nil {
		return format.Seal{}, w.fail(err)
	}
	if err := w.f.Sync(); err != nil {
		return format.Seal{}, w.fail(err)
	}
	w.setIdx(w.nextSeq-1, w.committed, uint32(len(rec)))
	w.committed += int64(len(rec))
	w.sealed = true
	w.hasSeal = true
	f := w.f
	w.f = nil
	return s, f.Close()
}

// Close releases the file without sealing. The volume stays open on disk;
// the owner must seal it (SealFile) before anything else writes to it.
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	f := w.f
	w.f = nil
	w.sealed = true
	return f.Close()
}

// SealFile seals the replica at path at the given committed length. It is
// the only way to seal a replica whose writer is gone or failed: restart,
// handover, replica failure. committed is the minimum acknowledged length
// across the volume's replicas (chosen by the caller); everything beyond it
// is unacknowledged and is discarded.
//
// Steps (a crash anywhere leaves the volume unsealed with the same valid
// prefix, possibly with the seal written but stale tail bytes behind it, or
// sealed; running SealFile again always finishes the job):
//
//  1. scan the prefix [0, committed): it must be valid and end exactly at
//     committed, else ErrShortReplica;
//  2. truncate to committed;
//  3. write the seal record at committed and sync once.
//
// The seal's seq is the prefix's last seq + 1. That may equal the seq of a
// discarded unacknowledged record; this is safe because the seal record is
// never encrypted (no tags, no nonce) and nothing is ever appended to a
// sealed volume, so no AES-GCM nonce can repeat. The "seq never reused"
// invariant is enforced where it matters: Writer never reuses a seq, even
// after a failed append, and a failed Writer stays dead.
//
// Sealing a volume that is already sealed at committed is a no-op returning
// the existing seal; sealed at another length is ErrSealMismatch.
func SealFile(fsys disk.FS, path string, committed int64, epoch uint32, rnd func() uint32) (format.Seal, error) {
	f, err := fsys.OpenFile(path, disk.Write)
	if err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	if full, err := scan(f, size, nil); err == nil && full.Sealed {
		if int64(full.Seal.CommittedLength) != committed {
			return format.Seal{}, fmt.Errorf("volume seal %s: sealed at %d, asked %d: %w",
				path, full.Seal.CommittedLength, committed, ErrSealMismatch)
		}
		// Already sealed here, perhaps by an attempt whose fsync failed:
		// make sure the seal record is durable before reporting success.
		if err := f.Sync(); err != nil {
			return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
		}
		return *full.Seal, nil
	}
	if committed < 0 || committed > size {
		return format.Seal{}, fmt.Errorf("volume seal %s: committed %d, file %d: %w", path, committed, size, ErrShortReplica)
	}
	res, err := scan(f, committed, nil)
	if err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	if res.End != committed || res.Seal != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: valid prefix ends at %d, committed %d: %w",
			path, res.End, committed, ErrShortReplica)
	}
	if res.LastSeq >= 1<<32-1 {
		return format.Seal{}, fmt.Errorf("volume seal %s: seq space exhausted: %w", path, ErrFull)
	}
	if size > committed {
		if err := f.Truncate(committed); err != nil {
			return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
		}
	}
	s := format.Seal{CommittedLength: uint64(committed), Epoch: epoch, RecordCount: res.Count, LastSeq: res.LastSeq}
	rec, err := format.AppendSealRecord(nil, res.LastSeq+1, rnd(), s)
	if err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	if _, err := f.WriteAt(rec, committed); err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return format.Seal{}, fmt.Errorf("volume seal %s: %w", path, err)
	}
	return s, nil
}
