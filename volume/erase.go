package volume

import (
	"bytes"
	"fmt"

	"storage4/disk"
	"storage4/format"
)

// EraseItem names a record to erase.
type EraseItem struct {
	Loc  Locator
	Name string
}

// Erase erases records in place (format-v1 §7), one fsync for the whole
// batch. It is conservative: for every item it first reads and verifies the
// record head (header_CRC, seq, check, length and name against the locator)
// and only then writes; nothing is written for an item that does not verify.
//
// Per record: one contiguous write of the erased head (type erased-file,
// region A zeros, new header_CRC), then zeros over region B in chunks.
// payload_CRC is left as is. Items are processed in order; on the first
// error Erase returns without syncing, and the caller retries the whole
// list later (erasure is idempotent, and the erasure list is the source of
// truth).
//
// An item whose record is already erased (or whose head write was torn by
// a crash: header_CRC bad, but magic, seq and check intact, the retry case of
// format-v1 §7) is finished without a name check; the name was zeroed, so
// the 24-byte header and the locator are all there is to compare.
//
// Erase is meant for sealed volumes. On an open volume a crash inside the
// head write can end the valid prefix at that record.
func Erase(f disk.File, items []EraseItem) error {
	zeros := make([]byte, chunk)
	for _, it := range items {
		if err := eraseOne(f, it, zeros); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("volume erase: sync: %w", err)
	}
	return nil
}

func eraseOne(f disk.File, it EraseItem, zeros []byte) error {
	loc := it.Loc
	var fixed [format.RecordHeaderLen]byte
	if err := readFull(f, fixed[:], loc.Off); err != nil {
		return fmt.Errorf("volume erase @%d: %w", loc.Off, err)
	}
	fh, err := format.ParseFixedHeader(fixed[:])
	if err != nil || fh.Seq != loc.Seq || fh.Check != loc.Check || uint32(fh.Len(0)) != loc.Len {
		return fmt.Errorf("volume erase @%d: %w", loc.Off, ErrMismatch)
	}
	if fh.Type != format.TypeFile && fh.Type != format.TypeErased {
		return fmt.Errorf("volume erase @%d: type %d: %w", loc.Off, fh.Type, ErrNotFileRecord)
	}
	head := make([]byte, fh.HeadLen(0))
	if err := readFull(f, head, loc.Off); err != nil {
		return fmt.Errorf("volume erase @%d: %w", loc.Off, err)
	}
	if r, _, _, err := format.DecodeHead(head, 0); err == nil && r.Type == format.TypeFile {
		if !bytes.Equal(r.Name, []byte(it.Name)) {
			return fmt.Errorf("volume erase @%d: name: %w", loc.Off, ErrMismatch)
		}
	}
	// Otherwise: already erased, or a torn head write being retried.
	eh, err := format.AppendErasedHead(nil, fh, 0)
	if err != nil {
		return fmt.Errorf("volume erase @%d: %w", loc.Off, err)
	}
	if _, err := f.WriteAt(eh, loc.Off); err != nil {
		return fmt.Errorf("volume erase @%d: %w", loc.Off, err)
	}
	pos, end := loc.Off+int64(len(eh)), loc.Off+int64(len(eh))+int64(fh.RegionBLen(0))
	for pos < end {
		n := min(int64(len(zeros)), end-pos)
		if _, err := f.WriteAt(zeros[:n], pos); err != nil {
			return fmt.Errorf("volume erase @%d: %w", loc.Off, err)
		}
		pos += n
	}
	return nil
}
