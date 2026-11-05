package volume

import (
	"encoding/binary"
	"errors"
	"fmt"

	"storage4/disk"
	"storage4/format"
)

// The seq index (`<volume-id>.idx`) is a rebuildable cache that maps a seq
// to the offset and length of its record in a SEALED volume. Layout and
// rules: docs/volume.md. Nothing is ever served from it without verifying
// the record header (ReadHead).

var idxMagic = [8]byte{0x89, 'S', '4', 'I', 'D', 'X', '\r', '\n'}

const (
	idxVersion   = 1
	idxHeaderLen = 32
	idxFooterLen = 16
	idxEntryLen  = 8
)

var idxFooterMagic = [4]byte{'S', '4', 'I', 'F'}

// ErrBadIdx means an index file is unusable (bad magic, version, volume ID,
// CRC, footer or size). The caller rebuilds it; this is never fatal.
var ErrBadIdx = errors.New("volume: unusable index file")

// ErrNotSealed means an operation that needs a sealed volume got an open one.
var ErrNotSealed = errors.New("volume: not sealed")

// IdxEntry locates one record. The zero value means "no record with this seq".
type IdxEntry struct {
	Off int64  // multiple of 8, > 0 when present
	Len uint32 // total record length including padding
}

// Idx is a decoded seq index of a sealed volume.
type Idx struct {
	VolumeID uint64
	Entries  []IdxEntry // Entries[seq-1]
}

// Lookup returns the entry for seq.
func (x *Idx) Lookup(seq uint32) (IdxEntry, bool) {
	if seq == 0 || int64(seq) > int64(len(x.Entries)) {
		return IdxEntry{}, false
	}
	e := x.Entries[seq-1]
	return e, e.Len != 0
}

// Encode returns the index file bytes.
func (x *Idx) Encode() ([]byte, error) {
	if len(x.Entries) > 1<<32-1 {
		return nil, fmt.Errorf("idx: too many entries")
	}
	le := binary.LittleEndian
	b := make([]byte, idxHeaderLen, idxHeaderLen+idxEntryLen*len(x.Entries)+idxFooterLen)
	copy(b, idxMagic[:])
	le.PutUint16(b[8:], idxVersion)
	le.PutUint32(b[12:], idxHeaderLen)
	le.PutUint64(b[16:], x.VolumeID)
	le.PutUint32(b[24:], uint32(len(x.Entries)))
	le.PutUint32(b[28:], format.CRC(b[:28]))
	var present uint32
	var crc uint32
	for _, e := range x.Entries {
		var raw [idxEntryLen]byte
		if e.Len != 0 {
			if e.Off <= 0 || e.Off%8 != 0 || e.Off/8 > 1<<32-1 {
				return nil, fmt.Errorf("idx: offset %d: %w", e.Off, ErrBadIdx)
			}
			le.PutUint32(raw[0:], uint32(e.Off/8))
			le.PutUint32(raw[4:], e.Len)
			present++
		} else if e.Off != 0 {
			return nil, fmt.Errorf("idx: empty entry with offset: %w", ErrBadIdx)
		}
		crc = format.CRCUpdate(crc, raw[:])
		b = append(b, raw[:]...)
	}
	b = append(b, idxFooterMagic[:]...)
	b = le.AppendUint32(b, present)
	b = le.AppendUint32(b, crc)
	b = le.AppendUint32(b, 0)
	return b, nil
}

// DecodeIdx parses and fully validates index bytes for volume volumeID.
// Any problem is an error wrapping ErrBadIdx. It never panics.
func DecodeIdx(b []byte, volumeID uint64) (*Idx, error) {
	bad := func(what string) (*Idx, error) { return nil, fmt.Errorf("idx: %s: %w", what, ErrBadIdx) }
	le := binary.LittleEndian
	if len(b) < idxHeaderLen+idxFooterLen {
		return bad("short")
	}
	if [8]byte(b[:8]) != idxMagic {
		return bad("magic")
	}
	if le.Uint16(b[8:]) != idxVersion || le.Uint16(b[10:]) != 0 || le.Uint32(b[12:]) != idxHeaderLen {
		return bad("version")
	}
	if format.CRC(b[:28]) != le.Uint32(b[28:]) {
		return bad("header crc")
	}
	if le.Uint64(b[16:]) != volumeID {
		return bad("volume id")
	}
	slots := uint64(le.Uint32(b[24:]))
	if uint64(len(b)) != idxHeaderLen+slots*idxEntryLen+idxFooterLen {
		return bad("size")
	}
	body := b[idxHeaderLen : idxHeaderLen+slots*idxEntryLen]
	foot := b[len(b)-idxFooterLen:]
	if [4]byte(foot[:4]) != idxFooterMagic || le.Uint32(foot[12:]) != 0 ||
		format.CRC(body) != le.Uint32(foot[8:]) {
		return bad("footer")
	}
	x := &Idx{VolumeID: volumeID, Entries: make([]IdxEntry, slots)}
	var present uint32
	for i := range x.Entries {
		off, n := le.Uint32(body[i*8:]), le.Uint32(body[i*8+4:])
		if (off == 0) != (n == 0) {
			return bad("half-empty entry")
		}
		if n != 0 {
			x.Entries[i] = IdxEntry{Off: int64(off) * 8, Len: n}
			present++
		}
	}
	if present != le.Uint32(foot[4:]) {
		return bad("entry count")
	}
	return x, nil
}

// BuildIdx scans a volume file and returns its index. The volume must be
// sealed; an open volume has no .idx (its map lives in memory) and yields
// ErrNotSealed.
func BuildIdx(f disk.File) (*Idx, ScanResult, error) {
	var ents []IdxEntry
	res, err := Scan(f, func(r Rec) error {
		for uint32(len(ents)) < r.Seq {
			ents = append(ents, IdxEntry{})
		}
		ents[r.Seq-1] = IdxEntry{Off: r.Off, Len: r.Len}
		return nil
	})
	if err != nil {
		return nil, res, err
	}
	if !res.Sealed {
		return nil, res, ErrNotSealed
	}
	return &Idx{VolumeID: res.Header.VolumeID, Entries: ents}, res, nil
}

// WriteIdx publishes the index atomically (tmp, fsync, rename, SyncDir).
func WriteIdx(fsys disk.FS, bucket string, x *Idx) error {
	b, err := x.Encode()
	if err != nil {
		return err
	}
	return disk.WriteFileAtomic(fsys, IdxPath(bucket, x.VolumeID), b)
}

// RebuildIdx rebuilds and writes the index of a sealed volume with a full
// pass over the volume file.
func RebuildIdx(fsys disk.FS, bucket string, id uint64) (*Idx, error) {
	f, err := fsys.OpenFile(Path(bucket, id), 0)
	if err != nil {
		return nil, fmt.Errorf("volume idx rebuild: %w", err)
	}
	defer f.Close()
	x, res, err := BuildIdx(f)
	if err != nil {
		return nil, fmt.Errorf("volume idx rebuild: %w", err)
	}
	if res.Header.VolumeID != id || res.Header.Bucket != bucket {
		return nil, fmt.Errorf("volume idx rebuild: header names volume %x in %q, not %x in %q: %w",
			res.Header.VolumeID, res.Header.Bucket, id, bucket, ErrMismatch)
	}
	if err := WriteIdx(fsys, bucket, x); err != nil {
		return nil, fmt.Errorf("volume idx rebuild: %w", err)
	}
	return x, nil
}

// EnsureIdx returns a usable index for a sealed volume: the existing file if
// it decodes, matches the volume ID and agrees with the volume's end (its
// highest entry is the seal record, ending exactly at the file's end);
// otherwise it rebuilds and rewrites it. rebuilt reports which happened.
// A bad or stale index is never an error.
func EnsureIdx(fsys disk.FS, bucket string, id uint64) (x *Idx, rebuilt bool, err error) {
	if b, rerr := disk.ReadFile(fsys, IdxPath(bucket, id)); rerr == nil {
		if x, derr := DecodeIdx(b, id); derr == nil {
			if ok, cerr := idxAgreesWithVolume(fsys, bucket, x); cerr != nil {
				return nil, false, cerr
			} else if ok {
				return x, false, nil
			}
		}
	}
	x, err = RebuildIdx(fsys, bucket, id)
	return x, true, err
}

func idxAgreesWithVolume(fsys disk.FS, bucket string, x *Idx) (bool, error) {
	f, err := fsys.OpenFile(Path(bucket, x.VolumeID), 0)
	if err != nil {
		return false, fmt.Errorf("volume idx check: %w", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return false, fmt.Errorf("volume idx check: %w", err)
	}
	for seq := len(x.Entries); seq >= 1; seq-- {
		e := x.Entries[seq-1]
		if e.Len == 0 {
			continue
		}
		var b [format.RecordHeaderLen]byte
		if readFull(f, b[:], e.Off) != nil {
			return false, nil
		}
		fh, err := format.ParseFixedHeader(b[:])
		return err == nil && fh.Type == format.TypeSeal && fh.Seq == uint32(seq) &&
			uint32(fh.Len(0)) == e.Len && e.Off+int64(e.Len) == size, nil
	}
	return false, nil
}
