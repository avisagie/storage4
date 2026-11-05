package volume

import (
	"errors"
	"fmt"
	"io"

	"storage4/disk"
	"storage4/format"
)

// Locator addresses a record in one volume file. Readers verify the record
// against it (ReadHead); an index entry that no longer matches is stale.
type Locator struct {
	Seq, Check uint32
	Off        int64  // multiple of 8
	Len        uint32 // total record length including padding
}

// Rec is one record seen by Scan. Name aliases a buffer that is reused after
// the callback returns.
type Rec struct {
	Locator
	Type       format.RecordType
	Name       []byte
	MetaLen    int
	PayloadLen int
}

// ScanResult describes the valid prefix of a volume file.
type ScanResult struct {
	Header   format.VolumeHeader
	FirstOff int64 // offset of the first record
	End      int64 // end of the valid prefix
	Size     int64 // file size (or the scan limit)
	Count    uint32
	LastSeq  uint32 // highest seq in the valid prefix, seal included
	Seal     *format.Seal
	Sealed   bool // format-v1 §6: seal is last valid record, at committed_length, file ends there
}

const (
	scanWindow = 64 << 10
	chunk      = 1 << 20
)

// Scan reads the volume file front to back and calls fn (if not nil) for
// every record of the longest valid prefix, in order. A record is valid per
// format-v1 §5.2 and additionally has a seq greater than every earlier one.
// The prefix ends at the first invalid record (docs/disk.md recovery rule)
// or right after the first seal record. Payloads are streamed through the
// CRC in chunks, never held whole. An unreadable header is an error;
// everything after it is data, not error. If fn returns an error Scan stops
// and returns it.
func Scan(f disk.File, fn func(Rec) error) (ScanResult, error) {
	size, err := f.Size()
	if err != nil {
		return ScanResult{}, fmt.Errorf("volume scan: %w", err)
	}
	return scan(f, size, fn)
}

func scan(f disk.File, size int64, fn func(Rec) error) (ScanResult, error) {
	var res ScanResult
	res.Size = size
	w := &window{f: f, size: size}
	hb, err := w.peek(0, format.PreludeLen)
	if err != nil {
		return res, fmt.Errorf("volume scan: header: %w", err)
	}
	p, err := format.DecodePrelude(hb)
	if err != nil {
		return res, fmt.Errorf("volume scan: %w", err)
	}
	hb, err = w.peek(0, format.PreludeLen+int(p.HeaderLen))
	if err != nil {
		return res, fmt.Errorf("volume scan: header: %w", err)
	}
	hdr, first, err := format.DecodeVolumeHeader(hb)
	if err != nil {
		return res, fmt.Errorf("volume scan: %w", err)
	}
	res.Header, res.FirstOff, res.End = hdr, int64(first), int64(first)

	off := int64(first)
	var seals bool
	for off < size && !seals {
		fh, head, ok := w.head(off)
		if !ok {
			break
		}
		if fh.Seq <= res.LastSeq {
			break
		}
		recLen := int64(fh.Len(0))
		if off+recLen > size {
			break
		}
		var seal *format.Seal
		switch {
		case fh.Type == format.TypeSeal:
			if recLen > 256 {
				return res, nil
			}
			whole, err := w.peek(off, int(recLen))
			if err != nil {
				return res, fmt.Errorf("volume scan: %w", err)
			}
			rec, _, err := format.DecodeRecord(whole, 0)
			if err != nil {
				return res, nil
			}
			s, err := format.DecodeSeal(rec)
			if err != nil {
				return res, nil
			}
			seal = &s
		case fh.Type != format.TypeErased:
			if ok, err := verifyPayload(f, off, fh, head); err != nil {
				return res, fmt.Errorf("volume scan: %w", err)
			} else if !ok {
				return res, nil
			}
		}
		loc := Locator{Seq: fh.Seq, Check: fh.Check, Off: off, Len: uint32(recLen)}
		if fn != nil {
			r := Rec{Locator: loc, Type: fh.Type, MetaLen: fh.MetaLen, PayloadLen: fh.PayloadLen}
			r.Name = head[format.RecordHeaderLen : format.RecordHeaderLen+fh.NameLen]
			if err := fn(r); err != nil {
				return res, err
			}
		}
		if fh.Type == format.TypeFile || fh.Type == format.TypeErased {
			res.Count++
		}
		res.LastSeq = fh.Seq
		off += recLen
		res.End = off
		if seal != nil {
			seals = true
			res.Seal = seal
			res.Sealed = seal.CommittedLength == uint64(loc.Off) && off == size
		}
	}
	return res, nil
}

// verifyPayload streams region B and compares payload_CRC. A read error
// other than a short file is an I/O error; a short or corrupt payload
// reports ok=false (end of the valid prefix).
func verifyPayload(f disk.File, off int64, fh format.FixedHeader, head []byte) (bool, error) {
	n := int64(fh.RegionBLen(0))
	pos := off + int64(fh.HeadLen(0))
	buf := make([]byte, min(n+4, chunk))
	var sum uint32
	for left := n; left > 0; {
		c := min(left, int64(len(buf)))
		if err := readFull(f, buf[:c], pos); err != nil {
			return false, ioErr(err)
		}
		sum = format.CRCUpdate(sum, buf[:c])
		pos += c
		left -= c
	}
	if err := readFull(f, buf[:4], pos); err != nil {
		return false, ioErr(err)
	}
	return format.CheckPayloadCRC(sum, buf[:4]) == nil, nil
}

// ioErr maps a clean end-of-file to nil-equivalent (torn data) and keeps
// real I/O errors.
func ioErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil
	}
	return err
}

func readFull(f disk.File, p []byte, off int64) error {
	n, err := f.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// window is a small read-ahead cache for record heads.
type window struct {
	f    disk.File
	size int64
	off  int64
	buf  []byte
}

// peek returns n bytes at off. The slice is valid until the next call.
func (w *window) peek(off int64, n int) ([]byte, error) {
	if off < 0 || off+int64(n) > w.size {
		return nil, io.ErrUnexpectedEOF
	}
	if off >= w.off && off+int64(n) <= w.off+int64(len(w.buf)) {
		return w.buf[off-w.off : off-w.off+int64(n)], nil
	}
	want := max(n, scanWindow)
	if rest := w.size - off; int64(want) > rest {
		want = int(rest)
	}
	if cap(w.buf) < want {
		w.buf = make([]byte, want)
	}
	w.buf = w.buf[:want]
	w.off = off
	if err := readFull(w.f, w.buf, off); err != nil {
		w.buf = w.buf[:0]
		return nil, err
	}
	return w.buf[:n], nil
}

// head parses and verifies the record head at off. ok=false means the
// record is invalid or torn (end of prefix); I/O errors are treated the same
// here and surface again on the next read of that range.
func (w *window) head(off int64) (format.FixedHeader, []byte, bool) {
	b, err := w.peek(off, format.RecordHeaderLen)
	if err != nil {
		return format.FixedHeader{}, nil, false
	}
	fh, err := format.ParseFixedHeader(b)
	if err != nil || off+int64(fh.HeadLen(0)) > w.size {
		return format.FixedHeader{}, nil, false
	}
	hl := fh.HeadLen(0)
	b, err = w.peek(off, hl)
	if err != nil {
		return format.FixedHeader{}, nil, false
	}
	if _, _, _, err := format.DecodeHead(b, 0); err != nil {
		return format.FixedHeader{}, nil, false
	}
	return fh, b, true
}
