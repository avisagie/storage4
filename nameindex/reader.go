package nameindex

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"storage4/disk"
	"storage4/format"
)

// Reader answers fingerprint lookups from a .nidx with positioned reads: one
// bucket per lookup (more only if buckets overflowed). It holds no data; the
// OS page cache does the caching. The file must stay open while it is used.
type Reader struct {
	f disk.File
	h header
	// tableCRC is the footer's claim, checked by Verify.
	tableCRC uint32
}

// OpenReader validates the header, the file size and the footer of f for
// volumeID. It does not read the table; bucket CRCs are checked as buckets
// are read, the table CRC by Verify.
func OpenReader(f disk.File, volumeID uint64) (*Reader, error) {
	size, err := f.Size()
	if err != nil {
		return nil, fmt.Errorf("nidx open: %w", err)
	}
	if size < headerLen+bucketSize+footerLen {
		return nil, bad("short")
	}
	hb := make([]byte, headerLen)
	if err := readAt(f, hb, 0); err != nil {
		return nil, err
	}
	h, err := parseHeader(hb, volumeID)
	if err != nil {
		return nil, err
	}
	if size != h.size() {
		return nil, bad("size")
	}
	fb := make([]byte, footerLen)
	if err := readAt(f, fb, size-footerLen); err != nil {
		return nil, err
	}
	crc, err := parseFooter(fb, h)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f, h: h, tableCRC: crc}, nil
}

// Count returns the number of names in the index.
func (r *Reader) Count() int { return int(r.h.count) }

// Candidates returns the entries whose fingerprint is fp, highest seq first.
// They are candidates only: the caller must verify the record. A damaged
// bucket yields ErrBadNidx; a read error that is not truncation is returned
// as is.
func (r *Reader) Candidates(fp uint64) ([]Entry, error) {
	var out []Entry
	raw := make([]byte, bucketSize)
	b := int64(fp % uint64(r.h.buckets))
	for range r.h.buckets {
		if err := readAt(r.f, raw, headerLen+b*bucketSize); err != nil {
			return nil, err
		}
		n, err := parseBucket(raw)
		if err != nil {
			return nil, err
		}
		for i := range n {
			if e := slot(raw, i); e.FP == fp {
				out = append(out, e)
			}
		}
		if n < slotsPerBucket {
			break
		}
		b = (b + 1) % int64(r.h.buckets)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	return out, nil
}

// Verify reads the whole table and checks the footer's CRC and every bucket.
func (r *Reader) Verify() error {
	const batch = 256
	var crc uint32
	buf := make([]byte, batch*bucketSize)
	for b := int64(0); b < int64(r.h.buckets); b += batch {
		n := min(batch, int64(r.h.buckets)-b)
		p := buf[:n*bucketSize]
		if err := readAt(r.f, p, headerLen+b*bucketSize); err != nil {
			return err
		}
		for i := range int(n) {
			if _, err := parseBucket(p[i*bucketSize : (i+1)*bucketSize]); err != nil {
				return err
			}
		}
		crc = format.CRCUpdate(crc, p)
	}
	if crc != r.tableCRC {
		return bad("table crc")
	}
	return nil
}

// readAt fills p from off. A file that ends early is a bad index; any other
// error is the disk's and is returned unchanged (wrapped).
func readAt(f disk.File, p []byte, off int64) error {
	n, err := f.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || err == nil {
		return bad("truncated")
	}
	return fmt.Errorf("nidx read @%d: %w", off, err)
}
