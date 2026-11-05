package format

import (
	"fmt"
	"io"
)

// Next decodes the record at offset off of b (the whole volume file or a
// buffer starting at a record boundary that is a multiple of 8 from the
// file start) and returns it with the offset of the next record. It returns
// io.EOF when off is exactly len(b). Records of unknown or reserved type are
// returned like any other; callers skip them.
func Next(b []byte, off, tagLen int) (Record, int, error) {
	if off < 0 || off > len(b) || off%8 != 0 {
		return Record{}, 0, fmt.Errorf("scan: offset %d: %w", off, ErrInvalid)
	}
	if off == len(b) {
		return Record{}, 0, io.EOF
	}
	r, n, err := DecodeRecord(b[off:], tagLen)
	if err != nil {
		return Record{}, 0, err
	}
	return r, off + n, nil
}

// Resync looks for the next plausible record at 8-byte-aligned offsets >=
// from: magic matches, header_CRC verifies and seq > lastSeq (format-v1
// §5.2). Payload CRC is not checked here; callers decode the candidate.
func Resync(b []byte, from int, lastSeq uint32, tagLen int) (int, bool) {
	if from < 0 {
		from = 0
	}
	for off := roundup8(from); off+RecordHeaderLen <= len(b); off += 8 {
		if b[off] != recordMagic[0] {
			continue
		}
		if h, err := parseHead(b[off:], tagLen, true); err == nil && h.Seq > lastSeq {
			return off, true
		}
	}
	return 0, false
}

// IsSealed applies the definition of format-v1 §6 to the volume bytes b,
// whose first record is at firstRecordOff: the last record of the valid
// prefix is a seal record, its committed_length equals its own start offset
// and the file ends right after it.
func IsSealed(b []byte, firstRecordOff, tagLen int) (Seal, bool) {
	var last Record
	lastOff := -1
	off := firstRecordOff
	for {
		r, next, err := Next(b, off, tagLen)
		if err != nil {
			break
		}
		last, lastOff, off = r, off, next
	}
	if lastOff < 0 || last.Type != TypeSeal || off != len(b) {
		return Seal{}, false
	}
	s, err := DecodeSeal(last)
	if err != nil || s.CommittedLength != uint64(lastOff) {
		return Seal{}, false
	}
	return s, true
}
