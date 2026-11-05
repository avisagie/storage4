package format

import (
	"encoding/binary"
	"fmt"
)

// AppendErasedHead appends the bytes that start an erasure (format-v1 §7):
// the fixed header with type = erased-file, region A as zeros, and the
// rewritten header_CRC. Everything else about the record is unchanged, so
// the length is HeadLen(tagLen) and it overwrites the record's first
// HeadLen bytes in one contiguous write.
//
// Erasure is driven by volume/, which owns the I/O:
//
//  1. read the first HeadLen bytes (fixed header, region A, header_CRC),
//     verify them with DecodeHead and compare seq, check and name with the
//     locator;
//  2. write AppendErasedHead's output at the record's offset;
//  3. write zeros over [HeadLen, HeadLen+RegionBLen) in chunks, in any order;
//  4. fsync.
//
// After step 2 the record is a valid erased-file record (payload_CRC is
// ignored for that type), so a crash during the long step 3 never makes the
// record invalid and never ends the valid prefix of an open volume. A crash
// inside step 2 can leave an invalid record; the erasure list drives a retry,
// which may start from ParseFixedHeader since the 24-byte header differs only
// in the type byte. The operation is idempotent: h may be a file or an
// erased-file header.
func AppendErasedHead(dst []byte, h FixedHeader, tagLen int) ([]byte, error) {
	if h.Type != TypeFile && h.Type != TypeErased {
		return dst, fmt.Errorf("erase: type %d: %w", h.Type, ErrInvalid)
	}
	if h.Seq == 0 {
		return dst, fmt.Errorf("erase: seq 0: %w", ErrInvalid)
	}
	h.Type = TypeErased
	start := len(dst)
	dst = appendFixedHeader(dst, h)
	dst = append(dst, make([]byte, h.RegionALen(tagLen))...)
	return binary.LittleEndian.AppendUint32(dst, CRC(dst[start:])), nil
}
