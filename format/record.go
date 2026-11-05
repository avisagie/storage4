package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"storage4/names"
)

// RecordHeaderLen is the size of the fixed record header (format-v1 §5).
const RecordHeaderLen = 24

const recordMagic = "S4R1"

// RecordType is the record's type byte (format-v1 §5.1).
type RecordType uint8

// Record types. Readers skip other values by length.
const (
	TypeInvalid        RecordType = 0
	TypeFile           RecordType = 1
	TypeErased         RecordType = 2
	TypeSeal           RecordType = 3
	TypeMetadataFooter RecordType = 4 // reserved
	TypeChunk          RecordType = 5 // reserved
)

// Record is a decoded record. Decoded slices alias the input buffer. For
// cipher 0, Payload is the plaintext; with a cipher it is region B as stored.
type Record struct {
	Type     RecordType
	Seq      uint32
	Check    uint32
	Name     []byte
	SHA256   [32]byte // zero for types other than TypeFile
	Metadata []byte
	Payload  []byte
}

// TagLen returns the size of tag_A and tag_B for a cipher ID.
func TagLen(cipherID uint16) (int, error) {
	if cipherID == 0 {
		return 0, nil
	}
	return 0, fmt.Errorf("cipher %d: %w", cipherID, ErrUnsupported)
}

// effTag is the tag size that applies to a record of type t: seal records
// carry no tags whatever the cipher (format-v1 §6).
func effTag(t RecordType, tagLen int) int {
	if t == TypeSeal {
		return 0
	}
	return tagLen
}

// RecordLen returns the total on-disk length of a record, including padding.
// All lengths are plaintext lengths.
func RecordLen(nameLen, metaLen, payloadLen, tagLen int) int {
	a := nameLen + 32 + metaLen + tagLen
	b := payloadLen + tagLen
	return roundup8(RecordHeaderLen + a + 4 + b + 4)
}

// FixedHeader holds the fields of the 24-byte record header. It is the
// unverified view: ParseFixedHeader checks magic, flags and limits but not
// header_CRC, so a caller that has read only 24 bytes can learn the record's
// geometry (where region B is, how long the record is).
type FixedHeader struct {
	Type                         RecordType
	Seq, Check                   uint32
	NameLen, MetaLen, PayloadLen int // plaintext lengths
}

// RegionALen is the size of region A as stored (name, sha256, metadata, tag).
func (h FixedHeader) RegionALen(tagLen int) int {
	return h.NameLen + 32 + h.MetaLen + effTag(h.Type, tagLen)
}

// RegionBLen is the size of region B as stored (payload, tag), without its CRC.
func (h FixedHeader) RegionBLen(tagLen int) int { return h.PayloadLen + effTag(h.Type, tagLen) }

// HeadLen is the offset of region B within the record: header, region A and
// header_CRC.
func (h FixedHeader) HeadLen(tagLen int) int { return RecordHeaderLen + h.RegionALen(tagLen) + 4 }

// Len is the total record length including payload_CRC and padding.
func (h FixedHeader) Len(tagLen int) int {
	return roundup8(h.HeadLen(tagLen) + h.RegionBLen(tagLen) + 4)
}

// ParseFixedHeader parses the first 24 bytes of a record: magic, flags and
// the length limits of format-v1 §1. It does not verify header_CRC and does
// not reject type 0 or seq 0; use DecodeHead for a verified header.
func ParseFixedHeader(b []byte) (FixedHeader, error) {
	if len(b) < RecordHeaderLen {
		return FixedHeader{}, fmt.Errorf("record header: %w", ErrShort)
	}
	if string(b[:4]) != recordMagic {
		return FixedHeader{}, fmt.Errorf("record header: %w", ErrBadMagic)
	}
	if b[5] != 0 {
		return FixedHeader{}, fmt.Errorf("record header: flags %#x: %w", b[5], ErrUnsupported)
	}
	le := binary.LittleEndian
	nameLen := le.Uint16(b[6:])
	metaLen := le.Uint32(b[16:])
	payloadLen := le.Uint32(b[20:])
	if nameLen > MaxNameLen || metaLen > MaxMetaLen || payloadLen > MaxPayloadLen {
		return FixedHeader{}, fmt.Errorf("record header: length over limit: %w", ErrInvalid)
	}
	return FixedHeader{
		Type: RecordType(b[4]), Seq: le.Uint32(b[8:]), Check: le.Uint32(b[12:]),
		NameLen: int(nameLen), MetaLen: int(metaLen), PayloadLen: int(payloadLen),
	}, nil
}

func appendFixedHeader(dst []byte, h FixedHeader) []byte {
	le := binary.LittleEndian
	dst = append(dst, recordMagic...)
	dst = append(dst, byte(h.Type), 0)
	dst = le.AppendUint16(dst, uint16(h.NameLen))
	dst = le.AppendUint32(dst, h.Seq)
	dst = le.AppendUint32(dst, h.Check)
	dst = le.AppendUint32(dst, uint32(h.MetaLen))
	return le.AppendUint32(dst, uint32(h.PayloadLen))
}

// AppendRecordHead appends the fixed header, region A and header_CRC of r
// (cipher 0) for a payload of payloadLen bytes; r.Payload is ignored. The
// writer then appends the payload itself (no copy), followed by
// AppendRecordTail. sha256 sits in region A, so the payload must be fully
// known, and hashed, before the head is written.
func AppendRecordHead(dst []byte, r Record, payloadLen int) ([]byte, error) {
	switch {
	case r.Type == TypeInvalid:
		return dst, fmt.Errorf("record: type 0: %w", ErrInvalid)
	case r.Seq == 0:
		return dst, fmt.Errorf("record: seq 0: %w", ErrInvalid)
	case len(r.Name) > MaxNameLen, len(r.Metadata) > MaxMetaLen, payloadLen < 0, payloadLen > MaxPayloadLen:
		return dst, fmt.Errorf("record: length over limit: %w", ErrInvalid)
	}
	if r.Type == TypeFile {
		if err := names.ValidateName(string(r.Name)); err != nil {
			return dst, fmt.Errorf("record: %w", err)
		}
	}
	start := len(dst)
	dst = appendFixedHeader(dst, FixedHeader{
		Type: r.Type, Seq: r.Seq, Check: r.Check,
		NameLen: len(r.Name), MetaLen: len(r.Metadata), PayloadLen: payloadLen,
	})
	dst = append(dst, r.Name...)
	dst = append(dst, r.SHA256[:]...)
	dst = append(dst, r.Metadata...)
	return binary.LittleEndian.AppendUint32(dst, CRC(dst[start:])), nil
}

// AppendRecordTail appends payload_CRC and the zero padding that complete a
// record. headLen is the length of what AppendRecordHead appended;
// payloadCRC is CRC-32C of the payload (build it with CRCUpdate while
// streaming).
func AppendRecordTail(dst []byte, payloadCRC uint32, headLen, payloadLen int) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, payloadCRC)
	for n := headLen + payloadLen + 4; n%8 != 0; n++ {
		dst = append(dst, 0)
	}
	return dst
}

// AppendRecord appends r encoded with cipher 0 as head, payload and tail.
// It validates the name for TypeFile and every limit of format-v1 §1.
func AppendRecord(dst []byte, r Record) ([]byte, error) {
	start := len(dst)
	dst, err := AppendRecordHead(dst, r, len(r.Payload))
	if err != nil {
		return dst[:start], err
	}
	headLen := len(dst) - start
	dst = append(dst, r.Payload...)
	return AppendRecordTail(dst, CRC(r.Payload), headLen, len(r.Payload)), nil
}

// CRCUpdate extends a CRC-32C over more bytes: CRCUpdate(0, b) == CRC(b) and
// CRCUpdate(CRCUpdate(0, x), y) == CRC(x ‖ y). Streaming readers and writers
// use it so a 100 MiB payload never needs to sit in memory for its CRC.
func CRCUpdate(crc uint32, b []byte) uint32 { return crc32.Update(crc, castagnoli, b) }

// CheckPayloadCRC compares a CRC computed while streaming region B with the
// stored payload_CRC in trailer (the 4 bytes after region B).
func CheckPayloadCRC(sum uint32, trailer []byte) error {
	if len(trailer) < 4 {
		return fmt.Errorf("record payload: %w", ErrShort)
	}
	if sum != binary.LittleEndian.Uint32(trailer) {
		return fmt.Errorf("record payload: %w", ErrCorrupt)
	}
	return nil
}

// parseHead is ParseFixedHeader plus: region A and header_CRC must lie in b
// and, if verify is set, header_CRC and the type/seq sanity rules must hold.
// It does not look at region B.
func parseHead(b []byte, tagLen int, verify bool) (FixedHeader, error) {
	h, err := ParseFixedHeader(b)
	if err != nil {
		return FixedHeader{}, err
	}
	headLen := h.HeadLen(tagLen)
	if len(b) < headLen {
		return FixedHeader{}, fmt.Errorf("record header: %w", ErrShort)
	}
	if !verify {
		return h, nil
	}
	if CRC(b[:headLen-4]) != binary.LittleEndian.Uint32(b[headLen-4:]) {
		return FixedHeader{}, fmt.Errorf("record header: %w", ErrCorrupt)
	}
	if h.Type == TypeInvalid || h.Seq == 0 {
		return FixedHeader{}, fmt.Errorf("record header: type %d seq %d: %w", h.Type, h.Seq, ErrInvalid)
	}
	return h, nil
}

// DecodeHead verifies and decodes the fixed header and region A of the
// record at the start of b: magic, flags, limits and header_CRC. It returns
// the record without Payload, the plaintext payload length, and the number
// of bytes consumed (the offset of region B). A reader that has fetched only
// this prefix can check seq, check and name against its index entry before
// reading region B (see DecodePayload).
func DecodeHead(b []byte, tagLen int) (r Record, payloadLen, headLen int, err error) {
	h, err := parseHead(b, tagLen, true)
	if err != nil {
		return Record{}, 0, 0, err
	}
	r = Record{Type: h.Type, Seq: h.Seq, Check: h.Check}
	off := RecordHeaderLen
	r.Name = b[off : off+h.NameLen : off+h.NameLen]
	off += h.NameLen
	copy(r.SHA256[:], b[off:off+32])
	off += 32
	r.Metadata = b[off : off+h.MetaLen : off+h.MetaLen]
	return r, h.PayloadLen, h.HeadLen(tagLen), nil
}

// DecodePayload verifies payload_CRC and returns region B as stored. b must
// start at region B and hold at least payloadLen+tag+4 bytes. It must not be
// used for erased-file records, whose payload_CRC is stale by design.
func DecodePayload(typ RecordType, b []byte, payloadLen, tagLen int) ([]byte, error) {
	if payloadLen < 0 || payloadLen > MaxPayloadLen {
		return nil, fmt.Errorf("record payload: length %d: %w", payloadLen, ErrInvalid)
	}
	n := payloadLen + effTag(typ, tagLen)
	if len(b) < n+4 {
		return nil, fmt.Errorf("record payload: %w", ErrShort)
	}
	if CRC(b[:n]) != binary.LittleEndian.Uint32(b[n:]) {
		return nil, fmt.Errorf("record payload: %w", ErrCorrupt)
	}
	return b[:n:n], nil
}

// DecodeRecord decodes the record at the start of b and returns it with its
// total length including padding. A record is valid iff it lies wholly in b,
// magic and flags are right, header_CRC verifies and, unless it is an
// erased-file record, payload_CRC verifies. Padding is not checked.
func DecodeRecord(b []byte, tagLen int) (Record, int, error) {
	r, payloadLen, headLen, err := DecodeHead(b, tagLen)
	if err != nil {
		return Record{}, 0, err
	}
	tl := effTag(r.Type, tagLen)
	n := roundup8(headLen + payloadLen + tl + 4)
	if len(b) < n {
		return Record{}, 0, fmt.Errorf("record: %w", ErrShort)
	}
	if r.Type == TypeErased {
		end := headLen + payloadLen + tl
		r.Payload = b[headLen:end:end]
		return r, n, nil
	}
	r.Payload, err = DecodePayload(r.Type, b[headLen:], payloadLen, tagLen)
	if err != nil {
		return Record{}, 0, err
	}
	return r, n, nil
}
