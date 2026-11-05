package format

import (
	"encoding/binary"
	"fmt"

	"storage4/names"
)

// fixedHeaderLen is the size of the fixed part of the header, counted from
// the end of the prelude up to the bucket name (format-v1 §4).
const fixedHeaderLen = 64

// bucketOff is the file offset of the bucket name.
const bucketOff = PreludeLen + fixedHeaderLen

// VolumeHeader is the decoded volume header (format-v1 §4).
type VolumeHeader struct {
	VolumeID        uint64
	ClusterID       [16]byte
	Shard           uint32
	ShardCount      uint32
	VolumeNumber    uint64
	CreatedUnixNano int64 // informational only
	Epoch           uint32
	KeyGeneration   uint32
	CipherID        uint16
	Tier            uint16
	Bucket          string
}

// HeaderLen returns header_len for a bucket name of n bytes.
func HeaderLen(bucketLen int) int { return roundup8(fixedHeaderLen + bucketLen + 4) }

// AppendVolumeHeader appends the prelude and volume header for h. The
// first record starts right after the appended bytes. The encoder refuses
// anything the decoder would refuse.
func AppendVolumeHeader(dst []byte, h VolumeHeader) ([]byte, error) {
	if err := names.ValidateBucket(h.Bucket); err != nil {
		return dst, fmt.Errorf("volume header: %w", err)
	}
	if err := names.ValidateShardCount(h.ShardCount); err != nil {
		return dst, fmt.Errorf("volume header: %w", err)
	}
	if h.Shard >= h.ShardCount {
		return dst, fmt.Errorf("volume header: shard %d >= shard count %d: %w", h.Shard, h.ShardCount, ErrInvalid)
	}
	if h.CipherID != 0 {
		return dst, fmt.Errorf("volume header: cipher %d: %w", h.CipherID, ErrUnsupported)
	}
	hl := HeaderLen(len(h.Bucket))
	start := len(dst)
	dst = AppendPrelude(dst, uint32(hl))
	le := binary.LittleEndian
	dst = le.AppendUint64(dst, h.VolumeID)
	dst = append(dst, h.ClusterID[:]...)
	dst = le.AppendUint32(dst, h.Shard)
	dst = le.AppendUint32(dst, h.ShardCount)
	dst = le.AppendUint64(dst, h.VolumeNumber)
	dst = le.AppendUint64(dst, uint64(h.CreatedUnixNano))
	dst = le.AppendUint32(dst, h.Epoch)
	dst = le.AppendUint32(dst, h.KeyGeneration)
	dst = le.AppendUint16(dst, h.CipherID)
	dst = le.AppendUint16(dst, h.Tier)
	dst = le.AppendUint16(dst, uint16(len(h.Bucket)))
	dst = le.AppendUint16(dst, 0) // reserved
	dst = append(dst, h.Bucket...)
	for len(dst)-start < PreludeLen+hl-4 {
		dst = append(dst, 0)
	}
	return le.AppendUint32(dst, CRC(dst[start:])), nil
}

// DecodeVolumeHeader parses the prelude and volume header at the start of b.
// It returns the header and the file offset of the first record. A header
// with a bad CRC, an invalid bucket, shard >= shard count, zero shard count
// or an unsupported cipher is an error. Bytes added by a later minor version
// are skipped using header_len.
func DecodeVolumeHeader(b []byte) (VolumeHeader, int, error) {
	p, err := DecodePrelude(b)
	if err != nil {
		return VolumeHeader{}, 0, err
	}
	end := PreludeLen + int(p.HeaderLen) // header_len < 2^32 fits int on 64-bit; checked against len(b) next
	if end < PreludeLen || len(b) < end {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: %w", ErrShort)
	}
	le := binary.LittleEndian
	if got, want := CRC(b[:end-4]), le.Uint32(b[end-4:]); got != want {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: %w", ErrCorrupt)
	}
	var h VolumeHeader
	h.VolumeID = le.Uint64(b[16:])
	copy(h.ClusterID[:], b[24:40])
	h.Shard = le.Uint32(b[40:])
	h.ShardCount = le.Uint32(b[44:])
	h.VolumeNumber = le.Uint64(b[48:])
	h.CreatedUnixNano = int64(le.Uint64(b[56:]))
	h.Epoch = le.Uint32(b[64:])
	h.KeyGeneration = le.Uint32(b[68:])
	h.CipherID = le.Uint16(b[72:])
	h.Tier = le.Uint16(b[74:])
	bucketLen := int(le.Uint16(b[76:]))
	reserved := le.Uint16(b[78:])

	if bucketLen < 1 || bucketLen > names.MaxBucketLen || bucketOff+bucketLen+4 > end {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: bucket_len %d: %w", bucketLen, ErrInvalid)
	}
	h.Bucket = string(b[bucketOff : bucketOff+bucketLen])
	if p.Minor == 0 {
		// v1.0 layout is exact: no unknown fields, zero reserved and padding.
		if int(p.HeaderLen) != HeaderLen(bucketLen) || reserved != 0 {
			return VolumeHeader{}, 0, fmt.Errorf("volume header: header_len/reserved: %w", ErrInvalid)
		}
		for _, c := range b[bucketOff+bucketLen : end-4] {
			if c != 0 {
				return VolumeHeader{}, 0, fmt.Errorf("volume header: nonzero padding: %w", ErrInvalid)
			}
		}
	}
	if err := names.ValidateBucket(h.Bucket); err != nil {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: %w: %w", ErrInvalid, err)
	}
	if h.ShardCount == 0 || h.Shard >= h.ShardCount {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: shard %d of %d: %w", h.Shard, h.ShardCount, ErrInvalid)
	}
	if h.CipherID != 0 {
		return VolumeHeader{}, 0, fmt.Errorf("volume header: cipher %d: %w", h.CipherID, ErrUnsupported)
	}
	return h, end, nil
}
