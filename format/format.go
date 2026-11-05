package format

import (
	"errors"
	"hash/crc32"
)

// Limits from docs/format-v1.md §1. They may be raised in a later format
// version, never lowered.
const (
	MaxNameLen    = 512
	MaxMetaLen    = 16 << 10
	MaxPayloadLen = 100 << 20
)

// Errors returned by decoders; match with errors.Is.
var (
	// ErrShort means the input ends before the structure does.
	ErrShort = errors.New("format: truncated input")
	// ErrBadMagic means the magic bytes do not match.
	ErrBadMagic = errors.New("format: bad magic")
	// ErrCorrupt means a CRC does not verify.
	ErrCorrupt = errors.New("format: checksum mismatch")
	// ErrInvalid means the structure violates the format (bad length, field
	// out of range, nonzero padding or reserved field, ...).
	ErrInvalid = errors.New("format: invalid structure")
	// ErrUnsupported means a well-formed structure that this reader refuses:
	// unknown format_major, cipher_id other than 0, nonzero record flags.
	ErrUnsupported = errors.New("format: unsupported")
)

// castagnoli is the CRC-32C table (RFC 3720). It is immutable after init.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC returns the CRC-32C of b as defined in format-v1 §1.
func CRC(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

func roundup8(n int) int { return (n + 7) &^ 7 }
