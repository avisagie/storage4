package format

import (
	"encoding/binary"
	"fmt"
)

// PreludeLen is the fixed size of the prelude (format-v1 §3).
const PreludeLen = 16

// Format version written by this package.
const (
	Major = 1
	Minor = 0
)

// minHeaderLen is the smallest legal header_len: roundup8(64 + 1 + 4).
const minHeaderLen = 72

// preludeMagic is "\x89 S4VOL \r\n".
var preludeMagic = [8]byte{0x89, 0x53, 0x34, 0x56, 0x4F, 0x4C, 0x0D, 0x0A}

// Prelude is the version-independent start of every volume file.
type Prelude struct {
	Major, Minor uint16
	HeaderLen    uint32 // bytes after the prelude up to the first record
}

// AppendPrelude appends a prelude for this package's version.
func AppendPrelude(dst []byte, headerLen uint32) []byte {
	return appendPrelude(dst, Major, Minor, headerLen)
}

func appendPrelude(dst []byte, major, minor uint16, headerLen uint32) []byte {
	dst = append(dst, preludeMagic[:]...)
	dst = binary.LittleEndian.AppendUint16(dst, major)
	dst = binary.LittleEndian.AppendUint16(dst, minor)
	return binary.LittleEndian.AppendUint32(dst, headerLen)
}

// DecodePrelude parses the first 16 bytes of b. It refuses an unknown major
// version; a higher minor of a known major is accepted.
func DecodePrelude(b []byte) (Prelude, error) {
	if len(b) < PreludeLen {
		return Prelude{}, fmt.Errorf("prelude: %w", ErrShort)
	}
	if [8]byte(b[:8]) != preludeMagic {
		return Prelude{}, fmt.Errorf("prelude: %w", ErrBadMagic)
	}
	p := Prelude{
		Major:     binary.LittleEndian.Uint16(b[8:]),
		Minor:     binary.LittleEndian.Uint16(b[10:]),
		HeaderLen: binary.LittleEndian.Uint32(b[12:]),
	}
	if p.Major != Major {
		return Prelude{}, fmt.Errorf("prelude: format_major %d: %w", p.Major, ErrUnsupported)
	}
	if p.HeaderLen%8 != 0 || p.HeaderLen < minHeaderLen {
		return Prelude{}, fmt.Errorf("prelude: header_len %d: %w", p.HeaderLen, ErrInvalid)
	}
	return p, nil
}
