package format

import (
	"errors"
	"testing"
)

func TestCRCVector(t *testing.T) {
	if got := CRC([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("CRC-32C check value = %#x, want 0xe3069283", got)
	}
}

func TestDecodePrelude(t *testing.T) {
	good := AppendPrelude(nil, 72)
	tests := []struct {
		name string
		b    []byte
		want error
	}{
		{"ok", good, nil},
		{"higher minor", appendPrelude(nil, 1, 9, 80), nil},
		{"short", good[:15], ErrShort},
		{"empty", nil, ErrShort},
		{"bad magic", append([]byte{0x00}, good[1:]...), ErrBadMagic},
		{"text mode mangled", append([]byte{0x53}, good[1:]...), ErrBadMagic},
		{"major 2", appendPrelude(nil, 2, 0, 72), ErrUnsupported},
		{"major 0", appendPrelude(nil, 0, 0, 72), ErrUnsupported},
		{"header_len not multiple of 8", appendPrelude(nil, 1, 0, 75), ErrInvalid},
		{"header_len too small", appendPrelude(nil, 1, 0, 64), ErrInvalid},
		{"header_len zero", appendPrelude(nil, 1, 0, 0), ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodePrelude(tc.b)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPreludeBytes(t *testing.T) {
	want := []byte{0x89, 'S', '4', 'V', 'O', 'L', '\r', '\n', 1, 0, 0, 0, 72, 0, 0, 0}
	if got := AppendPrelude(nil, 72); string(got) != string(want) {
		t.Fatalf("prelude = % x, want % x", got, want)
	}
}
