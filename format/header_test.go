package format

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestVolumeHeaderRoundTrip(t *testing.T) {
	for _, bucket := range []string{"a", "events", strings.Repeat("b", 128), "x.y_z-1"} {
		h := fixtureHeader()
		h.Bucket = bucket
		b := mustHeader(t, h)
		if len(b) != PreludeLen+HeaderLen(len(bucket)) || len(b)%8 != 0 {
			t.Fatalf("bucket %q: len %d", bucket, len(b))
		}
		got, off, err := DecodeVolumeHeader(b)
		if err != nil {
			t.Fatalf("bucket %q: %v", bucket, err)
		}
		if got != h || off != len(b) {
			t.Fatalf("bucket %q: got %+v off %d, want %+v off %d", bucket, got, off, h, len(b))
		}
	}
}

func TestVolumeHeaderEncodeRefusals(t *testing.T) {
	tests := map[string]func(*VolumeHeader){
		"empty bucket":      func(h *VolumeHeader) { h.Bucket = "" },
		"bucket dot start":  func(h *VolumeHeader) { h.Bucket = ".x" },
		"bucket too long":   func(h *VolumeHeader) { h.Bucket = strings.Repeat("b", 129) },
		"zero shard count":  func(h *VolumeHeader) { h.ShardCount = 0 },
		"too many shards":   func(h *VolumeHeader) { h.ShardCount = 1025 },
		"shard >= count":    func(h *VolumeHeader) { h.Shard = h.ShardCount },
		"cipher 1 reserved": func(h *VolumeHeader) { h.CipherID = 1 },
	}
	for name, mut := range tests {
		t.Run(name, func(t *testing.T) {
			h := fixtureHeader()
			mut(&h)
			if _, err := AppendVolumeHeader(nil, h); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestVolumeHeaderDecodeRefusals(t *testing.T) {
	// Patch a valid header, re-seal the CRC, expect the structural error.
	tests := []struct {
		name  string
		patch func(b []byte)
		want  error
	}{
		{"shard >= count", func(b []byte) { binary.LittleEndian.PutUint32(b[40:], 16) }, ErrInvalid},
		{"zero shard count", func(b []byte) { binary.LittleEndian.PutUint32(b[44:], 0) }, ErrInvalid},
		{"cipher 1", func(b []byte) { b[72] = 1 }, ErrUnsupported},
		{"cipher 0xffff", func(b []byte) { b[72], b[73] = 0xff, 0xff }, ErrUnsupported},
		{"bucket_len 0", func(b []byte) { b[76], b[77] = 0, 0 }, ErrInvalid},
		{"bucket_len 129", func(b []byte) { b[76], b[77] = 129, 0 }, ErrInvalid},
		{"bucket_len beyond header", func(b []byte) { b[76], b[77] = 100, 0 }, ErrInvalid},
		{"bucket_len mismatch", func(b []byte) { b[76] = 5 }, ErrInvalid}, // header_len no longer matches
		{"reserved nonzero", func(b []byte) { b[78] = 1 }, ErrInvalid},
		{"bad bucket byte", func(b []byte) { b[80] = '/' }, ErrInvalid},
		{"bucket starts with dot", func(b []byte) { b[80] = '.' }, ErrInvalid},
		{"nonzero padding", func(b []byte) { b[len(b)-5] = 1 }, ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := mustHeader(t, fixtureHeader())
			tc.patch(b)
			refixHeaderCRC(b)
			if _, _, err := DecodeVolumeHeader(b); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestVolumeHeaderBadCRCAndTruncation(t *testing.T) {
	good := mustHeader(t, fixtureHeader())
	for i := range good {
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := DecodeVolumeHeader(b); err == nil {
			t.Fatalf("flip in byte %d accepted", i)
		}
	}
	for n := 0; n < len(good); n++ {
		if _, _, err := DecodeVolumeHeader(good[:n]); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
}

// A later minor version may add fields after the bucket name; v1.0 readers
// skip them using header_len.
func TestVolumeHeaderHigherMinor(t *testing.T) {
	h := fixtureHeader()
	b := mustHeader(t, h)
	bl := len(h.Bucket)
	extra := []byte("EXTRAFLD")
	hl := HeaderLen(bl + len(extra))
	nb := appendPrelude(nil, Major, 3, uint32(hl))
	nb = append(nb, b[PreludeLen:bucketOff+bl]...)
	nb = append(nb, extra...)
	for len(nb) < PreludeLen+hl-4 {
		nb = append(nb, 0)
	}
	nb = append(nb, 0, 0, 0, 0)
	nb[78] = 0x7f // reserved bytes may gain meaning in a later minor
	refixHeaderCRC(nb)

	got, off, err := DecodeVolumeHeader(nb)
	if err != nil {
		t.Fatal(err)
	}
	if got != h || off != len(nb) {
		t.Fatalf("got %+v off %d", got, off)
	}
}
