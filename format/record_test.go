package format

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	long := strings.Repeat("a/", 255) + "z" // 511 bytes
	tests := []struct {
		name string
		r    Record
	}{
		{"file", fileRecord("a/b.txt", 1, 1, "", "hello")},
		{"file with metadata", fileRecord("a", 7, 0xffffffff, `{"x":[1,2]}`, "payload")},
		{"empty payload", fileRecord("empty", 2, 3, "", "")},
		{"max-ish name", fileRecord(long, 9, 0, "", "x")},
		{"binary metadata", fileRecord("m", 3, 3, "\x00\xff\x80", "x")},
		{"unknown type", Record{Type: 9, Seq: 1, Check: 2, Payload: []byte("future")}},
		{"reserved type", Record{Type: TypeChunk, Seq: 1, Name: []byte("n"), Payload: []byte("c")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Encode at an odd dst offset to check padding is relative to the record.
			enc := mustAppendRecord(t, []byte("xyz"), tc.r)[3:]
			if want := RecordLen(len(tc.r.Name), len(tc.r.Metadata), len(tc.r.Payload), 0); len(enc) != want || len(enc)%8 != 0 {
				t.Fatalf("len = %d, want %d", len(enc), want)
			}
			got, n, err := DecodeRecord(enc, 0)
			if err != nil || n != len(enc) {
				t.Fatalf("decode: n=%d err=%v", n, err)
			}
			if got.Type != tc.r.Type || got.Seq != tc.r.Seq || got.Check != tc.r.Check ||
				!bytes.Equal(got.Name, tc.r.Name) || got.SHA256 != tc.r.SHA256 ||
				!bytes.Equal(got.Metadata, tc.r.Metadata) || !bytes.Equal(got.Payload, tc.r.Payload) {
				t.Fatalf("round trip mismatch: %+v vs %+v", got, tc.r)
			}
			// Trailing bytes after the record are ignored.
			if _, n2, err := DecodeRecord(append(enc[:len(enc):len(enc)], 0xEE, 0xEE), 0); err != nil || n2 != len(enc) {
				t.Fatalf("with trailing bytes: n=%d err=%v", n2, err)
			}
		})
	}
}

func TestRecordLayout(t *testing.T) {
	enc := mustAppendRecord(t, nil, fileRecord("ab", 5, 0xAABBCCDD, "M", "PAY"))
	// 24 header + (2 name + 32 sha + 1 meta) + 4 crc + 3 payload + 4 crc = 70 -> 72
	if len(enc) != 72 {
		t.Fatalf("len = %d, want 72", len(enc))
	}
	want := []byte{'S', '4', 'R', '1', 1, 0, 2, 0, 5, 0, 0, 0, 0xDD, 0xCC, 0xBB, 0xAA, 1, 0, 0, 0, 3, 0, 0, 0, 'a', 'b'}
	if !bytes.Equal(enc[:26], want) {
		t.Fatalf("header = % x, want % x", enc[:26], want)
	}
	if enc[24+2+32] != 'M' || string(enc[24+2+32+1+4:][:3]) != "PAY" || enc[70] != 0 || enc[71] != 0 {
		t.Fatalf("regions misplaced: % x", enc)
	}
}

func TestAppendRecordRefusals(t *testing.T) {
	ok := fileRecord("a", 1, 0, "", "x")
	tests := map[string]func(*Record){
		"type 0":           func(r *Record) { r.Type = TypeInvalid },
		"seq 0":            func(r *Record) { r.Seq = 0 },
		"empty name":       func(r *Record) { r.Name = nil },
		"dot segment":      func(r *Record) { r.Name = []byte("a/../b") },
		"leading slash":    func(r *Record) { r.Name = []byte("/a") },
		"name too long":    func(r *Record) { r.Name = bytes.Repeat([]byte("a"), 513) },
		"metadata too big": func(r *Record) { r.Metadata = make([]byte, MaxMetaLen+1) },
	}
	for name, mut := range tests {
		t.Run(name, func(t *testing.T) {
			r := ok
			mut(&r)
			if _, err := AppendRecord(nil, r); err == nil {
				t.Fatal("want error")
			}
		})
	}
	big := ok
	big.Metadata = make([]byte, MaxMetaLen) // exactly at the limit is fine
	if _, err := AppendRecord(nil, big); err != nil {
		t.Fatalf("metadata at limit: %v", err)
	}
}

// Every byte outside the zero padding is covered by a CRC or is a CRC.
func TestRecordEveryByteFlipDetected(t *testing.T) {
	r := fileRecord("a/b", 3, 9, "meta", "payload!")
	enc := mustAppendRecord(t, nil, r)
	used := RecordHeaderLen + len(r.Name) + 32 + len(r.Metadata) + 4 + len(r.Payload) + 4
	if used == len(enc) {
		t.Fatal("test wants a record with padding")
	}
	for i := range enc {
		b := append([]byte(nil), enc...)
		b[i] ^= 0x01
		_, _, err := DecodeRecord(b, 0)
		if i < used && err == nil {
			t.Errorf("flip in byte %d (of %d used) accepted", i, used)
		}
		if i >= used && err != nil {
			t.Errorf("flip in padding byte %d rejected: %v", i, err)
		}
	}
}

func TestDecodeRecordErrors(t *testing.T) {
	enc := mustAppendRecord(t, nil, fileRecord("a", 1, 0, "", "hello"))
	for n := 0; n < len(enc); n++ {
		if _, _, err := DecodeRecord(enc[:n], 0); !errors.Is(err, ErrShort) {
			t.Fatalf("truncated to %d: err = %v, want ErrShort", n, err)
		}
	}
	patch := func(f func(b []byte)) error {
		b := append([]byte(nil), enc...)
		f(b)
		refixRecordCRCs(b, 0)
		_, _, err := DecodeRecord(b, 0)
		return err
	}
	if err := patch(func(b []byte) { b[5] = 1 }); !errors.Is(err, ErrUnsupported) {
		t.Errorf("flags: %v", err)
	}
	if err := patch(func(b []byte) { b[0] = 'X' }); !errors.Is(err, ErrBadMagic) {
		t.Errorf("magic: %v", err)
	}
	if err := patch(func(b []byte) { b[4] = 0 }); !errors.Is(err, ErrInvalid) {
		t.Errorf("type 0: %v", err)
	}
	if err := patch(func(b []byte) { b[8], b[9], b[10], b[11] = 0, 0, 0, 0 }); !errors.Is(err, ErrInvalid) {
		t.Errorf("seq 0: %v", err)
	}
	if err := patch(func(b []byte) { b[20], b[21], b[22], b[23] = 0xff, 0xff, 0xff, 0xff }); !errors.Is(err, ErrInvalid) {
		t.Errorf("huge payload_len: %v", err)
	}
	if err := patch(func(b []byte) { b[6], b[7] = 0xff, 0xff }); !errors.Is(err, ErrInvalid) {
		t.Errorf("huge name_len: %v", err)
	}
	if err := patch(func(b []byte) { b[16], b[17], b[18], b[19] = 0xff, 0xff, 0xff, 0x7f }); !errors.Is(err, ErrInvalid) {
		t.Errorf("huge meta_len: %v", err)
	}
	// Header CRC vs payload CRC are distinguishable only by position, both ErrCorrupt.
	b := append([]byte(nil), enc...)
	b[RecordHeaderLen+1+32+4] ^= 1 // first payload byte
	if _, _, err := DecodeRecord(b, 0); !errors.Is(err, ErrCorrupt) {
		t.Errorf("payload flip: %v", err)
	}
}

func TestDecodeHeadThenPayload(t *testing.T) {
	r := fileRecord("a/b", 4, 77, "m", "body")
	enc := mustAppendRecord(t, nil, r)
	got, plen, headLen, err := DecodeHead(enc[:RecordHeaderLen+3+32+1+4], 0)
	if err != nil || got.Seq != 4 || got.Check != 77 || string(got.Name) != "a/b" || plen != 4 || got.Payload != nil {
		t.Fatalf("DecodeHead: %+v %d %d %v", got, plen, headLen, err)
	}
	p, err := DecodePayload(got.Type, enc[headLen:], plen, 0)
	if err != nil || string(p) != "body" {
		t.Fatalf("DecodePayload: %q %v", p, err)
	}
	if _, err := DecodePayload(got.Type, enc[headLen:headLen+plen+3], plen, 0); !errors.Is(err, ErrShort) {
		t.Fatalf("short payload: %v", err)
	}
}

func TestTagLen(t *testing.T) {
	if n, err := TagLen(0); n != 0 || err != nil {
		t.Fatalf("cipher 0: %d %v", n, err)
	}
	if _, err := TagLen(1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("cipher 1: %v", err)
	}
	// With 16-byte tags the sizes follow §5 and seal stays tagless.
	if got := RecordLen(3, 0, 10, 16); got != roundup8(24+3+32+16+4+10+16+4) {
		t.Fatalf("RecordLen with tags = %d", got)
	}
	if effTag(TypeSeal, 16) != 0 || effTag(TypeFile, 16) != 16 {
		t.Fatal("effTag")
	}
}
