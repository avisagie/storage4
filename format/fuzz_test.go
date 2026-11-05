package format

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func FuzzDecodePrelude(f *testing.F) {
	f.Add(AppendPrelude(nil, 72))
	f.Add([]byte("garbage"))
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := DecodePrelude(b)
		if err == nil && p.Major == Major && p.Minor == Minor {
			if !bytes.Equal(AppendPrelude(nil, p.HeaderLen), b[:PreludeLen]) {
				t.Fatal("prelude does not re-encode identically")
			}
		}
	})
}

func FuzzDecodeVolumeHeader(f *testing.F) {
	f.Add(mustHeader(f, fixtureHeader()), false)
	f.Add(mustHeader(f, fixtureHeader()), true)
	f.Add([]byte{0x89, 'S', '4', 'V', 'O', 'L', 13, 10, 1, 0, 0, 0, 72, 0, 0, 0}, true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		b = append([]byte(nil), b...)
		if fix && len(b) >= PreludeLen {
			// Get past the CRC so the structural checks are reachable.
			hl := int(binary.LittleEndian.Uint32(b[12:]))
			if hl >= 4 && PreludeLen+hl <= len(b) {
				refixHeaderCRC(b)
			}
		}
		h, off, err := DecodeVolumeHeader(b)
		if err != nil {
			return
		}
		if off < PreludeLen || off > len(b) || off%8 != 0 {
			t.Fatalf("bad first record offset %d for len %d", off, len(b))
		}
		p, _ := DecodePrelude(b)
		if p.Minor != 0 {
			return
		}
		enc, err := AppendVolumeHeader(nil, h)
		if err != nil {
			return // decoder is allowed to accept shard counts above today's encoder cap
		}
		if !bytes.Equal(enc, b[:off]) {
			t.Fatalf("header does not re-encode identically:\n% x\n% x", enc, b[:off])
		}
	})
}

func FuzzDecodeRecord(f *testing.F) {
	seed := func(r Record) []byte { return mustAppendRecord(f, nil, r) }
	f.Add(seed(fileRecord("a/b", 1, 2, "meta", "payload")), true)
	f.Add(seed(fileRecord("a", 1, 2, "", "")), false)
	f.Add(seed(Record{Type: 9, Seq: 3, Payload: []byte("x")}), true)
	seal, _ := AppendSealRecord(nil, 4, 5, Seal{CommittedLength: 8, Epoch: 1})
	f.Add(seal, true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		b = append([]byte(nil), b...)
		if fix {
			refixRecordCRCs(b, 0)
		}
		r, n, err := DecodeRecord(b, 0)
		if err != nil {
			return
		}
		if n < RecordHeaderLen || n > len(b) || n%8 != 0 {
			t.Fatalf("bad length %d for input %d", n, len(b))
		}
		if r.Type == TypeSeal {
			_, _ = DecodeSeal(r)
		}
		if r.Type == TypeErased {
			return // payload_CRC is stale by design: erased records do not re-encode
		}
		enc, err := AppendRecord(nil, r)
		if err != nil {
			return // e.g. a file name the API would not accept today
		}
		used := RecordHeaderLen + len(r.Name) + 32 + len(r.Metadata) + 4 + len(r.Payload) + 4
		if !bytes.Equal(enc[:used], b[:used]) {
			t.Fatal("record does not re-encode identically")
		}
	})
}

func FuzzScan(f *testing.F) {
	v := fixtureVolume(f)
	f.Add(v, 0, uint32(0))
	f.Add(v[:len(v)-5], 8, uint32(2))
	f.Add(bytes.Repeat([]byte("S4R1"), 40), 4, uint32(1))
	f.Fuzz(func(t *testing.T, b []byte, from int, lastSeq uint32) {
		if off, ok := Resync(b, from, lastSeq, 0); ok {
			if off < from || off%8 != 0 || off+RecordHeaderLen > len(b) {
				t.Fatalf("Resync returned %d (from %d, len %d)", off, from, len(b))
			}
			h, err := parseHead(b[off:], 0, true)
			if err != nil || h.Seq <= lastSeq {
				t.Fatalf("Resync candidate invalid: %v seq %d last %d", err, h.Seq, lastSeq)
			}
		}
		_, first, err := DecodeVolumeHeader(b)
		if err != nil {
			first = 0
		}
		off := first
		for i := 0; i < len(b)+1; i++ {
			_, next, err := Next(b, off, 0)
			if err == io.EOF || err != nil {
				break
			}
			if next <= off {
				t.Fatal("Next did not advance")
			}
			off = next
		}
		IsSealed(b, first, 0)
	})
}

func FuzzParseFixedHeader(f *testing.F) {
	f.Add(mustAppendRecord(f, nil, fileRecord("a/b", 1, 2, "m", "p")))
	f.Add([]byte("S4R1\x01\x00"))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := ParseFixedHeader(b)
		if err != nil {
			return
		}
		if h.Len(0) < RecordHeaderLen+32+4+4 || h.Len(0)%8 != 0 || h.HeadLen(0) > h.Len(0) {
			t.Fatalf("inconsistent geometry for %+v", h)
		}
		head, err := AppendErasedHead(nil, h, 0)
		if err != nil {
			return // not a file/erased record, or seq 0
		}
		if len(head) != h.HeadLen(0) {
			t.Fatalf("erased head is %d bytes, want %d", len(head), h.HeadLen(0))
		}
		r, payloadLen, headLen, err := DecodeHead(head, 0)
		if err != nil || r.Type != TypeErased || r.Seq != h.Seq || r.Check != h.Check ||
			payloadLen != h.PayloadLen || headLen != len(head) ||
			!bytes.Equal(r.Name, make([]byte, h.NameLen)) || r.SHA256 != [32]byte{} {
			t.Fatalf("erased head does not decode as an erased header: %+v %v", r, err)
		}
	})
}
