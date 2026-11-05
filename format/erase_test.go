package format

import (
	"bytes"
	"errors"
	"testing"
)

func TestEraseResult(t *testing.T) {
	orig := mustAppendRecord(t, nil, fileRecord("secret/name", 6, 0xCAFE, "private-meta", "very secret payload"))
	b := append([]byte(nil), orig...)
	eraseRecord(t, b)
	r, n, err := DecodeRecord(b, 0)
	if err != nil || n != len(orig) {
		t.Fatalf("erased record invalid: n=%d err=%v", n, err)
	}
	if r.Type != TypeErased || r.Seq != 6 || r.Check != 0xCAFE {
		t.Fatalf("header fields: %+v", r)
	}
	zero := func(p []byte) bool { return bytes.Equal(p, make([]byte, len(p))) }
	if len(r.Name) != 11 || !zero(r.Name) || r.SHA256 != [32]byte{} || len(r.Metadata) != 12 || !zero(r.Metadata) ||
		len(r.Payload) != 19 || !zero(r.Payload) {
		t.Fatalf("content not zeroed: %+v", r)
	}
	if bytes.Contains(b, []byte("secret")) || bytes.Contains(b, []byte("private")) {
		t.Fatal("plaintext survived")
	}
	// Only type, region A, region B and header_CRC differ; payload_CRC and padding are untouched.
	aEnd := RecordHeaderLen + 11 + 32 + 12
	bStart := aEnd + 4
	for i := range b {
		inA := i >= RecordHeaderLen && i < aEnd
		inB := i >= bStart && i < bStart+19
		crc := i >= aEnd && i < aEnd+4
		if i != 4 && !inA && !inB && !crc && b[i] != orig[i] {
			t.Errorf("byte %d changed outside erased areas", i)
		}
	}
	// Idempotent.
	again := append([]byte(nil), b...)
	eraseRecord(t, again)
	if !bytes.Equal(again, b) {
		t.Fatal("second erase differs")
	}
}

// The erased head is written first and in one piece; zeroing region B comes
// after. Every state from the end of the first write on must be a valid
// record, so a crash during the long part never ends the valid prefix of an
// open volume. Within the first write any prefix may be torn; re-erasing from
// the (unverified) fixed header must then complete the erasure.
func TestEraseWriteOrderKeepsRecordValid(t *testing.T) {
	orig := mustAppendRecord(t, nil, fileRecord("n/m", 3, 7, "meta", string(bytes.Repeat([]byte("P"), 100))))
	want := append([]byte(nil), orig...)
	eraseRecord(t, want)

	fh, err := ParseFixedHeader(orig)
	if err != nil {
		t.Fatal(err)
	}
	head, err := AppendErasedHead(nil, fh, 0)
	if err != nil || len(head) != fh.HeadLen(0) {
		t.Fatalf("head len %d, want %d (%v)", len(head), fh.HeadLen(0), err)
	}

	// Torn first write at every prefix length.
	for k := 0; k <= len(head); k++ {
		s := append([]byte(nil), orig...)
		copy(s, head[:k])
		_, _, derr := DecodeRecord(s, 0)
		switch k {
		case 0:
			if derr != nil {
				t.Fatalf("untouched record invalid: %v", derr)
			}
		case len(head):
			if derr != nil {
				t.Fatalf("record invalid after the head write: %v", derr)
			}
		}
		// The 24-byte header differs only in the type byte, so a retry can always parse it.
		again, err := ParseFixedHeader(s)
		if err != nil {
			t.Fatalf("prefix %d: unparseable fixed header: %v", k, err)
		}
		eraseRecord(t, s)
		if !bytes.Equal(s, want) {
			t.Fatalf("prefix %d: retry did not converge (%+v)", k, again)
		}
	}

	// Zeroing region B in small chunks: valid at every step.
	s := append([]byte(nil), orig...)
	copy(s, head)
	bStart, bEnd := fh.HeadLen(0), fh.HeadLen(0)+fh.RegionBLen(0)
	for off := bStart; off < bEnd; off += 7 {
		clear(s[off:min(off+7, bEnd)])
		if _, _, err := DecodeRecord(s, 0); err != nil {
			t.Fatalf("invalid after zeroing up to %d: %v", off, err)
		}
	}
	if !bytes.Equal(s, want) {
		t.Fatal("chunked erase differs from whole erase")
	}
}

func TestAppendErasedHeadRefusals(t *testing.T) {
	seal, _ := AppendSealRecord(nil, 1, 0, Seal{})
	unknown := mustAppendRecord(t, nil, Record{Type: 9, Seq: 1, Payload: []byte("x")})
	for name, rec := range map[string][]byte{"seal": seal, "unknown type": unknown} {
		h, err := ParseFixedHeader(rec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := AppendErasedHead(nil, h, 0); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := AppendErasedHead(nil, FixedHeader{Type: TypeFile}, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("seq 0: %v", err)
	}
}

func TestParseFixedHeader(t *testing.T) {
	rec := mustAppendRecord(t, nil, fileRecord("ab", 5, 9, "M", "PAY"))
	h, err := ParseFixedHeader(rec[:RecordHeaderLen]) // 24 bytes are enough
	if err != nil {
		t.Fatal(err)
	}
	want := FixedHeader{Type: TypeFile, Seq: 5, Check: 9, NameLen: 2, MetaLen: 1, PayloadLen: 3}
	if h != want {
		t.Fatalf("got %+v, want %+v", h, want)
	}
	if h.RegionALen(0) != 35 || h.RegionBLen(0) != 3 || h.HeadLen(0) != 63 || h.Len(0) != len(rec) {
		t.Fatalf("geometry: A=%d B=%d head=%d len=%d (record %d)", h.RegionALen(0), h.RegionBLen(0), h.HeadLen(0), h.Len(0), len(rec))
	}
	if _, err := ParseFixedHeader(rec[:23]); !errors.Is(err, ErrShort) {
		t.Errorf("short: %v", err)
	}
	// Not verified: a flipped name byte still parses; DecodeHead catches it.
	bad := append([]byte(nil), rec...)
	bad[RecordHeaderLen] ^= 1
	if _, err := ParseFixedHeader(bad); err != nil {
		t.Errorf("unverified parse failed: %v", err)
	}
	if _, _, _, err := DecodeHead(bad, 0); !errors.Is(err, ErrCorrupt) {
		t.Errorf("DecodeHead on flipped name: %v", err)
	}
}

func TestSplitEncoderMatchesAppendRecord(t *testing.T) {
	for _, r := range []Record{
		fileRecord("a/b.txt", 1, 1, "", "hello"),
		fileRecord("a", 2, 2, "meta", ""),
		fileRecord("big", 3, 3, "m", string(bytes.Repeat([]byte("x"), 1000))),
		{Type: 9, Seq: 4, Payload: []byte("future")},
	} {
		whole := mustAppendRecord(t, []byte("pre"), r)[3:]
		head, err := AppendRecordHead([]byte("pre"), r, len(r.Payload))
		if err != nil {
			t.Fatal(err)
		}
		head = head[3:]
		var sum uint32
		for p := r.Payload; len(p) > 0; p = p[min(7, len(p)):] { // as a stream would
			sum = CRCUpdate(sum, p[:min(7, len(p))])
		}
		split := append(append([]byte(nil), head...), r.Payload...)
		split = AppendRecordTail(split, sum, len(head), len(r.Payload))
		if !bytes.Equal(split, whole) {
			t.Errorf("%q: split encoding differs from AppendRecord", r.Name)
		}
	}
}

func TestStreamingCRC(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 1000)
	var sum uint32
	for p := data; len(p) > 0; p = p[min(333, len(p)):] {
		sum = CRCUpdate(sum, p[:min(333, len(p))])
	}
	if sum != CRC(data) || CRCUpdate(0, data) != CRC(data) {
		t.Fatal("chunked CRC differs from CRC")
	}
	rec := mustAppendRecord(t, nil, fileRecord("n", 1, 1, "", string(data)))
	h, _ := ParseFixedHeader(rec)
	trailer := rec[h.HeadLen(0)+h.RegionBLen(0):]
	if err := CheckPayloadCRC(sum, trailer); err != nil {
		t.Fatal(err)
	}
	if err := CheckPayloadCRC(sum+1, trailer); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong sum: %v", err)
	}
	if err := CheckPayloadCRC(sum, trailer[:3]); !errors.Is(err, ErrShort) {
		t.Fatalf("short trailer: %v", err)
	}
}
