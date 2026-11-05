package format

import (
	"errors"
	"testing"
)

func TestSealRoundTrip(t *testing.T) {
	s := Seal{CommittedLength: 1 << 33, Epoch: 9, RecordCount: 12345, LastSeq: 99999}
	enc, err := AppendSealRecord(nil, 7, 0xdeadbeef, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != 88 {
		t.Fatalf("seal record len = %d, want 88", len(enc))
	}
	r, n, err := DecodeRecord(enc, 0)
	if err != nil || n != 88 || r.Type != TypeSeal || r.Seq != 7 || r.Check != 0xdeadbeef {
		t.Fatalf("decode: %+v %d %v", r, n, err)
	}
	got, err := DecodeSeal(r)
	if err != nil || got != s {
		t.Fatalf("DecodeSeal = %+v %v, want %+v", got, err, s)
	}
	// Seal records are tagless under any cipher.
	if r2, n2, err := DecodeRecord(enc, 16); err != nil || n2 != 88 || r2.Seq != 7 {
		t.Fatalf("decode with tagLen 16: %v", err)
	}
}

func TestDecodeSealRefusals(t *testing.T) {
	enc, _ := AppendSealRecord(nil, 1, 0, Seal{})
	base, _, err := DecodeRecord(enc, 0)
	if err != nil {
		t.Fatal(err)
	}
	mut := map[string]func(*Record){
		"wrong type":    func(r *Record) { r.Type = TypeFile },
		"has name":      func(r *Record) { r.Name = []byte("x") },
		"has metadata":  func(r *Record) { r.Metadata = []byte("x") },
		"sha not zero":  func(r *Record) { r.SHA256[0] = 1 },
		"short payload": func(r *Record) { r.Payload = r.Payload[:23] },
		"reserved set":  func(r *Record) { r.Payload = append([]byte(nil), r.Payload...); r.Payload[20] = 1 },
	}
	for name, f := range mut {
		r := base
		f(&r)
		if _, err := DecodeSeal(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
