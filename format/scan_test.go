package format

import (
	"io"
	"testing"
)

func TestScanSealedVolume(t *testing.T) {
	v := fixtureVolume(t)
	_, first, err := DecodeVolumeHeader(v)
	if err != nil {
		t.Fatal(err)
	}
	var types []RecordType
	var seqs []uint32
	off := first
	for {
		r, next, err := Next(v, off, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next at %d: %v", off, err)
		}
		types = append(types, r.Type)
		seqs = append(seqs, r.Seq)
		off = next
	}
	wantT := []RecordType{TypeFile, TypeFile, TypeErased, TypeSeal}
	wantS := []uint32{1, 2, 4, 5}
	for i := range wantT {
		if types[i] != wantT[i] || seqs[i] != wantS[i] {
			t.Fatalf("records = %v %v", types, seqs)
		}
	}
	if len(types) != 4 || off != len(v) {
		t.Fatalf("scan ended at %d of %d with %d records", off, len(v), len(types))
	}
	s, ok := IsSealed(v, first, 0)
	if !ok || s.RecordCount != 3 || s.LastSeq != 4 || s.Epoch != 1 {
		t.Fatalf("IsSealed = %+v %v", s, ok)
	}
}

func TestIsSealedNegatives(t *testing.T) {
	v := fixtureVolume(t)
	_, first, _ := DecodeVolumeHeader(v)
	sealOff := int(mustSeal(t, v, first).CommittedLength)

	cases := map[string][]byte{
		"seal removed":        v[:sealOff],
		"torn seal":           v[:len(v)-5],
		"trailing garbage":    append(append([]byte(nil), v...), make([]byte, 8)...),
		"header only":         v[:first],
		"first record only":   v[:first+RecordLen(7, 0, 5, 0)],
		"seal not at the end": append(append([]byte(nil), v...), mustAppendRecord(t, nil, fileRecord("z", 6, 0, "", "z"))...),
	}
	for name, b := range cases {
		if _, ok := IsSealed(b, first, 0); ok {
			t.Errorf("%s: reported sealed", name)
		}
	}
	// committed_length that does not match the seal's own offset.
	bad := append([]byte(nil), v[:sealOff]...)
	bad, _ = AppendSealRecord(bad, 5, 0, Seal{CommittedLength: uint64(sealOff) + 8})
	if _, ok := IsSealed(bad, first, 0); ok {
		t.Error("wrong committed_length reported sealed")
	}
}

func mustSeal(t *testing.T, v []byte, first int) Seal {
	t.Helper()
	s, ok := IsSealed(v, first, 0)
	if !ok {
		t.Fatal("fixture not sealed")
	}
	return s
}

func TestUnknownTypeIsSkippedByLength(t *testing.T) {
	v := mustHeader(t, fixtureHeader())
	_, first, _ := DecodeVolumeHeader(v)
	v = mustAppendRecord(t, v, fileRecord("a", 1, 0, "", "one"))
	v = mustAppendRecord(t, v, Record{Type: 9, Seq: 2, Payload: []byte("from the future")})
	v = mustAppendRecord(t, v, fileRecord("b", 3, 0, "", "two"))
	var got []uint32
	for off := first; ; {
		r, next, err := Next(v, off, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if r.Type == TypeFile {
			got = append(got, r.Seq)
		}
		off = next
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("file seqs = %v", got)
	}
}

func TestNextBadOffsets(t *testing.T) {
	v := fixtureVolume(t)
	for _, off := range []int{-8, 3, len(v) + 8} {
		if _, _, err := Next(v, off, 0); err == nil || err == io.EOF {
			t.Errorf("offset %d: err = %v", off, err)
		}
	}
}

func TestResync(t *testing.T) {
	a := mustAppendRecord(t, nil, fileRecord("a", 1, 0, "", "one"))
	b := mustAppendRecord(t, nil, fileRecord("b", 2, 0, "", "two"))
	c := mustAppendRecord(t, nil, fileRecord("c", 5, 0, "", "three"))
	garbage := make([]byte, 40)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	vol := append(append(append([]byte(nil), a...), garbage...), c...)
	vol = append(vol, b...) // seq 2 after seq 5: not acceptable once lastSeq=5

	if _, _, err := Next(vol, len(a), 0); err == nil {
		t.Fatal("garbage decoded")
	}
	off, ok := Resync(vol, len(a), 1, 0)
	if !ok || off != len(a)+len(garbage) {
		t.Fatalf("Resync = %d %v, want %d", off, ok, len(a)+len(garbage))
	}
	if r, _, err := Next(vol, off, 0); err != nil || r.Seq != 5 {
		t.Fatalf("resynced record: %+v %v", r, err)
	}
	// seq must be greater than the last good seq: the trailing seq-2 record is skipped.
	if off, ok := Resync(vol, off+len(c), 5, 0); ok {
		t.Fatalf("accepted stale seq at %d", off)
	}
	// From an unaligned offset, the search rounds up.
	if off, ok := Resync(vol, len(a)+1, 1, 0); !ok || off != len(a)+len(garbage) {
		t.Fatalf("unaligned from: %d %v", off, ok)
	}
	if _, ok := Resync(nil, -5, 0, 0); ok {
		t.Fatal("found record in nothing")
	}
}
