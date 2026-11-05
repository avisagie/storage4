package nameindex

import (
	"bytes"
	"fmt"
	"testing"
)

const vid = 0x0001_0000_0000_0007

func mkEntries(n int) []Entry {
	es := make([]Entry, n)
	for i := range es {
		es[i] = Entry{FP: Fingerprint([]byte(fmt.Sprintf("name-%d", i))), Seq: uint32(i + 1), Off: int64(i+1) * 64}
	}
	return es
}

func TestFingerprintVector(t *testing.T) {
	// SHA-256("a") = ca978112ca1bbdca fac231b39a23dc4d a786eff8147c4e72 b9807785afee48bb
	if got := Fingerprint([]byte("a")); got != 0xfac231b39a23dc4d {
		t.Fatalf("%x", got)
	}
}

func TestRoundTripAndDeterminism(t *testing.T) {
	for _, n := range []int{0, 1, 177, 178, 179, 1000, 5000} {
		es := mkEntries(n)
		b, err := Encode(vid, es)
		if err != nil {
			t.Fatal(err)
		}
		// Input order must not matter.
		rev := make([]Entry, n)
		for i := range es {
			rev[n-1-i] = es[i]
		}
		b2, err := Encode(vid, rev)
		if err != nil || !bytes.Equal(b, b2) {
			t.Fatalf("n=%d: not deterministic (%v)", n, err)
		}
		tab, err := DecodeTable(b, vid)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(tab.Entries) != n {
			t.Fatalf("n=%d: got %d entries", n, len(tab.Entries))
		}
		for _, e := range es {
			c := tab.Candidates(e.FP)
			if len(c) != 1 || c[0] != e {
				t.Fatalf("n=%d: %+v -> %+v", n, e, c)
			}
		}
		if want := (headerLen + bucketCount(n)*bucketSize + footerLen); len(b) != want {
			t.Fatalf("n=%d: size %d want %d", n, len(b), want)
		}
	}
}

func TestDecodeRejectsWrongVolume(t *testing.T) {
	b, _ := Encode(vid, mkEntries(3))
	if _, err := DecodeTable(b, vid+1); err == nil {
		t.Fatal("accepted other volume")
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	for _, e := range []Entry{{FP: 1, Seq: 0, Off: 8}, {FP: 1, Seq: 1, Off: 0}, {FP: 1, Seq: 1, Off: 12}, {FP: 1, Seq: 1, Off: 8 << 32}} {
		if _, err := Encode(vid, []Entry{e}); err == nil {
			t.Fatalf("accepted %+v", e)
		}
	}
	if _, err := Encode(vid, []Entry{{FP: 1, Seq: 1, Off: 8}, {FP: 2, Seq: 1, Off: 16}}); err == nil {
		t.Fatal("accepted duplicate seq")
	}
}

func TestEveryByteFlipAndTruncationRejected(t *testing.T) {
	b, _ := Encode(vid, mkEntries(20))
	for i := range b {
		c := bytes.Clone(b)
		c[i] ^= 0x01
		if _, err := DecodeTable(c, vid); err == nil {
			t.Fatalf("flip at %d accepted", i)
		}
	}
	for n := 0; n < len(b); n += 7 {
		if _, err := DecodeTable(b[:n], vid); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
	if _, err := DecodeTable(append(bytes.Clone(b), 0), vid); err == nil {
		t.Fatal("extension accepted")
	}
}

// Entries that overflow their home bucket spill into the next one, and are
// still all found; the output stays canonical.
func TestSpillAcrossBuckets(t *testing.T) {
	var es []Entry
	for i := range 400 { // same fingerprint: all want one bucket
		es = append(es, Entry{FP: 42, Seq: uint32(i + 1), Off: int64(i+1) * 8})
	}
	b, err := Encode(vid, es)
	if err != nil {
		t.Fatal(err)
	}
	tab, err := DecodeTable(b, vid)
	if err != nil {
		t.Fatal(err)
	}
	if c := tab.Candidates(42); len(c) != 400 || c[0].Seq != 400 {
		t.Fatalf("%d candidates", len(c))
	}
}

func TestBuilderKeepsHighestSeqPerName(t *testing.T) {
	b := NewBuilder()
	for _, a := range []struct {
		n   string
		seq uint32
	}{{"a", 1}, {"b", 2}, {"a", 5}, {"a", 3}} {
		if err := b.Add([]byte(a.n), a.seq, int64(a.seq)*8); err != nil {
			t.Fatal(err)
		}
	}
	if b.Len() != 2 {
		t.Fatalf("len %d", b.Len())
	}
	raw, err := b.Encode(vid)
	if err != nil {
		t.Fatal(err)
	}
	tab, err := DecodeTable(raw, vid)
	if err != nil {
		t.Fatal(err)
	}
	if c := tab.Candidates(Fingerprint([]byte("a"))); len(c) != 1 || c[0].Seq != 5 {
		t.Fatalf("%+v", c)
	}
	if b.Add([]byte("x"), 0, 8) == nil || b.Add([]byte("x"), 1, 4) == nil {
		t.Fatal("accepted invalid add")
	}
}

func FuzzDecodeTable(f *testing.F) {
	for _, n := range []int{0, 1, 30} {
		b, _ := Encode(vid, mkEntries(n))
		f.Add(b)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		tab, err := DecodeTable(b, vid)
		if err != nil {
			return
		}
		again, err := Encode(vid, tab.Entries)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded table does not re-encode identically: %v", err)
		}
	})
}
