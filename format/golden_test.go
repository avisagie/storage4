package format

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// -update writes golden files that do not exist yet. It never overwrites an
// existing file: golden files are permanent (CLAUDE.md), a change in output
// is a new format version and new files.
var update = flag.Bool("update", false, "write missing golden files")

func goldenCases(t *testing.T) map[string][]byte {
	t.Helper()
	g := map[string][]byte{}
	g["header.bin"] = mustHeader(t, fixtureHeader())
	g["record-file.bin"] = mustAppendRecord(t, nil, fileRecord("a/b.txt", 1, 0x01020304, "", "hello"))
	g["record-file-meta.bin"] = mustAppendRecord(t, nil, fileRecord("dir/x:y.json", 2, 0x0a0b0c0d, `{"k":1}`, "{}\n"))
	erased := mustAppendRecord(t, nil, fileRecord("gone.bin", 4, 0x11223344, "secret", "payload to erase"))
	eraseRecord(t, erased)
	g["record-erased.bin"] = erased
	seal, err := AppendSealRecord(nil, 5, 0x55667788, Seal{CommittedLength: 1000, Epoch: 1, RecordCount: 3, LastSeq: 4})
	if err != nil {
		t.Fatal(err)
	}
	g["record-seal.bin"] = seal
	g["record-unknown-type.bin"] = mustAppendRecord(t, nil, Record{Type: 9, Seq: 1, Check: 0x99, Payload: []byte("future")})

	vol := fixtureVolume(t)
	g["volume-sealed.bin"] = vol

	// Corrupted variants.
	hdrCRC := append([]byte(nil), g["record-file.bin"]...)
	hdrCRC[RecordHeaderLen] ^= 0x01 // first name byte
	g["bad-header-crc.bin"] = hdrCRC
	payCRC := append([]byte(nil), g["record-file.bin"]...)
	payCRC[RecordHeaderLen+7+32+4] ^= 0x01 // first payload byte
	g["bad-payload-crc.bin"] = payCRC
	g["torn-tail.bin"] = append([]byte(nil), vol[:len(vol)-5]...)

	hdr := g["header.bin"]
	garbage := bytes.Repeat([]byte{0xFF}, 40)
	g["garbage-then-record.bin"] = bytes.Join([][]byte{hdr, g["record-file.bin"], garbage,
		mustAppendRecord(t, nil, fileRecord("after", 3, 0x77, "", "resynced"))}, nil)
	return g
}

func TestGolden(t *testing.T) {
	for name, want := range goldenCases(t) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", name)
			got, err := os.ReadFile(path)
			if os.IsNotExist(err) && *update {
				if err := os.WriteFile(path, want, 0o444); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s (%d bytes)", path, len(want))
				return
			}
			if err != nil {
				t.Fatalf("%v (run with -update to create a missing golden file)", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("encoder output differs from frozen %s: a format change needs a new version and new golden files", path)
			}
		})
	}
}

// The frozen files must stay decodable by every future version of the code.
func TestGoldenDecode(t *testing.T) {
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	h, first, err := DecodeVolumeHeader(read("header.bin"))
	if err != nil || h != fixtureHeader() || first != len(read("header.bin")) {
		t.Fatalf("header: %+v %d %v", h, first, err)
	}
	for name, seq := range map[string]uint32{"record-file.bin": 1, "record-file-meta.bin": 2, "record-erased.bin": 4, "record-seal.bin": 5, "record-unknown-type.bin": 1} {
		r, n, err := DecodeRecord(read(name), 0)
		if err != nil || r.Seq != seq || n != len(read(name)) {
			t.Errorf("%s: %+v %d %v", name, r, n, err)
		}
	}
	vol := read("volume-sealed.bin")
	if s, ok := IsSealed(vol, first, 0); !ok || s.LastSeq != 4 {
		t.Errorf("volume-sealed: %+v %v", s, ok)
	}
	if _, ok := IsSealed(read("torn-tail.bin"), first, 0); ok {
		t.Error("torn-tail reported sealed")
	}
	if _, _, err := DecodeRecord(read("bad-header-crc.bin"), 0); err == nil {
		t.Error("bad-header-crc accepted")
	}
	if _, _, err := DecodeRecord(read("bad-payload-crc.bin"), 0); err == nil {
		t.Error("bad-payload-crc accepted")
	}
	g := read("garbage-then-record.bin")
	r1, next, err := Next(g, first, 0)
	if err != nil || r1.Seq != 1 {
		t.Fatalf("garbage file first record: %v", err)
	}
	if _, _, err := Next(g, next, 0); err == nil {
		t.Fatal("garbage decoded")
	}
	off, ok := Resync(g, next, r1.Seq, 0)
	if r, _, err := Next(g, off, 0); !ok || err != nil || r.Seq != 3 || string(r.Name) != "after" {
		t.Fatalf("resync: %d %v %v", off, ok, err)
	}
}
