package format

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

// Fixed inputs shared by table tests and golden files. Changing any of them
// changes the golden files, which are permanent: add new ones instead.

func fixtureHeader() VolumeHeader {
	var cluster [16]byte
	for i := range cluster {
		cluster[i] = byte(0xA0 + i)
	}
	return VolumeHeader{
		VolumeID:        0x0001_0000_0000_0001,
		ClusterID:       cluster,
		Shard:           3,
		ShardCount:      16,
		VolumeNumber:    7,
		CreatedUnixNano: 1730000000000000000,
		Epoch:           1,
		Bucket:          "events",
	}
}

func fileRecord(name string, seq, check uint32, meta, payload string) Record {
	r := Record{Type: TypeFile, Seq: seq, Check: check, Name: []byte(name), Payload: []byte(payload)}
	if meta != "" {
		r.Metadata = []byte(meta)
	}
	r.SHA256 = sha256.Sum256(r.Payload)
	return r
}

func mustAppendRecord(t testing.TB, dst []byte, r Record) []byte {
	t.Helper()
	out, err := AppendRecord(dst, r)
	if err != nil {
		t.Fatalf("AppendRecord: %v", err)
	}
	return out
}

func mustHeader(t testing.TB, h VolumeHeader) []byte {
	t.Helper()
	out, err := AppendVolumeHeader(nil, h)
	if err != nil {
		t.Fatalf("AppendVolumeHeader: %v", err)
	}
	return out
}

// fixtureVolume builds header + file(seq1) + file with metadata(seq2) +
// erased(seq4, seq 3 is a gap) + seal(seq5).
func fixtureVolume(t testing.TB) []byte {
	t.Helper()
	v := mustHeader(t, fixtureHeader())
	v = mustAppendRecord(t, v, fileRecord("a/b.txt", 1, 0x01020304, "", "hello"))
	v = mustAppendRecord(t, v, fileRecord("dir/x:y.json", 2, 0x0a0b0c0d, `{"k":1}`, "{}\n"))
	off := len(v)
	v = mustAppendRecord(t, v, fileRecord("gone.bin", 4, 0x11223344, "secret", "payload to erase"))
	eraseRecord(t, v[off:])
	seal := Seal{CommittedLength: uint64(len(v)), Epoch: 1, RecordCount: 3, LastSeq: 4}
	out, err := AppendSealRecord(v, 5, 0x55667788, seal)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// refixHeaderCRC rewrites the volume header CRC after a test patched bytes.
func refixHeaderCRC(b []byte) {
	hl := int(binary.LittleEndian.Uint32(b[12:]))
	end := PreludeLen + hl
	binary.LittleEndian.PutUint32(b[end-4:], CRC(b[:end-4]))
}

// refixRecordCRCs rewrites header_CRC and payload_CRC of the record at the
// start of b, best effort (used by tests and fuzzers to get past checksums).
func refixRecordCRCs(b []byte, tagLen int) {
	h, err := parseHead(b, tagLen, false)
	if err != nil {
		return
	}
	hl, bl := h.HeadLen(tagLen), h.RegionBLen(tagLen)
	binary.LittleEndian.PutUint32(b[hl-4:], CRC(b[:hl-4]))
	if len(b) >= hl+bl+4 {
		binary.LittleEndian.PutUint32(b[hl+bl:], CRC(b[hl:hl+bl]))
	}
}

// eraseRecord models what volume/ does on a record read into rec: write the
// erased head, then zero region B.
func eraseRecord(t testing.TB, rec []byte) {
	t.Helper()
	h, err := ParseFixedHeader(rec)
	if err != nil {
		t.Fatal(err)
	}
	head, err := AppendErasedHead(nil, h, 0)
	if err != nil {
		t.Fatal(err)
	}
	copy(rec, head)
	clear(rec[h.HeadLen(0) : h.HeadLen(0)+h.RegionBLen(0)])
}
