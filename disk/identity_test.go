package disk

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func testIdentity() Identity {
	var id Identity
	for i := range id.ClusterID {
		id.ClusterID[i] = byte(0xA0 + i)
		id.DiskID[i] = byte(i)
	}
	id.NodeID = 0x0102
	id.CreatedUnixNano = 1762300000123456789
	return id
}

func TestIdentityRoundTripAndGolden(t *testing.T) {
	id := testIdentity()
	enc := EncodeIdentity(id)
	// The format is permanent once disks exist; this literal pins it.
	want := "storage4-disk 1\n" +
		"cluster_id a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\n" +
		"node_id 0102\n" +
		"disk_id 000102030405060708090a0b0c0d0e0f\n" +
		"created_unix_nano 1762300000123456789\n"
	if !strings.HasPrefix(string(enc), want) || !strings.HasPrefix(string(enc[len(want):]), "crc32c ") {
		t.Fatalf("unexpected encoding:\n%s", enc)
	}
	got, err := DecodeIdentity(enc)
	if err != nil || got != id {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestIdentityRejectsDamage(t *testing.T) {
	enc := EncodeIdentity(testIdentity())
	for i := range enc {
		b := append([]byte(nil), enc...)
		b[i] ^= 0x01
		if _, err := DecodeIdentity(b); !errors.Is(err, ErrBadIdentity) {
			t.Fatalf("flip at %d accepted (err %v)", i, err)
		}
	}
	for n := 0; n < len(enc); n++ {
		if _, err := DecodeIdentity(enc[:n]); !errors.Is(err, ErrBadIdentity) {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
	if _, err := DecodeIdentity(append(append([]byte(nil), enc...), '\n')); !errors.Is(err, ErrBadIdentity) {
		t.Fatal("trailing data accepted")
	}
}

func FuzzDecodeIdentity(f *testing.F) {
	f.Add(EncodeIdentity(testIdentity()))
	f.Add([]byte("storage4-disk 1\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		id, err := DecodeIdentity(b)
		if err != nil {
			return
		}
		if string(EncodeIdentity(id)) != string(b) {
			t.Fatal("decoded identity does not re-encode identically")
		}
	})
}

func TestWriteReadIdentity(t *testing.T) {
	for name, mk := range implementations() {
		t.Run(name, func(t *testing.T) {
			fsys := mk(t)
			if _, err := ReadIdentity(fsys); !errors.Is(err, ErrNotExist) {
				t.Fatalf("missing identity: %v", err)
			}
			id := testIdentity()
			must(t, WriteIdentity(fsys, id))
			got, err := ReadIdentity(fsys)
			must(t, err)
			if got != id {
				t.Fatal("mismatch")
			}
			other := id
			other.NodeID++
			if err := WriteIdentity(fsys, other); !errors.Is(err, ErrExist) {
				t.Fatalf("overwrite not refused: %v", err)
			}
			if got, _ := ReadIdentity(fsys); got != id {
				t.Fatal("identity changed")
			}
		})
	}
}

func TestIdentityCorruptFileDetected(t *testing.T) {
	m := NewMem(MemOptions{})
	must(t, WriteFileAtomic(m, IdentityFile, []byte("garbage\n")))
	if _, err := ReadIdentity(m); !errors.Is(err, ErrBadIdentity) {
		t.Fatal(err)
	}
}

// After a crash at any point while writing the identity, the disk has no
// identity or a valid one, never a damaged one.
func TestWriteIdentityCrashSweep(t *testing.T) {
	probe, _ := newFaultyMem(0, CrashAny)
	must(t, WriteIdentity(probe, testIdentity()))
	total := probe.Ops()
	for _, mode := range []CrashMode{CrashPrefix, CrashAny} {
		for n := 1; n <= total; n++ {
			for seed := uint64(0); seed < 30; seed++ {
				t.Run(fmt.Sprintf("%v/op%d/seed%d", mode, n, seed), func(t *testing.T) {
					f, m := newFaultyMem(seed, mode)
					f.SetInjector(CrashAt(n, false, nil))
					if err := WriteIdentity(f, testIdentity()); !errors.Is(err, ErrCrashed) {
						t.Fatalf("expected crash, got %v", err)
					}
					f.Restart()
					id, err := ReadIdentity(m)
					switch {
					case err == nil:
						if id != testIdentity() {
							t.Fatal("wrong identity")
						}
					case errors.Is(err, ErrNotExist):
					default:
						t.Fatalf("damaged identity: %v", err)
					}
				})
			}
		}
	}
}
