package disk

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"storage4/format"
)

// IdentityFile is the name of the per-disk identity file, at the FS root.
const IdentityFile = "storage4.disk"

const identityMagic = "storage4-disk 1"

// ErrBadIdentity means the identity file exists but is not valid.
var ErrBadIdentity = errors.New("disk: invalid identity file")

// Identity says whose disk this is. Written once when an operator adds the
// disk; a node refuses to use a disk whose cluster or node ID does not
// match its configuration, and refuses a disk without one.
type Identity struct {
	ClusterID       [16]byte
	NodeID          uint16 // the node_id part of volume IDs
	DiskID          [16]byte
	CreatedUnixNano int64 // informational only
}

// EncodeIdentity renders the identity file: plain text, one "key value"
// line each, ending with a CRC-32C (as in format) over everything before
// that last line. Readable with `cat`.
func EncodeIdentity(id Identity) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s\n", identityMagic)
	fmt.Fprintf(&b, "cluster_id %s\n", hex.EncodeToString(id.ClusterID[:]))
	fmt.Fprintf(&b, "node_id %04x\n", id.NodeID)
	fmt.Fprintf(&b, "disk_id %s\n", hex.EncodeToString(id.DiskID[:]))
	fmt.Fprintf(&b, "created_unix_nano %d\n", id.CreatedUnixNano)
	fmt.Fprintf(&b, "crc32c %08x\n", format.CRC(b.Bytes()))
	return b.Bytes()
}

// DecodeIdentity parses an identity file strictly: exact line order, no
// extra content, CRC must match.
func DecodeIdentity(data []byte) (Identity, error) {
	bad := func(msg string) (Identity, error) {
		return Identity{}, fmt.Errorf("%w: %s", ErrBadIdentity, msg)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return bad("truncated")
	}
	lines := strings.Split(string(data[:len(data)-1]), "\n")
	if len(lines) != 6 || lines[0] != identityMagic {
		return bad("bad header or line count")
	}
	crcLine := lines[5]
	body := data[:len(data)-len(crcLine)-1]
	field := func(i int, key string) (string, bool) {
		v, ok := strings.CutPrefix(lines[i], key+" ")
		return v, ok
	}
	var id Identity
	if v, ok := field(1, "cluster_id"); !ok || !hexInto(id.ClusterID[:], v) {
		return bad("cluster_id")
	}
	if v, ok := field(2, "node_id"); !ok || len(v) != 4 {
		return bad("node_id")
	} else if n, err := strconv.ParseUint(v, 16, 16); err != nil {
		return bad("node_id")
	} else {
		id.NodeID = uint16(n)
	}
	if v, ok := field(3, "disk_id"); !ok || !hexInto(id.DiskID[:], v) {
		return bad("disk_id")
	}
	if v, ok := field(4, "created_unix_nano"); !ok {
		return bad("created_unix_nano")
	} else if n, err := strconv.ParseInt(v, 10, 64); err != nil {
		return bad("created_unix_nano")
	} else {
		id.CreatedUnixNano = n
	}
	v, ok := field(5, "crc32c")
	if !ok || len(v) != 8 {
		return bad("crc32c")
	}
	want, err := strconv.ParseUint(v, 16, 32)
	if err != nil {
		return bad("crc32c")
	}
	if uint32(want) != format.CRC(body) {
		return Identity{}, fmt.Errorf("%w: checksum mismatch", ErrBadIdentity)
	}
	// Canonical form only: anything that re-encodes differently (upper-case
	// hex, leading zeros in the number) is refused.
	if !bytes.Equal(EncodeIdentity(id), data) {
		return bad("not in canonical form")
	}
	return id, nil
}

func hexInto(dst []byte, s string) bool {
	if len(s) != 2*len(dst) {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}

// WriteIdentity writes the identity file durably and atomically. It
// refuses to overwrite an existing identity (ErrExist): a disk's identity
// never changes.
func WriteIdentity(fsys FS, id Identity) error {
	if _, err := fsys.Stat(IdentityFile); err == nil {
		return fmt.Errorf("%s: %w", IdentityFile, ErrExist)
	} else if !errors.Is(err, ErrNotExist) {
		return fmt.Errorf("write identity: %w", err)
	}
	if err := WriteFileAtomic(fsys, IdentityFile, EncodeIdentity(id)); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}
	return nil
}

// ReadIdentity reads and validates the identity file. A missing file is
// reported as an error wrapping ErrNotExist.
func ReadIdentity(fsys FS) (Identity, error) {
	data, err := ReadFile(fsys, IdentityFile)
	if err != nil {
		return Identity{}, fmt.Errorf("read identity: %w", err)
	}
	id, err := DecodeIdentity(data)
	if err != nil {
		return Identity{}, fmt.Errorf("read identity: %w", err)
	}
	return id, nil
}
