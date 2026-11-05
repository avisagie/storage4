package nameindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"storage4/format"
)

// On-disk table of a `.nidx`; layout and rules in docs/nameindex.md.

var magic = [8]byte{0x89, 'S', '4', 'N', 'I', 'X', '\r', '\n'}
var footerMagic = [4]byte{'S', '4', 'N', 'F'}

const (
	version = 1
	// headerLen is the padded header size; padding makes every bucket one
	// page-aligned 4 KiB read.
	headerLen      = 4096
	headerFields   = 44 // bytes of real header fields, covered by header_CRC
	bucketSize     = 4096
	slotLen        = 16                       // fingerprint u64 | seq u32 | offset/8 u32
	slotsPerBucket = 255                      // 255*16 = 4080, leaving 16 bytes for the bucket trailer
	trailerOff     = slotsPerBucket * slotLen // 4080: count u32, CRC u32, 8 reserved
	footerLen      = 16
	// targetPerBucket keeps the load factor at or below 0.7 (178/255).
	targetPerBucket = 178
	// maxOff8 is the largest offset/8 a slot can hold (offsets up to 32 GiB).
	maxOff8 = 1<<32 - 1
)

// ErrBadNidx means a name index file is unusable. The caller rebuilds it;
// this is never fatal.
var ErrBadNidx = errors.New("nameindex: unusable index file")

// Entry is one table slot: a candidate record for some name. It holds no
// name and no content hash.
type Entry struct {
	// FP is the name's fingerprint. Equal names always give equal FPs, but
	// different names may collide, so a match is only a candidate.
	FP uint64
	// Seq is the seq of the record in the volume (>= 1). Within one name the
	// highest seq is the latest version.
	Seq uint32
	// Off is the record's byte offset in the .vol file: a multiple of 8,
	// > 0, at most 32 GiB (stored as offset/8 in a u32).
	Off int64
}

// Fingerprint is bytes 8–16 of SHA-256(name), big endian. Deliberately not
// the bytes the shard function uses. Once volumes are encrypted this becomes
// an HMAC with an index sub-key (docs/nameindex.md).
func Fingerprint(name []byte) uint64 {
	h := sha256.Sum256(name)
	return binary.BigEndian.Uint64(h[8:16])
}

func bad(what string) error { return fmt.Errorf("nidx: %s: %w", what, ErrBadNidx) }

func bucketCount(entries int) int {
	return max(1, (entries+targetPerBucket-1)/targetPerBucket)
}

// Encode returns the file bytes for the entries. The output is a pure
// function of volumeID and the set of entries.
func Encode(volumeID uint64, entries []Entry) ([]byte, error) {
	if uint64(len(entries)) > 1<<31 {
		return nil, errors.New("nidx: too many entries")
	}
	es := append([]Entry(nil), entries...)
	sort.Slice(es, func(i, j int) bool {
		if es[i].FP != es[j].FP {
			return es[i].FP < es[j].FP
		}
		return es[i].Seq < es[j].Seq
	})
	seen := make(map[uint32]bool, len(es))
	for _, e := range es {
		if e.Seq == 0 || e.Off <= 0 || e.Off%8 != 0 || e.Off/8 > maxOff8 {
			return nil, fmt.Errorf("nidx: invalid entry seq %d offset %d", e.Seq, e.Off)
		}
		if seen[e.Seq] {
			return nil, fmt.Errorf("nidx: seq %d twice", e.Seq)
		}
		seen[e.Seq] = true
	}
	bc := bucketCount(len(es))
	le := binary.LittleEndian
	out := make([]byte, headerLen+bc*bucketSize+footerLen)
	copy(out, magic[:])
	le.PutUint16(out[8:], version)
	le.PutUint32(out[12:], headerLen)
	le.PutUint64(out[16:], volumeID)
	le.PutUint32(out[24:], uint32(len(es)))
	le.PutUint32(out[28:], uint32(bc))
	le.PutUint32(out[32:], bucketSize)
	le.PutUint32(out[headerFields:], format.CRC(out[:headerFields]))
	table := out[headerLen : headerLen+bc*bucketSize]
	counts := make([]int, bc)
	for _, e := range es {
		b := int(e.FP % uint64(bc))
		for counts[b] == slotsPerBucket {
			b = (b + 1) % bc
		}
		s := table[b*bucketSize+counts[b]*slotLen:]
		le.PutUint64(s, e.FP)
		le.PutUint32(s[8:], e.Seq)
		le.PutUint32(s[12:], uint32(e.Off/8))
		counts[b]++
	}
	for b := range bc {
		raw := table[b*bucketSize : (b+1)*bucketSize]
		le.PutUint32(raw[trailerOff:], uint32(counts[b]))
		le.PutUint32(raw[trailerOff+4:], format.CRC(raw[:trailerOff+4]))
	}
	f := out[len(out)-footerLen:]
	copy(f, footerMagic[:])
	le.PutUint32(f[4:], uint32(len(es)))
	le.PutUint32(f[8:], format.CRC(table))
	return out, nil
}

// header holds the validated fields of a .nidx header.
type header struct {
	volumeID uint64 // the volume this index belongs to; a mismatch means a foreign file
	count    uint32 // distinct names (entries) in the table
	buckets  uint32 // number of 4 KiB buckets, >= 1; fixes the file size
}

// size is the exact file size the header implies.
func (h header) size() int64 { return headerLen + int64(h.buckets)*bucketSize + footerLen }

// parseHeader validates the first headerLen bytes.
func parseHeader(b []byte, volumeID uint64) (header, error) {
	le := binary.LittleEndian
	if len(b) < headerLen {
		return header{}, bad("short header")
	}
	if [8]byte(b[:8]) != magic {
		return header{}, bad("magic")
	}
	if le.Uint16(b[8:]) != version || le.Uint16(b[10:]) != 0 ||
		le.Uint32(b[12:]) != headerLen || le.Uint32(b[32:]) != bucketSize ||
		le.Uint64(b[36:]) != 0 {
		return header{}, bad("version")
	}
	if format.CRC(b[:headerFields]) != le.Uint32(b[headerFields:]) {
		return header{}, bad("header crc")
	}
	if !allZero(b[headerFields+4 : headerLen]) {
		return header{}, bad("header padding")
	}
	h := header{volumeID: le.Uint64(b[16:]), count: le.Uint32(b[24:]), buckets: le.Uint32(b[28:])}
	if h.volumeID != volumeID {
		return header{}, bad("volume id")
	}
	if h.buckets == 0 || h.buckets > 1<<24 || uint64(h.count) > uint64(h.buckets)*slotsPerBucket {
		return header{}, bad("counts")
	}
	return h, nil
}

// parseFooter validates the footer against the header. It returns the
// table CRC the footer claims.
func parseFooter(f []byte, h header) (tableCRC uint32, err error) {
	le := binary.LittleEndian
	if len(f) != footerLen || [4]byte(f[:4]) != footerMagic || le.Uint32(f[12:]) != 0 ||
		le.Uint32(f[4:]) != h.count {
		return 0, bad("footer")
	}
	return le.Uint32(f[8:]), nil
}

// parseBucket validates one bucket and returns its slots in use.
func parseBucket(raw []byte) (n int, err error) {
	le := binary.LittleEndian
	if len(raw) != bucketSize {
		return 0, bad("short bucket")
	}
	if format.CRC(raw[:trailerOff+4]) != le.Uint32(raw[trailerOff+4:]) || !allZero(raw[trailerOff+8:]) {
		return 0, bad("bucket crc")
	}
	c := le.Uint32(raw[trailerOff:])
	if c > slotsPerBucket {
		return 0, bad("bucket count")
	}
	n = int(c)
	for i := range slotsPerBucket {
		s := raw[i*slotLen : (i+1)*slotLen]
		if i < n {
			if le.Uint32(s[8:]) == 0 || le.Uint32(s[12:]) == 0 {
				return 0, bad("empty slot in use")
			}
		} else if !allZero(s) {
			return 0, bad("slot beyond count")
		}
	}
	return n, nil
}

func slot(raw []byte, i int) Entry {
	le := binary.LittleEndian
	s := raw[i*slotLen:]
	return Entry{FP: le.Uint64(s), Seq: le.Uint32(s[8:]), Off: int64(le.Uint32(s[12:])) * 8}
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Table is a fully decoded name index, for scrub, tests and fuzzing. Lookups
// on disk use Reader instead.
type Table struct {
	// VolumeID is the volume the index was decoded for.
	VolumeID uint64
	// Entries holds every slot, sorted by (FP, Seq): the order of Encode's
	// placement, not of the file.
	Entries []Entry
}

// Candidates returns the entries with fingerprint fp, highest seq first.
func (t *Table) Candidates(fp uint64) []Entry {
	var out []Entry
	for i := len(t.Entries) - 1; i >= 0; i-- {
		if t.Entries[i].FP == fp {
			out = append(out, t.Entries[i])
		}
	}
	return out
}

// DecodeTable parses and fully validates a .nidx for volumeID: header,
// footer, every bucket, the table CRC, and canonical placement (the bytes
// must equal what Encode produces for the same entries). Any problem is an
// error wrapping ErrBadNidx. It never panics.
func DecodeTable(b []byte, volumeID uint64) (*Table, error) {
	h, err := parseHeader(b, volumeID)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != h.size() {
		return nil, bad("size")
	}
	table := b[headerLen : len(b)-footerLen]
	crc, err := parseFooter(b[len(b)-footerLen:], h)
	if err != nil {
		return nil, err
	}
	if format.CRC(table) != crc {
		return nil, bad("table crc")
	}
	t := &Table{VolumeID: volumeID}
	for i := range int(h.buckets) {
		raw := table[i*bucketSize : (i+1)*bucketSize]
		n, err := parseBucket(raw)
		if err != nil {
			return nil, err
		}
		for j := range n {
			t.Entries = append(t.Entries, slot(raw, j))
		}
	}
	if uint32(len(t.Entries)) != h.count {
		return nil, bad("entry count")
	}
	again, err := Encode(volumeID, t.Entries)
	if err != nil || !bytes.Equal(again, b) {
		return nil, bad("not canonical")
	}
	sort.Slice(t.Entries, func(i, j int) bool {
		if t.Entries[i].FP != t.Entries[j].FP {
			return t.Entries[i].FP < t.Entries[j].FP
		}
		return t.Entries[i].Seq < t.Entries[j].Seq
	})
	return t, nil
}

// Builder collects the distinct names of a volume, keeping the highest seq
// per name, and encodes them. It sees names, the table never stores them.
type Builder struct {
	// fp computes fingerprints; injectable so tests can force collisions.
	fp func([]byte) uint64
	// best maps each distinct name to its entry with the highest seq so far.
	// It is the only place names are held, in memory, and never written.
	best map[string]Entry
}

// NewBuilder returns a Builder using Fingerprint.
func NewBuilder() *Builder { return newBuilder(Fingerprint) }

func newBuilder(fp func([]byte) uint64) *Builder {
	return &Builder{fp: fp, best: map[string]Entry{}}
}

// Add records that name has a record at (seq, off).
func (b *Builder) Add(name []byte, seq uint32, off int64) error {
	if seq == 0 || off <= 0 || off%8 != 0 || off/8 > maxOff8 {
		return fmt.Errorf("nidx: invalid entry seq %d offset %d", seq, off)
	}
	if e, ok := b.best[string(name)]; ok && e.Seq >= seq {
		return nil
	}
	b.best[string(name)] = Entry{FP: b.fp(name), Seq: seq, Off: off}
	return nil
}

// Len returns the number of distinct names.
func (b *Builder) Len() int { return len(b.best) }

// Encode returns the .nidx bytes for volumeID.
func (b *Builder) Encode(volumeID uint64) ([]byte, error) {
	es := make([]Entry, 0, len(b.best))
	for _, e := range b.best {
		es = append(es, e)
	}
	return Encode(volumeID, es)
}
