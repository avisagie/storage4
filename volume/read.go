package volume

import (
	"bytes"
	"fmt"
	"io"

	"storage4/disk"
	"storage4/format"
)

// Head is a record whose header has been verified against its locator.
type Head struct {
	Type       format.RecordType
	SHA256     [32]byte
	Metadata   []byte
	PayloadLen int

	f       disk.File
	payload int64 // file offset of region B
	loc     Locator
}

// ReadHead reads the record at loc and verifies, before returning anything:
// magic, flags, header_CRC, seq, check, total length and name (docs/format-v1
// §5; CLAUDE.md invariant "every read verifies the record header"). An
// index entry that points at the wrong record yields ErrMismatch, never
// another file's data. An erased record yields ErrErased (Head is still
// returned for its geometry). name is the plaintext name the caller looked
// up. f must stay open while the Head's payload is read.
func ReadHead(f disk.File, loc Locator, name string) (*Head, error) {
	var fixed [format.RecordHeaderLen]byte
	if err := readFull(f, fixed[:], loc.Off); err != nil {
		return nil, fmt.Errorf("volume read @%d: %w", loc.Off, err)
	}
	fh, err := format.ParseFixedHeader(fixed[:])
	if err != nil {
		return nil, fmt.Errorf("volume read @%d: %w: %w", loc.Off, ErrMismatch, err)
	}
	if fh.Seq != loc.Seq || fh.Check != loc.Check || uint32(fh.Len(0)) != loc.Len {
		return nil, fmt.Errorf("volume read @%d: %w", loc.Off, ErrMismatch)
	}
	head := make([]byte, fh.HeadLen(0))
	if err := readFull(f, head, loc.Off); err != nil {
		return nil, fmt.Errorf("volume read @%d: %w", loc.Off, err)
	}
	r, payloadLen, headLen, err := format.DecodeHead(head, 0)
	if err != nil {
		return nil, fmt.Errorf("volume read @%d: %w: %w", loc.Off, ErrMismatch, err)
	}
	h := &Head{Type: r.Type, SHA256: r.SHA256, Metadata: r.Metadata, PayloadLen: payloadLen,
		f: f, payload: loc.Off + int64(headLen), loc: loc}
	switch r.Type {
	case format.TypeErased:
		return h, ErrErased
	case format.TypeFile:
	default:
		return nil, fmt.Errorf("volume read @%d: type %d: %w", loc.Off, r.Type, ErrNotFileRecord)
	}
	if !bytes.Equal(r.Name, []byte(name)) {
		return nil, fmt.Errorf("volume read @%d: name: %w", loc.Off, ErrMismatch)
	}
	return h, nil
}

// Payload returns a reader for the payload. It streams from disk and checks
// payload_CRC as it goes: when the last byte has been read it reports
// format.ErrCorrupt instead of io.EOF if the CRC does not verify. A caller
// that has already sent bytes must abort the response on that error
// (docs/api.md; decided 2026-11-05).
func (h *Head) Payload() io.Reader {
	return &payloadReader{h: h, left: int64(h.PayloadLen)}
}

// ReadAll returns the verified payload in memory.
func (h *Head) ReadAll() ([]byte, error) {
	r := h.Payload()
	b := make([]byte, h.PayloadLen)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	var one [1]byte
	if _, err := r.Read(one[:]); err != io.EOF {
		return nil, err
	}
	return b, nil
}

type payloadReader struct {
	h    *Head
	pos  int64 // bytes delivered
	left int64
	crc  uint32
	done bool
}

func (r *payloadReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.left > 0 {
		if len(p) == 0 {
			return 0, nil
		}
		n := int(min(int64(len(p)), r.left))
		if err := readFull(r.h.f, p[:n], r.h.payload+r.pos); err != nil {
			return 0, fmt.Errorf("volume read payload: %w", err)
		}
		r.crc = format.CRCUpdate(r.crc, p[:n])
		r.pos += int64(n)
		r.left -= int64(n)
		return n, nil
	}
	var t [4]byte
	if err := readFull(r.h.f, t[:], r.h.payload+int64(r.h.PayloadLen)); err != nil {
		return 0, fmt.Errorf("volume read payload: %w", err)
	}
	r.done = true
	if err := format.CheckPayloadCRC(r.crc, t[:]); err != nil {
		return 0, err
	}
	return 0, io.EOF
}
