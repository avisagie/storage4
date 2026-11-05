package format

import (
	"encoding/binary"
	"fmt"
)

const sealPayloadLen = 24

// Seal is the content of a seal record (format-v1 §6).
type Seal struct {
	CommittedLength uint64 // offset at which the seal record starts
	Epoch           uint32 // final epoch of the volume
	RecordCount     uint32 // file and erased-file records before the seal
	LastSeq         uint32 // highest seq before the seal; 0 if none
}

// AppendSealRecord appends a seal record with the given seq and check.
func AppendSealRecord(dst []byte, seq, check uint32, s Seal) ([]byte, error) {
	var p [sealPayloadLen]byte
	le := binary.LittleEndian
	le.PutUint64(p[0:], s.CommittedLength)
	le.PutUint32(p[8:], s.Epoch)
	le.PutUint32(p[12:], s.RecordCount)
	le.PutUint32(p[16:], s.LastSeq)
	return AppendRecord(dst, Record{Type: TypeSeal, Seq: seq, Check: check, Payload: p[:]})
}

// DecodeSeal extracts the seal from a decoded seal record.
func DecodeSeal(r Record) (Seal, error) {
	if r.Type != TypeSeal || len(r.Name) != 0 || len(r.Metadata) != 0 ||
		r.SHA256 != [32]byte{} || len(r.Payload) != sealPayloadLen {
		return Seal{}, fmt.Errorf("seal: %w", ErrInvalid)
	}
	le := binary.LittleEndian
	if le.Uint32(r.Payload[20:]) != 0 {
		return Seal{}, fmt.Errorf("seal: reserved field: %w", ErrInvalid)
	}
	return Seal{
		CommittedLength: le.Uint64(r.Payload[0:]),
		Epoch:           le.Uint32(r.Payload[8:]),
		RecordCount:     le.Uint32(r.Payload[12:]),
		LastSeq:         le.Uint32(r.Payload[16:]),
	}, nil
}
