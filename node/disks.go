package node

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"storage4/catalogue"
	"storage4/disk"
	"storage4/format"
	"storage4/nameindex"
)

// counterFile holds, on every disk, the highest volume-ID counter value this
// node may have handed out (docs/node.md §3).
const counterFile = "node-counter"

// errCounterUnavailable: the counter reserve could not be written to any disk (503).
var errCounterUnavailable = errors.New("node: volume ID counter not writable on any disk")

// counterBlock is how many IDs are reserved per write of the counter files.
const counterBlock = 256

// localDisk is one disk of this node.
type localDisk struct {
	fs    disk.FS
	id    catalogue.DiskID
	store *nameindex.Store

	mu    sync.Mutex
	state catalogue.DiskState // from the catalogue, refreshed by the maintenance loop
	open  int                 // open volumes with a replica here
	// suspectUntil: after a replica failed here, no new volume is placed on
	// the disk until this time (injected clock). Operators decide "gone".
	suspectUntil time.Time
}

// suspectFor is how long a disk is avoided after a replica append failed.
const suspectFor = 10 * time.Minute

// placeable reports whether new volumes may be placed on the disk now.
func (d *localDisk) placeable(now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state == catalogue.DiskActive && !now.Before(d.suspectUntil)
}

func (d *localDisk) markSuspect(now time.Time) {
	d.mu.Lock()
	d.suspectUntil = now.Add(suspectFor)
	d.mu.Unlock()
}

func encodeCounter(v uint64) []byte {
	body := fmt.Sprintf("storage4-node-counter 1 %012x\n", v)
	return []byte(fmt.Sprintf("%s%08x\n", body, format.CRC([]byte(body))))
}

func decodeCounter(b []byte) (uint64, error) {
	s := string(b)
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	if len(lines) != 2 || !strings.HasSuffix(s, "\n") {
		return 0, errors.New("node counter: bad layout")
	}
	body := lines[0] + "\n"
	crc, err := strconv.ParseUint(lines[1], 16, 32)
	if err != nil || uint32(crc) != format.CRC([]byte(body)) {
		return 0, errors.New("node counter: bad checksum")
	}
	var v uint64
	if _, err := fmt.Sscanf(lines[0], "storage4-node-counter 1 %012x", &v); err != nil {
		return 0, errors.New("node counter: bad value")
	}
	return v, nil
}

// idCounter hands out the node-local 48-bit part of volume IDs. The reserved
// value is written to every disk before any ID beyond the previous reserve
// is used, so a crash can skip IDs but never repeat one.
type idCounter struct {
	mu       sync.Mutex
	node     catalogue.NodeID
	next     uint64 // next counter value to hand out
	reserved uint64 // highest value covered by the files
	disks    []*localDisk
}

// loadCounter reads the counter files of all disks, takes the maximum and
// raises it above every volume ID the catalogue knows for this node's disks
// (a safety net for lost or damaged counter files).
func loadCounter(ctx context.Context, node catalogue.NodeID, disks []*localDisk, cat catalogue.Catalogue) (*idCounter, error) {
	var hi uint64
	for _, d := range disks {
		b, err := disk.ReadFile(d.fs, counterFile)
		if err != nil {
			continue // missing or unreadable: the other disks and the catalogue cover it
		}
		if v, err := decodeCounter(b); err == nil && v > hi {
			hi = v
		}
	}
	for _, d := range disks {
		vols, err := cat.ListDiskVolumes(ctx, d.id)
		if err != nil {
			return nil, fmt.Errorf("node counter: %w", err)
		}
		for _, v := range vols {
			if v.ID.Node() == node {
				if c := uint64(v.ID) & (1<<48 - 1); c > hi {
					hi = c
				}
			}
		}
	}
	return &idCounter{node: node, next: hi + 1, reserved: hi, disks: disks}, nil
}

// allocate returns a fresh volume ID.
func (c *idCounter) allocate() (catalogue.VolumeID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next > 1<<48-1 {
		return 0, errors.New("node counter exhausted")
	}
	if c.next > c.reserved {
		want := c.next + counterBlock - 1
		data := encodeCounter(want)
		wrote := 0
		var firstErr error
		for _, d := range c.disks {
			if err := disk.WriteFileAtomic(d.fs, counterFile, data); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			wrote++
		}
		// Best effort: one disk holding the new reserve is enough. A start
		// reads the maximum over the disks it can read and the catalogue's
		// highest volume ID for this node covers the rest; an ID that was
		// handed out but never registered names no file, so reusing it is
		// harmless. Only failing everywhere refuses the ID.
		if wrote == 0 {
			return 0, fmt.Errorf("%w: %w", errCounterUnavailable, firstErr)
		}
		c.reserved = want
	}
	id := catalogue.VolumeID(uint64(c.node)<<48 | c.next)
	c.next++
	return id, nil
}
