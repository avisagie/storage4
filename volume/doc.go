// Package volume manages one replica of a volume file on one disk: create,
// append with group commit, seal, scan/recovery, header-verifying reads and
// in-place erasure. Design and invariants: docs/volume.md; byte layout:
// docs/format-v1.md.
//
// Replication, epochs, leases and the choice of the committed length belong
// to node/; this package only guarantees what one file can guarantee.
package volume
