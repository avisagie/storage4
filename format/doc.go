// Package format encodes and decodes the on-disk structures of a volume file:
// prelude, volume header, record framing, seal record and in-place erasure.
// The normative definition is docs/format-v1.md; this code implements it and
// never the reverse. The package is pure: no I/O, no clocks, no randomness.
// Decoders never panic on arbitrary bytes; every length is checked against
// the input size and the format limits before use.
package format
