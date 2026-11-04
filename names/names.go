// Package names validates bucket and file names and implements the shard
// function. It is pure: no I/O, no globals. The rules are defined in
// docs/api.md §1 and are permanent.
package names

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// Limits fixed by the API contract.
const (
	MaxBucketLen = 128
	MaxNameLen   = 512
	MaxShards    = 1024
)

// Sentinel errors; callers match them with errors.Is.
var (
	ErrInvalidBucket     = errors.New("invalid bucket name")
	ErrInvalidName       = errors.New("invalid file name")
	ErrInvalidShardCount = errors.New("invalid shard count")
)

// ValidateBucket reports whether b is a valid bucket name: 1–128 characters
// from [A-Za-z0-9._-], not starting with '.' or '-' (which also excludes "."
// and "..").
func ValidateBucket(b string) error {
	if len(b) == 0 || len(b) > MaxBucketLen {
		return fmt.Errorf("%w: length %d not in 1..%d", ErrInvalidBucket, len(b), MaxBucketLen)
	}
	if b[0] == '.' || b[0] == '-' {
		return fmt.Errorf("%w: must not start with %q", ErrInvalidBucket, b[0])
	}
	for i := 0; i < len(b); i++ {
		if !isBucketByte(b[i]) {
			return fmt.Errorf("%w: byte %#02x at offset %d not allowed", ErrInvalidBucket, b[i], i)
		}
	}
	return nil
}

// ValidateName reports whether n is a valid file name: 1–512 bytes,
// '/'-separated segments of [A-Za-z0-9._:-], no empty, "." or ".." segment
// (so no leading or trailing '/'). Comparison is byte-exact and case-sensitive.
func ValidateName(n string) error {
	if len(n) == 0 || len(n) > MaxNameLen {
		return fmt.Errorf("%w: length %d not in 1..%d", ErrInvalidName, len(n), MaxNameLen)
	}
	start := 0
	for i := 0; i <= len(n); i++ {
		if i < len(n) && n[i] != '/' {
			if !isNameByte(n[i]) {
				return fmt.Errorf("%w: byte %#02x at offset %d not allowed", ErrInvalidName, n[i], i)
			}
			continue
		}
		seg := n[start:i]
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: empty, \".\" or \"..\" segment at offset %d", ErrInvalidName, start)
		}
		start = i + 1
	}
	return nil
}

// ValidateShardCount reports whether s is an allowed number of shards (1..MaxShards).
func ValidateShardCount(s uint32) error {
	if s < 1 || s > MaxShards {
		return fmt.Errorf("%w: %d not in 1..%d", ErrInvalidShardCount, s, MaxShards)
	}
	return nil
}

// Shard returns the shard of name in a bucket with shardCount shards:
// the first 8 bytes of SHA-256(name) as a big-endian uint64, modulo shardCount.
// The name is not validated here. shardCount must be at least 1.
func Shard(name string, shardCount uint32) (uint32, error) {
	if shardCount == 0 {
		return 0, fmt.Errorf("%w: 0", ErrInvalidShardCount)
	}
	sum := sha256.Sum256([]byte(name))
	return uint32(binary.BigEndian.Uint64(sum[:8]) % uint64(shardCount)), nil
}

func isBucketByte(c byte) bool {
	return isAlnum(c) || c == '.' || c == '_' || c == '-'
}

func isNameByte(c byte) bool {
	return isBucketByte(c) || c == ':'
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}
