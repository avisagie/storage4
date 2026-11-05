package volume

import "fmt"

// Layout of a volume on a disk (docs/disk.md): <bucket>/<16 hex digits>.vol.
// Paths are built here and nowhere else.

// Dir returns the directory of a bucket on a disk.
func Dir(bucket string) string { return bucket }

// Path returns the volume file path.
func Path(bucket string, id uint64) string { return fmt.Sprintf("%s/%016x.vol", bucket, id) }

// IdxPath returns the seq index path.
func IdxPath(bucket string, id uint64) string { return fmt.Sprintf("%s/%016x.idx", bucket, id) }

// NidxPath returns the name index path.
func NidxPath(bucket string, id uint64) string { return fmt.Sprintf("%s/%016x.nidx", bucket, id) }
