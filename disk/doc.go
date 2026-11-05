// Package disk is the file-system boundary of storage4. Everything that
// touches a disk goes through the FS interface; this package is the only
// place (with cmd/) allowed to call package os for files.
//
// Three implementations exist:
//
//   - OS: a real directory tree (one FS per disk, rooted at its mount point).
//   - Mem: an in-memory FS with an explicit crash model (see Mem.Crash).
//   - Faulty: a wrapper that injects errors, short writes and crashes at
//     chosen operations.
//
// The interface is positioned I/O only (no mmap, no append mode, no
// seeking): callers know their offsets, and the fault layers can reason
// about each write individually.
//
// Durability contract (what ext4/XFS give, and what Mem models):
//
//   - A write is durable only after File.Sync succeeds on that file.
//   - A file's existence, name and rename are durable only after SyncDir
//     on its directory succeeds.
//   - Between barriers, any subset of writes may reach the disk, in any
//     order, and a write may be torn at a 512-byte sector boundary. Nothing
//     larger than one sector is atomic.
//   - After a failed Sync, nothing is known about the unsynced data and the
//     caller must not retry Sync and believe it.
//
// Paths are slash-separated, relative to the FS root, and must satisfy
// io/fs.ValidPath. "." is the root.
package disk
