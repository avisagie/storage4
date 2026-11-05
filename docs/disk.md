# Disk layer (`disk/`)

Status: draft for review. The identity file below is the only on-disk artefact owned by `disk/`; once disks exist in the field it is as permanent as the volume format.

## Durability contract

The `disk.FS` interface is positioned I/O only. It promises what ext4 and XFS give, and the in-memory FS (`disk.Mem`) models exactly that:

- A write is durable only after `Sync` on that file succeeds. A name (create, rename, remove, mkdir) is durable only after `SyncDir` on its directory.
- Between barriers, **any subset** of the writes may reach the disk, in any order, and each write may be **torn at a 512-byte sector boundary**. Nothing larger than one sector is atomic; do not assume a 24-byte record header is, either, because records are only 8-aligned.
- After a failed `Sync` the unsynced data may be gone while later calls succeed. Callers treat the failure as fatal for that file (seal, do not retry).
- Atomic publish of a small file: `WriteFileAtomic` (write `name.tmp`, fsync, rename, fsync dir).

## Recovery rule (decided 2026-11-05)

Crash recovery of an open volume keeps the **longest valid prefix** and stops at the first invalid record, even if valid-looking records follow. Acked data is always inside the prefix, because an ack needs an fsync on every replica and fsync is a barrier. There is no "replicate the longest replica" step: seal at the minimum committed length and retry on a new volume. `format.Resync` exists for offline tooling (`scan`) only. A bad record *inside* the committed length of a sealed volume is bit rot, not a crash: read another replica.

## Crash model of `Mem`

`Mem.Crash(rng, mode)` rolls back to the durable state and then lets through some unsynced changes:

| Mode | Survivors |
|---|---|
| `CrashDropAll` | none |
| `CrashPrefix` | a random prefix of the pending writes (and name operations) |
| `CrashAny` | each sector-piece of each pending write independently (realistic; default for tests) |
| `CrashKeepAll` | everything |

Handles from before the crash fail with `ErrCrashed`. Sector size is a parameter (default 512; every 4 KiB tear is also a 512 B tear). `Faulty` adds error injection, short writes, ENOSPC, failed fsync (optionally dropping the dirty data) and crash at the nth operation (`CrashAt`).

## Identity file

`storage4.disk` at the root of every disk. Plain text, written once by the operator tooling, never overwritten. A node refuses a disk without a valid identity or whose cluster or node ID differs from its configuration.

```
storage4-disk 1
cluster_id <32 lower-case hex digits>
node_id <4 lower-case hex digits>
disk_id <32 lower-case hex digits>
created_unix_nano <decimal>
crc32c <8 lower-case hex digits>
```

The CRC-32C (same as `format`) covers every byte before the `crc32c` line. The decoder accepts only this exact canonical form. `node_id` is the 16-bit node part of volume IDs.

## Directory layout (decided 2026-11-05)

A node is configured with a list of directories, normally mount points (tests use temporary directories). Each becomes one `disk.OS` rooted there; the node never mounts anything. At startup every directory must hold a valid identity file whose cluster and node ID match the configuration, and no two directories may share a `disk_id`.

```
<disk root>/
  storage4.disk                  identity file (written once)
  FORMAT.txt                     copy of the plain-text format spec, written by the admin "add disk" command
  <bucket>/                      bucket names are filename-safe by rule
    <volume-id>.vol              volume (16 lower-case hex digits)
    <volume-id>.idx              seq index, cache
    <volume-id>.nidx             name index, written at seal, cache
    *.tmp                        leftovers of atomic writes; deleted at startup
```

- Paths are built on demand by `volume/` (the only package that knows the layout); `disk/` stays path-agnostic. Identity always comes from file contents, never from names.
- Creating a bucket directory on a disk needs `SyncDir` on the disk root as well as on the bucket directory.
- An `.idx` or `.nidx` without its `.vol` is an orphan: reported by `scan`, never deleted because the catalogue does not know the volume. Deletion happens only for volumes of buckets recorded as deleted.
