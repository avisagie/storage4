# MVP plan

Working document; update as milestones finish. Specs and design live in `docs/`.

Assumptions: one node manages many disks (identity file per disk: cluster, node, disk ID); public embedding API in package `node` (`Start`/`Stop`, `Alarms`, `Metrics`; design-notes §15); catalogue has PostgreSQL (production), SQLite (tests/dev only) and in-memory fake implementations behind one interface and one contract suite.

| # | Milestone | Layer | Human gate |
|---|---|---|---|
| 0 ✅ | **Specs first.** `docs/format-v1.md`, `docs/api.md` from design-notes §6, §7, §10. No code before they are reviewed. | docs | Approve both specs (permanent) |
| 1 | **`names/` ✅ (reviewed 2026-11-04), `format/` ✅ implemented, awaiting human review (format code, golden files).** Name validation, shard function; prelude, volume header, record framing (two regions, empty tag fields), seal record, reserved types. Fuzzing (decoders never panic, scanner resyncs), golden files. | L0 | Shard function, format code, golden files |
| 2 ✅ | **`disk/` implemented (docs/disk.md); identity file format awaiting a look.** FS interface: real, in-memory, fault-injecting (short write, fsync error, ENOSPC, torn write, crash-at-any-write). Per-disk identity file. | L1 | — |
| 3 | **`db/`, `catalogue/`.** Sketch and review the `Catalogue` interface first. Migration 0001 (buckets, shard leases, volumes, erasure list, `catalogue_meta`). In-memory fake, PostgreSQL, SQLite (`modernc.org/sqlite`); one contract suite over all three. `store db init/migrate/status/verify`. | L2, contract | Interface; migrations; lease/ordinal/epoch SQL |
| 4 | **`volume/`.** Single-writer append, never-reused seq, group commit (measure one fsync per append first), seal, crash recovery, `.idx` tail scan/rebuild, header-verifying read. | L1 | Seal and seq logic |
| 5 | **`nameindex/`.** Open-volume name map, `.nidx` build and positioned-read lookup (candidate, then verify name), handle LRU with idle close. No content hashes in index files. | L0, L1 | — |
| 6 | **`node` single-node.** `Start`/`Stop`, `Alarms`, `Metrics`; HTTP PUT/GET/HEAD/DELETE, dedup against latest only, conditional writes, digest headers, memory-budget `503`; 3 disks with local replica I/O; restart and goroutine-leak tests. | L1, in-process e2e | Dedup and version ordering |
| 7 | **`sim/` and multi-node.** Simulator and invariants first. Then transport, forwarding, shard leases and handover, 3× replication with all-replica fsync, seal-on-failure, placement (power of two choices), restart-seals, reconcile/GC. Each new production fault gets a sim fault event. | L3 | Leases, handover, epochs, seal, GC deletion |
| 8 | **Tools.** `scan verify` / `scan rebuild`, in-place erasure and erasure list, `admin` (delete bucket, repair, erase, read-only/gone, assign node ID). | L1, L3 | Erasure, GC |
| 9 | **`cmd/store` and L4.** Flags/signals only; Docker Compose drills (`kill -9`, disk full, PG failover runbook, handover, `scan rebuild`). | L4 | Failover runbook |

Notes
- 1 and 2 can run in parallel after specs are approved; 3 can run alongside 4 and 5. Milestone 7 carries the most risk.
- Milestone 6 precedes the simulator so the node's shape (per-disk FS, `Deps`) is fixed before `sim/` drives it.
- SQLite cannot mimic PostgreSQL advisory locks or `SKIP LOCKED`; lease semantics must rest on plain transactions so all three implementations pass the same contract suite. SQLite is never a production catalogue.
- Open: listener ownership (node-owned vs host-mounted `Handler()`), settled in milestone 6.
- Decided 2026-11-05: PUT is fully buffered under the memory budget (sha256 sits in region A before the payload; buffering also enables parallel and atomic writes; revisit only if the size cap is lifted). GET streams and aborts the connection if payload_CRC fails (optimistic, fast in the normal case). Erasure writes plain zeros (no hole-punching), head first (format-v1 §7), one fsync per volume per batch, and the erase RPC never carries the zeros. Header CRC is verified before erasing.
- Decided 2026-11-05: crash model is "any subset of unsynced writes survives, torn at 512 B sectors" (`Mem.Crash`, `CrashAny`); names need `SyncDir` (ext4/XFS). Recovery of an open volume = longest valid prefix, stop at the first invalid record; no longest-replica replication (seal at the minimum committed length, retry on a new volume); `format.Resync` is for tooling only. Details in `docs/disk.md`.
