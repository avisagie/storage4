# Volume layer (`volume/`)

Status: draft for review (seal and seq logic are a human gate). `volume/` manages **one replica of one volume file on one disk**. Replication, epochs, leases and the choice of committed length belong to `node/`. Byte layout: `docs/format-v1.md`; durability and recovery rule: `docs/disk.md`. The `.idx` seq index is not implemented yet (next step).

## API

| Function | What it does |
|---|---|
| `Create(fsys, header, opts)` | Writes the header via `WriteFileAtomic` (tmp, fsync, rename, `SyncDir`), so the path holds either nothing or a valid empty volume. Refuses an existing volume. Creates the bucket directory with `SyncDir` on root and bucket. |
| `Writer.Append(entries)` | Group commit: all entries become consecutive records, one `Sync`. Returns one `Locator{Seq, Check, Off, Len}` per entry, final only after success. |
| `Writer.Seal(epoch)` | Seal record at `Committed()`, one `Sync`; closes the writer. |
| `SealFile(fsys, path, committed, epoch, rnd)` | Seal a replica without a (working) writer: restart, handover, replica failure. |
| `Scan(file, fn)` | Streaming scan of the longest valid prefix; calls `fn` per record; payloads are CRC-checked in 1 MiB chunks. |
| `ReadHead` / `Head.Payload` / `Head.ReadAll` | Header-verifying read. |
| `Erase(file, items)` | In-place erasure per format-v1 §7, one fsync per batch. |

## Seq logic

- A `Writer` hands out seqs from a counter that only moves forward. All seqs of a batch are consumed **before** anything is written.
- Any I/O error (write or sync) disables the writer for good (`ErrFailed`); it never retries and never reuses a seq. `Committed()` still names the last acknowledged length.
- Validation errors (`ErrInvalidEntry`) and `ErrFull` are detected before any seq is consumed, so they leave the writer usable.
- Seqs are strictly increasing in file order, so the valid prefix also ends where a seq does not increase.
- **The seal's seq is the prefix's last seq + 1, which may equal the seq of a discarded unacknowledged record.** This is deliberate. The seal record is never encrypted (no tag, no nonce), and nothing is ever appended to a sealed volume, so no AES-GCM nonce (volume ID ‖ seq) can repeat. Reviewer: please confirm this reading of the invariant.

## Seal logic

- Committed length = end of the last record made durable by a successful `Sync`; for a volume with several replicas, the caller passes the **minimum** over replicas.
- `SealFile`: scan `[0, committed)` (must be valid and end exactly at `committed`, else `ErrShortReplica` and nothing is changed); truncate to `committed`; write the seal record at `committed`; sync once. Any crash leaves either an unsealed volume with the same valid prefix (possibly with the seal written and stale tail bytes behind it, which is not "sealed" per format-v1 §6 because the file does not end at the seal) or a sealed one. Running `SealFile` again always finishes.
- Already sealed at `committed`: no-op returning the existing seal. Sealed at another length: `ErrSealMismatch`.
- The scan after a seal ignores everything after the first seal record.

## Reads

`ReadHead` verifies magic, flags, header CRC, seq, check, total length and name against the locator before returning anything (`ErrMismatch` otherwise: stale index or corruption). `Head.Payload()` streams and returns `format.ErrCorrupt` instead of EOF when the payload CRC fails; callers that already sent bytes must abort the connection.

## Erasure

Verify first (header CRC, seq, check, length, name), then write the erased head in one contiguous write, then zeros over region B, then one fsync for the batch. A record whose head write was torn by a crash (or that is already erased) is finished from the 24-byte header and the locator alone, without a name check. Meant for sealed volumes: on an open volume a crash inside the head write can end the valid prefix there.

## Tests (L1, `disk.Mem` + `disk.Faulty`)

Crash sweeps (crash before/after every operation, all four crash modes) over: append batches (every acknowledged record is in the prefix with its locator; the volume can then be sealed at the acknowledged length), `SealFile`, and erasure (retry completes; other records intact). Plus: one sync per batch, failed append then `SealFile`, refusals that consume no seq, stop at first invalid record, non-increasing seq, locator verification, streaming CRC failure.
