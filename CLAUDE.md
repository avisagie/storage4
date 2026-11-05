# CLAUDE.md

Air-gapped, long-lived store for many small files (≤ 100 MiB each), addressed by (bucket, name) with idempotent writes. One static Go binary, PostgreSQL as the only source of truth. Data must stay readable for 15–20 years.

Design notes: `docs/design-notes.md`. Specs (source of truth — code implements the specs, never the reverse): on-disk format `docs/format-v1.md`, HTTP API `docs/api.md`. Read the relevant section before changing a package.

## Definition of done

```
make check   # gofmt, go vet, staticcheck, go test -race ./...
```

A task is done only when `make check` passes. Fast loop (L0–L1 + short simulation) must stay under ~30 s. Add tests in the same change as the code.

## Permanent things — do not change without explicit human approval

- **On-disk format** (prelude, volume header incl. bucket/shard/S/volume number/cipher ID/key generation, record framing incl. the two regions name‖sha256‖metadata / payload with their tag fields, seal record). Any change = new format version; old versions stay readable forever.
- **HTTP API contract**: `PUT|GET|HEAD|DELETE /b/{bucket}/{name...}`, `GET /m/{bucket}/{name...}`, `PUT|DELETE /b/{bucket}`; `ETag` = SHA-256, `Repr-Digest`, `Content-Digest` (400 on mismatch), `If-None-Match: *` / `If-Match` (412); `201` stored vs `200` identical-to-latest. Internal locators are never exposed.
- **Name rules**: bucket 1–128 chars of `[A-Za-z0-9._-]`, not starting with `.`/`-`, not `.`/`..`. File name 1–512 bytes, `/`-separated segments of `[A-Za-z0-9._:-]`, no empty/`.`/`..` segments, no leading/trailing `/`. Byte-exact, case-sensitive.
- **Shard function**: `shard = first 8 bytes of SHA-256(name) as big-endian u64 mod S`; S fixed per bucket and recorded in every volume header.
- **ID spaces**: volume ID = `node_id(16) | node-local counter(48)`; node IDs never reused; per-(bucket, shard) volume numbers assigned by the catalogue.
- **Released migrations** in `db/migrations/` and **golden files** in `format/testdata/`. CI rejects edits. Add a new migration / new golden file instead.

## Invariants — never break, never weaken a test that checks them

- Only the node holding the catalogue's shard lease for (bucket, shard) opens or appends to that shard's volumes. On handover, the previous owner's open volumes are sealed (epoch bump) before the new owner writes.
- Latest version of a name = highest (volume number, seq). Never order versions by clocks.
- Dedup compares only with the latest version: identical sha256 → `200`, store nothing; otherwise append a new version.
- A seq is never reused within a volume, even after a failed append (required for AES-GCM nonces: nonce = volume ID ‖ seq).
- Ack only after **all** replicas fsync. Never ack on quorum.
- One writer per open volume; replicas reject stale epochs.
- A restarted owner never resumes an open volume: it seals it (epoch bump, minimum committed length) and opens a new one.
- On replica failure: **seal, don't repair** — seal at committed length, bump epoch, retry on a new volume.
- Every read verifies the record header (magic, header CRC, seq, check, **name**) against the index entry before returning data. `.idx` and `.nidx` are caches: never trust them without that check; on mismatch, rebuild from the volume.
- `.idx` / `.nidx` formats carry their own version; unknown version, volume-ID mismatch or bad footer → discard and rebuild, never fail permanently. A fingerprint match in `.nidx` is only a candidate: verify the name in the record (a collision costs a read, never a wrong answer).
- Index files never contain content hashes or (once encryption exists) plaintext names.
- Records never move or change size. Erasure zeroes name, sha256, metadata and payload in place, sets type `erased-file`, rewrites header CRC only; all versions of the name go on the erasure list.
- GC deletes only volumes of buckets **recorded as deleted** — never volumes merely unknown to the catalogue.
- Deleted buckets never accept writes again; restore is an explicit state.
- Metadata is optional, opaque pass-through (≤ 16 KiB): stored and returned byte-for-byte, never parsed. Accepted only in the request body — never in URLs, query parameters or headers, and never logged.
- No business concepts in storage code (event time, retention rules, event IDs, ES keys).
- No per-file rows in PostgreSQL — only buckets, shard leases, volumes, erasure list.
- Nodes never run DDL and never auto-initialise the catalogue; refuse to start on an empty DB or cluster-ID mismatch.
- Volume IDs and volume numbers never come from anything that can go back in time.
- Encryption (cipher 0 = none is the MVP default): keys are never written to node disks and never logged; a bucket key is committed to the catalogue before first use; destroying a bucket key is irreversible and needs explicit operator confirmation.

## Code rules

- Inject `Clock`, random source, FS, transport and `Catalogue`. No `time.Now()`, `time.Sleep`, global `math/rand` or direct `os` file calls outside `disk/` and `cmd/`.
- `format/` and `names/` are pure: no I/O, no globals.
- Decoders must never panic on arbitrary bytes; return errors.
- No CGO (no RocksDB). Standard library first (including `crypto/aes`, `crypto/cipher`, `crypto/hkdf`). `.nidx` is our own immutable hash table, built in memory, written once and read with positioned reads through the FS interface (no mmap, no Pebble). Approved dependencies: `pgx` (PostgreSQL), `testcontainers-go` (L2 tests), `modernc.org/sqlite` (pure Go; SQLite catalogue for tests/dev, never production). Other new dependencies need human approval.
- Bounded resources: request buffering under a byte-sized memory budget (`503` when full); index file handles in a bounded LRU, opened on demand and closed on idle timeout.
- SQL is plain, readable SQL — no ORM. Migrations: transactional statements only, `text` + `CHECK` instead of enums, no `CREATE INDEX CONCURRENTLY`.
- The node is embeddable (`docs/design-notes.md` §15): `Start`/`Stop` restartable in-process; no package-level state, `os.Exit` or `log.Fatal` outside `cmd/`; alarms and metrics go through the injected `Alarms` / `Metrics` interfaces, never carrying names, metadata or keys.
- Errors wrap with context (`fmt.Errorf("…: %w", err)`); no silent swallowing on the write path.

## Package layout

```
format/     pure encode/decode of on-disk structures
names/      pure bucket/name validation, shard function
disk/       FS interface: real, in-memory, fault-injecting
clock/      Clock interface + Fake (real clock lives in cmd/)
catalogue/  Catalogue interface; sqlcat/ = PostgreSQL (production) + SQLite (tests/dev only) over database/sql; mem/ = in-memory fake; cattest/ = contract suite
volume/     single-writer append, group commit, seal, erase, .idx
nameindex/  open-volume name map, .nidx build (in memory, write once) / read (positioned reads), file-handle LRU
transport/  node-to-node RPC (forwarding, replica I/O): HTTP impl + in-memory
node/       public embedding API (Start/Stop/Alarms/Metrics, §15); API handlers, routing, shard leases and handover, lookup, reconcile/GC
scan/ admin/ db/   tools
sim/        deterministic multi-node simulator
cmd/store/  flags, signals, calls node.Start — wiring only
```

## Testing layers — put tests in the right one

| Layer | Use for | Tools |
|---|---|---|
| L0 pure | format, names, shard function, CRC, nonce construction, `.nidx` table encode/decode | table tests, round-trip, `go test -fuzz`, golden files |
| L1 component | volume writer, erasure, `.idx`, `.nidx`, restart recovery | in-memory / fault-injecting FS (short write, fsync error, ENOSPC, torn write, crash-at-any-write); corrupted/wrong-version index files; fingerprint collisions |
| L2 catalogue | SQL, shard/volume leases, volume numbers, epochs, migrations | PostgreSQL, either local (`STORAGE4_TEST_PG_DSN`) or testcontainers-go — both are first class and run the same suite; migrations empty→latest, snapshot→latest, run twice |
| Contract | fake vs real catalogue | one suite run against both; extend it whenever the `Catalogue` interface changes |
| L3 simulation | anything involving more than one node (forwarding, handover, replication) | `sim/`: seeded scheduler, virtual clock, fault events, invariant checks every step |
| L4 e2e | real binaries, nightly | Docker Compose, `kill -9`, disk full, PG failover, shard handover, `scan rebuild` |

- L2 and contract tests need a PostgreSQL. Start a throwaway local one with `make pg-start` (`scripts/pg-start.sh`; no root, trust auth, localhost, fsync off, data in `~/pgdata`) and stop it with `make pg-stop` (`scripts/pg-stop.sh`, `--purge` deletes the data). The script prints the `STORAGE4_TEST_PG_DSN` to export. This is the default for agent sessions: they already run inside a container, so testcontainers (Docker-in-container) is generally unavailable there. L2 tests use the DSN when set and testcontainers otherwise (CI, developer machines). You may start and stop the local one yourself; never point tests at any other database.
- A failing simulation seed becomes a named regression test with that seed.
- New fault type in production code paths → add a matching fault event to `sim/`.
- Never fix a failing test by loosening its assertion; if the test is wrong, say so and stop for review.

## Always stop and ask a human before

- Changing anything under "Permanent things".
- Changing shard lease, handover, epoch, seal, version-ordering, dedup or GC-deletion logic.
- Removing or weakening an invariant check or assertion.
- Adding a dependency.

## Working style

- One package or behaviour per task. Read the design-notes section first; state which invariant the change touches and which test layer covers it.
- Keep diffs small and reviewable; no drive-by refactors.
- Update `docs/` in the same change when behaviour, API or format documentation changes.
- Update `mvp-plan.md` in every commit that finishes, starts or changes a milestone (status, decisions with dates, open items), so the plan never lags the code.
