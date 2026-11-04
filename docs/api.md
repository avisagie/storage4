# HTTP API v1

**Status: APPROVED 2026-11-04 (decisions in §5 confirmed). Permanent once a client integrates.** Rationale: `design-notes.md` §6, §10, §11. Sections marked **[REVIEW]** are choices the design notes left open.

Plain HTTP/1.1 (stdlib), served by any node; a node forwards to the shard owner internally. Internal locators (volume ID, seq, check, disk, node) are **never** exposed in any response, header or error body.

## 1. Names

**Bucket:** 1–128 characters from `[A-Za-z0-9._-]`; must not start with `.` or `-`; must not be `.` or `..`. Case-sensitive.

**File name:** 1–512 **bytes**; `/`-separated segments, each from `[A-Za-z0-9._:-]`; no empty segment, no `.` or `..` segment, no leading or trailing `/`. Case-sensitive, compared byte for byte. No percent-decoding beyond what the HTTP layer already does: the name is the raw URL path after `/b/{bucket}/`, and a path that contains `%` escapes of any character outside the set is invalid (`400`). **[REVIEW]**

Names are never used as filesystem paths. The shard of a name is `first 8 bytes of SHA-256(name) as big-endian u64, mod S`; S is fixed per bucket.

## 2. Endpoints

| Request | Result |
|---|---|
| `PUT /b/{bucket}/{name...}` | Store a version. `201` stored, `200` identical to the latest version (nothing stored) |
| `GET /b/{bucket}/{name...}` | Latest version's bytes |
| `HEAD /b/{bucket}/{name...}` | Headers only |
| `GET /m/{bucket}/{name...}` | Latest version's metadata bytes |
| `DELETE /b/{bucket}/{name...}` | Best-effort erase of **all** versions; `202` |
| `PUT /b/{bucket}` | Create bucket (idempotent) |
| `DELETE /b/{bucket}` | Delete bucket; `202` |

Any other method or path → `404` / `405`.

### 2.1 `PUT /b/{bucket}/{name...}`

Two body encodings, nothing else (`415` otherwise):

1. **Raw:** `Content-Type: application/octet-stream` (or absent); the body is the file. No metadata.
2. **Multipart:** `multipart/form-data` with an optional part named `metadata` **first**, then exactly one part named `file`. Used if and only if there is metadata. Unknown parts, `metadata` after `file`, a repeated part, or a missing `file` → `400`. The `metadata` part's content type is ignored; its bytes are stored as-is. An empty `metadata` part means no metadata.

Request headers (all optional):
- `Content-Digest: sha-256=:<base64>:` (RFC 9530) over the **file bytes**. Mismatch → `400`, nothing stored. Only `sha-256` is understood; other algorithms in the field are ignored.
- `If-None-Match: *` → `412` if the name already has any version.
- `If-Match: "<etag>"` → `412` unless the latest version's ETag equals it (and `412` if no version exists).
- Both conditions present → `400`. **[REVIEW]**

Behaviour:
- Compared **only with the latest version**: if the new file's SHA-256 equals the latest version's, nothing is stored and the response is `200`; otherwise a new version is stored and the response is `201`. Content flipping A → B → A stores A again. Metadata differences alone do not create a version. **[REVIEW]**
- Conditions are evaluated against the latest version before the dedup comparison.
- The response is sent only after the version is durable on **all** replicas.
- Success responses carry `ETag: "<hex sha-256>"` and `Repr-Digest: sha-256=:<base64>:`, with an empty body.
- The name's shard, volume and record are not reported.

Limits: file > 100 MiB or metadata > 16 KiB → `413` (checked as early as possible: the `metadata` part first lets the node reject before buffering the file). `Content-Length` over the limit is rejected before the body is read.

### 2.2 `GET` and `HEAD /b/{bucket}/{name...}`

- `200` with the latest version's bytes (`GET`), or headers only (`HEAD`). Headers: `ETag`, `Repr-Digest`, `Content-Length`, `Content-Type: application/octet-stream`.
- `If-None-Match: "<etag>"` equal to the latest ETag → `304`.
- Range requests are not supported in v1: `Range` is ignored and the full body returned. **[REVIEW]**
- `404` name unknown (or bucket unknown); `410` the name was erased, or the bucket is deleted.
- The response body is verified against the record (name, CRCs) before the first byte is sent; a corrupt replica is skipped in favour of another.

### 2.3 `GET /m/{bucket}/{name...}`

Returns the latest version's metadata bytes, byte for byte, `Content-Type: application/octet-stream`. `204` if the version has none. Same `404` / `410` rules as `GET`. Metadata is never returned in headers and never appears in logs.

### 2.4 `DELETE /b/{bucket}/{name...}`

Erases the name, content and metadata of **all versions**, best effort (in-place zeroing on every replica; tape, remapped SSD blocks and drives that left the site are not reached). Returns `202` once the erasure is recorded; reads return `410` from then on. Idempotent: erasing an unknown name returns `202` as well, and erasing again returns `202`. A later `PUT` of the same name stores a new version normally. Space is not reclaimed until the bucket is deleted.

### 2.5 `PUT /b/{bucket}`

Creates the bucket explicitly. Optional JSON body: `{"shards": N}` (`N` in 1..1024 **[REVIEW]**; default from deployment configuration). `201` created, `200` already exists with identical options, `409` exists with different options, `410` the bucket was deleted (a deleted bucket name never accepts writes again). Unknown JSON fields → `400`. Writing to a name in a bucket that does not exist creates it with default options implicitly.

### 2.6 `DELETE /b/{bucket}`

Marks the bucket deleted (its volumes are removed by pull-based GC). `202`; idempotent. Deleted buckets are remembered forever: every later request on the bucket returns `410`, including `PUT /b/{bucket}`. Restoring a bucket is an operator action, not an API call.

## 3. Errors

Error responses have a short plain-text body (`text/plain`, one line, no names, metadata or internal identifiers) and these statuses:

| Status | Meaning |
|---|---|
| `400` | invalid bucket/name, malformed multipart or JSON, `Content-Digest` mismatch, conflicting conditions |
| `404` | name or bucket unknown |
| `405` | method not allowed on this path |
| `410` | name erased, or bucket deleted |
| `412` | `If-None-Match` / `If-Match` condition failed |
| `413` | file > 100 MiB or metadata > 16 KiB |
| `415` | unsupported request content type |
| `503` + `Retry-After` | node memory budget full, shard in handover, catalogue unavailable for writes, or no healthy replica set |
| `500` | internal error; nothing is promised about the write |

## 4. Semantics clients must know

- **Idempotent writes:** retrying a `PUT` of the same bytes is safe; the retry returns `200` or `201` with the same ETag.
- **Indeterminate outcomes:** after a `503` (or a dropped connection) during a `PUT`, the write may or may not become visible. If the client never retries and the name had an earlier version, the unacknowledged record can still become the latest version. Clients that care must retry the same bytes, or read back.
- **Ordering:** the latest version of a name is defined by the shard owner's append order, never by clocks. The application guarantees no concurrent writers of the same name; use `If-None-Match` / `If-Match` for single-writer checks.
- **Read-your-writes:** after a successful `PUT` response, a `GET` on any node returns that version or a newer one.
- **Metadata** is opaque, ≤ 16 KiB, accepted only in the multipart `metadata` part, returned only by `GET /m/…`. Never in URLs, query strings or headers.
- **ETag** is the lower-case hex SHA-256 of the file bytes, in double quotes; a weak validator is never used. Identical content therefore has the same ETag across names and versions.
- **Authentication:** a shared token inside the air gap, sent as `Authorization: Bearer <token>`; missing or wrong → `401`. TLS later. **[REVIEW]**
- **Query strings** on `/b/` and `/m/` requests are rejected with `400`.

## 5. Decisions (confirmed 2026-11-04)

1. Percent-escapes outside the name alphabet → `400`.
2. `If-None-Match: *` together with `If-Match` → `400`.
3. Metadata-only changes do not create a new version; dedup looks at file bytes only.
4. Conditions are checked before dedup.
5. No `Range` support in v1.
6. Bucket `shards` option range 1..1024; implicit bucket creation on first write.
7. `DELETE` of an unknown name returns `202`.
8. Bearer-token authentication and `401`.
9. Query strings rejected.
