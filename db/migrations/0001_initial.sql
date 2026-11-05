-- kind: expand
-- Initial catalogue schema. Portable PostgreSQL/SQLite subset (docs/catalogue.md).
-- Times are Unix milliseconds from the injected clock. IDs are lower-case hex text
-- so a rescue session can match them with file names. States are text + CHECK.

CREATE TABLE catalogue_meta (
  singleton      int    NOT NULL PRIMARY KEY CHECK (singleton = 1),
  cluster_id     text   NOT NULL CHECK (length(cluster_id) = 32 AND cluster_id = lower(cluster_id)),
  cluster_name   text   NOT NULL CHECK (length(cluster_name) BETWEEN 1 AND 128),
  schema_version int    NOT NULL,
  min_compat     int    NOT NULL,
  incarnation    bigint NOT NULL,
  created_ms     bigint NOT NULL
);

CREATE TABLE nodes (
  node_id    int    NOT NULL PRIMARY KEY CHECK (node_id BETWEEN 1 AND 65535),
  name       text   NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 128),
  created_ms bigint NOT NULL
);

CREATE TABLE node_beats (
  node_id        int    NOT NULL PRIMARY KEY REFERENCES nodes (node_id),
  instance       text   NOT NULL,
  addr           text   NOT NULL,
  binary_version text   NOT NULL,
  compat         int    NOT NULL,
  seen_ms        bigint NOT NULL
);

CREATE TABLE disks (
  disk_id        text   NOT NULL PRIMARY KEY CHECK (length(disk_id) = 32 AND disk_id = lower(disk_id)),
  node_id        int    NOT NULL REFERENCES nodes (node_id),
  state          text   NOT NULL CHECK (state IN ('active', 'readonly', 'gone')),
  capacity_bytes bigint NOT NULL DEFAULT 0,
  used_bytes     bigint NOT NULL DEFAULT 0,
  open_volumes   int    NOT NULL DEFAULT 0,
  seen_ms        bigint,
  created_ms     bigint NOT NULL
);

CREATE TABLE buckets (
  name       text   NOT NULL PRIMARY KEY,
  shards     int    NOT NULL CHECK (shards BETWEEN 1 AND 1024),
  state      text   NOT NULL CHECK (state IN ('active', 'deleted', 'restored')),
  cipher_id  int    NOT NULL DEFAULT 0 CHECK (cipher_id IN (0, 1)),
  created_ms bigint NOT NULL,
  deleted_ms bigint
);

CREATE TABLE shards (
  bucket           text   NOT NULL REFERENCES buckets (name),
  shard            int    NOT NULL CHECK (shard >= 0),
  next_ordinal     bigint NOT NULL DEFAULT 1,
  lease_node       int    REFERENCES nodes (node_id),
  lease_generation bigint NOT NULL DEFAULT 0,
  lease_expires_ms bigint,
  PRIMARY KEY (bucket, shard)
);

CREATE TABLE volumes (
  volume_id        text   NOT NULL PRIMARY KEY CHECK (length(volume_id) = 16 AND volume_id = lower(volume_id)),
  bucket           text   NOT NULL,
  shard            int    NOT NULL,
  ordinal          bigint NOT NULL,
  state            text   NOT NULL CHECK (state IN ('open', 'sealing', 'sealed')),
  epoch            int    NOT NULL,
  format_major     int    NOT NULL,
  format_minor     int    NOT NULL,
  cipher_id        int    NOT NULL,
  tier             int    NOT NULL,
  committed_length bigint,
  record_count     bigint,
  created_ms       bigint NOT NULL,
  sealed_ms        bigint,
  UNIQUE (bucket, shard, ordinal),
  FOREIGN KEY (bucket, shard) REFERENCES shards (bucket, shard)
);

CREATE INDEX volumes_by_shard ON volumes (bucket, shard, ordinal DESC);

CREATE TABLE volume_replicas (
  volume_id text NOT NULL REFERENCES volumes (volume_id),
  disk_id   text NOT NULL REFERENCES disks (disk_id),
  state     text NOT NULL CHECK (state IN ('ok', 'failed', 'lost')),
  PRIMARY KEY (volume_id, disk_id)
);

CREATE INDEX replicas_by_disk ON volume_replicas (disk_id);

CREATE TABLE erasures (
  volume_id  text   NOT NULL REFERENCES volumes (volume_id),
  seq        bigint NOT NULL CHECK (seq >= 1),
  chk        bigint NOT NULL,
  created_ms bigint NOT NULL,
  PRIMARY KEY (volume_id, seq)
);

CREATE TABLE erasure_targets (
  volume_id  text   NOT NULL,
  seq        bigint NOT NULL,
  disk_id    text   NOT NULL REFERENCES disks (disk_id),
  applied_ms bigint,
  PRIMARY KEY (volume_id, seq, disk_id),
  FOREIGN KEY (volume_id, seq) REFERENCES erasures (volume_id, seq)
);

CREATE INDEX erasure_targets_pending ON erasure_targets (disk_id) WHERE applied_ms IS NULL;
