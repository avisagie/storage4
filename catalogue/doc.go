// Package catalogue defines the runtime API that nodes and tools use to talk
// to the catalogue (PostgreSQL in production): buckets, shard leases and
// ordinals, volumes and replicas, the erasure list, nodes and disks.
//
// It never runs DDL and never initialises a database; that is db/. The
// implementations are catalogue/pg (production), catalogue/sqlite (tests and
// dev only) and catalogue/mem (simulator fake). All pass one contract suite,
// catalogue/cattest. Design, rules and SQL: docs/catalogue.md.
package catalogue
