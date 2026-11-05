package dbtest

import (
	"context"
	"crypto/rand"
	"testing"

	"storage4/catalogue"
	"storage4/catalogue/sqlcat"
	"storage4/clock"
	"storage4/db"
)

// Catalogue returns a fresh, initialised SQL catalogue for a dialect with
// the given identity, time source and options. The database is closed and
// dropped when the test ends. PostgreSQL tests skip when no DSN is set.
func Catalogue(t *testing.T, d db.Dialect, clk clock.Clock, meta catalogue.Meta, o catalogue.Options) catalogue.Catalogue {
	t.Helper()
	sdb := Open(t, d)
	tl := &db.Tool{DB: sdb, Dialect: d, Clock: clk, Rand: rand.Reader, AppliedBy: "test"}
	if _, err := tl.Init(context.Background(), db.InitOptions{ClusterName: meta.ClusterName, ClusterID: &meta.ClusterID}); err != nil {
		t.Fatal(err)
	}
	// Tests expect their fixed meta; set the fields Init chose itself.
	if _, err := sdb.Exec(d.Rebind("UPDATE catalogue_meta SET schema_version = ?, min_compat = ?, incarnation = ?"),
		meta.SchemaVersion, meta.MinCompat, meta.Incarnation); err != nil {
		t.Fatal(err)
	}
	return sqlcat.New(sdb, d, clk, o)
}
