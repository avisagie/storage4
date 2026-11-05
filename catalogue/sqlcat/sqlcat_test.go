package sqlcat_test

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"storage4/catalogue"
	"storage4/catalogue/cattest"
	"storage4/catalogue/sqlcat"
	"storage4/clock"
	"storage4/db"
	"storage4/db/dbtest"
)

func factory(d db.Dialect) cattest.Factory {
	return func(t *testing.T, clk clock.Clock, meta catalogue.Meta, o catalogue.Options) catalogue.Catalogue {
		sdb := dbtest.Open(t, d)
		tl := &db.Tool{DB: sdb, Dialect: d, Clock: clk, Rand: rand.Reader, AppliedBy: "test"}
		r, err := tl.Init(context.Background(), db.InitOptions{ClusterName: meta.ClusterName, ClusterID: &meta.ClusterID})
		if err != nil {
			t.Fatal(err)
		}
		// The suite expects the fixed test meta; set the fields Init chose itself.
		if _, err := sdb.Exec(d.Rebind("UPDATE catalogue_meta SET schema_version = ?, min_compat = ?, incarnation = ?"),
			meta.SchemaVersion, meta.MinCompat, meta.Incarnation); err != nil {
			t.Fatal(err)
		}
		_ = r
		return sqlcat.New(sdb, d, clk, o)
	}
}

func TestSQLiteContract(t *testing.T)   { cattest.Run(t, factory(db.SQLite)) }
func TestPostgresContract(t *testing.T) { cattest.Run(t, factory(db.Postgres)) }

func TestMetaNotInitialised(t *testing.T) {
	for _, d := range []db.Dialect{db.SQLite, db.Postgres} {
		t.Run(d.String(), func(t *testing.T) {
			c := sqlcat.New(dbtest.Open(t, d), d, clock.NewFake(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)), catalogue.Options{})
			_, err := c.Meta(context.Background())
			if err == nil || !isNotInit(err) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func isNotInit(err error) bool { return errors.Is(err, catalogue.ErrNotInitialised) }
