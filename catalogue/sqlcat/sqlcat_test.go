package sqlcat_test

import (
	"context"
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
		return dbtest.Catalogue(t, d, clk, meta, o)
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
