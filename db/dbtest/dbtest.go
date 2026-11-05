// Package dbtest opens throwaway databases for tests: a SQLite file in the
// test's temp directory, or a private schema in the PostgreSQL named by
// STORAGE4_TEST_PG_DSN (see `make pg-start`). It is the only place besides
// cmd/ that registers database drivers.
package dbtest

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"storage4/db"
)

// PGEnv is the environment variable holding the test PostgreSQL DSN.
const PGEnv = "STORAGE4_TEST_PG_DSN"

// OpenSQLite returns an empty SQLite database in a temp file. One
// connection, so transactions serialise like PostgreSQL's advisory lock.
func OpenSQLite(t testing.TB) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalogue.sqlite")
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	t.Cleanup(func() { d.Close() })
	return d
}

// PostgresAvailable reports whether a test PostgreSQL is configured.
func PostgresAvailable() bool { return os.Getenv(PGEnv) != "" }

// OpenPostgres returns a connection pool whose search_path is a fresh, empty
// schema, dropped when the test ends. It skips the test if no PostgreSQL is
// configured.
func OpenPostgres(t testing.TB) *sql.DB {
	t.Helper()
	dsn := os.Getenv(PGEnv)
	if dsn == "" {
		t.Skipf("%s not set (run `make pg-start`)", PGEnv)
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("t_%016x", rand.Uint64())
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	d, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
		admin.Exec("DROP SCHEMA " + schema + " CASCADE")
		admin.Close()
	})
	return d
}

// Open returns a database for a dialect.
func Open(t testing.TB, d db.Dialect) *sql.DB {
	if d == db.SQLite {
		return OpenSQLite(t)
	}
	return OpenPostgres(t)
}
