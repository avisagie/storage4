package db_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"storage4/clock"
	"storage4/db"
	"storage4/db/dbtest"
)

var update = flag.Bool("update-schema", false, "write MISSING schema snapshots (never rewrites an existing one)")

var dialects = []db.Dialect{db.SQLite, db.Postgres}

var ctx = context.Background()

func eachDialect(t *testing.T, fn func(t *testing.T, d db.Dialect)) {
	for _, d := range dialects {
		t.Run(d.String(), func(t *testing.T) { fn(t, d) })
	}
}

func tool(t *testing.T, d db.Dialect, extra ...string) (*db.Tool, *clock.Fake, *sql.DB) {
	t.Helper()
	sdb := dbtest.Open(t, d)
	clk := clock.NewFake(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	tl := &db.Tool{DB: sdb, Dialect: d, Clock: clk, Rand: rand.Reader, AppliedBy: "test", NodeTTL: 30 * time.Second}
	if len(extra) > 0 {
		tl.Migrations = withExtra(t, extra...)
	}
	return tl, clk, sdb
}

// withExtra returns the embedded migrations plus extra files, each given as
// "NAME\nbody" with NAME like "0002_add_x.sql".
func withExtra(t *testing.T, extra ...string) []db.Migration {
	t.Helper()
	real, err := db.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{}
	for _, m := range real {
		fsys[fmt.Sprintf("%04d_%s.sql", m.Version, m.Name)] = &fstest.MapFile{Data: []byte(m.SQL)}
	}
	for _, e := range extra {
		name, body, _ := strings.Cut(e, "\n")
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	ms, err := db.LoadMigrations(fsys)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func tableCount(t *testing.T, sdb *sql.DB, d db.Dialect) int {
	t.Helper()
	q := "SELECT count(*) FROM sqlite_master WHERE type = 'table'"
	if d == db.Postgres {
		q = "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema()"
	}
	var n int
	if err := sdb.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEmbeddedMigrationsLoad(t *testing.T) {
	ms, err := db.Embedded()
	if err != nil || len(ms) == 0 || ms[0].Version != 1 || ms[0].Kind != "expand" || len(ms[0].SHA256) != 64 {
		t.Fatalf("%+v %v", ms, err)
	}
}

func TestLoadMigrationsRejectsBadFiles(t *testing.T) {
	for name, files := range map[string]fstest.MapFS{
		"gap":        {"0002_x.sql": {Data: []byte("-- kind: expand\nSELECT 1;")}},
		"bad name":   {"init.sql": {Data: []byte("-- kind: expand\n")}},
		"no kind":    {"0001_x.sql": {Data: []byte("SELECT 1;")}},
		"contract":   {"0001_x.sql": {Data: []byte("-- kind: contract\nSELECT 1;")}},
		"expand+min": {"0001_x.sql": {Data: []byte("-- kind: expand\n-- min-compat: 2\nSELECT 1;")}},
		"bad compat": {"0001_x.sql": {Data: []byte("-- kind: contract\n-- min-compat: x\n")}},
	} {
		if _, err := db.LoadMigrations(files); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestInit(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, clk, sdb := tool(t, d)
		r, err := tl.Init(ctx, db.InitOptions{ClusterName: "prod"})
		if err != nil || !r.Created {
			t.Fatalf("%+v %v", r, err)
		}
		m := r.Meta
		if m.ClusterName != "prod" || m.SchemaVersion != 1 || m.MinCompat != 1 || m.Incarnation != clk.Now().UnixMilli() || m.ClusterID == [16]byte{} {
			t.Fatalf("%+v", m)
		}
		// Idempotent: same name is a no-op and returns the same identity.
		r2, err := tl.Init(ctx, db.InitOptions{ClusterName: "prod"})
		if err != nil || r2.Created || r2.Meta != m {
			t.Fatalf("%+v %v", r2, err)
		}
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "prod", ClusterID: &m.ClusterID}); err != nil {
			t.Fatal(err)
		}
		other := [16]byte{9}
		for _, o := range []db.InitOptions{{ClusterName: "other"}, {ClusterName: "prod", ClusterID: &other}} {
			if _, err := tl.Init(ctx, o); !errors.Is(err, db.ErrClusterMismatch) {
				t.Fatalf("%+v: %v", o, err)
			}
		}
		for _, bad := range []string{"", strings.Repeat("x", 129)} {
			if _, err := tl.Init(ctx, db.InitOptions{ClusterName: bad}); err == nil {
				t.Fatal("accepted bad name")
			}
		}
		_ = sdb
	})
}

func TestInitReusesGivenClusterID(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, _ := tool(t, d)
		id := [16]byte{0xAB, 0xCD}
		r, err := tl.Init(ctx, db.InitOptions{ClusterName: "rebuilt", ClusterID: &id})
		if err != nil || r.Meta.ClusterID != id {
			t.Fatalf("%+v %v", r, err)
		}
	})
}

// A failing migration leaves the database untouched: init is all or nothing.
func TestInitIsAtomic(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, sdb := tool(t, d, "0002_bad.sql\n-- kind: expand\nCREATE TABLE ok_table (a int);\nCREATE TABLE ok_table (a int);")
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "x"}); err == nil {
			t.Fatal("init succeeded with a broken migration")
		}
		if n := tableCount(t, sdb, d); n != 0 {
			t.Fatalf("%d tables left behind", n)
		}
	})
}

func TestMigrateRefusesUninitialised(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, sdb := tool(t, d)
		if _, err := tl.Migrate(ctx, false); !errors.Is(err, db.ErrNotInitialised) {
			t.Fatalf("%v", err)
		}
		if n := tableCount(t, sdb, d); n != 0 {
			t.Fatalf("migrate created %d tables on an uninitialised database", n)
		}
		if _, err := tl.Verify(ctx); !errors.Is(err, db.ErrNotInitialised) {
			t.Fatalf("%v", err)
		}
		if st, err := tl.Status(ctx); err != nil || st.Initialised {
			t.Fatalf("%+v %v", st, err)
		}
	})
}

func TestMigrateAppliesPendingOnceAndDryRunChangesNothing(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, _ := tool(t, d)
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "x"}); err != nil {
			t.Fatal(err)
		}
		tl.Migrations = withExtra(t,
			"0002_more.sql\n-- kind: expand\nCREATE TABLE extra_one (a int);",
			"0003_more.sql\n-- kind: expand\nCREATE TABLE extra_two (a int);")
		r, err := tl.Migrate(ctx, true)
		if err != nil || len(r.Applied) != 2 || !r.DryRun {
			t.Fatalf("%+v %v", r, err)
		}
		if st, _ := tl.Status(ctx); len(st.Pending) != 2 || st.Meta.SchemaVersion != 1 {
			t.Fatalf("dry run changed the database: %+v", st)
		}
		r, err = tl.Migrate(ctx, false)
		if err != nil || len(r.Applied) != 2 {
			t.Fatalf("%+v %v", r, err)
		}
		r, err = tl.Migrate(ctx, false) // second run: no-op
		if err != nil || len(r.Applied) != 0 || r.Blocked != nil {
			t.Fatalf("%+v %v", r, err)
		}
		st, err := tl.Status(ctx)
		if err != nil || st.Meta.SchemaVersion != 3 || len(st.Applied) != 3 || len(st.Pending) != 0 {
			t.Fatalf("%+v %v", st, err)
		}
	})
}

func TestChecksumAndBinaryTooOld(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, _ := tool(t, d, "0002_more.sql\n-- kind: expand\nCREATE TABLE extra_one (a int);")
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "x"}); err != nil {
			t.Fatal(err)
		}
		// Same version, different file: the binary refuses everything.
		edited := withExtra(t, "0002_more.sql\n-- kind: expand\nCREATE TABLE extra_one (a bigint);")
		tl.Migrations = edited
		if _, err := tl.Migrate(ctx, false); !errors.Is(err, db.ErrChecksum) {
			t.Fatalf("migrate: %v", err)
		}
		if _, err := tl.Status(ctx); !errors.Is(err, db.ErrChecksum) {
			t.Fatalf("status: %v", err)
		}
		if p, err := tl.Verify(ctx); err != nil || len(p) == 0 || !strings.Contains(p[0], "differs") {
			t.Fatalf("verify: %v %v", p, err)
		}
		// An older binary against a newer database.
		tl.Migrations = nil
		if _, err := tl.Migrate(ctx, false); !errors.Is(err, db.ErrBinaryTooOld) {
			t.Fatalf("old binary: %v", err)
		}
	})
}

func insertLiveNode(t *testing.T, sdb *sql.DB, d db.Dialect, id, compat int, seen time.Time) {
	t.Helper()
	for _, q := range []string{
		"INSERT INTO nodes (node_id, name, created_ms) VALUES (?, ?, 0)",
		"INSERT INTO node_beats (node_id, instance, addr, binary_version, compat, seen_ms) VALUES (?, 'i', 'a', 'v1', ?, ?)",
	} {
		args := []any{id, fmt.Sprintf("n%d", id)}
		if strings.Contains(q, "node_beats") {
			args = []any{id, compat, seen.UnixMilli()}
		}
		if _, err := sdb.Exec(d.Rebind(q), args...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContractMigrationWaitsForLiveNodes(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, clk, sdb := tool(t, d, "0002_expand.sql\n-- kind: expand\nCREATE TABLE extra_one (a int);",
			"0003_contract.sql\n-- kind: contract\n-- min-compat: 2\nCREATE TABLE extra_two (a int);")
		// Init applies everything, so start from a database that only has 0001.
		base := tl.Migrations
		tl.Migrations = base[:1]
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "x"}); err != nil {
			t.Fatal(err)
		}
		tl.Migrations = base
		insertLiveNode(t, sdb, d, 1, 1, clk.Now()) // compat 1 < 2
		r, err := tl.Migrate(ctx, false)
		if err != nil || len(r.Applied) != 1 || r.Applied[0].Version != 2 || r.Blocked == nil || r.Blocked.Version != 3 ||
			len(r.Blockers) != 1 || r.Blockers[0].ID != 1 || r.Blockers[0].Compat != 1 {
			t.Fatalf("%+v %v", r, err)
		}
		st, _ := tl.Status(ctx)
		if st.Meta.SchemaVersion != 2 || st.Meta.MinCompat != 1 || len(st.Blockers) != 1 || len(st.Pending) != 1 {
			t.Fatalf("%+v", st)
		}
		// The blocking node goes quiet (offline nodes do not block): the contract applies and raises min_compat.
		clk.Advance(time.Minute)
		r, err = tl.Migrate(ctx, false)
		if err != nil || len(r.Applied) != 1 || r.Blocked != nil {
			t.Fatalf("%+v %v", r, err)
		}
		if st, _ = tl.Status(ctx); st.Meta.SchemaVersion != 3 || st.Meta.MinCompat != 2 {
			t.Fatalf("%+v", st)
		}
	})
}

func TestVerifyAndSnapshots(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, sdb := tool(t, d)
		if _, err := tl.Init(ctx, db.InitOptions{ClusterName: "x"}); err != nil {
			t.Fatal(err)
		}
		got, err := tl.SchemaSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		file := fmt.Sprintf("schema/%s-0001.txt", d)
		if want, _ := os.ReadFile(file); len(want) == 0 && *update {
			if err := os.WriteFile(file, []byte(got), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if p, err := tl.Verify(ctx); err != nil || len(p) != 0 {
			t.Fatalf("clean database has problems: %v %v", p, err)
		}
		// A hand-made change after a rescue is caught.
		if _, err := sdb.Exec("ALTER TABLE buckets ADD COLUMN sneaky int"); err != nil {
			t.Fatal(err)
		}
		p, err := tl.Verify(ctx)
		if err != nil || len(p) != 1 || !strings.Contains(p[0], "unexpected: buckets|sneaky") {
			t.Fatalf("%v %v", p, err)
		}
	})
}

func TestConcurrentInitsAreSerialised(t *testing.T) {
	eachDialect(t, func(t *testing.T, d db.Dialect) {
		tl, _, _ := tool(t, d)
		var wg sync.WaitGroup
		results := make([]db.InitResult, 4)
		errs := make([]error, 4)
		for i := range results {
			wg.Add(1)
			go func() { defer wg.Done(); results[i], errs[i] = tl.Init(ctx, db.InitOptions{ClusterName: "x"}) }()
		}
		wg.Wait()
		created := 0
		for i, r := range results {
			if errs[i] != nil {
				t.Fatal(errs[i])
			}
			if r.Created {
				created++
			}
			if r.Meta != results[0].Meta {
				t.Fatalf("different identities: %+v vs %+v", r.Meta, results[0].Meta)
			}
		}
		if created != 1 {
			t.Fatalf("%d inits created the catalogue", created)
		}
	})
}
