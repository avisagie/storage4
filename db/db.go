package db

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"storage4/catalogue"
	"storage4/clock"
)

// Errors; test with errors.Is.
var (
	// ErrNotInitialised: run `store db init` first. Same value nodes see.
	ErrNotInitialised = catalogue.ErrNotInitialised
	// ErrClusterMismatch: init on a catalogue with another name or cluster ID.
	ErrClusterMismatch = errors.New("db: catalogue already initialised for another cluster")
	// ErrChecksum: an applied migration's stored SHA-256 differs from the binary's file.
	ErrChecksum = errors.New("db: applied migration differs from the binary's")
	// ErrBinaryTooOld: the database has migrations this binary does not know.
	ErrBinaryTooOld = errors.New("db: database schema is newer than this binary")
)

// Tool runs the `store db` operations. Every operation is one transaction
// and starts by taking a database-wide lock, so concurrent operators and
// retried scripts cannot interleave. Every operation is safe to repeat.
type Tool struct {
	DB         *sql.DB
	Dialect    Dialect
	Clock      clock.Clock
	Rand       io.Reader   // for the cluster ID; used by Init only
	Migrations []Migration // default: Embedded()
	NodeTTL    time.Duration
	AppliedBy  string // recorded in schema_migrations: binary version and host
}

// advisoryKey is the fixed pg_advisory_xact_lock key of `store db`.
const advisoryKey = 7_270_447_424_521

func (t *Tool) migrations() ([]Migration, error) {
	if t.Migrations != nil {
		return t.Migrations, nil
	}
	return Embedded()
}

func (t *Tool) nodeTTL() time.Duration {
	if t.NodeTTL > 0 {
		return t.NodeTTL
	}
	return 30 * time.Second
}

// tx runs fn in a transaction holding the db lock; rollback forces a
// rollback after fn succeeds (dry run).
func (t *Tool) tx(ctx context.Context, rollback bool, fn func(*sql.Tx) error) error {
	tx, err := t.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("db: begin: %w", err)
	}
	defer tx.Rollback()
	if t.Dialect == Postgres {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryKey); err != nil {
			return fmt.Errorf("db: lock: %w", err)
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	if rollback {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("db: commit: %w", err)
	}
	return nil
}

func (t *Tool) exec(ctx context.Context, tx *sql.Tx, q string, args ...any) error {
	_, err := tx.ExecContext(ctx, t.Dialect.Rebind(q), args...)
	return err
}

func (t *Tool) tableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	q := "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?"
	if t.Dialect == Postgres {
		q = "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?"
	}
	var n int
	err := tx.QueryRowContext(ctx, t.Dialect.Rebind(q), name).Scan(&n)
	return n > 0, err
}

const migrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations (
  version    int    NOT NULL PRIMARY KEY,
  name       text   NOT NULL,
  kind       text   NOT NULL CHECK (kind IN ('expand', 'contract')),
  sha256     text   NOT NULL,
  applied_ms bigint NOT NULL,
  applied_by text   NOT NULL
)`

// readMeta returns the catalogue_meta row, or ok=false if the catalogue is
// not initialised.
func (t *Tool) readMeta(ctx context.Context, tx *sql.Tx) (m catalogue.Meta, ok bool, err error) {
	has, err := t.tableExists(ctx, tx, "catalogue_meta")
	if err != nil || !has {
		return m, false, err
	}
	var id string
	err = tx.QueryRowContext(ctx, "SELECT cluster_id, cluster_name, schema_version, min_compat, incarnation FROM catalogue_meta").
		Scan(&id, &m.ClusterName, &m.SchemaVersion, &m.MinCompat, &m.Incarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	b, err := hex.DecodeString(id)
	if err != nil || len(b) != 16 {
		return m, false, fmt.Errorf("db: corrupt cluster_id %q", id)
	}
	copy(m.ClusterID[:], b)
	return m, true, nil
}

type applied struct {
	Version int
	Name    string
	SHA256  string
}

func (t *Tool) readApplied(ctx context.Context, tx *sql.Tx) ([]applied, error) {
	has, err := t.tableExists(ctx, tx, "schema_migrations")
	if err != nil || !has {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT version, name, sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []applied
	for rows.Next() {
		var a applied
		if err := rows.Scan(&a.Version, &a.Name, &a.SHA256); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// checkApplied verifies the applied migrations are a prefix of ms with equal
// checksums.
func checkApplied(ap []applied, ms []Migration) error {
	for i, a := range ap {
		if a.Version != i+1 {
			return fmt.Errorf("db: applied migrations are not 1..N (found %d at position %d)", a.Version, i+1)
		}
		if a.Version > len(ms) {
			return fmt.Errorf("%w: database has migration %d (%s)", ErrBinaryTooOld, a.Version, a.Name)
		}
		if ms[a.Version-1].SHA256 != a.SHA256 {
			return fmt.Errorf("%w: %04d_%s", ErrChecksum, a.Version, a.Name)
		}
	}
	return nil
}

func (t *Tool) apply(ctx context.Context, tx *sql.Tx, m Migration) error {
	for _, stmt := range splitStatements(m.SQL) {
		if err := t.exec(ctx, tx, stmt); err != nil {
			return fmt.Errorf("db: migration %04d_%s: %w", m.Version, m.Name, err)
		}
	}
	return t.exec(ctx, tx, "INSERT INTO schema_migrations (version, name, kind, sha256, applied_ms, applied_by) VALUES (?, ?, ?, ?, ?, ?)",
		m.Version, m.Name, m.Kind, m.SHA256, t.Clock.Now().UnixMilli(), t.AppliedBy)
}

// InitOptions are the arguments of `store db init`.
type InitOptions struct {
	ClusterName string
	// ClusterID, if set, is reused instead of generated: the rebuild case
	// (empty database, ID taken from volume headers).
	ClusterID *[16]byte
}

// InitResult says what Init did.
type InitResult struct {
	Created bool // false: already initialised for this cluster, nothing changed
	Meta    catalogue.Meta
}

// Init creates the schema and the catalogue_meta row in one transaction: the
// database is either fully initialised or untouched. On an initialised
// database with the same name (and ID, if given) it does nothing; with
// another name or ID it fails with ErrClusterMismatch.
func (t *Tool) Init(ctx context.Context, o InitOptions) (InitResult, error) {
	var res InitResult
	if n := len(o.ClusterName); n < 1 || n > 128 {
		return res, fmt.Errorf("db: cluster name must be 1..128 bytes")
	}
	ms, err := t.migrations()
	if err != nil {
		return res, err
	}
	err = t.tx(ctx, false, func(tx *sql.Tx) error {
		if m, ok, err := t.readMeta(ctx, tx); err != nil {
			return err
		} else if ok {
			if m.ClusterName != o.ClusterName || (o.ClusterID != nil && m.ClusterID != *o.ClusterID) {
				return fmt.Errorf("%w: it is %q", ErrClusterMismatch, m.ClusterName)
			}
			res = InitResult{Meta: m}
			return nil
		}
		if has, err := t.tableExists(ctx, tx, "catalogue_meta"); err != nil {
			return err
		} else if has {
			return errors.New("db: catalogue_meta exists but is empty; refusing to initialise over it")
		}
		if err := t.exec(ctx, tx, migrationsTable); err != nil {
			return fmt.Errorf("db: %w", err)
		}
		var id [16]byte
		if o.ClusterID != nil {
			id = *o.ClusterID
		} else if _, err := io.ReadFull(t.Rand, id[:]); err != nil {
			return fmt.Errorf("db: cluster id: %w", err)
		}
		minCompat := 1
		for _, m := range ms {
			if err := t.apply(ctx, tx, m); err != nil {
				return err
			}
			minCompat = max(minCompat, m.MinCompat)
		}
		now := t.Clock.Now().UnixMilli()
		meta := catalogue.Meta{ClusterID: id, ClusterName: o.ClusterName, SchemaVersion: len(ms), MinCompat: minCompat, Incarnation: now}
		if err := t.exec(ctx, tx, "INSERT INTO catalogue_meta (singleton, cluster_id, cluster_name, schema_version, min_compat, incarnation, created_ms) VALUES (1, ?, ?, ?, ?, ?, ?)",
			hex.EncodeToString(id[:]), meta.ClusterName, meta.SchemaVersion, meta.MinCompat, meta.Incarnation, now); err != nil {
			return fmt.Errorf("db: %w", err)
		}
		res = InitResult{Created: true, Meta: meta}
		return nil
	})
	return res, err
}

// LiveNode is a node with a recent heartbeat, as `status` shows it.
type LiveNode struct {
	ID      int
	Name    string
	Version string
	Compat  int
}

func (t *Tool) liveNodes(ctx context.Context, tx *sql.Tx) ([]LiveNode, error) {
	has, err := t.tableExists(ctx, tx, "node_beats")
	if err != nil || !has {
		return nil, err
	}
	since := t.Clock.Now().Add(-t.nodeTTL()).UnixMilli()
	rows, err := tx.QueryContext(ctx, t.Dialect.Rebind(`SELECT n.node_id, n.name, b.binary_version, b.compat
		FROM node_beats b JOIN nodes n ON n.node_id = b.node_id WHERE b.seen_ms > ? ORDER BY n.node_id`), since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LiveNode
	for rows.Next() {
		var n LiveNode
		if err := rows.Scan(&n.ID, &n.Name, &n.Version, &n.Compat); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// MigrateResult says what Migrate did.
type MigrateResult struct {
	Applied []Migration // applied (or, in a dry run, would be applied and was rolled back)
	// Blocked is the contract migration that was not applied because live
	// nodes are below its min-compat; Blockers are those nodes.
	Blocked  *Migration
	Blockers []LiveNode
	DryRun   bool
}

// Migrate applies pending migrations in order, each with its schema_migrations
// row, stopping before a contract migration that live nodes cannot yet
// tolerate. With dryRun everything is applied for real inside the
// transaction, reported, and rolled back. On an uninitialised database it
// refuses (ErrNotInitialised): pointing at the wrong host never creates a
// catalogue.
func (t *Tool) Migrate(ctx context.Context, dryRun bool) (MigrateResult, error) {
	res := MigrateResult{DryRun: dryRun}
	ms, err := t.migrations()
	if err != nil {
		return res, err
	}
	err = t.tx(ctx, dryRun, func(tx *sql.Tx) error {
		meta, ok, err := t.readMeta(ctx, tx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: run `store db init`", ErrNotInitialised)
		}
		ap, err := t.readApplied(ctx, tx)
		if err != nil {
			return err
		}
		if err := checkApplied(ap, ms); err != nil {
			return err
		}
		live, err := t.liveNodes(ctx, tx)
		if err != nil {
			return err
		}
		for _, m := range ms[len(ap):] {
			if m.Kind == "contract" {
				var below []LiveNode
				for _, n := range live {
					if n.Compat < m.MinCompat {
						below = append(below, n)
					}
				}
				if len(below) > 0 {
					m := m
					res.Blocked, res.Blockers = &m, below
					return nil
				}
			}
			if err := t.apply(ctx, tx, m); err != nil {
				return err
			}
			meta.SchemaVersion, meta.MinCompat = m.Version, max(meta.MinCompat, m.MinCompat)
			if err := t.exec(ctx, tx, "UPDATE catalogue_meta SET schema_version = ?, min_compat = ?", meta.SchemaVersion, meta.MinCompat); err != nil {
				return fmt.Errorf("db: %w", err)
			}
			res.Applied = append(res.Applied, m)
		}
		return nil
	})
	return res, err
}

// Status is what `store db status` reports.
type Status struct {
	Initialised bool
	Meta        catalogue.Meta
	Applied     []Migration // as known to this binary
	Pending     []Migration
	LiveNodes   []LiveNode
	// Blockers are live nodes below the next pending contract migration's min-compat.
	Blockers []LiveNode
}

// Status reports the schema version, pending migrations and live nodes'
// versions. It changes nothing. ErrChecksum / ErrBinaryTooOld are returned
// if the applied migrations do not match the binary.
func (t *Tool) Status(ctx context.Context) (Status, error) {
	var st Status
	ms, err := t.migrations()
	if err != nil {
		return st, err
	}
	err = t.tx(ctx, true, func(tx *sql.Tx) error {
		meta, ok, err := t.readMeta(ctx, tx)
		if err != nil || !ok {
			return err
		}
		st.Initialised, st.Meta = true, meta
		ap, err := t.readApplied(ctx, tx)
		if err != nil {
			return err
		}
		if err := checkApplied(ap, ms); err != nil {
			return err
		}
		st.Applied, st.Pending = ms[:len(ap)], ms[len(ap):]
		if st.LiveNodes, err = t.liveNodes(ctx, tx); err != nil {
			return err
		}
		for _, m := range st.Pending {
			if m.Kind == "contract" {
				for _, n := range st.LiveNodes {
					if n.Compat < m.MinCompat {
						st.Blockers = append(st.Blockers, n)
					}
				}
				break
			}
		}
		return nil
	})
	return st, err
}

// SchemaSnapshot returns the live schema as sorted "table|column|type|nullable"
// lines: what Verify compares with the snapshot embedded for the applied
// version. It covers tables and columns, not constraints or indexes.
func (t *Tool) SchemaSnapshot(ctx context.Context) (string, error) {
	var lines []string
	err := t.tx(ctx, true, func(tx *sql.Tx) error {
		q := `SELECT m.name, p.name, lower(p.type), CASE p."notnull" WHEN 1 THEN 'NO' ELSE 'YES' END
			FROM sqlite_master m, pragma_table_info(m.name) p WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'`
		if t.Dialect == Postgres {
			q = `SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = current_schema()`
		}
		rows, err := tx.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a, b, c, d string
			if err := rows.Scan(&a, &b, &c, &d); err != nil {
				return err
			}
			lines = append(lines, strings.Join([]string{a, b, c, d}, "|"))
		}
		return rows.Err()
	})
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n", err
}

// Verify checks the applied migrations' checksums against the binary's files
// and the live schema against the expected snapshot of the applied version
// (catches hand-made changes after a manual rescue). It returns the problems
// found; an empty list means the catalogue is as the binary expects.
func (t *Tool) Verify(ctx context.Context) ([]string, error) {
	var problems []string
	ms, err := t.migrations()
	if err != nil {
		return nil, err
	}
	var applied []applied
	err = t.tx(ctx, true, func(tx *sql.Tx) error {
		_, ok, err := t.readMeta(ctx, tx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: run `store db init`", ErrNotInitialised)
		}
		applied, err = t.readApplied(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := checkApplied(applied, ms); err != nil {
		problems = append(problems, err.Error())
	}
	want, ok := expectedSnapshot(t.Dialect, len(applied))
	if !ok {
		return append(problems, fmt.Sprintf("no expected-schema snapshot for %s version %d", t.Dialect, len(applied))), nil
	}
	got, err := t.SchemaSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if got != want {
		problems = append(problems, "live schema differs from the expected snapshot:\n"+diffLines(want, got))
	}
	return problems, nil
}

func diffLines(want, got string) string {
	w, g := map[string]bool{}, map[string]bool{}
	for _, l := range strings.Split(want, "\n") {
		w[l] = true
	}
	for _, l := range strings.Split(got, "\n") {
		g[l] = true
	}
	var out []string
	for l := range w {
		if !g[l] && l != "" {
			out = append(out, "  missing: "+l)
		}
	}
	for l := range g {
		if !w[l] && l != "" {
			out = append(out, "  unexpected: "+l)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}
