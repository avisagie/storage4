// Package db is the DDL side of the catalogue: embedded migrations and the
// operations behind `store db init|migrate|status|verify`. Only this package
// (and tests) change the schema; nodes and tools use catalogue.Catalogue.
// It works on a *sql.DB; drivers are registered by cmd/ and tests, never here.
// Design: docs/design-notes.md §13, docs/catalogue.md.
package db

import (
	"strconv"
	"strings"
)

// Dialect selects the few places where PostgreSQL and SQLite differ.
type Dialect int

const (
	Postgres Dialect = iota
	SQLite
)

func (d Dialect) String() string {
	if d == SQLite {
		return "sqlite"
	}
	return "postgres"
}

// Rebind rewrites ? placeholders to $1, $2, ... for PostgreSQL and returns
// the query unchanged for SQLite. Question marks inside single-quoted
// literals are left alone.
func (d Dialect) Rebind(q string) string {
	if d == SQLite {
		return q
	}
	var b strings.Builder
	n := 0
	inStr := false
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case c == '\'':
			inStr = !inStr
			b.WriteByte(c)
		case c == '?' && !inStr:
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// splitStatements splits a migration into statements: ';' outside single
// quotes ends one, and "--" comments to the end of the line are dropped.
// Dollar-quoted bodies are not supported; keep PL/pgSQL out of portable
// migrations.
func splitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	inStr := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case !inStr && c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			cur.WriteByte('\n')
		case c == '\'':
			inStr = !inStr
			cur.WriteByte(c)
		case c == ';' && !inStr:
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}
