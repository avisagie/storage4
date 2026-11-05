package db

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var embedded embed.FS

// Migration is one numbered SQL file. Released migrations are immutable:
// the SHA-256 of each applied file is stored and checked.
type Migration struct {
	Version int
	Name    string
	Kind    string // "expand" or "contract"
	// MinCompat is the compat level every live node must have before a
	// contract migration applies; it becomes the catalogue's min_compat.
	MinCompat int
	SQL       string
	SHA256    string // lower-case hex of the file bytes
}

var fileName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Embedded returns the migrations compiled into the binary.
func Embedded() ([]Migration, error) {
	sub, err := fs.Sub(embedded, "migrations")
	if err != nil {
		return nil, err
	}
	return LoadMigrations(sub)
}

// LoadMigrations reads NNNN_name.sql files from fsys. Versions must run from
// 1 without gaps. Every file starts its header comments with
// "-- kind: expand" or "-- kind: contract"; a contract also has
// "-- min-compat: N".
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var ms []Migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := fileName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("db: migration file %q: want NNNN_name.sql", e.Name())
		}
		raw, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		v, _ := strconv.Atoi(m[1])
		sum := sha256.Sum256(raw)
		mig := Migration{Version: v, Name: m[2], SQL: string(raw), SHA256: hex.EncodeToString(sum[:])}
		for _, line := range strings.Split(mig.SQL, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "--") {
				if line == "" {
					continue
				}
				break
			}
			if k, ok := strings.CutPrefix(line, "-- kind:"); ok {
				mig.Kind = strings.TrimSpace(k)
			}
			if k, ok := strings.CutPrefix(line, "-- min-compat:"); ok {
				mig.MinCompat, err = strconv.Atoi(strings.TrimSpace(k))
				if err != nil || mig.MinCompat < 1 {
					return nil, fmt.Errorf("db: migration %s: bad min-compat", e.Name())
				}
			}
		}
		switch {
		case mig.Kind != "expand" && mig.Kind != "contract":
			return nil, fmt.Errorf("db: migration %s: missing or unknown '-- kind:'", e.Name())
		case mig.Kind == "contract" && mig.MinCompat == 0:
			return nil, fmt.Errorf("db: migration %s: contract migration needs '-- min-compat:'", e.Name())
		case mig.Kind == "expand" && mig.MinCompat != 0:
			return nil, fmt.Errorf("db: migration %s: expand migration must not raise min-compat", e.Name())
		}
		ms = append(ms, mig)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	for i, m := range ms {
		if m.Version != i+1 {
			return nil, fmt.Errorf("db: migration versions must run 1..N without gaps (found %d at position %d)", m.Version, i+1)
		}
	}
	return ms, nil
}
