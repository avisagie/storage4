package db

import (
	"embed"
	"fmt"
)

//go:embed schema/*.txt
var snapshots embed.FS

// expectedSnapshot returns the embedded schema snapshot for a dialect and
// schema version. Snapshots are released artefacts like migrations: never
// edited, a new migration gets a new file.
func expectedSnapshot(d Dialect, version int) (string, bool) {
	b, err := snapshots.ReadFile(fmt.Sprintf("schema/%s-%04d.txt", d, version))
	if err != nil {
		return "", false
	}
	return string(b), true
}
