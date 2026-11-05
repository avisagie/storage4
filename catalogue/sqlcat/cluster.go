package sqlcat

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"

	"storage4/catalogue"
	"storage4/db"
)

func (c *Catalogue) AssignNodeID(ctx context.Context, name string) (catalogue.NodeID, error) {
	if len(name) < 1 || len(name) > 128 {
		return 0, catalogue.ErrInvalidArgument
	}
	var id catalogue.NodeID
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		if c.d == db.Postgres { // max+1 must not race
			if _, err := tx.ExecContext(ctx, "LOCK TABLE nodes IN SHARE ROW EXCLUSIVE MODE"); err != nil {
				return err
			}
		}
		var n int
		switch err := c.row(ctx, tx, "SELECT node_id FROM nodes WHERE name = ?", name).Scan(&n); {
		case err == nil:
			id = catalogue.NodeID(n)
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if err := c.row(ctx, tx, "SELECT COALESCE(MAX(node_id), 0) FROM nodes").Scan(&n); err != nil {
			return err
		}
		if n >= 65535 {
			return catalogue.ErrNodeIDsExhausted
		}
		if _, err := c.exec(ctx, tx, "INSERT INTO nodes (node_id, name, created_ms) VALUES (?, ?, ?)", n+1, name, c.now()); err != nil {
			return err
		}
		id = catalogue.NodeID(n + 1)
		return nil
	})
	return id, err
}

func (c *Catalogue) RegisterDisk(ctx context.Context, id catalogue.DiskID, node catalogue.NodeID) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		var one int
		if err := c.row(ctx, tx, "SELECT 1 FROM nodes WHERE node_id = ?"+c.lockShare(), int(node)).Scan(&one); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrNodeNotFound
		} else if err != nil {
			return err
		}
		if _, err := c.exec(ctx, tx, "INSERT INTO disks (disk_id, node_id, state, created_ms) VALUES (?, ?, 'active', ?) ON CONFLICT (disk_id) DO NOTHING",
			id.String(), int(node), c.now()); err != nil {
			return err
		}
		var owner int
		if err := c.row(ctx, tx, "SELECT node_id FROM disks WHERE disk_id = ?", id.String()).Scan(&owner); err != nil {
			return err
		}
		if catalogue.NodeID(owner) != node {
			return catalogue.ErrDiskConflict
		}
		return nil
	})
}

func (c *Catalogue) SetDiskState(ctx context.Context, id catalogue.DiskID, s catalogue.DiskState) error {
	if s != catalogue.DiskActive && s != catalogue.DiskReadOnly && s != catalogue.DiskGone {
		return catalogue.ErrInvalidArgument
	}
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		var cur string
		if err := c.row(ctx, tx, "SELECT state FROM disks WHERE disk_id = ?"+c.lockUpdate(), id.String()).Scan(&cur); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrUnknownDisk
		} else if err != nil {
			return err
		}
		if cur == string(catalogue.DiskGone) && s != catalogue.DiskGone {
			return catalogue.ErrDiskGone
		}
		_, err := c.exec(ctx, tx, "UPDATE disks SET state = ? WHERE disk_id = ?", string(s), id.String())
		return err
	})
}

func (c *Catalogue) Heartbeat(ctx context.Context, n catalogue.NodeBeat, disks []catalogue.DiskBeat) (catalogue.Meta, error) {
	if len(n.Version) < 1 || len(n.Version) > 128 {
		return catalogue.Meta{}, catalogue.ErrInvalidArgument
	}
	var meta catalogue.Meta
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		// The node row lock serialises heartbeats of one node ID, so two
		// instances cannot both pass the check below.
		var one int
		if err := c.row(ctx, tx, "SELECT 1 FROM nodes WHERE node_id = ?"+c.lockUpdate(), int(n.ID)).Scan(&one); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrNodeNotFound
		} else if err != nil {
			return err
		}
		now := c.now()
		inst := hex.EncodeToString(n.Instance[:])
		var prev string
		var seen int64
		switch err := c.row(ctx, tx, "SELECT instance, seen_ms FROM node_beats WHERE node_id = ?", int(n.ID)).Scan(&prev, &seen); {
		case err == nil:
			if prev != inst && now-seen < c.nodeTTL.Milliseconds() {
				return catalogue.ErrNodeIDInUse
			}
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		for _, b := range disks {
			var owner int
			if err := c.row(ctx, tx, "SELECT node_id FROM disks WHERE disk_id = ?", b.ID.String()).Scan(&owner); errors.Is(err, sql.ErrNoRows) || err == nil && catalogue.NodeID(owner) != n.ID {
				return catalogue.ErrUnknownDisk
			} else if err != nil {
				return err
			}
		}
		if _, err := c.exec(ctx, tx, `INSERT INTO node_beats (node_id, instance, addr, binary_version, compat, seen_ms) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (node_id) DO UPDATE SET instance = excluded.instance, addr = excluded.addr,
			binary_version = excluded.binary_version, compat = excluded.compat, seen_ms = excluded.seen_ms`,
			int(n.ID), inst, n.Addr, n.Version, n.Compat, now); err != nil {
			return err
		}
		for _, b := range disks {
			if _, err := c.exec(ctx, tx, "UPDATE disks SET capacity_bytes = ?, used_bytes = ?, open_volumes = ?, seen_ms = ? WHERE disk_id = ?",
				b.CapacityBytes, b.UsedBytes, b.OpenVolumes, now, b.ID.String()); err != nil {
				return err
			}
		}
		var id string
		if err := c.row(ctx, tx, "SELECT cluster_id, cluster_name, schema_version, min_compat, incarnation FROM catalogue_meta").
			Scan(&id, &meta.ClusterName, &meta.SchemaVersion, &meta.MinCompat, &meta.Incarnation); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrNotInitialised
		} else if err != nil {
			return err
		}
		b, _ := hex.DecodeString(id)
		copy(meta.ClusterID[:], b)
		return nil
	})
	return meta, err
}

func (c *Catalogue) Goodbye(ctx context.Context, node catalogue.NodeID, instance [16]byte) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		var one int
		if err := c.row(ctx, tx, "SELECT 1 FROM nodes WHERE node_id = ?"+c.lockUpdate(), int(node)).Scan(&one); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrNodeNotFound
		} else if err != nil {
			return err
		}
		_, err := c.exec(ctx, tx, "DELETE FROM node_beats WHERE node_id = ? AND instance = ?", int(node), hex.EncodeToString(instance[:]))
		return err
	})
}

func (c *Catalogue) LiveNodes(ctx context.Context) ([]catalogue.NodeInfo, error) {
	var out []catalogue.NodeInfo
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, `SELECT n.node_id, n.name, b.addr, b.binary_version, b.compat, b.seen_ms
			FROM nodes n JOIN node_beats b ON b.node_id = n.node_id WHERE b.seen_ms > ? ORDER BY n.node_id`,
			c.now()-c.nodeTTL.Milliseconds())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ni catalogue.NodeInfo
			var id int
			var seen int64
			if err := rows.Scan(&id, &ni.Name, &ni.Addr, &ni.Version, &ni.Compat, &seen); err != nil {
				return err
			}
			ni.ID, ni.LastSeen = catalogue.NodeID(id), tm(seen)
			out = append(out, ni)
		}
		return rows.Err()
	})
	return out, err
}

func (c *Catalogue) Disks(ctx context.Context) ([]catalogue.Disk, error) {
	var out []catalogue.Disk
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, "SELECT disk_id, node_id, state, capacity_bytes, used_bytes, open_volumes, seen_ms FROM disks ORDER BY disk_id")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d catalogue.Disk
			var id, state string
			var node int
			var seen sql.NullInt64
			if err := rows.Scan(&id, &node, &state, &d.CapacityBytes, &d.UsedBytes, &d.OpenVolumes, &seen); err != nil {
				return err
			}
			if d.ID, err = catalogue.ParseDiskID(id); err != nil {
				return err
			}
			d.Node, d.State, d.LastSeen = catalogue.NodeID(node), catalogue.DiskState(state), tmN(seen)
			out = append(out, d)
		}
		return rows.Err()
	})
	return out, err
}
