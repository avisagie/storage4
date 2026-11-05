package sqlcat

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"storage4/catalogue"
)

func scanLocators(rows *sql.Rows) ([]catalogue.Locator, error) {
	defer rows.Close()
	var out []catalogue.Locator
	for rows.Next() {
		var vid string
		var seq, chk int64
		if err := rows.Scan(&vid, &seq, &chk); err != nil {
			return nil, err
		}
		v, err := catalogue.ParseVolumeID(vid)
		if err != nil {
			return nil, err
		}
		out = append(out, catalogue.Locator{Volume: v, Seq: uint32(seq), Check: uint32(chk)})
	}
	return out, rows.Err()
}

func (c *Catalogue) AddErasures(ctx context.Context, l catalogue.Lease, locs []catalogue.Locator) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		if err := c.lockLease(ctx, tx, l); err != nil {
			return err
		}
		// Lock the volumes in a fixed order (against AddReplica and other
		// AddErasures), ignoring ones that are not ours: those fail below.
		seen := map[catalogue.VolumeID]bool{}
		var vols []string
		for _, loc := range locs {
			if !seen[loc.Volume] {
				seen[loc.Volume] = true
				vols = append(vols, loc.Volume.String())
			}
		}
		sort.Strings(vols)
		for _, v := range vols {
			var one int
			err := c.row(ctx, tx, "SELECT 1 FROM volumes WHERE volume_id = ? AND bucket = ? AND shard = ?"+c.lockUpdate(), v, l.Bucket, l.Shard).Scan(&one)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// Validate everything before changing anything.
		var fresh []catalogue.Locator
		for _, loc := range locs {
			if loc.Seq == 0 {
				return catalogue.ErrInvalidArgument
			}
			var one int
			if err := c.row(ctx, tx, "SELECT 1 FROM volumes WHERE volume_id = ? AND bucket = ? AND shard = ?", loc.Volume.String(), l.Bucket, l.Shard).Scan(&one); errors.Is(err, sql.ErrNoRows) {
				return catalogue.ErrVolumeNotFound
			} else if err != nil {
				return err
			}
			var chk int64
			switch err := c.row(ctx, tx, "SELECT chk FROM erasures WHERE volume_id = ? AND seq = ?", loc.Volume.String(), int64(loc.Seq)).Scan(&chk); {
			case err == nil:
				if uint32(chk) != loc.Check {
					return catalogue.ErrInvalidArgument
				}
			case errors.Is(err, sql.ErrNoRows):
				fresh = append(fresh, loc)
			default:
				return err
			}
		}
		now := c.now()
		for _, loc := range fresh {
			n, err := c.exec(ctx, tx, "INSERT INTO erasures (volume_id, seq, chk, created_ms) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
				loc.Volume.String(), int64(loc.Seq), int64(loc.Check), now)
			if err != nil {
				return err
			}
			if n == 0 { // repeated within this call
				continue
			}
			if _, err := c.exec(ctx, tx, `INSERT INTO erasure_targets (volume_id, seq, disk_id)
				SELECT volume_id, ?, disk_id FROM volume_replicas WHERE volume_id = ? AND state <> 'lost'`,
				int64(loc.Seq), loc.Volume.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Catalogue) ShardErasures(ctx context.Context, bucket string, shard int) ([]catalogue.Locator, error) {
	var out []catalogue.Locator
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, `SELECT e.volume_id, e.seq, e.chk FROM erasures e JOIN volumes v ON v.volume_id = e.volume_id
			WHERE v.bucket = ? AND v.shard = ? ORDER BY e.volume_id, e.seq`, bucket, shard)
		if err != nil {
			return err
		}
		out, err = scanLocators(rows)
		return err
	})
	return out, err
}

func (c *Catalogue) PendingErasures(ctx context.Context, disk catalogue.DiskID, limit int) ([]catalogue.Locator, error) {
	if limit < 1 {
		return nil, catalogue.ErrInvalidArgument
	}
	var out []catalogue.Locator
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, `SELECT t.volume_id, t.seq, e.chk FROM erasure_targets t
			JOIN erasures e ON e.volume_id = t.volume_id AND e.seq = t.seq
			WHERE t.disk_id = ? AND t.applied_ms IS NULL ORDER BY t.volume_id, t.seq LIMIT ?`, disk.String(), limit)
		if err != nil {
			return err
		}
		out, err = scanLocators(rows)
		return err
	})
	return out, err
}

func (c *Catalogue) MarkErasureApplied(ctx context.Context, disk catalogue.DiskID, locs []catalogue.Locator) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		for _, loc := range locs {
			var one int
			if err := c.row(ctx, tx, "SELECT 1 FROM erasure_targets WHERE volume_id = ? AND seq = ? AND disk_id = ?",
				loc.Volume.String(), int64(loc.Seq), disk.String()).Scan(&one); errors.Is(err, sql.ErrNoRows) {
				return catalogue.ErrErasureNotFound
			} else if err != nil {
				return err
			}
		}
		now := c.now()
		for _, loc := range locs {
			if _, err := c.exec(ctx, tx, "UPDATE erasure_targets SET applied_ms = ? WHERE volume_id = ? AND seq = ? AND disk_id = ? AND applied_ms IS NULL",
				now, loc.Volume.String(), int64(loc.Seq), disk.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Catalogue) ListErasures(ctx context.Context, after catalogue.Locator, limit int) ([]catalogue.Locator, error) {
	if limit < 1 {
		return nil, catalogue.ErrInvalidArgument
	}
	var out []catalogue.Locator
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		v := after.Volume.String()
		rows, err := c.query(ctx, tx, `SELECT volume_id, seq, chk FROM erasures
			WHERE volume_id > ? OR (volume_id = ? AND seq > ?) ORDER BY volume_id, seq LIMIT ?`, v, v, int64(after.Seq), limit)
		if err != nil {
			return err
		}
		out, err = scanLocators(rows)
		return err
	})
	return out, err
}
