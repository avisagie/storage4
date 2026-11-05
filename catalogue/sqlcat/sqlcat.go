// Package sqlcat implements catalogue.Catalogue over database/sql for
// PostgreSQL (production) and SQLite (tests and dev only). The schema is the
// portable subset of db/migrations, so one set of queries serves both; the
// few differences (placeholders, row locks) are dialect switches here.
//
// The caller opens the *sql.DB with its driver (cmd/ or tests); for SQLite
// use a single connection. Concurrency control is plain row-level SQL:
// compare-and-set UPDATEs and, on PostgreSQL, SELECT ... FOR SHARE/UPDATE
// to hold the lease and volume rows while a transaction relies on them.
package sqlcat

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"storage4/catalogue"
	"storage4/clock"
	"storage4/db"
	"storage4/names"
)

// Catalogue is the SQL implementation.
type Catalogue struct {
	db      *sql.DB
	d       db.Dialect
	clk     clock.Clock
	nodeTTL time.Duration
}

var _ catalogue.Catalogue = (*Catalogue)(nil)

// New returns a Catalogue over an initialised database. It does not check
// the schema; Meta reports ErrNotInitialised on an empty database.
func New(sdb *sql.DB, d db.Dialect, clk clock.Clock, o catalogue.Options) *Catalogue {
	if o.NodeTTL <= 0 {
		o.NodeTTL = 30 * time.Second
	}
	return &Catalogue{db: sdb, d: d, clk: clk, nodeTTL: o.NodeTTL}
}

// Close closes the database handle.
func (c *Catalogue) Close() error { return c.db.Close() }

func (c *Catalogue) now() int64 { return c.clk.Now().UnixMilli() }

func tm(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func tmN(n sql.NullInt64) time.Time {
	if !n.Valid {
		return time.Time{}
	}
	return tm(n.Int64)
}

// ---- plumbing ----

// lockShare and lockUpdate are row-lock suffixes (PostgreSQL only; SQLite
// has a single writer connection).
func (c *Catalogue) lockShare() string {
	if c.d == db.Postgres {
		return " FOR SHARE"
	}
	return ""
}

func (c *Catalogue) lockUpdate() string {
	if c.d == db.Postgres {
		return " FOR UPDATE"
	}
	return ""
}

// wrap maps driver errors: lost connections become ErrUnavailable, a missing
// catalogue table becomes ErrNotInitialised; context errors pass through.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isSentinel(err) {
		return err
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "42P01" || strings.Contains(err.Error(), "no such table") {
		return fmt.Errorf("%w: %w", catalogue.ErrNotInitialised, err)
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, sql.ErrConnDone) || strings.Contains(err.Error(), "bad connection") ||
		strings.Contains(err.Error(), "conn closed") || strings.Contains(err.Error(), "connection refused") {
		return fmt.Errorf("%w: %w", catalogue.ErrUnavailable, err)
	}
	return fmt.Errorf("catalogue: %w", err)
}

func isSentinel(err error) bool {
	for _, s := range []error{catalogue.ErrInvalidArgument, catalogue.ErrBucketNotFound, catalogue.ErrBucketConflict,
		catalogue.ErrBucketDeleted, catalogue.ErrLeaseHeld, catalogue.ErrLeaseLost, catalogue.ErrVolumeNotFound,
		catalogue.ErrVolumeExists, catalogue.ErrVolumeSealed, catalogue.ErrVolumeNotSealed, catalogue.ErrEpochMismatch,
		catalogue.ErrSealMismatch, catalogue.ErrReplicaNotFound, catalogue.ErrErasureNotFound, catalogue.ErrNodeNotFound,
		catalogue.ErrNodeIDInUse, catalogue.ErrNodeIDsExhausted, catalogue.ErrUnknownDisk, catalogue.ErrDiskConflict,
		catalogue.ErrDiskUnavailable, catalogue.ErrDiskGone, catalogue.ErrNotInitialised, catalogue.ErrUnavailable} {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// tx runs fn in a transaction. A read-only fn on PostgreSQL gets a
// repeatable-read snapshot so multi-statement reads are consistent.
func (c *Catalogue) tx(ctx context.Context, readOnly bool, fn func(*sql.Tx) error) error {
	var opts *sql.TxOptions
	if readOnly && c.d == db.Postgres {
		opts = &sql.TxOptions{Isolation: sql.LevelRepeatableRead}
	}
	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return wrap(err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return wrap(err)
	}
	return wrap(tx.Commit())
}

func (c *Catalogue) exec(ctx context.Context, tx *sql.Tx, q string, args ...any) (int64, error) {
	r, err := tx.ExecContext(ctx, c.d.Rebind(q), args...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

func (c *Catalogue) query(ctx context.Context, tx *sql.Tx, q string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(ctx, c.d.Rebind(q), args...)
}

func (c *Catalogue) row(ctx context.Context, tx *sql.Tx, q string, args ...any) *sql.Row {
	return tx.QueryRowContext(ctx, c.d.Rebind(q), args...)
}

// ---- meta ----

func (c *Catalogue) Meta(ctx context.Context) (catalogue.Meta, error) {
	var m catalogue.Meta
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		var id string
		err := c.row(ctx, tx, "SELECT cluster_id, cluster_name, schema_version, min_compat, incarnation FROM catalogue_meta").
			Scan(&id, &m.ClusterName, &m.SchemaVersion, &m.MinCompat, &m.Incarnation)
		if errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrNotInitialised
		}
		if err != nil {
			return err
		}
		b, err := hex.DecodeString(id)
		if err != nil || len(b) != 16 {
			return fmt.Errorf("corrupt cluster_id %q", id)
		}
		copy(m.ClusterID[:], b)
		return nil
	})
	return m, err
}

// ---- buckets ----

const bucketCols = "name, shards, state, cipher_id, created_ms, deleted_ms"

type scanner interface{ Scan(...any) error }

func scanBucket(r scanner) (catalogue.Bucket, error) {
	var b catalogue.Bucket
	var state string
	var created int64
	var deleted sql.NullInt64
	var cipher int
	if err := r.Scan(&b.Name, &b.Shards, &state, &cipher, &created, &deleted); err != nil {
		return b, err
	}
	b.State, b.CipherID, b.CreatedAt, b.DeletedAt = catalogue.BucketState(state), uint16(cipher), tm(created), tmN(deleted)
	return b, nil
}

func (c *Catalogue) CreateBucket(ctx context.Context, name string, shards int) (catalogue.Bucket, bool, error) {
	if names.ValidateBucket(name) != nil || shards < 1 || shards > 1<<31-1 || names.ValidateShardCount(uint32(shards)) != nil {
		return catalogue.Bucket{}, false, catalogue.ErrInvalidArgument
	}
	var b catalogue.Bucket
	var created bool
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		n, err := c.exec(ctx, tx, "INSERT INTO buckets (name, shards, state, cipher_id, created_ms) VALUES (?, ?, 'active', 0, ?) ON CONFLICT (name) DO NOTHING",
			name, shards, c.now())
		if err != nil {
			return err
		}
		created = n == 1
		if b, err = scanBucket(c.row(ctx, tx, "SELECT "+bucketCols+" FROM buckets WHERE name = ?", name)); err != nil {
			return err
		}
		switch {
		case b.State == catalogue.BucketDeleted:
			return catalogue.ErrBucketDeleted
		case b.Shards != shards:
			return catalogue.ErrBucketConflict
		}
		return nil
	})
	if err != nil {
		return catalogue.Bucket{}, false, err
	}
	return b, created, nil
}

func (c *Catalogue) GetBucket(ctx context.Context, name string) (catalogue.Bucket, error) {
	var b catalogue.Bucket
	err := c.tx(ctx, true, func(tx *sql.Tx) (err error) {
		b, err = scanBucket(c.row(ctx, tx, "SELECT "+bucketCols+" FROM buckets WHERE name = ?", name))
		if errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrBucketNotFound
		}
		return err
	})
	return b, err
}

func (c *Catalogue) DeleteBucket(ctx context.Context, name string) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		var state string
		err := c.row(ctx, tx, "SELECT state FROM buckets WHERE name = ?"+c.lockUpdate(), name).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrBucketNotFound
		}
		if err != nil || state == string(catalogue.BucketDeleted) {
			return err
		}
		if _, err := c.exec(ctx, tx, "UPDATE buckets SET state = 'deleted', deleted_ms = ? WHERE name = ?", c.now(), name); err != nil {
			return err
		}
		_, err = c.exec(ctx, tx, "UPDATE shards SET lease_node = NULL, lease_expires_ms = NULL WHERE bucket = ?", name)
		return err
	})
}

func (c *Catalogue) ListBuckets(ctx context.Context) ([]catalogue.Bucket, error) {
	var out []catalogue.Bucket
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, "SELECT "+bucketCols+" FROM buckets ORDER BY name")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			b, err := scanBucket(rows)
			if err != nil {
				return err
			}
			out = append(out, b)
		}
		return rows.Err()
	})
	return out, err
}

// ---- leases ----

func (c *Catalogue) AcquireShard(ctx context.Context, bucket string, shard int, node catalogue.NodeID, ttl time.Duration) (catalogue.Lease, error) {
	if node == 0 || ttl < time.Millisecond {
		return catalogue.Lease{}, catalogue.ErrInvalidArgument
	}
	var l catalogue.Lease
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		// Hold the bucket row so a concurrent DeleteBucket cannot slip between the check and the grant.
		var state string
		var shards int
		err := c.row(ctx, tx, "SELECT state, shards FROM buckets WHERE name = ?"+c.lockShare(), bucket).Scan(&state, &shards)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return catalogue.ErrBucketNotFound
		case err != nil:
			return err
		case state == string(catalogue.BucketDeleted):
			return catalogue.ErrBucketDeleted
		case shard < 0 || shard >= shards:
			return catalogue.ErrInvalidArgument
		}
		if _, err := c.exec(ctx, tx, "INSERT INTO shards (bucket, shard) VALUES (?, ?) ON CONFLICT DO NOTHING", bucket, shard); err != nil {
			return err
		}
		now := c.now()
		var gen int64
		err = c.row(ctx, tx, `UPDATE shards SET lease_node = ?, lease_generation = lease_generation + 1, lease_expires_ms = ?
			WHERE bucket = ? AND shard = ? AND (lease_node IS NULL OR lease_node = ? OR lease_expires_ms <= ?)
			RETURNING lease_generation`, int(node), now+ttl.Milliseconds(), bucket, shard, int(node), now).Scan(&gen)
		if errors.Is(err, sql.ErrNoRows) {
			var holder int
			var exp sql.NullInt64
			if err := c.row(ctx, tx, "SELECT lease_node, lease_expires_ms FROM shards WHERE bucket = ? AND shard = ?", bucket, shard).Scan(&holder, &exp); err != nil {
				return err
			}
			return &catalogue.HeldError{Node: catalogue.NodeID(holder), Expires: tmN(exp)}
		}
		if err != nil {
			return err
		}
		l = catalogue.Lease{Bucket: bucket, Shard: shard, Node: node, Generation: gen, TTL: ttl}
		return nil
	})
	return l, err
}

func (c *Catalogue) RenewShard(ctx context.Context, l catalogue.Lease) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		n, err := c.exec(ctx, tx, `UPDATE shards SET lease_expires_ms = ?
			WHERE bucket = ? AND shard = ? AND lease_node = ? AND lease_generation = ?`,
			c.now()+l.TTL.Milliseconds(), l.Bucket, l.Shard, int(l.Node), l.Generation)
		if err == nil && n == 0 {
			err = catalogue.ErrLeaseLost
		}
		return err
	})
}

func (c *Catalogue) ReleaseShard(ctx context.Context, l catalogue.Lease) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		_, err := c.exec(ctx, tx, `UPDATE shards SET lease_node = NULL, lease_expires_ms = NULL
			WHERE bucket = ? AND shard = ? AND lease_node = ? AND lease_generation = ?`,
			l.Bucket, l.Shard, int(l.Node), l.Generation)
		return err
	})
}

func (c *Catalogue) ShardOwner(ctx context.Context, bucket string, shard int) (catalogue.Owner, bool, error) {
	var o catalogue.Owner
	var ok bool
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		var node int
		var exp int64
		err := c.row(ctx, tx, `SELECT lease_node, lease_generation, lease_expires_ms FROM shards
			WHERE bucket = ? AND shard = ? AND lease_node IS NOT NULL AND lease_expires_ms > ?`, bucket, shard, c.now()).
			Scan(&node, &o.Generation, &exp)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		o.Node, o.Expires, ok = catalogue.NodeID(node), tm(exp), err == nil
		return err
	})
	return o, ok, err
}

// lockLease verifies that l is the shard's current, unexpired lease and, on
// PostgreSQL, holds the shard row until the transaction ends, so the lease
// cannot be taken over while the transaction relies on it.
func (c *Catalogue) lockLease(ctx context.Context, tx *sql.Tx, l catalogue.Lease) error {
	var one int
	err := c.row(ctx, tx, `SELECT 1 FROM shards WHERE bucket = ? AND shard = ? AND lease_node = ? AND lease_generation = ?
		AND lease_expires_ms > ?`+c.lockShare(), l.Bucket, l.Shard, int(l.Node), l.Generation, c.now()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return catalogue.ErrLeaseLost
	}
	return err
}

// ---- volumes ----

const volumeCols = "volume_id, bucket, shard, volume_number, state, epoch, format_major, format_minor, cipher_id, tier, committed_length, record_count, created_ms, sealed_ms"

func scanVolume(r scanner) (catalogue.Volume, error) {
	var v catalogue.Volume
	var id, state string
	var epoch, fmaj, fmin, cipher, tier int
	var committed, records, sealed sql.NullInt64
	var created int64
	if err := r.Scan(&id, &v.Bucket, &v.Shard, &v.VolumeNumber, &state, &epoch, &fmaj, &fmin, &cipher, &tier,
		&committed, &records, &created, &sealed); err != nil {
		return v, err
	}
	vid, err := catalogue.ParseVolumeID(id)
	if err != nil {
		return v, err
	}
	v.ID, v.State, v.Epoch = vid, catalogue.VolumeState(state), uint32(epoch)
	v.FormatMajor, v.FormatMinor, v.CipherID, v.Tier = uint16(fmaj), uint16(fmin), uint16(cipher), uint16(tier)
	v.CommittedLength, v.RecordCount = committed.Int64, uint32(records.Int64)
	v.CreatedAt, v.SealedAt = tm(created), tmN(sealed)
	return v, nil
}

// withReplicas fills Replicas of vols from the query, which must select
// (volume_id, disk_id, state) ordered by volume_id, disk_id.
func (c *Catalogue) withReplicas(ctx context.Context, tx *sql.Tx, vols []catalogue.Volume, q string, args ...any) error {
	if len(vols) == 0 {
		return nil
	}
	idx := map[catalogue.VolumeID]int{}
	for i, v := range vols {
		idx[v.ID] = i
	}
	rows, err := c.query(ctx, tx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var vid, did, state string
		if err := rows.Scan(&vid, &did, &state); err != nil {
			return err
		}
		v, err := catalogue.ParseVolumeID(vid)
		if err != nil {
			return err
		}
		d, err := catalogue.ParseDiskID(did)
		if err != nil {
			return err
		}
		if i, ok := idx[v]; ok {
			vols[i].Replicas = append(vols[i].Replicas, catalogue.Replica{Disk: d, State: catalogue.ReplicaState(state)})
		}
	}
	return rows.Err()
}

func (c *Catalogue) OpenVolume(ctx context.Context, l catalogue.Lease, p catalogue.OpenParams) (catalogue.Volume, error) {
	var v catalogue.Volume
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		// The lease check and the volume number are one statement; a refusal later rolls it back.
		var volumeNumber int64
		err := c.row(ctx, tx, `UPDATE shards SET next_volume_number = next_volume_number + 1
			WHERE bucket = ? AND shard = ? AND lease_node = ? AND lease_generation = ? AND lease_expires_ms > ?
			RETURNING next_volume_number - 1`, l.Bucket, l.Shard, int(l.Node), l.Generation, c.now()).Scan(&volumeNumber)
		if errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if p.ID == 0 || p.ID.Node() != l.Node || len(p.Disks) == 0 {
			return catalogue.ErrInvalidArgument
		}
		var cipher int
		if err := c.row(ctx, tx, "SELECT cipher_id FROM buckets WHERE name = ?", l.Bucket).Scan(&cipher); err != nil {
			return err
		}
		if int(p.CipherID) != cipher {
			return catalogue.ErrInvalidArgument
		}
		var one int
		switch err := c.row(ctx, tx, "SELECT 1 FROM volumes WHERE volume_id = ?", p.ID.String()).Scan(&one); {
		case err == nil:
			return catalogue.ErrVolumeExists
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		seen := map[catalogue.DiskID]bool{}
		for _, d := range p.Disks {
			if seen[d] {
				return catalogue.ErrInvalidArgument
			}
			seen[d] = true
			var state string
			err := c.row(ctx, tx, "SELECT state FROM disks WHERE disk_id = ?", d.String()).Scan(&state)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return catalogue.ErrUnknownDisk
			case err != nil:
				return err
			case state != string(catalogue.DiskActive):
				return catalogue.ErrDiskUnavailable
			}
		}
		now := c.now()
		if _, err := c.exec(ctx, tx, `INSERT INTO volumes (volume_id, bucket, shard, volume_number, state, epoch, format_major, format_minor, cipher_id, tier, created_ms)
			VALUES (?, ?, ?, ?, 'open', 1, ?, ?, ?, ?, ?)`, p.ID.String(), l.Bucket, l.Shard, volumeNumber,
			int(p.FormatMajor), int(p.FormatMinor), int(p.CipherID), int(p.Tier), now); err != nil {
			return err
		}
		v = catalogue.Volume{ID: p.ID, Bucket: l.Bucket, Shard: l.Shard, VolumeNumber: volumeNumber, State: catalogue.VolumeOpen, Epoch: 1,
			FormatMajor: p.FormatMajor, FormatMinor: p.FormatMinor, CipherID: p.CipherID, Tier: p.Tier, CreatedAt: tm(now)}
		for _, d := range p.Disks {
			if _, err := c.exec(ctx, tx, "INSERT INTO volume_replicas (volume_id, disk_id, state) VALUES (?, ?, 'ok')", p.ID.String(), d.String()); err != nil {
				return err
			}
			v.Replicas = append(v.Replicas, catalogue.Replica{Disk: d, State: catalogue.ReplicaOK})
		}
		sortReplicas(v.Replicas)
		return nil
	})
	return v, err
}

func sortReplicas(rs []catalogue.Replica) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && strings.Compare(rs[j-1].Disk.String(), rs[j].Disk.String()) > 0; j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}

// lockOwned locks the volume row and returns its state, epoch, committed
// length and record count if it belongs to the lease's shard.
func (c *Catalogue) lockOwned(ctx context.Context, tx *sql.Tx, l catalogue.Lease, id catalogue.VolumeID) (state string, epoch int, committed, records sql.NullInt64, err error) {
	err = c.row(ctx, tx, "SELECT state, epoch, committed_length, record_count FROM volumes WHERE volume_id = ? AND bucket = ? AND shard = ?"+c.lockUpdate(),
		id.String(), l.Bucket, l.Shard).Scan(&state, &epoch, &committed, &records)
	if errors.Is(err, sql.ErrNoRows) {
		err = catalogue.ErrVolumeNotFound
	}
	return
}

func (c *Catalogue) BeginSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID) (uint32, error) {
	var epoch int
	err := c.tx(ctx, false, func(tx *sql.Tx) error {
		if err := c.lockLease(ctx, tx, l); err != nil {
			return err
		}
		state, _, _, _, err := c.lockOwned(ctx, tx, l, id)
		if err != nil {
			return err
		}
		if state == string(catalogue.VolumeSealed) {
			return catalogue.ErrVolumeSealed
		}
		return c.row(ctx, tx, "UPDATE volumes SET epoch = epoch + 1, state = 'sealing' WHERE volume_id = ? RETURNING epoch", id.String()).Scan(&epoch)
	})
	return uint32(epoch), err
}

func (c *Catalogue) FinishSeal(ctx context.Context, l catalogue.Lease, id catalogue.VolumeID, epoch uint32, committed int64, records uint32) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		if err := c.lockLease(ctx, tx, l); err != nil {
			return err
		}
		state, cur, com, rec, err := c.lockOwned(ctx, tx, l, id)
		if err != nil {
			return err
		}
		if committed < 0 {
			return catalogue.ErrInvalidArgument
		}
		if state == string(catalogue.VolumeSealed) {
			if uint32(cur) == epoch && com.Int64 == committed && uint32(rec.Int64) == records {
				return nil
			}
			return catalogue.ErrSealMismatch
		}
		if uint32(cur) != epoch {
			return catalogue.ErrEpochMismatch
		}
		_, err = c.exec(ctx, tx, "UPDATE volumes SET state = 'sealed', committed_length = ?, record_count = ?, sealed_ms = ? WHERE volume_id = ?",
			committed, int64(records), c.now(), id.String())
		return err
	})
}

func (c *Catalogue) GetVolume(ctx context.Context, id catalogue.VolumeID) (catalogue.Volume, error) {
	var v catalogue.Volume
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		var err error
		if v, err = scanVolume(c.row(ctx, tx, "SELECT "+volumeCols+" FROM volumes WHERE volume_id = ?", id.String())); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrVolumeNotFound
		} else if err != nil {
			return err
		}
		vs := []catalogue.Volume{v}
		if err := c.withReplicas(ctx, tx, vs, "SELECT volume_id, disk_id, state FROM volume_replicas WHERE volume_id = ? ORDER BY disk_id", id.String()); err != nil {
			return err
		}
		v = vs[0]
		return nil
	})
	return v, err
}

func (c *Catalogue) listVolumes(ctx context.Context, q, rq string, args ...any) ([]catalogue.Volume, error) {
	var out []catalogue.Volume
	err := c.tx(ctx, true, func(tx *sql.Tx) error {
		rows, err := c.query(ctx, tx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			v, err := scanVolume(rows)
			if err != nil {
				return err
			}
			out = append(out, v)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		return c.withReplicas(ctx, tx, out, rq, args...)
	})
	return out, err
}

func (c *Catalogue) ListShardVolumes(ctx context.Context, bucket string, shard int) ([]catalogue.Volume, error) {
	return c.listVolumes(ctx,
		"SELECT "+volumeCols+" FROM volumes WHERE bucket = ? AND shard = ? ORDER BY volume_number DESC",
		"SELECT r.volume_id, r.disk_id, r.state FROM volume_replicas r JOIN volumes v ON v.volume_id = r.volume_id WHERE v.bucket = ? AND v.shard = ? ORDER BY r.volume_id, r.disk_id",
		bucket, shard)
}

func (c *Catalogue) ListDiskVolumes(ctx context.Context, disk catalogue.DiskID) ([]catalogue.Volume, error) {
	return c.listVolumes(ctx,
		"SELECT "+volumeCols+" FROM volumes WHERE volume_id IN (SELECT volume_id FROM volume_replicas WHERE disk_id = ?) ORDER BY volume_id",
		"SELECT volume_id, disk_id, state FROM volume_replicas WHERE volume_id IN (SELECT volume_id FROM volume_replicas WHERE disk_id = ?) ORDER BY volume_id, disk_id",
		disk.String())
}

func (c *Catalogue) SetReplicaState(ctx context.Context, id catalogue.VolumeID, disk catalogue.DiskID, s catalogue.ReplicaState) error {
	if s != catalogue.ReplicaOK && s != catalogue.ReplicaFailed && s != catalogue.ReplicaLost {
		return catalogue.ErrInvalidArgument
	}
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		var one int
		if err := c.row(ctx, tx, "SELECT 1 FROM volumes WHERE volume_id = ?", id.String()).Scan(&one); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrVolumeNotFound
		} else if err != nil {
			return err
		}
		n, err := c.exec(ctx, tx, "UPDATE volume_replicas SET state = ? WHERE volume_id = ? AND disk_id = ?", string(s), id.String(), disk.String())
		if err == nil && n == 0 {
			err = catalogue.ErrReplicaNotFound
		}
		return err
	})
}

func (c *Catalogue) AddReplica(ctx context.Context, id catalogue.VolumeID, disk catalogue.DiskID) error {
	return c.tx(ctx, false, func(tx *sql.Tx) error {
		// The volume row lock orders this against a concurrent AddErasures on the same volume.
		var state string
		if err := c.row(ctx, tx, "SELECT state FROM volumes WHERE volume_id = ?"+c.lockUpdate(), id.String()).Scan(&state); errors.Is(err, sql.ErrNoRows) {
			return catalogue.ErrVolumeNotFound
		} else if err != nil {
			return err
		}
		if state != string(catalogue.VolumeSealed) {
			return catalogue.ErrVolumeNotSealed
		}
		var dstate string
		switch err := c.row(ctx, tx, "SELECT state FROM disks WHERE disk_id = ?", disk.String()).Scan(&dstate); {
		case errors.Is(err, sql.ErrNoRows):
			return catalogue.ErrUnknownDisk
		case err != nil:
			return err
		case dstate != string(catalogue.DiskActive):
			return catalogue.ErrDiskUnavailable
		}
		if _, err := c.exec(ctx, tx, `INSERT INTO volume_replicas (volume_id, disk_id, state) VALUES (?, ?, 'ok')
			ON CONFLICT (volume_id, disk_id) DO UPDATE SET state = 'ok'`, id.String(), disk.String()); err != nil {
			return err
		}
		_, err := c.exec(ctx, tx, `INSERT INTO erasure_targets (volume_id, seq, disk_id)
			SELECT volume_id, seq, ? FROM erasures WHERE volume_id = ? ON CONFLICT DO NOTHING`, disk.String(), id.String())
		return err
	})
}
