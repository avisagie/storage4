package volume

import "errors"

var (
	// ErrFailed means an earlier append, sync or seal on this writer failed.
	// The file's state is unknown beyond Committed(); the volume must be
	// sealed with SealFile and replaced. Never retried.
	ErrFailed = errors.New("volume: writer failed")
	// ErrSealed means the writer was sealed and is closed.
	ErrSealed = errors.New("volume: sealed")
	// ErrFull means the batch does not fit under the volume size cap or
	// the seq space. Nothing was consumed; use another volume.
	ErrFull = errors.New("volume: full")
	// ErrInvalidEntry means an entry violates name or size rules. Nothing
	// was consumed.
	ErrInvalidEntry = errors.New("volume: invalid entry")
	// ErrShortReplica means the file has no valid record boundary at the
	// requested committed length: this replica lost acknowledged data.
	ErrShortReplica = errors.New("volume: replica shorter than committed length")
	// ErrSealMismatch means the volume is already sealed at another length.
	ErrSealMismatch = errors.New("volume: sealed at a different committed length")
	// ErrMismatch means a record does not match its locator (seq, check,
	// length or name): a stale or wrong index entry, or corruption.
	ErrMismatch = errors.New("volume: record does not match locator")
	// ErrErased means the record has been erased.
	ErrErased = errors.New("volume: record erased")
	// ErrNotFileRecord means the locator points at a record that is not a
	// file or erased-file record.
	ErrNotFileRecord = errors.New("volume: not a file record")
)
