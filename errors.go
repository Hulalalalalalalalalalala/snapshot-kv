package snapshot

import (
	"errors"
	"io/fs"
)

// Sentinel errors. They wrap the standard fs errors so callers can use
// errors.Is(err, fs.ErrInvalid) and errors.Is(err, fs.ErrClosed).
var (
	errEmptyKey = errors.Join(
		errors.New("snapshot: key must not be empty"),
		fs.ErrInvalid,
	)
	errNotDir = errors.Join(
		errors.New("snapshot: path is not a directory"),
		fs.ErrInvalid,
	)
	errStoreClosed = errors.Join(
		errors.New("snapshot: store is closed"),
		fs.ErrClosed,
	)
	errCursorArgument = errors.Join(
		errors.New("snapshot: cursor continuation offset or limit is invalid"),
		fs.ErrInvalid,
	)
	errEmptyPath = errors.Join(
		errors.New("snapshot: path must not be empty"),
		fs.ErrInvalid,
	)
	errBackupInvalid = errors.Join(
		errors.New("snapshot: backup is incomplete or corrupt"),
		fs.ErrInvalid,
	)
	errRestoreTarget = errors.Join(
		errors.New("snapshot: restore target already contains store data"),
		fs.ErrInvalid,
	)
	errSnapshotClosed = errors.Join(
		errors.New("snapshot: snapshot is closed"),
		fs.ErrClosed,
	)
)
