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
	// errBackupInvalid rejects a backup artifact as a whole: the path is
	// absent or not a directory, a segment or the manifest is missing,
	// truncated, checksum-bad, of an unknown format version, or disagrees
	// with the manifest.
	errBackupInvalid = errors.Join(
		errors.New("snapshot: backup artifact is missing, incomplete or corrupt"),
		fs.ErrInvalid,
	)
	// errBackupTarget rejects an unusable output path for a backup or a
	// restore: it exists and is not an empty directory.
	errBackupTarget = errors.Join(
		errors.New("snapshot: backup output path exists and is not an empty directory"),
		fs.ErrInvalid,
	)
)
