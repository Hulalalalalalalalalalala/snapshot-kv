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

	// errCheckpointInterrupted is returned only by the test-only crash hook of
	// Checkpoint to simulate a process kill at a chosen instant. Production
	// code paths never return it, and it deliberately wraps no fs error class:
	// no third error type is introduced.
	errCheckpointInterrupted = errors.New("snapshot: checkpoint interrupted by test hook")
)
