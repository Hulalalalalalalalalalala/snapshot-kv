# snapshot-kv

Key value store whose readers take a point-in-time snapshot: a snapshot never observes writes that were committed after it was opened.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/snapshotctl --dir <path> stats

## Public interface

`snapshot.Open(dir string) (*Store, error)` opens or creates a store.
- `(*Store).Put(key string, value []byte) error` stores a value.
- `(*Store).Get(key string) ([]byte, bool, error)` reads the latest value.
- `(*Store).Delete(key string) error` removes a key.
- `(*Store).Snapshot() (*Snapshot, error)` opens a stable view.
- `(*Store).Compact() error` reclaims overwritten and deleted history.
- `(*Snapshot).Get(key string) ([]byte, bool)` reads from that view.
- `(*Snapshot).Close() error`, `(*Store).Close() error`.

`Compact` takes no arguments and may be called at any time. It reclaims old
versions that have been overwritten or deleted and are no longer referenced
by an open snapshot, both from memory and from the write-ahead log, and
rewrites the log atomically. Every open snapshot keeps serving its exact
fixed view through and after compaction; reopening the directory after a
crash mid-compaction restores a complete committed state with the cumulative
snapshot count intact. Repeated or concurrent compactions queue and all
return nil; compacting a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`.

## Tests

    go test ./...

## Limits

Single process; no network exposure.
No replication.
Values are byte slices owned by the caller after return.
