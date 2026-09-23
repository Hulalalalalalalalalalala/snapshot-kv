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
- `(*Snapshot).Get(key string) ([]byte, bool)` reads from that view.
- `(*Store).Compact() error` reclaims overwritten or deleted history that no
  open snapshot can still observe, shrinking the log on disk. Open snapshots
  keep serving their fixed views unchanged. Safe to call at any time;
  concurrent calls queue and each completes. Crash-safe: a process killed
  mid-compaction reopens to a complete committed state.
- `(*Snapshot).Close() error`, `(*Store).Close() error`.

## Tests

    go test ./...

## Limits

Single process; no network exposure.
No replication.
Values are byte slices owned by the caller after return.
