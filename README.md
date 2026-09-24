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
- `(*Store).ScanRange(start, end string) ([]KVPair, error)` returns the live keys in the closed byte-order interval `[start, end]` (empty bound = open on that side), sorted.
- `(*Store).ScanPrefix(prefix string) ([]KVPair, error)` returns the live keys beginning with `prefix`, sorted; an empty prefix scans everything.
- `(*Store).Cursor(start, end string) (*Cursor, error)` and `(*Store).CursorPrefix(prefix string) (*Cursor, error)` open a paged scan fixed to the view current at that instant.
- `(*Store).Snapshot() (*Snapshot, error)` opens a stable view.
- `(*Store).Compact() error` reclaims overwritten and deleted history.
- `(*Snapshot).Get(key string) ([]byte, bool)` reads from that view.
- `(*Snapshot).ScanRange(start, end string) []KVPair` and `(*Snapshot).ScanPrefix(prefix string) []KVPair` scan the fixed view, sorted.
- `(*Snapshot).Cursor(start, end string) *Cursor` and `(*Snapshot).CursorPrefix(prefix string) *Cursor` page over the fixed view (the cursor outlives the snapshot).
- `(*Cursor).Next(limit int) (pairs []KVPair, ok bool, err error)` returns the next page of up to `limit` pairs; `ok` is false at the end. `limit <= 0` returns an error wrapping `fs.ErrInvalid` without advancing.
- `(*Cursor).Close() error` releases the pinned view; idempotent.
- `(*Snapshot).Close() error`, `(*Store).Close() error`.

`KVPair` is `struct { Key string; Value []byte }`; value bytes are owned by
the caller. Scans and cursor pages share the snapshot read path: they serve
the in-memory view, return byte-sorted keys, count a present empty value as a
hit exactly like `Get`, and never list empty, deleted or never-written keys.
A reversed interval or a prefix matching nothing yields an empty result and
no error. Scanning a closed store returns an error wrapping `fs.ErrClosed`;
reads from a closed snapshot or cursor are plain misses, and closing either
twice returns nil.

A cursor locks its view when opened: later writes, deletes, compactions, new
snapshots and closing the store never change its pages, and paging one cursor
never repeats or loses a key. Versions an open cursor references are exempt
from compaction reclamation; the first compaction after the cursor closes
reclaims them and shrinks the directory. Cursors are safe for concurrent use;
`Next` and `Close` serialize, so concurrent scans and repeated closes queue
and all succeed without disturbing a page in flight.

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
