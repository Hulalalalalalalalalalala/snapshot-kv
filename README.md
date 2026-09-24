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
- `(*Store).CommitBatch(ops []BatchOp) error` commits several changes atomically.
- `(*Store).Snapshot() (*Snapshot, error)` opens a stable view.
- `(*Store).Checkpoint() error` freezes complete committed state for a fast reopen.
- `(*Store).Compact() error` reclaims overwritten and deleted history.
- `(*Snapshot).Get(key string) ([]byte, bool)` reads from that view.
- `(*Snapshot).Close() error`, `(*Store).Close() error`.

### Atomic batches

`CommitBatch` applies several writes and deletes as one commit. A `BatchOp`
is `{Key string, Value []byte, Delete bool}`: `Delete: false` stores `Value`
under `Key` (a nil or empty value is still a hit, exactly as with `Put`),
`Delete: true` removes `Key`.

- Changes are applied in list order; within one batch a later change to the
  same key wins (put-then-delete leaves the key deleted; delete-then-put
  leaves it present).
- The whole batch becomes visible together or not at all. Every snapshot and
  every cursor sees either the state before the batch or the state after it,
  never a mixture. Concurrent batches take effect in commit order and a later
  batch's same-key change overrides an earlier one's.
- The batch is forced to stable storage as a single unit before
  `CommitBatch` returns. Reopening the directory after a crash restores the
  entire batch or none of it; a half-written batch tail is discarded
  wholesale, with already-committed batches and the cumulative snapshot
  count intact.
- If any op has an empty key, the entire batch is rejected before anything
  is written: no change becomes visible, no frame lands on disk, and the
  error satisfies `errors.Is(err, fs.ErrInvalid)`. `CommitBatch` on a closed
  store satisfies `errors.Is(err, fs.ErrClosed)`.
- A delete of a key that is absent or already deleted commits nothing, just
  like `Delete`; a batch containing only such no-ops (or an empty slice)
  commits nothing and returns nil.
- `Put` and `Delete` are exactly a one-op batch and keep their behavior.
- Reads still hit the in-memory view without taking the write lock and are
  not blocked by the batch syncing to disk.

### Range scans and paged cursors

Both the latest view (`*Store`) and a fixed view (`*Snapshot`) support
ordered scans over a half-open byte interval or a prefix:

- `Range{Start, End string}` selects `[Start, End)`; an empty `Start` or
  `End` means unbounded on that side.
- `(*Store).Scan(r Range, limit int) ([]KVPair, error)`
- `(*Store).ScanPrefix(prefix string, limit int) ([]KVPair, error)`
- `(*Snapshot).Scan(r Range, limit int) ([]KVPair, error)`
- `(*Snapshot).ScanPrefix(prefix string, limit int) ([]KVPair, error)`
- `KVPair{Key string, Value []byte}`; returned bytes are owned by the caller.
- A `limit <= 0` returns every matching pair. Pairs come back in ascending
  byte order. Hit semantics match `Get`: keys written with empty values are
  returned, while empty keys, deleted keys and keys never written are not.
- A reversed interval (`Start` not before `End`) and a prefix matching no
  keys return an empty slice and `nil`, never an error or partial data.
- Scanning a closed store returns an error satisfying
  `errors.Is(err, fs.ErrClosed)`; scanning a closed snapshot is an ordinary
  empty result, like `Get` on a closed snapshot is a miss.

A cursor locks a fixed view the instant it opens and pages through it in
stable byte order; later writes, deletes, compactions, new snapshots and
closing the store never change what it returns:

- `(*Store).OpenCursor(r Range) (*Cursor, error)`
- `(*Store).OpenCursorPrefix(prefix string) (*Cursor, error)`
- `(*Snapshot).OpenCursor(r Range) (*Cursor, error)`
- `(*Snapshot).OpenCursorPrefix(prefix string) (*Cursor, error)`
- `(*Cursor).Page(offset, limit int) ([]KVPair, error)` returns at most
  `limit` pairs starting at `offset`. Start at `0`, then pass the previous
  offset plus the number of pairs returned; the pages concatenate to the
  cursor's exact sequence with no duplicates and no gaps. An `offset` equal
  to the sequence length returns an empty page and `nil`.
- A non-positive `limit`, a negative `offset`, or an `offset` past the end
  returns an error satisfying `errors.Is(err, fs.ErrInvalid)`.
- `(*Cursor).Close() error` is idempotent and returns `nil`; paging a closed
  cursor is an empty result without error. Reads and repeated closes
  serialize and never fail.
- The versions a cursor references are kept out of compaction reclamation
  until it closes; the next compaction after it closes reclaims them and
  shrinks the directory further. Opening a cursor does not count as taking a
  snapshot.

Scans and cursor reads hit the in-memory view on the same lock-free path as
`snapshot.Get`; they are not blocked by a write synchronizing to disk.

`Compact` takes no arguments and may be called at any time. It reclaims old
versions that have been overwritten or deleted and are no longer referenced
by an open snapshot, both from memory and from the write-ahead log, and
rewrites the log atomically. Every open snapshot keeps serving its exact
fixed view through and after compaction; reopening the directory after a
crash mid-compaction restores a complete committed state with the cumulative
snapshot count intact. Repeated or concurrent compactions queue and all
return nil; compacting a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`. The old write-ahead log is released before
the replacement is renamed into place, so the swap also succeeds on
filesystems that refuse renaming over an open file. Compaction never splits
or reorders versions that belong to one batch.

### Checkpoints and fast reopen

`Checkpoint` freezes the store's complete committed state into one
self-describing file (`snapshot.ckpt`) so reopening is cheap: the checkpoint
is loaded into memory and only the write-ahead log written *after* it is
replayed, instead of replaying the whole log. The resulting state is
byte-for-byte the same as a full log replay — every successful write and
delete, the atomicity and within-batch last-write-wins ordering of batches,
and the cumulative snapshot count are all preserved.

The file carries the terminal key/value state (empty values stay hits,
deletions are tombstones), the highest commit-sequence watermark, the
cumulative snapshot count and a format version, all protected by a CRC-32.
It is an all-or-nothing artifact: built under a temporary name, forced whole
with one fsync, then atomically renamed into place. On reopen a missing,
truncated, checksum-bad or unknown-version checkpoint is rejected as a whole
— half a checkpoint is never installed. Rejection is not an error: recovery
silently falls back to the log-only reopen path and replays the write-ahead
log in full, and the rejected file is removed.

Producing a checkpoint and making it live are separate, interruptible steps.
A process killed at any point leaves only the pre-checkpoint state or the
post-checkpoint state, each a complete committed state recoverable on its
own; a half-built temporary file is swept on the next open, and no directory
ever needs a second checkpoint (or any other repair step) before it opens
normally. The write-ahead log is never modified by a checkpoint, so a
checkpoint pairs only with the exact log it was taken against; compaction
(the only log rewrite) durably removes a live checkpoint before swapping the
log. Checkpoints may be taken repeatedly in one run; each replaces the prior
file, which then occupies no further space.

`Checkpoint` changes nothing visible. It serializes with writes, batch
commits, snapshot acquisition, cursor opening and compaction, but never with
lock-free reads: `Get`, scans, snapshot reads and cursor paging keep hitting
memory and are not blocked by the checkpoint syncing to disk. It does not
split a batch or reclaim history that an open snapshot or cursor still
references. `Checkpoint` on a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`. A directory written by an older version with
no checkpoint reopens unchanged and upgrades to the new layout the first
time a checkpoint is taken.

## Tests

    go test ./...

## Limits

Single process; no network exposure.
No replication.
Values are byte slices owned by the caller after return.
