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
- `(*Store).Backup(outDir string) error` exports one consistent committed state as a portable, segmented artifact.
- `snapshot.Restore(backupDir, targetDir string) (*Store, error)` installs a backup into a fresh directory and returns it open.
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

`Checkpoint` freezes the store's complete committed state into a
self-describing, checksummed **chain of layers** (inside the `ckpt/`
directory) so reopening is cheap: the layers are loaded and only the
write-ahead log written after the newest layer is replayed, instead of
replaying the whole log. The resulting state is byte-for-byte the same as a
full log replay — every successful write and delete, the atomicity and
within-batch last-write-wins ordering of batches, and the cumulative snapshot
count are all preserved.

The first layer (the **base**, index 0) holds the complete key/value state at
its capture time (empty values stay hits, deletions are tombstones). Every
later layer is a **delta** covering only the keys touched by commits strictly
after the layer it builds on. Each layer carries a format version, its layer
index, a content hash linking it to the previous layer, the highest
commit-sequence watermark covered, the cumulative snapshot count, the WAL
byte offset of its cut and a per-entry commit sequence, all protected by a
CRC-32 over the whole file.

Reopen loads the layers in index order and stops at the last complete, intact
layer of one unbroken chain, then replays the WAL tail from that layer's cut.
Any layer that is missing, truncated, checksum-bad, of an unknown version or
whose link to the previous layer is broken rejects that layer and every later
layer as a whole — half a layer is never installed. Recovery then falls back
to the previous usable layer, or, when even the base is unusable, to a
log-only reopen that replays the write-ahead log in full; the rejected suffix
is removed. The cut of the selected layer must lie inside the exact WAL the
chain pairs with, otherwise that suffix is rejected too.

A layer is an all-or-nothing artifact: built under a temporary name, forced
whole with one fsync, then atomically renamed into place. Producing a layer
and making it live are separate, interruptible steps. A process killed at any
point leaves only the chain without that layer or the chain extended by the
complete layer, each a complete committed state recoverable on its own; a
half-built temporary file is swept on the next open, and no directory ever
needs a second checkpoint (or any other repair step) before it opens normally.

Only the newest `4` generations are kept (the retention window). Appending a
layer past the window rebuilds the chain around its newest members against the
unchanged WAL — the oldest surviving layer becomes a fresh full base — and the
retired layers are deleted immediately, so they occupy no further directory
space. Directory and memory usage track live data, queued batches, open
cursors/snapshots and the retained layers, never cumulative commits.

The write-ahead log is never modified by a checkpoint, so a layer pairs only
with the exact log it was captured against. Compaction (the only log rewrite)
durably withdraws the old chain before swapping the log; the layers inside the
retention window are re-anchored against the replacement log — the new log is
written as one segment per surviving layer at a fresh cut, with the rebuilt
layers referencing those cuts — while layers outside the window are retired
and deleted. Checkpoints may be taken repeatedly in one run.

`Checkpoint` changes nothing visible. It serializes with writes, batch
commits, snapshot acquisition, cursor opening and compaction, but never with
lock-free reads: `Get`, scans, snapshot reads and cursor paging keep hitting
memory and are not blocked by the checkpoint syncing to disk. It does not
split a batch or reclaim history that an open snapshot or cursor still
references. `Checkpoint` on a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`. A directory written by an older version with
the single-file `snapshot.ckpt` (or with no checkpoint at all) reopens
unchanged and upgrades to the layered layout the first time a checkpoint is
taken.

### Hot backup and restore

`Backup(outDir)` exports the store's complete committed state at one fixed
commit watermark as a portable artifact inside `outDir`. The export anchors to
the state current at one instant; writes and deletes committed after the
anchor do not enter the artifact, and the foreground keeps committing while
the export runs. The anchor is the only point at which `Backup` takes the
write lock — the keyspace is then streamed out without it, so writes, batch
commits, snapshot acquisition, cursor paging, checkpoints and compactions all
proceed during a backup and never block on its disk writes; lock-free reads
keep hitting memory exactly as usual.

The artifact is segmented and streamed: each segment is self-describing and
carries a format version, the export watermark, the cumulative snapshot count
and a CRC-32 over the whole segment, and entries flow to disk in bounded
chunks — the whole keyspace is never copied into memory before being written.
A `manifest.dat`, written and fsynced last, is the commit point: it records the
artifact-level metadata and the index, entry count, cumulative count and
content checksum of every segment. The artifact is therefore complete if and
only if the manifest is present, intact and agrees exactly with the segment
files on disk.

`snapshot.Restore(backupDir, targetDir)` validates the artifact as one whole
and installs a ready-to-open store into `targetDir`, returning it already
open. A missing segment, a truncated segment, a checksum mismatch, an unknown
format version, or a manifest that disagrees with the files rejects the entire
backup with an error satisfying `errors.Is(err, fs.ErrInvalid)` and creates no
target. A backup path that does not exist or is not a directory, and a target
path that already exists and is non-empty (including a regular file), are
rejected with the same error. The restored directory, once reopened, is
byte-for-byte the state a full replay reaches at the export watermark: batch
atomicity and within-batch last-write-wins ordering and the cumulative
snapshot count are all preserved.

Both export and import are all-or-nothing: a process killed at any instant
leaves either the previous contents or one complete result, never a
half-populated directory. Segments and the manifest are built and fsynced in a
`backup.tmp` staging directory and promoted one at a time; a restore builds the
complete store in a temporary sibling and renames it into place only once it is
fully durable. Residual temporary files are swept at the start of the next
backup into that directory or the next open of a related directory, and a store
that is backed up is never modified by the export. A reopened restored
directory already holds the complete committed state — no further export or
compaction is needed to use it, and the cumulative snapshot count is not reset.

`Backup` on a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`. Backup never copies the WAL or checkpoint
layout: the artifact is a self-contained, versioned snapshot of the data, not a
duplicate of the on-disk internals.

## Tests

    go test ./...

## Limits

Single process; no network exposure.
No replication.
Values are byte slices owned by the caller after return.
