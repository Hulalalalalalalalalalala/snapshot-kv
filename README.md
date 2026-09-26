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
- `(*Store).BackupIncremental(chainDir string) error` appends one incremental ring to an existing backup chain.
- `(*Store).MergeChain(chainDir string, rings int) error` folds the chain head and its first rings into one new full head and retires the absorbed rings.
- `snapshot.Restore(backupDir, targetDir string) (*Store, error)` installs a backup (a lone full artifact or a full head plus its incremental rings) into a fresh directory and returns it open.
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

### Incremental backup chains

`BackupIncremental(chainDir)` extends a backup chain that already starts with a
complete `Backup` artifact in `chainDir`: the full export is the chain head,
and each call appends one self-describing **ring** (`incr-0000000001.dat`,
`incr-0000000002.dat`, …) covering only the terminal state of keys touched
between the previous ring's watermark and a fresh fixed anchor. A ring records
later puts as values and keys deleted in the interval as tombstones — keys
touched several times appear once, in their terminal state; keys deleted and
since forgotten entirely by a compaction are still recorded as tombstones, so
a delete is never resurrected by later compaction of the backed-up store. The
ring anchors at one fixed watermark exactly like a full backup: writes and
deletes committed after the anchor do not enter it, and the foreground keeps
committing while the ring streams out. A ring with no touched keys is a legal
empty ring that still advances the cumulative snapshot count.

Rings share the head's format version, watermark and cumulative-snapshot
vocabulary: each ring header carries the format version, its 1-based ring
index, a content-CRC link to the artifact before it (the head manifest for
ring 1, the preceding ring's file CRC afterwards), the watermark it starts
from (exclusive) and ends at (inclusive), and the cumulative snapshot count at
that point. Its entries flow in bounded segments, each self-describing and
independently CRC-protected, and the ring finishes with a chained segment CRC
and a whole-file CRC.

`Restore` accepts both a lone full artifact and a head plus its rings: when
rings are present the **whole chain is validated first** and the result is
synthesized by applying the head and then the rings in chain order, each ring
overriding the one before it, so a key deleted in a later ring reads back as a
miss. A missing ring, a gap in the dense ring sequence, a broken predecessor
link, a non-continuous watermark, a truncated ring or segment, a checksum
mismatch, an unknown format version, or a foreign/stray file rejects the
entire chain with an error satisfying `errors.Is(err, fs.ErrInvalid)`, and no
target is created — half a chain is never installed. The head-only path and
its on-disk format are unchanged. The restored directory, reopened, is
byte-for-byte the state a full replay reaches at the last ring's watermark,
with batch semantics and the cumulative snapshot count intact.

`BackupIncremental` streams in bounded chunks and never copies the keyspace
into memory before writing; export and synthesis do not block writes,
deletes, batch commits, snapshot open/close, cursor paging, checkpoints or
compaction, and reads keep hitting memory. The ring is built and synced in an
`incr.tmp` staging directory and linked into place (a link never overwrites),
so a kill at any instant leaves either the previous chain or the chain
extended by one complete ring; staging debris is swept at the next export into
that directory or the next open of it, and the backed-up store is never
modified. A chain directory that does not exist, is not a directory, holds no
valid head, or is itself damaged is rejected wholesale with
`errors.Is(err, fs.ErrInvalid)` and leaves no partial ring;
`BackupIncremental` on a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`.

### Merging a backup chain

`MergeChain(chainDir, rings)` folds the chain head and its first `rings`
incremental rings into one new full head, so a chain that only grows can be
compacted back down. The merged head is exactly the complete committed state
at the last merged ring's watermark — keys, values and the cumulative
snapshot count are preserved, and deletes covered by the merged rings read
back as misses. The rings beyond the merged prefix stay on the chain with
their entries, watermarks and snapshot counts unchanged, renumbered from 1
behind the new head, and the absorbed rings are deleted. The merged chain
passes the same whole-chain validation `Restore` performs — dense ring
numbering, continuous watermarks, unbroken content-CRC links, the same format
version — and later `BackupIncremental` rings append to it exactly as before,
so restoring before and after a merge yields byte-for-byte the same terminal
state and cumulative snapshot count.

The merge streams every artifact in bounded chunks (a k-way merge over the
key-sorted head and ring streams) and never holds the whole keyspace in
memory. It runs entirely on the chain directory: the backed-up store is never
modified, and writes, batch commits, snapshot open/close, cursor paging,
checkpoints, compactions, incremental exports and lock-free reads proceed
while it runs. The new chain is assembled in a `<chain>.merge-new-*` sibling
directory, validated as a whole, and swapped into place (the old chain moves
to a `<chain>.merge-old-*` sibling and is deleted once the new one is
installed), so a kill at any instant leaves either the old chain or the
complete new chain; leftover staging or trash siblings are swept at the next
merge, backup or export into that directory or the next open of a related
directory.

A ring count below one or beyond the chain's ring count, a chain directory
that is missing, not a directory, missing its head, or itself damaged (a
missing ring, a broken link, a non-continuous watermark, truncation, a
checksum failure or an unknown version), or a chain advanced concurrently
during the merge, rejects the whole operation with an error satisfying
`errors.Is(err, fs.ErrInvalid)` and leaves the chain exactly as it was.
`MergeChain` on a closed store returns an error satisfying
`errors.Is(err, fs.ErrClosed)`.

### Cross-storage coordination and lease recovery

The chain directory is shared: more than one store (even in separate
processes) can be asked to run a full export, an incremental append, a merge
or a restore against the same path. Advancement is therefore coordinated by
one self-describing lease record. The authoritative lease is a single file in
the chain directory's parent, named `<chain>.chain-lease`; a descriptive
mirror (`lease.dat`) is also carried inside the chain directory. Every
record is checksummed and names its holder — operation kind, process id,
host, issue time, a random nonce — and an absolute expiry time.

Exactly one holder exists at a time: the lease is claimed with a
never-overwriting hard link, so concurrent contenders cannot both win. The
holder alone advances the chain; a contender that finds a live, unexpired
lease fails the whole operation with `errors.Is(err, fs.ErrInvalid)` and
changes nothing. A holder renews its lease while it streams, so long exports
and merges keep the lock without holding the keyspace; writes, deletes,
batches, snapshot open/close, cursor paging, checkpoints, compaction and
lock-free reads proceed as before and are never blocked by coordination. At
every commit point the holder re-checks that it still owns the lease, so a
ring appended and a new merged head can never overwrite or lose one another.

The lease is crash-safe. If a holder is killed mid-run it simply stops
renewing; once its expiry passes, the next full export, incremental export,
merge, restore or open of a related directory recognizes the stale record,
reclaims it and takes over. The chain underneath is always one complete old
chain or one complete new chain, so recovery needs no manual cleanup and can
never deadlock. Taking over before the expiry fails with `fs.ErrInvalid` and
leaves the chain untouched; a truncated or corrupt lease file is treated as
dead debris rather than a live holder. Recovery of a merge killed between its
two swap renames moves the whole parked old chain back into place (or leaves
the complete new chain in place) and never deletes it; coordination and
staging files are never mistaken for artifact files, while a chain directory
that does not exist, is not a directory, has no usable head, or whose rings
are gapped, broken, discontinuous in watermark, truncated, checksum-bad or of
an unknown version is rejected wholesale with `fs.ErrInvalid` and left as it
was. Every chain operation on a closed store satisfies
`errors.Is(err, fs.ErrClosed)`; coordination introduces no other error type.

## Tests

    go test ./...

## Limits

Single process; no network exposure.
No replication.
Values are byte slices owned by the caller after return.
