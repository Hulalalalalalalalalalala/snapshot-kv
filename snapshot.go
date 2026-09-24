// Package snapshot implements a persistent key-value store whose readers
// operate on immutable point-in-time snapshots. A snapshot never observes a
// write committed after it was acquired, and it keeps serving its fixed view
// even after the store that produced it has been closed.
package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// node is one immutable version in a key's history. Versions form a linked
// list from newest to oldest; a snapshot pins the head current when it was
// acquired, so overwritten and deleted values stay readable through every
// view that references them. seq is the version's commit number, unique and
// strictly increasing along each key's chain; it orders versions across the
// independent chains that meet again during a pinning compaction.
type node struct {
	value   []byte
	present bool   // false marks a delete; a present nil/empty value is still a hit
	seq     uint64 // commit order, 1-based
	older   *node
}

// view is an immutable map of keys to their newest version. Updates produce a
// fresh map; the current view and every snapshot simply point at one. Views
// are never mutated after publication, so reading them needs no lock. The
// maps share version chains with their successor views.
type view map[string]*node

// Store is an open key-value store backed by a directory on disk.
type Store struct {
	mu sync.Mutex // serializes writers and guards the fields below

	dir       string
	wal       *walWriter
	current   atomic.Pointer[view] // latest committed view; lock-free for readers
	snapshots atomic.Uint64        // cumulative number of snapshots handed out
	nextSeq   uint64               // highest commit sequence handed out; guarded by mu

	// Open cursors pin their fixed view's versions against compaction. The
	// set is read by Compact (which holds mu) and mutated at cursor
	// open/close, using cursorMu; reads never take either lock.
	cursorMu sync.Mutex
	cursors  map[*Cursor]struct{}

	// Group-commit queue. Batches that arrive while another commit is
	// forcing the log line up here; one leader drains the queue, encodes the
	// waiting batches and shares a single flush plus fsync across them.
	// queueMu only ever guards this queue and committer; it is never held
	// across disk I/O, so readers and enqueuers never wait on a sync.
	queueMu   sync.Mutex
	queue     []*pendingBatch
	committer bool

	// Test hook, nil outside tests. leaderHook runs once after a batch has
	// become leader, before its first queue drain, and lets tests hold a
	// leader until a known number of batches have coalesced.
	leaderHook func()

	// Test-only crash windows for Checkpoint, nil in production. When set, it
	// is invoked after the temp checkpoint has been fully synced
	// (checkpointTempSynced) and after the atomic rename but before the
	// directory is forced (checkpointRenamed). Returning true simulates a
	// process kill at that instant: Checkpoint stops immediately, leaves the
	// directory exactly as it is and returns errCheckpointInterrupted.
	checkpointHook func(stage checkpointStage) bool

	closed atomic.Bool
}

// checkpointStage names one interruptible point of a checkpoint freeze.
type checkpointStage int

const (
	checkpointTempSynced checkpointStage = iota // temp durable, rename not done
	checkpointRenamed                           // rename landed, dir not yet forced
)

// Snapshot is a stable read-only view of a store captured at one instant. It
// owns no store resources of its own and stays usable after the store has
// been closed; a cursor opened from it does register with the store while it
// pins versions.
type Snapshot struct {
	v     atomic.Pointer[view] // nil once the snapshot is closed
	store *Store               // back-reference for cursor pin accounting
}

// Open opens or creates a persistent store in dir. A path that names a
// regular file rather than a directory returns an error wrapping
// fs.ErrInvalid.
func Open(dir string) (*Store, error) {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return nil, errNotDir
		}
	} else {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}

	// A replacement log from a compaction killed before its rename can never
	// be complete state of record; the old wal.log is. Drop the stale file.
	if err := removeStaleCompact(dir); err != nil {
		return nil, err
	}
	// Likewise, a checkpoint temp from a freeze killed before its rename can
	// never be a complete checkpoint; the live checkpoint (if any) is.
	if err := removeStaleCheckpointTemp(dir); err != nil {
		return nil, err
	}

	latest, replaySeq, snapshotCount, validSize, err := recoverState(dir)
	if err != nil {
		return nil, err
	}

	wal, err := openWALWriter(dir, validSize)
	if err != nil {
		return nil, err
	}

	s := &Store{dir: dir, wal: wal, cursors: make(map[*Cursor]struct{}), nextSeq: replaySeq}
	s.snapshots.Store(snapshotCount)
	s.current.Store(&latest)
	return s, nil
}

// recoverState rebuilds the committed state, using a complete checkpoint plus
// only the log frames that follow it whenever a usable checkpoint exists. A
// checkpoint that is missing, truncated, corrupt, written by an unknown
// version, or inconsistent with the log is rejected wholesale — never half
// applied — and recovery falls back to replaying wal.log from the beginning,
// which always reconstructs the complete committed state. A directory with no
// checkpoint therefore reopens exactly as before, and the first checkpoint
// upgrades it to the checkpoint layout with no separate migration.
func recoverState(dir string) (view, uint64, uint64, int64, error) {
	var walSize int64
	if info, err := os.Stat(dir + string(os.PathSeparator) + walName); err == nil {
		walSize = info.Size()
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, 0, 0, 0, err
	}

	cp, _ := loadCheckpoint(dir, walSize)
	if cp != nil {
		latest := make(view, len(cp.entries))
		for i := range cp.entries {
			e := &cp.entries[i]
			latest[e.key] = &node{value: cloneBytes(e.value), present: true, seq: e.seq}
		}
		var replaySeq = cp.nextSeq
		validSize, snapshotCount, tailErr := replayWALFrom(dir, cp.walOffset,
			func(op byte, key string, value []byte, seq uint64) {
				if seq == 0 {
					// Defensive: a post-checkpoint tail only ever carries Seq
					// records, but number any legacy frame in commit order
					// rather than resetting the watermark.
					replaySeq++
					seq = replaySeq
				} else if seq > replaySeq {
					replaySeq = seq
				}
				switch op {
				case opPut, opPutSeq:
					latest[key] = &node{value: cloneBytes(value), present: true, seq: seq, older: latest[key]}
				case opDelete, opDeleteSeq:
					latest[key] = &node{present: false, seq: seq, older: latest[key]}
				}
			})
		if tailErr == nil {
			// Meta frames in the tail carry absolute cumulative counts; absent
			// any, the checkpoint's frozen count is current.
			if snapshotCount < cp.snapshotCount {
				snapshotCount = cp.snapshotCount
			}
			return latest, replaySeq, snapshotCount, validSize, nil
		}
		// The checkpoint did not pair with a usable tail; fall through and
		// recover from the log alone.
	}

	// Full-log recovery. A torn/corrupt WAL still yields its complete
	// committed prefix here, so this path is always the authoritative
	// fallback the rejection contract relies on.
	latest := make(view)
	var replaySeq uint64
	validSize, snapshotCount, err := replayWAL(dir, func(op byte, key string, value []byte, seq uint64) {
		// Legacy records (written before sequence numbers existed) carry no
		// seq; replay order is commit order, so number them sequentially.
		if seq == 0 {
			replaySeq++
			seq = replaySeq
		} else if seq > replaySeq {
			replaySeq = seq
		}
		switch op {
		case opPut, opPutSeq:
			latest[key] = &node{value: cloneBytes(value), present: true, seq: seq, older: latest[key]}
		case opDelete, opDeleteSeq:
			latest[key] = &node{present: false, seq: seq, older: latest[key]}
		}
	})
	if err != nil {
		return nil, 0, 0, 0, err
	}
	// Recovery from the log alone succeeded; a rejected checkpoint can never
	// accelerate this directory as-is, so retire it. Best effort: a failure
	// only leaves a harmless file the next open ignores. A directory that
	// never had a checkpoint takes no extra directory sync here.
	if rmErr := os.Remove(dir + string(os.PathSeparator) + checkpointName); rmErr == nil {
		_ = syncDirectory(dir)
	} else if !errors.Is(rmErr, fs.ErrNotExist) {
		// Keep the recovered state; leave removal for a later open.
	}
	return latest, replaySeq, snapshotCount, validSize, nil
}

// Put stores value under key. An empty key returns an error wrapping
// fs.ErrInvalid. A nil or empty value is stored as-is: a following Get on the
// key reports a hit. The store does not keep a reference to value.
func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if key == "" {
		return errEmptyKey
	}
	seq := s.nextSeq + 1
	if err := s.wal.appendPut(key, value, seq); err != nil {
		return err
	}
	s.nextSeq = seq
	next := s.deriveView()
	next[key] = &node{value: cloneBytes(value), present: true, seq: seq, older: (*s.current.Load())[key]}
	s.current.Store(&next)
	return nil
}

type pendingBatch struct {
	ops  []BatchOp
	done chan error // buffered(1); the leader delivers the commit result
}

// commitPlan is one queued batch resolved against the cumulative state built
// up across its whole sync group.
type commitPlan struct {
	view view // cumulative view after this batch; nil when it committed nothing
}

// BatchOp is one change in a batch committed with CommitBatch. Delete false
// stores Value under Key (a nil or empty Value is still a hit); Delete true
// removes Key.
type BatchOp struct {
	Key    string
	Value  []byte
	Delete bool
}

// CommitBatch applies a group of writes and deletes as one atomic commit:
// the whole batch becomes visible together or not at all, and snapshots and
// cursors fixed at any instant see either the state before the batch or the
// state after it, never an in-between mixture. Changes are applied in list
// order; a later change to the same key overrides an earlier one within the
// batch. Concurrent batches take effect in commit (enqueue) order, and a
// later committed batch's same-key change overrides an earlier one's.
//
// Batches committed concurrently share the disk sync: one leader drains the
// queue, encodes every waiting batch as its own recoverable framed commit
// and forces them all with one flush plus fsync, so N batches arriving
// together cost one sync while each stays whole on recovery; a batch that
// arrives while a sync is in flight rides the next one. Reads take none of
// these locks and are never blocked by the sync.
//
// If any change has an empty key the whole batch is rejected before it is
// queued or committed: an error wrapping fs.ErrInvalid is returned, no frame
// is written and the store's state is unchanged. A delete of a key that is
// absent or already deleted commits nothing, exactly like Delete; a batch
// containing only such no-ops (or an empty slice) commits nothing and
// returns nil. CommitBatch on a closed store returns an error wrapping
// fs.ErrClosed.
//
// Each batch is forced to stable storage before its call returns and is
// published as one new view; a process killed mid-commit therefore reopens
// with the entire batch present or entirely absent. The store does not keep
// references to the op Value slices after return.
func (s *Store) CommitBatch(ops []BatchOp) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	if err := validateBatchOps(ops); err != nil {
		return err
	}

	me := &pendingBatch{ops: ops, done: make(chan error, 1)}

	// Enqueue. queueMu guards only the queue and the leader flag and is
	// never held across disk I/O, so followers can join a group while the
	// leader is syncing and readers never wait.
	s.queueMu.Lock()
	if s.closed.Load() {
		s.queueMu.Unlock()
		return errStoreClosed
	}
	s.queue = append(s.queue, me)
	if s.committer {
		s.queueMu.Unlock()
		return <-me.done // the running leader will drain this batch
	}
	s.committer = true
	s.queueMu.Unlock()

	// Test-only rendezvous: invoked once by the elected leader before its
	// first queue drain. Production leaves it nil.
	if s.leaderHook != nil {
		s.leaderHook()
	}

	// Leadership: drain groups until the queue is empty. Each group takes
	// s.mu while it forces the log, which serializes it with Put, Delete,
	// Snapshot, Compact and Close, so a group never straddles a log swap.
	for {
		s.queueMu.Lock()
		// Resign and re-check the queue under one critical section, so a
		// batch enqueuing in this instant either observes the still-active
		// leader or becomes the next one; no batch can be left queued with
		// no leader to drain it.
		if len(s.queue) == 0 {
			s.committer = false
			s.queueMu.Unlock()
			break
		}
		group := s.queue
		s.queue = nil
		s.queueMu.Unlock()
		for p, err := range s.commitGroup(group) {
			group[p].done <- err
		}
	}

	return <-me.done
}

// validateBatchOps checks argument-only invariants before a batch is queued.
// Empty keys map to fs.ErrInvalid, exactly as for Put and Delete.
func validateBatchOps(ops []BatchOp) error {
	for i := range ops {
		if ops[i].Key == "" {
			return errEmptyKey
		}
	}
	for i := range ops {
		if len(ops[i].Key) > maxRecord || (!ops[i].Delete && seqSize+len(ops[i].Value) > maxRecord) {
			return fmt.Errorf("snapshot: record too large")
		}
	}
	return nil
}

// commitGroup encodes and forces every queued batch with one flush and one
// sync, then publishes one view per effective batch in queue (commit) order.
// It returns one result per batch. An encoding or sync failure fails the
// whole group: nothing it contained is published or sequenced. The single
// committer calls this; s.mu is taken for the entire force, exactly as the
// single-key commit path does.
func (s *Store) commitGroup(group []*pendingBatch) []error {
	results := make([]error, len(group))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() || s.wal == nil {
		// Close ran between the drain and the lock; nothing can commit.
		for i := range results {
			results[i] = errStoreClosed
		}
		return results
	}

	// Remember where the group starts so any failure can rewind the log:
	// unacknowledged frames must not linger at EOF and reappear under a
	// later commit or after a reopen. Nothing is buffered here (the previous
	// group always flushed), so EOF is the exact offset.
	groupStart, posErr := s.wal.position()
	if posErr != nil {
		for i := range results {
			results[i] = posErr
		}
		return results
	}
	rollback := func(cause error) []error {
		if err := s.wal.truncateTo(groupStart); err != nil {
			cause = errors.Join(cause, err)
		}
		for i := range results {
			results[i] = cause
		}
		return results
	}

	next := *s.current.Load()
	seq := s.nextSeq
	plans := make([]commitPlan, 0, len(group))
	encodedAny := false

	for _, p := range group {
		// Each batch gets its own cumulative, immutable map, so the view it
		// publishes is a valid point-in-time state independent of later
		// batches in the group.
		view := make(view, len(next)+len(p.ops))
		for k, v := range next {
			view[k] = v
		}
		var changes []batchChange
		for i := range p.ops {
			op := &p.ops[i]
			head := view[op.Key]
			if op.Delete {
				if head == nil || !head.present {
					continue // idempotent no-op, matching the single Delete
				}
				seq++
				changes = append(changes, batchChange{key: op.Key, seq: seq, delete: true})
				view[op.Key] = &node{present: false, seq: seq, older: head}
			} else {
				seq++
				value := cloneBytes(op.Value)
				changes = append(changes, batchChange{key: op.Key, seq: seq, value: value})
				view[op.Key] = &node{value: value, present: true, seq: seq, older: head}
			}
		}
		if len(changes) > 0 {
			if err := s.wal.encodeBatch(changes); err != nil {
				return rollback(err)
			}
			encodedAny = true
		}
		plans = append(plans, commitPlan{view: ternaryView(len(changes) > 0, view)})
		next = view
	}

	if encodedAny {
		// One flush, one fsync for the whole group. Return means every
		// encoded batch in it is durable as a self-contained framed commit.
		if err := s.wal.flushSync(); err != nil {
			return rollback(err)
		}
	}

	s.nextSeq = seq
	for _, plan := range plans {
		if plan.view != nil {
			s.current.Store(&plan.view)
		}
	}
	return results
}

// ternaryView returns v when ok, nil otherwise.
func ternaryView(ok bool, v view) view {
	if ok {
		return v
	}
	return nil
}

// Get returns the latest committed value for key. Empty keys, keys never
// written and deleted keys are all misses: ok is false and err is nil. The
// returned bytes are owned by the caller.
func (s *Store) Get(key string) ([]byte, bool, error) {
	if s.closed.Load() {
		return nil, false, errStoreClosed
	}
	if key == "" {
		return nil, false, nil
	}
	val, ok := lookup(*s.current.Load(), key)
	return val, ok, nil
}

// Delete removes key. Deleting an empty key returns an error wrapping
// fs.ErrInvalid; deleting a key that was never written or is already deleted
// is not an error and commits nothing.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if key == "" {
		return errEmptyKey
	}
	cur := *s.current.Load()
	if n := cur[key]; n == nil || !n.present {
		return nil // nothing live to delete: idempotent no-op
	}
	seq := s.nextSeq + 1
	if err := s.wal.appendDelete(key, seq); err != nil {
		return err
	}
	s.nextSeq = seq
	next := s.deriveView()
	next[key] = &node{present: false, seq: seq, older: cur[key]}
	s.current.Store(&next)
	return nil
}

// deriveView returns a shallow, mutable copy of the current view. Callers
// must hold s.mu and publish the result through s.current before unlocking.
func (s *Store) deriveView() view {
	cur := *s.current.Load()
	next := make(view, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	return next
}

// Compact reclaims history: values that were overwritten or deleted and are
// not needed by any open snapshot or cursor are dropped from memory and the
// write-ahead log is atomically rewritten, so the directory occupies less
// space than before whenever reclaimable history existed.
//
// Every open snapshot keeps its exact fixed view: its values are untouched
// and reads before and after compaction return byte-identical results. Every
// open cursor likewise keeps paging through its exact fixed sequence, and
// the one version per key its view shows stays in the rewritten log until
// the cursor closes; the compaction after that drops it and shrinks the
// directory further. A process killed at any point during compaction reopens
// in a complete commit state, with every successful write, deletion and the
// cumulative snapshot count intact; no intermediate file requires a second
// compaction to repair.
//
// Compact takes no arguments, may be called at any time and serializes with
// writes, deletes, snapshots, cursor opens, closing and other compactions;
// concurrent or repeated calls queue and all return nil. Compacting a closed
// store returns an error wrapping fs.ErrClosed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	cur := *s.current.Load()

	// Pinned versions, keyed by commit sequence (globally unique). The
	// current head of every live key in the latest view is retained; each
	// open cursor additionally pins the single head version its fixed view
	// shows for every present key in its range. A deleted key's tombstone is
	// retained only when a cursor still reads an older value under it, so
	// replay links that value as the tombstone's predecessor; otherwise the
	// key is dropped from the rewritten log altogether, as before.
	pinned := make(map[uint64]*node)
	keySeqs := make(map[string][]uint64)
	cursorPins := make(map[string]bool)
	pin := func(k string, n *node) {
		if n == nil {
			return
		}
		if _, ok := pinned[n.seq]; !ok {
			pinned[n.seq] = n
			keySeqs[k] = append(keySeqs[k], n.seq)
		}
	}
	for _, c := range s.openCursors() {
		v := *c.v
		for _, k := range c.keys {
			cursorPins[k] = true
			pin(k, v[k]) // exactly the version this cursor pages out
		}
	}
	for k, n := range cur {
		if n.present || cursorPins[k] {
			pin(k, n) // live head, or a tombstone shielding a pinned value
		}
	}

	// Emit one oldest-to-newest history per key so replay links the retained
	// versions into the correct chains. A key deleted with no cursor reading
	// an older value is absent from keySeqs altogether and drops out of both
	// the new view and the rewritten log, exactly as in the baseline.
	keys := make([]string, 0, len(keySeqs))
	for k := range keySeqs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	histories := make([]keyHistory, 0, len(keys))
	fresh := make(view, len(cur))
	for _, k := range keys {
		seqs := keySeqs[k]
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		h := keyHistory{key: k, versions: make([]nodeVer, 0, len(seqs))}
		for _, seq := range seqs {
			n := pinned[seq]
			h.versions = append(h.versions, nodeVer{
				seq: seq, present: n.present, value: n.value,
			})
		}
		histories = append(histories, h)

		// The new current view gets single-node heads: history nobody pins
		// becomes unreachable from memory here, while open snapshots and
		// cursors keep their own views and nodes alive.
		head := pinned[seqs[len(seqs)-1]]
		if head.present {
			fresh[k] = &node{value: head.value, present: true, seq: head.seq}
		}
	}

	// The rewrite is crash-atomic and split into three phases so the live
	// writer never holds the old wal.log open while it is being replaced:
	//
	//  1. build and sync the complete replacement in the temp name (the old
	//     log and writer are untouched until this succeeds);
	//  2. close the live writer so no descriptor occupies wal.log (renaming
	//     over a still-open file is refused by some native filesystems);
	//  3. atomically rename the temp over wal.log, force the directory and
	//     open a writer on the new log.
	//
	// A failure before the rename leaves the old log and the store exactly
	// as they were, so committing can continue and a later Compact retries.
	// A non-nil next writer after install means the rename landed even if
	// the final directory sync reported an error; adopt it (the old file is
	// unlinked), publish the matching view and surface the sync error.
	// A checkpoint describes a specific wal.log: its resume offset is a byte
	// position in that file. The rewrite below replaces wal.log wholesale, so
	// retire the checkpoint (and sweep a temp left by an interrupted freeze)
	// durably before touching the log. From this fsync until a later
	// Checkpoint, the directory carries no checkpoint and reopen simply
	// replays the complete (old or new) log; a crash at any point therefore
	// never pairs a checkpoint with a log it does not describe. Compaction
	// never needs the checkpoint to recover, and the next Checkpoint recreates
	// one against the rewritten log.
	if err := removeStaleCheckpointTemp(s.dir); err != nil {
		return err
	}
	if err := deactivateCheckpoint(s.dir); err != nil {
		return err
	}
	if err := buildCompactWAL(s.dir, histories, s.snapshots.Load()); err != nil {
		return err
	}
	oldWAL := s.wal
	if err := oldWAL.close(); err != nil {
		// The replacement is still staged under the temp name and has not
		// become state of record; remove it so no repair-only intermediate
		// file lingers, then reopen a writer on the untouched old log.
		removeStaleCompact(s.dir)
		reopened, openErr := openWALWriter(s.dir, -1)
		if openErr != nil {
			return err // reopen itself failed; surface the close failure
		}
		s.wal = reopened
		return err
	}
	nextWAL, renamed, installErr := installCompactWAL(s.dir)
	if nextWAL == nil {
		return installErr
	}
	s.wal = nextWAL
	if renamed {
		s.current.Store(&fresh)
	}
	return installErr
}

// Checkpoint freezes the current complete committed state into the
// self-describing checkpoint file so the next reopen loads it and replays only
// the log frames that follow, reaching the exact state a full replay would.
// The file records every live key's terminal value and commit sequence, the
// cumulative snapshot count, the sequence watermark, the resume offset in
// wal.log and a format version, all protected by a checksum.
//
// Freezing and activating are separate, crash-atomic steps: the complete
// checkpoint is written to a temp name and forced to stable storage, only then
// atomically renamed over any previous checkpoint (whose space is reclaimed in
// the same rename), and finally the directory entry is forced. A process
// killed at any point leaves either the previous complete checkpoint or the
// new one alongside an intact wal.log — never half a checkpoint, and never a
// state that needs another freeze or compaction to repair; reopening simply
// takes whichever committed state is on disk.
//
// Checkpoint serializes with writes, batches, snapshots, cursor opens,
// compaction and closing through the same lock, while lock-free readers keep
// hitting memory and are not held up by the checkpoint sync. Repeated calls
// keep only the newest checkpoint. On a closed store it returns an error
// wrapping fs.ErrClosed.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	data, err := s.buildCheckpointData()
	if err != nil {
		return err
	}
	// Phase 1: land the complete new checkpoint durably under the temp name.
	// Nothing live changes until the rename in phase 2.
	if err := writeCheckpointTemp(s.dir, data); err != nil {
		return err
	}
	if s.checkpointHook != nil && s.checkpointHook(checkpointTempSynced) {
		return errCheckpointInterrupted
	}
	// Phase 2: switch (atomically replacing and reclaiming any older
	// checkpoint), then force the directory so the switch survives a crash.
	if err := renameCheckpoint(s.dir); err != nil {
		return err
	}
	if s.checkpointHook != nil && s.checkpointHook(checkpointRenamed) {
		return errCheckpointInterrupted
	}
	return syncDirectory(s.dir)
}

// openCursors snapshots the set of currently open cursors. Cursor views and
// bounds are immutable after opening, so reading their fields here needs no
// per-cursor lock.
func (s *Store) openCursors() []*Cursor {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	cs := make([]*Cursor, 0, len(s.cursors))
	for c := range s.cursors {
		cs = append(cs, c)
	}
	return cs
}

// Snapshot acquires a stable view. It never observes writes committed after
// it returns, and keeps serving that view regardless of later writes,
// deletes or Store.Close, until the Snapshot itself is closed.
func (s *Store) Snapshot() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	count := s.snapshots.Add(1)
	if err := s.wal.appendMeta(count); err != nil {
		s.snapshots.Add(^uint64(0))
		return nil, err
	}
	snap := &Snapshot{store: s}
	snap.v.Store(s.current.Load())
	return snap, nil
}

// Get reads key from the snapshot's fixed view. Old values overwritten or
// deleted afterwards are still readable; keys created afterwards, empty
// keys and keys absent or deleted when the snapshot was taken are misses. A
// read from a closed snapshot is also a plain miss, never an error. The
// returned bytes are owned by the caller.
func (s *Snapshot) Get(key string) ([]byte, bool) {
	if key == "" {
		return nil, false
	}
	v := s.v.Load()
	if v == nil {
		return nil, false // closed snapshot
	}
	n := (*v)[key]
	if n == nil || !n.present {
		return nil, false
	}
	return cloneBytes(n.value), true
}

// Close releases the snapshot's fixed view. Closing more than once is a
// no-op returning nil.
func (s *Snapshot) Close() error {
	s.v.Store(nil)
	return nil
}

// Close flushes and closes the store. Data already committed stays complete
// and readable after reopening the directory. Snapshots still open keep
// serving their fixed views until closed themselves; closing an already
// closed store is a no-op returning nil.
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.mu.Unlock()
		return nil
	}
	wal := s.wal
	s.wal = nil
	s.mu.Unlock()

	return wal.close()
}

func lookup(v view, key string) ([]byte, bool) {
	n := v[key]
	if n == nil || !n.present {
		return nil, false
	}
	return cloneBytes(n.value), true
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
