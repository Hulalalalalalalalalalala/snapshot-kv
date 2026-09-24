// Package snapshot implements a persistent key-value store whose readers
// operate on immutable point-in-time snapshots. A snapshot never observes a
// write committed after it was acquired, and it keeps serving its fixed view
// even after the store that produced it has been closed.
package snapshot

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
)

// viewFlattenLimit bounds how deep an overlay chain a published view grows
// before the next commit builds it on a flattened copy. Without a bound the
// chain would gain one map per commit for the life of the store and memory
// would track cumulative commits; flattening every viewFlattenLimit commits
// keeps live depth (and the work a read or checkpoint walk does) independent
// of commit count, while the flattened copies stay immutable for every
// snapshot already holding an older view.
const viewFlattenLimit = 64

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

// view is an immutable map of keys to their newest version. Rather than
// copying the whole map for every commit, a view is a thin overlay: its own
// keys on top of a base view it never modifies. Reads descend the short chain
// of overlays; the current view, snapshots and cursors simply point at one.
//
// A view's keys map may itself hold the whole keyspace: the base layer
// (base == nil) used by recovery, compaction and periodic flattening. Maps
// are never mutated after publication, so reading them needs no lock; only
// the commit path, which has just created a fresh overlay, writes its keys.
type view struct {
	keys  map[string]*node
	base  *view
	depth int // number of overlays from this view to its flat base
}

// newView returns an empty overlay atop base (nil for a flat base view).
func newView(base *view) *view {
	d := 1
	if base != nil {
		d = base.depth + 1
	}
	return &view{keys: make(map[string]*node), base: base, depth: d}
}

// lookup returns the newest version recorded for key anywhere along the
// overlay chain, or nil when the key was never written.
func (v *view) lookup(key string) *node {
	for cur := v; cur != nil; cur = cur.base {
		if n, ok := cur.keys[key]; ok {
			return n
		}
	}
	return nil
}

// rangeEach visits the terminal version of every distinct key, fn once per
// key, walking newest-to-oldest so an overlay entry shadows its base.
func (v *view) rangeEach(fn func(key string, n *node) bool) {
	seen := make(map[string]struct{})
	for cur := v; cur != nil; cur = cur.base {
		for k, n := range cur.keys {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			if !fn(k, n) {
				return
			}
		}
	}
}

// flatten materializes one flat map holding the view's complete terminal
// state. It allocates a fresh map and shares only immutable nodes, so views
// already published stay untouched.
func (v *view) flatten() *view {
	flat := newView(nil)
	v.rangeEach(func(k string, n *node) bool {
		flat.keys[k] = n
		return true
	})
	return flat
}

// Store is an open key-value store backed by a directory on disk.
type Store struct {
	mu sync.Mutex // serializes writers and guards the fields below

	dir       string
	wal       *walWriter
	current   atomic.Pointer[view] // latest committed view; lock-free for readers
	snapshots atomic.Uint64        // cumulative number of snapshots handed out
	nextSeq   uint64               // highest commit sequence handed out; guarded by mu

	// chain is the in-memory record of the usable checkpoint layers paired
	// with the current WAL, oldest first. Empty means no checkpoint is live
	// (the next Checkpoint writes a base). Compaction re-anchors it against
	// the replacement log; Checkpoint appends a delta and prunes the window.
	chain []layerState
	// dirty keys were touched by effective commits strictly after the newest
	// layer in chain; they are exactly the keys the next delta layer covers.
	// nil until the first such commit.
	dirty []string
	// legacyLive reports that a version-1 single-file snapshot.ckpt was the
	// checkpoint paired with the WAL at open and has not been replaced by a
	// layered chain or retired by compaction. Compaction must durably retire
	// it before swapping the WAL, exactly as it retires a layered chain.
	legacyLive bool

	// Open cursors pin their fixed view's versions against compaction. The
	// set is read by Compact (which holds mu) and mutated at cursor
	// open/close, using cursorMu; reads never take either lock.
	cursorMu sync.Mutex
	cursors  map[*Cursor]struct{}

	// Group-commit queue. Every state-changing commit (single Put/Delete just
	// like CommitBatch) that arrives while another commit is forcing the log
	// lines up here; one leader drains the queue, encodes the waiting commits
	// and shares a single flush plus fsync across them.
	// queueMu only ever guards this queue and committer; it is never held
	// across disk I/O, so readers and enqueuers never wait on a sync.
	queueMu   sync.Mutex
	queue     []*pendingBatch
	committer bool

	// Test hook, nil outside tests. leaderHook runs once after a commit has
	// become leader, before its first queue drain, and lets tests hold a
	// leader until a known number of commits have coalesced.
	leaderHook func()

	closed atomic.Bool
}

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
	// Likewise, staging debris from a kill mid-checkpoint (a half-built layer
	// temp or a legacy single-file temp) is swept: only finished layers are
	// ever read.
	if err := removeStaleCompact(dir); err != nil {
		return nil, err
	}
	if err := sweepCheckpointDebris(dir); err != nil {
		return nil, err
	}
	if err := sweepStagedChain(dir); err != nil {
		return nil, err
	}

	latest := newView(nil)
	var replaySeq uint64
	var snapshotCount uint64
	var validSize int64
	var chain []layerState
	var tailTouched []string
	var legacyLive bool

	// apply installs one replayed record into the latest view. Legacy
	// records (written before sequence numbers existed) carry no seq; replay
	// order is commit order, so number them sequentially.
	apply := func(op byte, key string, value []byte, seq uint64) {
		if seq == 0 {
			replaySeq++
			seq = replaySeq
		} else if seq > replaySeq {
			replaySeq = seq
		}
		switch op {
		case opPut, opPutSeq:
			latest.keys[key] = &node{value: cloneBytes(value), present: true, seq: seq, older: latest.keys[key]}
		case opDelete, opDeleteSeq:
			latest.keys[key] = &node{present: false, seq: seq, older: latest.keys[key]}
		}
	}

	// Fast reopen: seed memory from the longest valid checkpoint-chain prefix
	// and replay only the WAL written after its last layer. A layer is
	// accepted only as a whole and only when it links to the layer before; a
	// missing, truncated, checksum-bad, unknown-version or unlinked layer
	// rejects it and every layer after it, and recovery falls back to the
	// previous layer, to the legacy single-file checkpoint, or finally to a
	// from-scratch WAL replay.
	chainInfo, chainErr := readChain(dir)
	if chainErr != nil {
		return nil, chainErr
	}
	// Reject the suffix outright: anything beyond the longest intact, linked
	// prefix is removed as a whole so a broken layer never pairs with the WAL
	// and never occupies space. Half a layer is never installed.
	pruneRejectedChainFiles(dir, len(chainInfo.layers))
	walLen, sizeErr := walFileSize(dir)
	if sizeErr != nil {
		return nil, sizeErr
	}
	// The selected layer's cut must lie inside this exact WAL. Cuts are
	// nondecreasing along the chain, so any suffix whose cut ran past the end
	// of the file is rejected as a whole down to the last pairable layer.
	for len(chainInfo.layers) > 0 && chainInfo.layers[len(chainInfo.layers)-1].offset > walLen {
		removeRejectedChainSuffix(dir, chainInfo.layers[len(chainInfo.layers)-1].index)
		chainInfo.layers = chainInfo.layers[:len(chainInfo.layers)-1]
	}

	switch {
	case len(chainInfo.layers) > 0:
		// Compose the valid prefix: each layer overrides the one it builds on.
		var last layerState
		for _, l := range chainInfo.layers {
			for _, e := range l.entries {
				latest.keys[e.key] = &node{value: cloneBytes(e.value), present: e.present, seq: e.seq}
			}
			last = l
		}
		chain = chainInfo.layers
		replaySeq = last.seq
		// Replay the tail, remembering touched keys so the first delta layer
		// taken in this opening also covers commits already past the cut.
		tailApply := func(op byte, key string, value []byte, seq uint64) {
			if op == opPut || op == opPutSeq || op == opDelete || op == opDeleteSeq {
				tailTouched = append(tailTouched, key)
			}
			apply(op, key, value, seq)
		}
		var rerr error
		validSize, snapshotCount, rerr = replayWALTail(dir, last.offset, last.snaps, tailApply)
		if rerr != nil {
			return nil, rerr
		}
	default:
		// No layered chain: accept a legacy version-1 checkpoint if one is
		// intact and pairs with this WAL, otherwise replay the whole log.
		cp, cpOK, cpErr := loadLegacyCheckpoint(dir)
		if cpErr != nil {
			removeRejectedLegacy(dir)
			cpOK = false
		}
		if cpOK && (cp.offset < 0 || cp.offset > walLen) {
			removeRejectedLegacy(dir)
			cpOK = false
		}
		if cpOK {
			legacyLive = true
			for _, e := range cp.entries {
				latest.keys[e.key] = &node{value: cloneBytes(e.value), present: e.present, seq: e.seq}
			}
			replaySeq = cp.seq
			validSize, snapshotCount, err = replayWALTail(dir, cp.offset, cp.snaps, apply)
			if err != nil {
				return nil, err
			}
		} else {
			validSize, snapshotCount, err = replayWAL(dir, apply)
			if err != nil {
				return nil, err
			}
		}
	}

	wal, err := openWALWriter(dir, validSize)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dir:        dir,
		wal:        wal,
		cursors:    make(map[*Cursor]struct{}),
		nextSeq:    replaySeq,
		chain:      chain,
		dirty:      dedupeKeys(nil, tailTouched),
		legacyLive: legacyLive,
	}
	s.snapshots.Store(snapshotCount)
	s.current.Store(latest)
	return s, nil
}

// dedupeKeys returns app's distinct keys appended to dst, first occurrence
// wins, preserving order.
func dedupeKeys(dst, src []string) []string {
	seen := make(map[string]struct{}, len(src))
	out := dst
	for _, k := range src {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

// Put stores value under key. An empty key returns an error wrapping
// fs.ErrInvalid. A nil or empty value is stored as-is: a following Get on the
// key reports a hit. The store does not keep a reference to value.
//
// Put is exactly a one-op batch: it joins the same group-commit queue as
// CommitBatch, so a single write and concurrent batches can share one disk
// sync while each keeps its own crash atomicity.
func (s *Store) Put(key string, value []byte) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	if key == "" {
		return errEmptyKey
	}
	return s.commit([]BatchOp{{Key: key, Value: value}})
}

// Delete removes key. Deleting an empty key returns an error wrapping
// fs.ErrInvalid; deleting a key that was never written or is already deleted
// is not an error and commits nothing. It joins the same group-commit queue a
// batch uses.
func (s *Store) Delete(key string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	if key == "" {
		return errEmptyKey
	}
	return s.commit([]BatchOp{{Key: key, Delete: true}})
}

type pendingBatch struct {
	ops  []BatchOp
	done chan error // buffered(1); the leader delivers the commit result
}

// commitPlan is one queued batch resolved against the cumulative state built
// up across its whole sync group.
type commitPlan struct {
	view  *view // cumulative view after this batch; nil when it committed nothing
	dirty []string
}

// BatchOp is one change in a batch committed with CommitBatch. Delete false
// stores Value under Key (a nil or empty Value is still a hit); Delete true
// removes Key.
type BatchOp struct {
	Key    string
	Value  []byte
	Delete bool
}

// commit enqueues ops as one atomic framed commit and drives the leader loop.
// Single-key Put/Delete and CommitBatch are all this one path.
func (s *Store) commit(ops []BatchOp) error {
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
	// s.mu while it forces the log, which serializes it with Snapshot,
	// Checkpoint, Compact and Close, so a group never straddles a log swap or
	// a layer capture.
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
		for p, rerr := range s.commitGroup(group) {
			group[p].done <- rerr
		}
	}

	return <-me.done
}

// CommitBatch applies a group of writes and deletes as one atomic commit:
// the whole batch becomes visible together or not at all, and snapshots and
// cursors fixed at any instant see either the state before the batch or the
// state after it, never an in-between mixture. Changes are applied in list
// order; a later change to the same key overrides an earlier one within the
// batch. Concurrent batches take effect in commit (enqueue) order, and a
// later committed batch's same-key change overrides an earlier one's.
//
// Batches committed concurrently (with single Put/Delete calls among them)
// share the disk sync: one leader drains the queue, encodes every waiting
// commit as its own recoverable framed commit and forces them all with one
// flush plus fsync, so N commits arriving together cost one sync while each
// stays whole on recovery; a commit that arrives while a sync is in flight
// rides the next one. Reads take none of these locks and are never blocked by
// the sync.
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
	return s.commit(ops)
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
			return errRecordTooLarge()
		}
	}
	return nil
}

func errRecordTooLarge() error { return errors.New("snapshot: record too large") }

// commitGroup encodes and forces every queued batch with one flush and one
// sync, then publishes one view per effective batch in queue (commit) order.
// It returns one result per batch. An encoding or sync failure fails the
// whole group: nothing it contained is published or sequenced. The single
// committer calls this; s.mu is taken for the entire force, exactly as the
// view transitions and checkpoint watermarks need.
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

	base := s.current.Load()
	seq := s.nextSeq
	plans := make([]commitPlan, 0, len(group))
	encodedAny := false
	var groupDirty []string

	for _, p := range group {
		// Each batch gets its own thin immutable overlay on the previous
		// batch's overlay, so the view it publishes is a valid point-in-time
		// state independent of later batches, and queued batches cost memory
		// proportional to their own ops rather than the whole live keyspace.
		// When the chain has grown deep, flatten first so live depth stays
		// bounded by viewFlattenLimit instead of cumulative commit count.
		if base.depth >= viewFlattenLimit {
			base = base.flatten()
		}
		v := newView(base)
		var changes []batchChange
		for i := range p.ops {
			op := &p.ops[i]
			head := v.lookup(op.Key)
			if op.Delete {
				if head == nil || !head.present {
					continue // idempotent no-op, matching the single Delete
				}
				seq++
				changes = append(changes, batchChange{key: op.Key, seq: seq, delete: true})
				v.keys[op.Key] = &node{present: false, seq: seq, older: head}
			} else {
				seq++
				value := cloneBytes(op.Value)
				changes = append(changes, batchChange{key: op.Key, seq: seq, value: value})
				v.keys[op.Key] = &node{value: value, present: true, seq: seq, older: head}
			}
		}
		if len(changes) > 0 {
			if err := s.wal.encodeBatch(changes); err != nil {
				return rollback(err)
			}
			encodedAny = true
			for i := range changes {
				groupDirty = append(groupDirty, changes[i].key)
			}
		}
		var planView *view
		if len(changes) > 0 {
			planView = v
		}
		plans = append(plans, commitPlan{view: planView})
		base = v
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
			s.current.Store(plan.view)
		}
	}
	s.dirty = dedupeKeys(s.dirty, groupDirty)
	return results
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
	val, ok := lookupView(s.current.Load(), key)
	return val, ok, nil
}

// Compact reclaims history: values that were overwritten or deleted and are
// not needed by any open snapshot or cursor are dropped from memory and the
// write-ahead log is atomically rewritten, so the directory occupies less
// space than before whenever reclaimable history existed.
//
// When a checkpoint chain is live, compaction keeps the generations inside
// the retention window and re-anchors them against the replacement log: that
// log is written as one segment per surviving layer (its prefix reproducing
// the layer's terminal state at a fresh cut), followed by the post-window
// changes and the snapshot-count meta, and the rebuilt layers reference those
// cuts. Generations outside the window are retired together with the old
// chain and stop occupying space immediately.
//
// Every open snapshot keeps its exact fixed view; every open cursor keeps
// paging through its exact fixed sequence, and the one version per key its
// view shows stays in the rewritten log until the cursor closes; the next
// compaction then reclaims it and shrinks the directory further. A process
// killed at any point during compaction reopens in a complete commit state,
// with every successful write, deletion and the cumulative snapshot count
// intact; no intermediate file needs a second compaction or checkpoint to
// repair.
//
// Compact serializes with writes, deletes, snapshots, cursor opens, closing
// and other compactions (the group-commit leader and every other mutator
// take the same lock); concurrent or repeated calls queue and all return nil.
// Compacting a closed store returns an error wrapping fs.ErrClosed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	cur := s.current.Load()

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
		for _, k := range c.keys {
			cursorPins[k] = true
			pin(k, c.v.lookup(k)) // exactly the version this cursor pages out
		}
	}
	for k, n := range keySetOf(cur) {
		if n.present || cursorPins[k] {
			pin(k, n) // live head, or a tombstone shielding a pinned value
		}
	}

	plan := buildCompactionPlan(pinned, keySeqs)
	snapshotCount := s.snapshots.Load()

	// The surviving window, oldest first. Layers before it are retired.
	window := checkpointWindow
	if window > len(s.chain) {
		window = len(s.chain)
	}
	first := len(s.chain) - window
	windowLayers := s.chain[first:]
	hadChain := len(s.chain) > 0
	// paired reports any checkpoint — layered chain or a legacy single file —
	// whose cut belongs to the WAL about to be replaced. It must be withdrawn
	// before the swap so no checkpoint is ever read against a different log.
	paired := hadChain || s.legacyLive

	// Phase 1 (nothing live is touched yet): build and sync the complete
	// replacement log, and when layers survive, the matching staged chain.
	// Any failure here removes the staged files and leaves the store and the
	// old WAL/chain exactly as they were, so committing can continue.
	var seg *segmentedWAL
	if !hadChain {
		if err := buildCompactWAL(s.dir, plan.histories(), snapshotCount); err != nil {
			return err
		}
	} else {
		// Full terminal state at each window layer's watermark, composed from
		// the complete old chain (including the generations about to retire).
		termStates := make([][]checkpointEntry, window)
		for w := 0; w < window; w++ {
			termStates[w] = composeLayers(s.chain[:first+w+1])
		}
		seg = planSegmentedWAL(plan, windowLayers, termStates, cur)
		if err := buildSegmentedCompactArtifacts(s.dir, seg, snapshotCount); err != nil {
			return err
		}
	}

	// Phase 2: the replacement must never meet a paired checkpoint on reopen.
	// Durably withdraw the whole layered chain and/or the legacy file while
	// the old wal.log is still in place; a crash after this but before the
	// rename simply replays the old log in full. Failure precedes any
	// stateful move, so drop the staged files and leave the store untouched.
	if paired {
		if err := retireChain(s.dir); err != nil {
			removeStaleCompact(s.dir)
			removeStagedChain(s.dir)
			return err
		}
		s.legacyLive = false
	}
	oldWAL := s.wal
	if err := oldWAL.close(); err != nil {
		// Staged files have not become state of record; remove them and
		// reopen a writer on the untouched old log, so no repair-only
		// intermediate lingers and the next commit proceeds.
		removeStaleCompact(s.dir)
		removeStagedChain(s.dir)
		reopened, openErr := openWALWriter(s.dir, -1)
		if openErr != nil {
			return err
		}
		s.wal = reopened
		return err
	}
	// Phase 3: atomically rename the temp over wal.log, force the directory
	// and open a writer on the new log. If the rename never landed the old
	// log is intact and committing continues (staged chain is dropped).
	nextWAL, renamed, installErr := installCompactWAL(s.dir)
	if nextWAL == nil {
		// The rename landed but the new log cannot be opened: the directory
		// is not in a state where further commits are safe, so mark the
		// store closed rather than leaving a handle only another compaction
		// could repair.
		s.closed.Store(true)
		s.wal = nil
		return installErr
	}
	s.wal = nextWAL
	if renamed {
		s.current.Store(plan.freshView())
		if hadChain {
			// dropCheckpoint clears any in-memory and on-disk chain so the
			// next Checkpoint starts a fresh base with no O_EXCL collision.
			dropCheckpoint := func() {
				s.chain = nil
				s.dirty = nil
				os.RemoveAll(checkpointDirPath(s.dir))
			}
			if window == 0 {
				dropCheckpoint()
				removeStagedChain(s.dir)
			} else if err := activateStagedChain(s.dir); err != nil {
				// The new WAL is already live; rather than leave a half
				// installed chain, run with no live checkpoint. The next
				// Checkpoint establishes a fresh base.
				dropCheckpoint()
			} else {
				info, ierr := readChain(s.dir)
				if ierr != nil || len(info.layers) != window {
					dropCheckpoint()
				} else {
					s.chain = info.layers
					s.dirty = seg.tailKeys
				}
			}
		}
	} else if hadChain {
		// Rename failed; the old WAL stayed. retireChain already removed the
		// old chain, so drop the staged replacement as well: no checkpoint
		// pairs with this WAL. Committing continues; the next Checkpoint heals.
		removeStagedChain(s.dir)
		s.chain = nil
		s.dirty = nil
	}
	return installErr
}

// keySetOf returns the distinct keys present anywhere in the view.
func keySetOf(v *view) map[string]*node {
	out := make(map[string]*node)
	v.rangeEach(func(k string, n *node) bool {
		out[k] = n
		return true
	})
	return out
}

// Checkpoint appends one self-describing, checksummed layer to the checkpoint
// chain so the next Open seeds memory from the longest intact layer prefix
// and replays only the write-ahead log written after it, instead of replaying
// the whole log. The first layer (the base) holds complete committed state;
// every later layer covers only commits after the layer it builds on.
// Checkpoint changes nothing visible: every committed write and deletion, the
// cumulative snapshot count and every open snapshot or cursor are unaffected,
// and reads keep hitting memory without touching disk.
//
// Producing a layer and making it live are two distinct steps: the layer is
// captured under the write lock, built fully under a temporary name and
// forced to stable storage, then atomically renamed into place and linked to
// the previous layer by a content hash. A kill at any instant leaves either
// the old complete chain or the chain extended by one complete layer; reopen
// observes one complete committed state, never a half layer (staging debris
// is swept on the next open).
//
// Only checkpointWindow newest generations are kept: appending past the
// window rebuilds the chain with the oldest surviving layer as a fresh full
// base against the unchanged WAL and deletes the retired layers immediately.
//
// Checkpoint serializes with writes, batch commits, snapshots, cursor opens,
// compaction and closing (it takes the same write lock), but never with
// lock-free reads: Get, scans, snapshot reads and cursor paging proceed while
// it runs. A batch stays wholly on one side of the captured cut, and history
// pinned by open snapshots or cursors is left untouched. On a closed store
// Checkpoint returns an error wrapping fs.ErrClosed.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	offset, err := s.wal.position()
	if err != nil {
		return err
	}
	cur := s.current.Load()
	snaps := s.snapshots.Load()

	if len(s.chain) == 0 {
		// First layer: full base state. This also upgrades a legacy
		// single-file directory: the old file is removed once the base is
		// staged, and the WAL it pairs with has not changed.
		layer := capturedLayer{
			index:   0,
			entries: captureView(cur),
			offset:  uint64(offset),
			seq:     s.nextSeq,
			snaps:   snaps,
		}
		if err := installLayer(s.dir, layer); err != nil {
			return err
		}
		if err := removeLegacyCheckpoint(s.dir); err != nil {
			return err
		}
		s.legacyLive = false
	} else {
		last := s.chain[len(s.chain)-1]
		layer := capturedLayer{
			index:   last.index + 1,
			prevCRC: last.crc,
			entries: captureDelta(cur, s.dirty),
			offset:  uint64(offset),
			seq:     s.nextSeq,
			snaps:   snaps,
		}
		if err := installLayer(s.dir, layer); err != nil {
			return err
		}
	}

	info, err := readChain(s.dir)
	if err != nil {
		return err
	}
	s.chain = info.layers
	s.dirty = nil

	// Enforce the retention window: keep only the newest checkpointWindow
	// layers, rebuilt as a standalone chain against the same unchanged WAL.
	if len(s.chain) > checkpointWindow {
		if err := s.rebaseChainToWindow(); err != nil {
			return err
		}
	}
	return nil
}

// rebaseChainToWindow rebuilds the live chain so only the newest
// checkpointWindow layers remain: the oldest kept layer becomes a fresh full
// base (materialized from the complete in-memory chain) at its existing WAL
// cut, and the layers after it keep their delta content at their existing
// cuts. The WAL is not touched.
func (s *Store) rebaseChainToWindow() error {
	keep := checkpointWindow
	if keep > len(s.chain) {
		keep = len(s.chain)
	}
	first := len(s.chain) - keep
	// Materialize full terminal state at the oldest kept layer.
	full := composeLayers(s.chain[:first+1])
	newLayers := make([]capturedLayer, 0, keep)
	newLayers = append(newLayers, capturedLayer{
		index: 0, entries: full,
		offset: uint64(s.chain[first].offset),
		seq:    s.chain[first].seq,
		snaps:  s.chain[first].snaps,
	})
	for j := first + 1; j < len(s.chain); j++ {
		newLayers = append(newLayers, capturedLayer{
			index:   uint32(j - first),
			entries: s.chain[j].entries,
			offset:  uint64(s.chain[j].offset),
			seq:     s.chain[j].seq,
			snaps:   s.chain[j].snaps,
		})
	}
	if err := replaceCheckpointChain(s.dir, newLayers); err != nil {
		return err
	}
	info, err := readChain(s.dir)
	if err != nil {
		return err
	}
	s.chain = info.layers
	return nil
}

// composeLayers returns the full terminal state after applying layers in
// order (each overriding the one before), sorted by key for a canonical base.
func composeLayers(layers []layerState) []checkpointEntry {
	term := make(map[string]checkpointEntry)
	for _, l := range layers {
		for _, e := range l.entries {
			term[e.key] = e // later layer wins; tombstones survive as tombstones
		}
	}
	out := make([]checkpointEntry, 0, len(term))
	for _, e := range term {
		out = append(out, e)
	}
	sortEntries(out)
	return out
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
	n := v.lookup(key)
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

func lookupView(v *view, key string) ([]byte, bool) {
	n := v.lookup(key)
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
