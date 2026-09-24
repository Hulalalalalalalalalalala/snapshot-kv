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

	// Installed checkpoint-chain state, guarded by mu. A checkpoint is a chain
	// of self-describing layers: a generation-0 base of complete state plus a
	// bounded run of delta layers covering later commits. cpHasChain reports
	// whether a live chain exists; cpGen is its highest layer generation and
	// cpTipWm that layer's commit watermark. Compaction retires the whole
	// chain (its cuts pair with the pre-rewrite log only) and clears the flag.
	cpHasChain bool
	cpGen      uint64
	cpTipWm    uint64

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
	// Reconciliation likewise settles an interrupted checkpoint-chain swap
	// and sweeps half-built layer and single-file temps, so the directory is
	// immediately usable with no repair step.
	if err := removeStaleCompact(dir); err != nil {
		return nil, err
	}
	if err := reconcileLayerDirs(dir); err != nil {
		return nil, err
	}

	latest := make(view)
	var replaySeq uint64
	var snapshotCount uint64
	var validSize int64

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
			latest[key] = &node{value: cloneBytes(value), present: true, seq: seq, older: latest[key]}
		case opDelete, opDeleteSeq:
			latest[key] = &node{present: false, seq: seq, older: latest[key]}
		}
	}

	// Fast reopen: seed memory from the checkpoint chain and replay only the
	// WAL written after its tip. Layers are accepted only as whole files and
	// only in an intact generation-0-based chain; a bad layer and everything
	// above it is withdrawn by the loader and recovery falls back to the
	// highest intact prefix, or to a from-scratch WAL replay when the base
	// itself is unusable. A version-1 single-file checkpoint from an older
	// release is honored only when no layered chain exists.
	walLen, err := walFileSize(dir)
	if err != nil {
		return nil, err
	}

	var chainTip *v2Header // non-nil when the layered reopen path was taken
	chain, chainErr := loadLayerChain(dir, walLen)
	if chainErr != nil {
		return nil, chainErr
	}
	if len(chain) > 0 {
		base := chain[0]
		for _, e := range base.full {
			latest[e.key] = &node{value: cloneBytes(e.value), present: e.present, seq: e.seq}
		}
		// Delta layers cover only commits after the preceding layer; applying
		// them in generation order over the base reproduces terminal state.
		for li := 1; li < len(chain); li++ {
			for _, e := range chain[li].delta {
				latest[e.key] = &node{value: cloneBytes(e.value), present: e.present, seq: e.seq}
			}
		}
		tip := chain[len(chain)-1].hdr
		replaySeq = tip.wm
		chainTip = &tip
		validSize, snapshotCount, err = replayWALTail(dir, tip.offset, tip.snaps, apply)
		if err != nil {
			return nil, err
		}
	} else {
		cp, cpOK, cpErr := loadCheckpointV1(dir)
		if cpErr != nil {
			// Discard the rejected artifact so it neither lingers nor pairs
			// with a future log; the WAL alone is a complete source of truth.
			removeRejectedV1Checkpoint(dir)
			cpOK = false
		}
		if cpOK && (cp.offset < 0 || cp.offset > walLen) {
			// The cut must lie inside this exact WAL; refuse a wrong pairing
			// rather than punch a hole in the log or skip records.
			removeRejectedV1Checkpoint(dir)
			cpOK = false
		}
		if cpOK {
			for _, e := range cp.entries {
				latest[e.key] = &node{value: cloneBytes(e.value), present: e.present, seq: e.seq}
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
		dir: dir, wal: wal, cursors: make(map[*Cursor]struct{}), nextSeq: replaySeq,
	}
	if chainTip != nil {
		s.cpHasChain = true
		s.cpGen = chainTip.gen
		s.cpTipWm = chainTip.wm
	}
	s.snapshots.Store(snapshotCount)
	s.current.Store(&latest)
	return s, nil
}

// Put stores value under key. An empty key returns an error wrapping
// fs.ErrInvalid. A nil or empty value is stored as-is: a following Get on the
// key reports a hit. The store does not keep a reference to value.
//
// Put is exactly a one-op batch: it joins the same group-commit queue as
// CommitBatch, so a single write landing while a batch is syncing shares that
// one disk force instead of serializing a separate fsync behind it. Its
// atomicity and durability are unchanged.
func (s *Store) Put(key string, value []byte) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	op := BatchOp{Key: key, Value: value}
	if err := validateBatchOps([]BatchOp{op}); err != nil {
		return err
	}
	return s.commitQueued([]BatchOp{op})
}

// Delete removes key. Empty keys return an error wrapping fs.ErrInvalid;
// deleting a key never written or already deleted is not an error and commits
// nothing. Like Put it rides the shared group-commit queue as a one-op batch.
func (s *Store) Delete(key string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	op := BatchOp{Key: key, Delete: true}
	if err := validateBatchOps([]BatchOp{op}); err != nil {
		return err
	}
	return s.commitQueued([]BatchOp{op})
}

type pendingBatch struct {
	ops  []BatchOp
	done chan error // buffered(1); the leader delivers the commit result
}

// keyedNode is one key's final node produced by a batch, applied to publish
// that batch's immutable view after the group has been forced.
type keyedNode struct {
	key string
	n   *node
}

// batchPlan is one queued batch's contribution to publication: the terminal
// node of each distinct key it touched, or nil when it committed nothing.
type batchPlan struct {
	nodes []keyedNode
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
	return s.commitQueued(ops)
}

// commitQueued enqueues one already-validated batch and drives the
// group-commit loop when it wins leadership. Put and Delete are one-op batches
// and take this same path, so they share a single in-flight disk force with
// concurrent CommitBatch calls.
func (s *Store) commitQueued(ops []BatchOp) error {
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
	// Compact and Close, so a group never straddles a log swap.
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
// sync, then publishes the resulting state. It returns one result per batch.
// An encoding or sync failure fails the whole group: the log is rewound to
// the group start and nothing it contained is published or sequenced. The
// single committer calls this; s.mu is taken for the entire force.
//
// Temporary memory while the group is staged and syncing is proportional to
// the keys the group actually touches, never to the number of live keys:
// heads resolve against the live view plus a scratch map holding only this
// group's nodes, and each batch keeps only its distinct-key changes. The one
// live-sized map is built only after the sync succeeds, to publish.
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

	live := *s.current.Load()
	seq := s.nextSeq
	// groupEdits holds the newest node produced within this group per key, so
	// later batches and later same-key ops link onto in-group versions
	// without copying the whole live map. It is the only resolution scratch.
	groupEdits := make(map[string]*node)
	plans := make([]batchPlan, 0, len(group))
	encodedAny := false

	headOf := func(key string) *node {
		if n, ok := groupEdits[key]; ok {
			return n
		}
		return live[key]
	}

	for _, p := range group {
		plan := batchPlan{}
		var changes []batchChange
		for i := range p.ops {
			op := &p.ops[i]
			head := headOf(op.Key)
			if op.Delete {
				if head == nil || !head.present {
					continue // idempotent no-op, matching the single Delete
				}
				seq++
				n := &node{present: false, seq: seq, older: head}
				changes = append(changes, batchChange{key: op.Key, seq: seq, delete: true})
				groupEdits[op.Key] = n
				plan.nodes = append(plan.nodes, keyedNode{key: op.Key, n: n})
			} else {
				seq++
				value := cloneBytes(op.Value)
				n := &node{value: value, present: true, seq: seq, older: head}
				changes = append(changes, batchChange{key: op.Key, seq: seq, value: value})
				groupEdits[op.Key] = n
				plan.nodes = append(plan.nodes, keyedNode{key: op.Key, n: n})
			}
		}
		if len(changes) > 0 {
			// Every batch keeps its own begin/end framing on disk, so each is
			// independently whole on recovery even though one sync covers them.
			if err := s.wal.encodeBatch(changes); err != nil {
				return rollback(err)
			}
			encodedAny = true
		}
		plans = append(plans, plan)
	}

	if encodedAny {
		// One flush, one fsync for the whole group. Return means every
		// encoded batch in it is durable as a self-contained framed commit.
		if err := s.wal.flushSync(); err != nil {
			return rollback(err)
		}
	}

	s.nextSeq = seq
	if encodedAny {
		// Publish once: the group was forced as a unit and no snapshot or
		// cursor can be acquired while s.mu is held, so exposing the
		// cumulative post-group state atomically gives each batch its
		// all-or-nothing visibility, in commit order. Nodes keep their full
		// older chains, so versions pinned just before the group stay
		// reachable for compaction.
		next := make(view, len(live)+len(groupEdits))
		for k, v := range live {
			next[k] = v
		}
		for _, plan := range plans {
			for _, kn := range plan.nodes {
				next[kn.key] = kn.n
			}
		}
		s.current.Store(&next)
	}
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
	val, ok := lookup(*s.current.Load(), key)
	return val, ok, nil
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
	if err := buildCompactWAL(s.dir, histories, s.snapshots.Load()); err != nil {
		return err
	}
	// The replacement must not meet the old checkpoint chain on reopen: its
	// frames and offsets are a different log from the one the layers cut.
	// Retire the whole chain (base and every delta, plus staging debris)
	// durably while the old wal.log is still in place; a crash after this but
	// before the rename simply replays the old log in full. Failure here
	// happens before anything stateful moved, so drop the staged temp and
	// leave the store untouched.
	if err := retireCheckpointChain(s.dir); err != nil {
		removeStaleCompact(s.dir)
		return err
	}
	s.cpHasChain = false
	s.cpGen = 0
	s.cpTipWm = 0
	oldWAL := s.wal
	if err := oldWAL.close(); err != nil {
		// The replacement is still staged under the temp name and has not
		// become state of record; remove it so no repair-only intermediate
		// file lingers, then reopen a writer on the untouched old log.
		removeStaleCompact(s.dir)
		reopened, openErr := openWALWriter(s.dir, -1)
		if openErr != nil {
			// The log handle is unusable: rather than leave a store that only
			// "another Compact" could repair, declare it failed. Every later
			// operation returns fs.ErrClosed; committed data is intact and a
			// fresh Open recovers it from the untouched log.
			s.closed.Store(true)
			s.wal = nil
			return errors.Join(err, openErr)
		}
		s.wal = reopened
		return err
	}
	nextWAL, renamed, installErr := installCompactWAL(s.dir)
	if nextWAL == nil {
		// No writer on either side of the swap. Marking closed is the only
		// honest state: the directory itself is a complete committed state and
		// a fresh Open reopens it, but this handle must not claim commits can
		// continue when they cannot.
		s.closed.Store(true)
		s.wal = nil
		return installErr
	}
	s.wal = nextWAL
	if renamed {
		s.current.Store(&fresh)
	} else if installErr != nil {
		// The rename never landed: the old log is live again and its chain
		// retirement already happened before this point, so there is no
		// checkpoint to pair with it. Committing continues; only the fast
		// reopen shortcut is gone until the next Checkpoint.
		s.cpHasChain = false
	}
	return installErr
}

// Checkpoint freezes the store's complete committed state into a chain of
// self-describing, checksummed layers so the next Open seeds memory from them
// and replays only the write-ahead log written after the newest layer,
// instead of replaying the whole log. It changes nothing visible: every
// committed write and deletion, the cumulative snapshot count and every open
// snapshot or cursor are unaffected, and reads keep hitting memory without
// touching disk.
//
// The first checkpoint (and the first one after a compaction retired the
// chain, or after reopening a legacy single-file directory) writes a base
// layer holding complete state. Each later checkpoint appends one delta
// layer covering only commits after the preceding layer, which is cheap no
// matter how large the base is. A bounded retention window keeps the chain
// short: once the live layers reach checkpointWindow generations the next
// checkpoint consolidates by atomically swapping in a fresh base and
// deleting the old chain, and compaction retires the whole chain when it
// rewrites the log. Directory space therefore tracks live data and the
// window, never cumulative commits.
//
// Producing a layer and making it live are two distinct steps: the complete
// file is built under a temporary name and forced to stable storage, then
// atomically renamed (a base swaps whole layer directories) and the
// directory is forced. A kill at any instant leaves only a complete earlier
// generation or a complete new one; a half-built temp is swept on the next
// open, and no directory ever needs a second checkpoint or any other repair
// before it opens normally. Repeated checkpoints never leave superseded
// layers occupying space beyond the window.
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
	cur := *s.current.Load()
	wm := s.nextSeq
	snaps := s.snapshots.Load()

	// Consolidate when there is no live chain (first checkpoint, post-compact
	// reopen, or a legacy v1 directory's first layered checkpoint) or when the
	// retention window is full; otherwise append one delta layer.
	consolidate := !s.cpHasChain || s.cpGen+1 >= checkpointWindow
	if consolidate {
		hdr, full := captureBase(cur, wm, snaps, offset)
		if err := installBaseLayer(s.dir, hdr, full); err != nil {
			return err
		}
		s.cpHasChain = true
		s.cpGen = 0
		s.cpTipWm = wm
		return nil
	}

	gen := s.cpGen + 1
	hdr, delta := captureDelta(cur, s.cpTipWm, wm, snaps, offset, gen)
	if err := installDeltaLayer(s.dir, hdr, delta); err != nil {
		return err
	}
	s.cpGen = gen
	s.cpTipWm = wm
	return nil
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
