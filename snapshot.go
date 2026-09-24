// Package snapshot implements a persistent key-value store whose readers
// operate on immutable point-in-time snapshots. A snapshot never observes a
// write committed after it was acquired, and it keeps serving its fixed view
// even after the store that produced it has been closed.
package snapshot

import (
	"errors"
	"io/fs"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// node is one immutable version in a key's history. Versions form a linked
// list from newest to oldest; a snapshot pins the head current when it was
// acquired, so overwritten and deleted values stay readable through every
// view that references them. seq is the version's commit number: one number
// per committed batch, shared by every key the batch touched, unique and
// strictly increasing along a key's chain; it orders versions across the
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

// OpKind selects what one batch operation does.
type OpKind int

const (
	// OpPut stores Value under Key.
	OpPut OpKind = iota + 1
	// OpDelete removes Key.
	OpDelete
)

// Op is one operation in a batch handed to Store.Commit.
type Op struct {
	Kind  OpKind
	Key   string
	Value []byte // used only for OpPut
}

// PutOp builds a put operation.
func PutOp(key string, value []byte) Op { return Op{Kind: OpPut, Key: key, Value: value} }

// DeleteOp builds a delete operation.
func DeleteOp(key string) Op { return Op{Kind: OpDelete, Key: key} }

// stagedOp is one effective (post-last-wins) operation of a queued batch.
type stagedOp struct {
	key   string
	put   bool
	value []byte
}

// commitReq is one caller's batch queued for the group-commit leader.
type commitReq struct {
	raw []Op
	seq uint64
	// eff is filled by the leader once the batch's turn in the group is
	// projected; empty means the batch had no durable effect (e.g. deleting
	// a key that was never live).
	eff  []stagedOp
	done chan error
}

// Store is an open key-value store backed by a directory on disk.
type Store struct {
	mu sync.Mutex // serializes state changes and the commit queue

	dir       string
	wal       *walWriter
	walMu     sync.Mutex           // exclusive access to the WAL buffer/file and s.wal
	current   atomic.Pointer[view] // latest committed view; lock-free for readers
	snapshots atomic.Uint64        // cumulative number of snapshots handed out
	nextSeq   uint64               // highest commit sequence handed out; guarded by mu

	// Group commit. Exactly one goroutine is leader while leaderBusy is set:
	// it gathers queued batches, encodes them back to back and performs one
	// flush plus one fsync for the whole group, then applies each batch as
	// its own atomic view. idleCond wakes Compact/Close once the leader has
	// drained the queue and gone idle.
	leaderBusy bool
	queue      []*commitReq
	idleCond   *sync.Cond
	// groupYield, when set, is how long the leader holds the arrival window
	// open before draining; production yields the scheduler, tests replace
	// it with a deterministic rendezvous.
	groupYield func()

	// Open cursors pin their fixed view's versions against compaction. The
	// set is read by Compact (which holds mu) and mutated at cursor
	// open/close, using cursorMu; reads never take either lock.
	cursorMu sync.Mutex
	cursors  map[*Cursor]struct{}

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
	if err := removeStaleCompact(dir); err != nil {
		return nil, err
	}

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
		return nil, err
	}

	wal, err := openWALWriter(dir, validSize)
	if err != nil {
		return nil, err
	}

	s := &Store{dir: dir, wal: wal, cursors: make(map[*Cursor]struct{}), nextSeq: replaySeq}
	s.idleCond = sync.NewCond(&s.mu)
	s.snapshots.Store(snapshotCount)
	s.current.Store(&latest)
	return s, nil
}

// Commit applies one batch atomically. ops is an ordered list of puts and
// deletes; when several operations touch the same key, the last one in slice
// order is the batch's effect on that key.
//
// The batch either becomes visible as a whole or not at all: a crash after
// Commit returns successfully never loses it, and a crash while it is in
// flight never persists a piece of it. Concurrent batches are ordered by
// commit sequence and each batch is one point on that total order; a snapshot
// sees all or none of it. Batches committed concurrently share a single
// flush and fsync of the log (group commit); success means the whole batch is
// already durable, while reads keep hitting the in-memory view without
// waiting for that synchronization.
//
// An empty ops slice is a no-op and returns nil, leaving no trace. Any empty
// key in the batch rejects the whole batch with an error wrapping
// fs.ErrInvalid, before anything is applied or persisted. Committing on a
// closed store returns an error wrapping fs.ErrClosed.
func (s *Store) Commit(ops []Op) error {
	if len(ops) == 0 {
		return nil // unconditional no-op: no state touched, no trace left
	}

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return errStoreClosed
	}
	for _, op := range ops {
		if op.Key == "" {
			s.mu.Unlock()
			return errEmptyKey // whole batch rejected, nothing queued or applied
		}
	}

	req := &commitReq{raw: ops, done: make(chan error, 1)}
	leader := !s.leaderBusy
	if leader {
		s.leaderBusy = true
	}
	s.queue = append(s.queue, req)
	if !leader {
		s.mu.Unlock()
		return <-req.done
	}
	s.runCommitLeader() // returns holding s.mu
	s.mu.Unlock()
	return <-req.done
}

// runCommitLeader is the group-commit loop. It is entered holding s.mu (with
// the caller's own req already queued) and returns holding s.mu once the
// queue has drained completely. Each iteration gathers an arrival window of
// batches and performs at most one flush and one fsync for all of them.
func (s *Store) runCommitLeader() {
	for {
		// Arrival window: concurrent callers queue while the lock is free.
		// Production spins only briefly: a lone batch is serviced at once,
		// while batches that actually arrive concurrently are gathered into
		// the same single flush+fsync. A test hook can replace the window to
		// gather a deterministic set of batches.
		s.mu.Unlock()
		if s.groupYield != nil {
			s.groupYield()
		} else {
			s.gatherArrivals()
		}
		s.mu.Lock()

		group := s.queue
		s.queue = nil

		// Project each batch's last-wins effect against the committed view
		// plus earlier batches in this same group, so a delete of a key
		// created by an earlier queued batch is kept, while no-op deletes
		// persist nothing (matching single Delete semantics). A commit
		// sequence number is consumed only by a batch with a real effect.
		proj := make(map[string]bool)
		for k, n := range *s.current.Load() {
			proj[k] = n.present
		}
		for _, req := range group {
			req.eff, proj = effectiveOps(req.raw, proj)
			if len(req.eff) > 0 {
				s.nextSeq++
				req.seq = s.nextSeq
			}
		}

		// Encode the whole group back to back, flush once and sync once.
		// walMu is held across the fsync so a meta record cannot interleave
		// with the group's bytes; the state lock is free throughout, so
		// reads never block on the synchronization. A group whose batches
		// all had no effect has nothing to persist and skips the fsync.
		anyEffect := false
		for _, req := range group {
			if len(req.eff) > 0 {
				anyEffect = true
				break
			}
		}
		var gErr error
		s.mu.Unlock()
		if anyEffect {
			s.walMu.Lock()
			w := s.wal
			start := w.committed()
			gErr = encodeGroup(w, group)
			if gErr == nil {
				gErr = w.flushSync()
			}
			if gErr != nil {
				// Strip this group's bytes so later commits append at the old
				// durable boundary and no half-encoded trace remains.
				gErr = errors.Join(gErr, w.rollbackTo(start))
			}
			s.walMu.Unlock()
		}
		s.mu.Lock()
		if gErr != nil {
			// Nothing durable was added and no view was touched, so every
			// batch of the group is fully absent. A failed fsync can leave
			// the file's durability state unknown; refuse further commits
			// rather than risk acknowledging data a crash would lose. Also
			// fail batches that queued while this group was syncing. Close
			// the log handle here so a later Close() does not leak it.
			s.walMu.Lock()
			if s.wal != nil {
				_ = s.wal.close()
				s.wal = nil
			}
			s.walMu.Unlock()
			s.closed.Store(true)
			for _, r := range group {
				r.done <- gErr
			}
			for _, r := range s.queue {
				r.done <- gErr
			}
			s.queue = nil
			s.leaderBusy = false
			s.idleCond.Broadcast()
			return
		}

		// Publish one view per batch in commit order, so each batch is a
		// single all-or-nothing point observable through snapshots.
		for _, r := range group {
			if len(r.eff) > 0 {
				next := s.deriveView()
				for _, op := range r.eff {
					if op.put {
						next[op.key] = &node{value: op.value, present: true, seq: r.seq, older: next[op.key]}
					} else {
						next[op.key] = &node{present: false, seq: r.seq, older: next[op.key]}
					}
				}
				s.current.Store(&next)
			}
			r.done <- nil
		}

		if len(s.queue) == 0 {
			s.leaderBusy = false
			s.idleCond.Broadcast()
			return
		}
	}
}

// encodeGroup buffers one begin…end envelope per effective batch.
func encodeGroup(w *walWriter, group []*commitReq) error {
	for _, req := range group {
		if len(req.eff) == 0 {
			continue
		}
		err := w.encodeBatch(req.seq, func() error {
			for _, op := range req.eff {
				if op.put {
					if err := w.encodeFrameSeq(opPutSeq, op.key, req.seq, op.value); err != nil {
						return err
					}
				} else {
					if err := w.encodeFrameSeq(opDeleteSeq, op.key, req.seq, nil); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// gatherArrivals is the production arrival window. It spins briefly to let
// genuinely concurrent batches reach the queue, and stops as soon as the
// queue stops growing so a lone batch is not delayed. It is called with no
// lock held; the queue is mutated under s.mu by arriving committers.
func (s *Store) gatherArrivals() {
	const maxSpins = 64
	prev := -1
	for i := 0; i < maxSpins; i++ {
		s.mu.Lock()
		n := len(s.queue)
		s.mu.Unlock()
		if n == prev {
			return // nobody else arrived: service the batch now
		}
		prev = n
		runtime.Gosched()
	}
}

// effectiveOps folds a raw batch to its last-wins-per-key operations and
// projects their outcome into present, which records whether each key is live
// after the batch. A put always takes effect; a delete takes effect only when
// the key was live going in, and otherwise commits nothing.
func effectiveOps(raw []Op, present map[string]bool) ([]stagedOp, map[string]bool) {
	type final struct {
		put   bool
		value []byte
	}
	last := make(map[string]final)
	order := make([]string, 0, len(raw))
	for _, op := range raw {
		if _, seen := last[op.Key]; !seen {
			order = append(order, op.Key)
		}
		last[op.Key] = final{put: op.Kind == OpPut, value: cloneBytes(op.Value)}
	}

	out := make([]stagedOp, 0, len(order))
	for _, k := range order {
		f := last[k]
		if f.put {
			out = append(out, stagedOp{key: k, put: true, value: f.value})
			present[k] = true
			continue
		}
		if present[k] {
			out = append(out, stagedOp{key: k, put: false})
			present[k] = false
		}
	}
	return out, present
}

// waitCommitIdle blocks until no group-commit leader is active. The caller
// must hold s.mu; idleCond.Wait releases it while a group finishes, so the
// leader can apply and broadcast.
func (s *Store) waitCommitIdle() {
	for s.leaderBusy {
		s.idleCond.Wait()
	}
}

// Put stores value under key. An empty key returns an error wrapping
// fs.ErrInvalid. A nil or empty value is stored as-is: a following Get on the
// key reports a hit. The store does not keep a reference to value. It is
// exactly a one-operation Commit.
func (s *Store) Put(key string, value []byte) error {
	return s.Commit([]Op{{Kind: OpPut, Key: key, Value: value}})
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
// is not an error and commits nothing. It is exactly a one-operation Commit.
func (s *Store) Delete(key string) error {
	return s.Commit([]Op{{Kind: OpDelete, Key: key}})
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
// open cursor likewise keeps paging through its exact fixed sequence, and the
// versions its view shows stay in the rewritten log until the cursor closes;
// the compaction after that drops them and shrinks the directory further. A
// process killed at any point during compaction reopens in a complete commit
// state, with every successful write, deletion and the cumulative snapshot
// count intact; no intermediate file requires a second compaction to repair.
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
	// A group may be syncing without holding mu; let it finish applying so
	// the rewrite covers every committed batch.
	s.waitCommitIdle()

	cur := *s.current.Load()

	// Pinned versions, keyed by node identity. Several keys touched by one
	// batch share a commit sequence, so sequence numbers alone cannot
	// identify a node. The current head of every live key in the latest view
	// is retained; each open cursor additionally pins the head version its
	// fixed view shows for every present key in its range. A deleted key's
	// tombstone is retained only when a cursor still reads an older value
	// under it, so replay links that value as the tombstone's predecessor;
	// otherwise the key is dropped from the rewritten log altogether, as
	// before.
	pinned := make(map[*node]struct{})
	keyNodes := make(map[string][]*node)
	cursorPins := make(map[string]bool)
	pin := func(k string, n *node) {
		if n == nil {
			return
		}
		if _, ok := pinned[n]; !ok {
			pinned[n] = struct{}{}
			keyNodes[k] = append(keyNodes[k], n)
		}
	}
	for _, c := range s.openCursors() {
		v := *c.v
		for _, k := range c.keys {
			cursorPins[k] = true
			pin(k, v[k]) // exactly the versions this cursor pages out
		}
	}
	for k, n := range cur {
		if n.present || cursorPins[k] {
			pin(k, n) // live head, or a tombstone shielding a pinned value
		}
	}

	// Emit one oldest-to-newest history per key so replay links the retained
	// versions into the correct chains. A key retains at most one version
	// per batch (later operations on a key within a batch fold to the last),
	// so ordering retained nodes by sequence is exact. A key deleted with no
	// cursor reading an older value is absent from keyNodes altogether and
	// drops out of both the new view and the rewritten log, as before.
	keys := make([]string, 0, len(keyNodes))
	for k := range keyNodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	histories := make([]keyHistory, 0, len(keys))
	fresh := make(view, len(cur))
	for _, k := range keys {
		nodes := keyNodes[k]
		sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].seq < nodes[j].seq })
		h := keyHistory{key: k, versions: make([]nodeVer, 0, len(nodes))}
		for _, n := range nodes {
			h.versions = append(h.versions, nodeVer{
				seq: n.seq, present: n.present, value: n.value,
			})
		}
		histories = append(histories, h)

		// The new current view gets single-node heads: history nobody pins
		// becomes unreachable from memory here, while open snapshots and
		// cursors keep their own views and nodes alive.
		head := nodes[len(nodes)-1]
		if head.present {
			fresh[k] = &node{value: head.value, present: true, seq: head.seq}
		}
	}

	// Build and fsync the replacement before touching the live log, then
	// close the old writer BEFORE replacing wal.log: on Windows an open
	// handle denies the replacement, which previously failed compaction and
	// left history unreclaimed. The rename itself is atomic, so a crash
	// leaves either the old log or the fully built new log, never a mix.
	if err := buildCompactWAL(s.dir, histories, s.snapshots.Load()); err != nil {
		return err
	}
	s.walMu.Lock()
	oldWAL := s.wal
	closeErr := oldWAL.close()
	s.walMu.Unlock()

	nextWAL, installErr := installCompactWAL(s.dir)
	s.walMu.Lock()
	if nextWAL == nil {
		// The rename never landed and wal.log is still the old file; reopen
		// a writer on it so the store stays usable, and keep the old view.
		reopened, reopenErr := openWALWriter(s.dir, -1)
		if reopenErr == nil {
			s.wal = reopened
		}
		s.walMu.Unlock()
		return errors.Join(closeErr, installErr, reopenErr)
	}
	s.wal = nextWAL
	s.walMu.Unlock()

	s.current.Store(&fresh)
	// The rename landed and the new log is live, so compaction as a whole
	// succeeded: the old handle's close error cannot undo it and is not
	// surfaced. Only a genuine install failure (directory sync) reports.
	return installErr
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
	s.walMu.Lock()
	err := s.wal.appendMeta(count)
	s.walMu.Unlock()
	if err != nil {
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

// Close flushes and closes the store. Batches already reported committed stay
// complete and readable after reopening the directory; Close waits for any
// batch still finishing its commit. Snapshots still open keep serving their
// fixed views until closed themselves; closing an already closed store is a
// no-op returning nil.
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.mu.Unlock()
		return nil
	}
	s.waitCommitIdle()
	s.walMu.Lock()
	wal := s.wal
	s.wal = nil
	s.walMu.Unlock()
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
