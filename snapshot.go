// Package snapshot implements a persistent key-value store whose readers
// operate on immutable point-in-time snapshots. A snapshot never observes a
// write committed after it was acquired, and it keeps serving its fixed view
// even after the store that produced it has been closed.
package snapshot

import (
	"errors"
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
	s.snapshots.Store(snapshotCount)
	s.current.Store(&latest)
	return s, nil
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

	// The rewrite is crash-atomic: on return either the old log or the fully
	// built new log is wal.log. Swapping the writer under s.mu means a
	// concurrent writer cannot straddle the two files. A non-nil nextWAL
	// means the rename landed even if the final directory sync reported an
	// error; adopt it (the old file is unlinked) and publish the matching
	// view, then surface the sync error.
	nextWAL, compactErr := compactWAL(s.dir, histories, s.snapshots.Load())
	if nextWAL == nil {
		return compactErr
	}
	oldWAL := s.wal
	s.wal = nextWAL
	s.current.Store(&fresh)
	if err := oldWAL.close(); err != nil && compactErr == nil {
		return err
	}
	return compactErr
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
