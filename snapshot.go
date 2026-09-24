// Package snapshot implements a persistent key-value store whose readers
// operate on immutable point-in-time snapshots. A snapshot never observes a
// write committed after it was acquired, and it keeps serving its fixed view
// even after the store that produced it has been closed.
package snapshot

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
)

// node is one immutable version in a key's history. Versions form a linked
// list from newest to oldest; a snapshot pins the head current when it was
// acquired, so overwritten and deleted values stay readable through every
// view that references them.
type node struct {
	value   []byte
	present bool // false marks a delete; a present nil/empty value is still a hit
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
	viewGen   uint64               // generation of the current view; bumped on each commit
	snapshots atomic.Uint64        // cumulative number of snapshots handed out
	pins      pinRegistry          // versions pinned by open cursors; own lock

	closed atomic.Bool
}

// Snapshot is a stable read-only view of a store captured at one instant. It
// owns no store resources and stays usable after the store has been closed.
type Snapshot struct {
	v atomic.Pointer[view] // nil once the snapshot is closed

	// gen is the view generation this snapshot captured; cursors opened on
	// it inherit it, so compaction orders retained versions by true view age
	// even when an old snapshot's cursor outlives a latest-view cursor.
	gen uint64

	// store back-references the pin registry so a cursor opened on this
	// snapshot can pin its versions through compaction; nil is impossible for
	// snapshots handed out by Store, and harmless after the store is gone.
	store *Store
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
	validSize, snapshotCount, err := replayWAL(dir, func(op byte, key string, value []byte) {
		switch op {
		case opPut:
			latest[key] = &node{value: cloneBytes(value), present: true, older: latest[key]}
		case opDelete:
			latest[key] = &node{present: false, older: latest[key]}
		}
	})
	if err != nil {
		return nil, err
	}

	wal, err := openWALWriter(dir, validSize)
	if err != nil {
		return nil, err
	}

	s := &Store{dir: dir, wal: wal}
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
	if err := s.wal.appendPut(key, value); err != nil {
		return err
	}
	next := s.deriveView()
	next[key] = &node{value: cloneBytes(value), present: true, older: (*s.current.Load())[key]}
	s.current.Store(&next)
	s.viewGen++
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
	if err := s.wal.appendDelete(key); err != nil {
		return err
	}
	next := s.deriveView()
	next[key] = &node{present: false, older: cur[key]}
	s.current.Store(&next)
	s.viewGen++
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
// not needed by any open snapshot are dropped from memory and the write-ahead
// log is atomically rewritten with just the live state, so the directory
// occupies less space than before whenever reclaimable history existed.
//
// Every open snapshot keeps its exact fixed view: its values are untouched
// and reads before and after compaction return byte-identical results. A
// process killed at any point during compaction reopens in a complete commit
// state, with every successful write, deletion and the cumulative snapshot
// count intact; no intermediate file requires a second compaction to repair.
//
// Compact takes no arguments, may be called at any time and serializes with
// writes, deletes, snapshots, closing and other compactions; concurrent or
// repeated calls queue and all return nil. Compacting a closed store returns
// an error wrapping fs.ErrClosed.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	cur := *s.current.Load()
	activePins := s.pins.ordered()

	fresh := make(view, len(cur))
	for k, n := range cur {
		if n.present {
			// A single-node history makes the old chain unreachable from the
			// current view; open snapshots and cursors keep pointing at their
			// own views, so only versions nobody reads become garbage.
			fresh[k] = &node{value: n.value, present: true}
		}
	}

	// Build the replacement log's record sequence. Open cursors pin history:
	// every version their fixed view can read must survive this rewrite, so
	// walk the pins from the oldest cursor to the newest and emit each pinned
	// key's value then. Views only move forward, so for any one key the
	// emitted puts run oldest-to-newest; the current view's end state is
	// appended last (a tombstone when the key ends deleted). Consecutive
	// identical puts collapse, and a key the replay would never see needs no
	// tombstone. After all cursors close, the next compaction writes none of
	// this history and the directory shrinks.
	records := make([]walRecord, 0, len(cur))
	type lastRec struct {
		put bool
		val []byte
	}
	emitted := make(map[string]lastRec, len(cur))
	appendPut := func(k string, v []byte) {
		if r, ok := emitted[k]; ok && r.put && bytes.Equal(r.val, v) {
			return
		}
		records = append(records, walRecord{key: k, value: v})
		emitted[k] = lastRec{put: true, val: v}
	}
	appendDelete := func(k string) {
		r, seen := emitted[k]
		if !seen || !r.put {
			// No earlier put in the replacement log: replay simply ends
			// with the key absent, exactly as a baseline compaction of
			// live state did, so no tombstone frame is needed.
			return
		}
		records = append(records, walRecord{key: k, deleted: true})
		emitted[k] = lastRec{put: false}
	}
	for _, p := range activePins {
		for _, k := range sortedKeysInRange(p.view, p.start, p.end, p.endInclusive) {
			appendPut(k, p.view[k].value)
		}
	}
	for k, n := range cur {
		if n.present {
			appendPut(k, n.value)
		} else {
			appendDelete(k)
		}
	}

	// The rewrite is crash-atomic: on return either the old log or the fully
	// built new log is wal.log. Swapping the writer under s.mu means a
	// concurrent writer cannot straddle the two files. A non-nil nextWAL
	// means the rename landed even if the final directory sync reported an
	// error; adopt it (the old file is unlinked) and publish the matching
	// view, then surface the sync error.
	nextWAL, compactErr := compactWAL(s.dir, records, s.snapshots.Load())
	if nextWAL == nil {
		return compactErr
	}
	oldWAL := s.wal
	s.wal = nextWAL
	s.current.Store(&fresh)
	s.viewGen++
	if err := oldWAL.close(); err != nil && compactErr == nil {
		return err
	}
	return compactErr
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
	snap := &Snapshot{store: s, gen: s.viewGen}
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
