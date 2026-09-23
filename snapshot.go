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

	wal       *walWriter
	current   atomic.Pointer[view] // latest committed view; lock-free for readers
	snapshots atomic.Uint64        // cumulative number of snapshots handed out

	closed atomic.Bool
}

// Snapshot is a stable read-only view of a store captured at one instant. It
// owns no store resources and stays usable after the store has been closed.
type Snapshot struct {
	v atomic.Pointer[view] // nil once the snapshot is closed
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

	s := &Store{wal: wal}
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
	snap := &Snapshot{}
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
