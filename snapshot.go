// Package snapshot implements a key value store whose readers take
// point-in-time snapshots: a snapshot never observes writes committed
// after it was opened.
package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const walName = "store.wal"

// Store is a durable key value store backed by an append-only log in a
// directory. A Store is safe for concurrent use.
type Store struct {
	mu        sync.Mutex
	dir       string
	f         *os.File
	walSize   int64
	live      map[string][]byte
	snapshots uint64
	closed    bool
}

// Open opens or creates a store rooted at dir. If dir is an existing
// regular file, the returned error matches fs.ErrInvalid.
func Open(dir string) (*Store, error) {
	if fi, err := os.Stat(dir); err == nil {
		if !fi.IsDir() {
			return nil, fmt.Errorf("snapshot: open %s: not a directory: %w", dir, fs.ErrInvalid)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("snapshot: open %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: open %s: %w", dir, err)
	}

	path := filepath.Join(dir, walName)
	created := false
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		created = true
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	if created {
		// Make a freshly created log file durable in the directory.
		if df, err := os.Open(dir); err == nil {
			_ = df.Sync()
			_ = df.Close()
		}
	}

	s := &Store{dir: dir, f: f, live: make(map[string][]byte)}
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Replay stops at the first torn record; truncate the tail so later
	// appends never sit behind a partial record.
	if _, err := f.Seek(s.walSize, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	if err := f.Truncate(s.walSize); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot: open %s: %w", path, err)
	}
	return s, nil
}

// Put stores value under key. The value is copied; the caller keeps
// ownership of the slice it passed. The write is committed to disk
// before Put returns.
func (s *Store) Put(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("snapshot: put: empty key: %w", fs.ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("snapshot: put: store closed: %w", fs.ErrClosed)
	}
	stored := make([]byte, len(value))
	copy(stored, value)
	if err := s.append(recPut, key, stored); err != nil {
		return err
	}
	s.live[key] = stored
	return nil
}

// Get returns the latest committed value for key. The returned slice is
// owned by the caller. The second result reports whether the key is
// currently live.
func (s *Store) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("snapshot: get: store closed: %w", fs.ErrClosed)
	}
	v, ok := s.live[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

// Delete removes key. Deleting a key that does not exist is a no-op and
// returns nil.
func (s *Store) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("snapshot: delete: empty key: %w", fs.ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("snapshot: delete: store closed: %w", fs.ErrClosed)
	}
	if err := s.append(recDelete, key, nil); err != nil {
		return err
	}
	delete(s.live, key)
	return nil
}

// Snapshot opens a stable view of the store. The snapshot never observes
// writes committed after it was opened.
func (s *Store) Snapshot() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("snapshot: snapshot: store closed: %w", fs.ErrClosed)
	}
	view := make(map[string][]byte, len(s.live))
	for k, v := range s.live {
		vc := make([]byte, len(v))
		copy(vc, v)
		view[k] = vc
	}
	if err := s.append(recSnapshot, "", nil); err != nil {
		return nil, err
	}
	s.snapshots++
	return &Snapshot{view: view}, nil
}

// Close closes the store. It is idempotent. Snapshots already taken keep
// serving their fixed view after the store is closed.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.f.Close()
}

// Snapshot is a stable point-in-time view of a store.
type Snapshot struct {
	mu     sync.Mutex
	view   map[string][]byte
	closed bool
}

// Get reads key from the snapshot's fixed view. The returned slice is
// owned by the caller. Reading a closed snapshot reports a miss.
func (sn *Snapshot) Get(key string) ([]byte, bool) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return nil, false
	}
	v, ok := sn.view[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Close releases the snapshot. It is idempotent.
func (sn *Snapshot) Close() error {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.closed = true
	sn.view = nil
	return nil
}

// Stats reads the store state in dir without modifying it and reports
// the number of live keys, the cumulative number of successfully taken
// snapshots, and the total length of all live values in bytes. The
// average value length is totalBytes/keys; it is left to the caller so
// rounding can be done in integer math.
func Stats(dir string) (keys int, snapshots uint64, totalBytes uint64, err error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("snapshot: stats %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return 0, 0, 0, fmt.Errorf("snapshot: stats %s: not a directory: %w", dir, fs.ErrInvalid)
	}
	f, err := os.Open(filepath.Join(dir, walName))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, 0, 0, nil
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("snapshot: stats %s: %w", dir, err)
	}
	defer f.Close()

	live, snaps, _, err := replayFrom(f)
	if err != nil {
		return 0, 0, 0, err
	}
	var total uint64
	for _, v := range live {
		total += uint64(len(v))
	}
	return len(live), snaps, total, nil
}
