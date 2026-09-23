package snapshot

import (
	"os"
	"sync"
)

// viewRegistry tracks the views pinned by currently open snapshots, so
// compaction can tell which history is still observable. It is shared with
// the snapshots themselves, so unregistration keeps working after the store
// that handed them out has been closed.
type viewRegistry struct {
	mu   sync.Mutex
	refs map[*view]int
}

func newViewRegistry() *viewRegistry {
	return &viewRegistry{refs: make(map[*view]int)}
}

func (r *viewRegistry) add(v *view) {
	r.mu.Lock()
	r.refs[v]++
	r.mu.Unlock()
}

func (r *viewRegistry) remove(v *view) {
	r.mu.Lock()
	if n := r.refs[v]; n <= 1 {
		delete(r.refs, v)
	} else {
		r.refs[v] = n - 1
	}
	r.mu.Unlock()
}

func (r *viewRegistry) pinned() []*view {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*view, 0, len(r.refs))
	for v := range r.refs {
		out = append(out, v)
	}
	return out
}

// Compact reclaims history that no open snapshot can still observe: versions
// overwritten or deleted since every open snapshot was taken are dropped from
// memory, and the log on disk is rewritten to just the live state, shrinking
// the directory. Open snapshots keep serving their fixed views unchanged,
// during and after compaction, and the latest committed view is untouched.
//
// Compact takes no lock on readers. Concurrent or repeated calls queue up
// and each completes in turn, all returning nil. Calling Compact on a closed
// store returns an error wrapping fs.ErrClosed.
//
// The log rewrite is crash-safe: the replacement log is written to a scratch
// file, synced and atomically renamed over the live one, so a process
// killed at any point leaves the directory at a complete committed state
// that the next Open uses directly.
func (s *Store) Compact() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}

	s.pruneLocked()
	return s.rewriteWALLocked()
}

// pruneLocked drops every version no open snapshot can still reach. Callers
// must hold s.mu.
func (s *Store) pruneLocked() {
	cur := s.current.Load()

	// Nodes referenced as the head of some pinned view, per key.
	refs := make(map[string]map[*node]struct{})
	mark := func(v *view) {
		for k, n := range *v {
			set := refs[k]
			if set == nil {
				set = make(map[*node]struct{})
				refs[k] = set
			}
			set[n] = struct{}{}
		}
	}
	mark(cur)
	for _, pv := range s.views.pinned() {
		mark(pv)
	}

	next := make(view, len(*cur))
	for k, head := range *cur {
		// The oldest node any pinned view still references: everything
		// below it is unreachable garbage. Readers only ever inspect the
		// head node of a chain, never the older links, so truncating here
		// cannot disturb an in-flight read.
		var floor *node
		for n := head; n != nil; n = n.older {
			if _, ok := refs[k][n]; ok {
				floor = n
			}
		}
		if !head.present && floor == head {
			continue // lone tombstone no view relies on: drop the whole chain
		}
		floor.older = nil
		next[k] = head
	}
	s.current.Store(&next)
}

// rewriteWALLocked replaces the log with one containing exactly the live
// state and the cumulative snapshot count. Callers must hold s.mu.
func (s *Store) rewriteWALLocked() error {
	w, tmpPath, err := writeCompactedWAL(s.dir, *s.current.Load(), s.snapshots.Load())
	if err != nil {
		return err
	}
	walPath := s.dir + string(os.PathSeparator) + walName
	if err := os.Rename(tmpPath, walPath); err != nil {
		w.close()
		os.Remove(tmpPath)
		return err
	}
	// The rename moved the scratch file (and w's open descriptor) over the
	// live log; swap writers only now that the durable state is in place.
	old := s.wal
	s.wal = w
	if old != nil {
		old.close()
	}
	// Make the rename itself durable. A failure here leaves a complete log
	// either way, so report it without breaking the store.
	return syncDir(s.dir)
}
