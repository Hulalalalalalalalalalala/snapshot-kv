package snapshot

import (
	"sort"
	"sync"
)

// KVPair is one key/value pair returned by a scan. Value is owned by the
// caller after return, exactly as the bytes returned by Get.
type KVPair struct {
	Key   string
	Value []byte
}

// Range selects the half-open key interval [Start, End), scanned in
// ascending byte order. An empty Start means the smallest key; an empty End
// means the largest key. Empty keys cannot be stored, so an empty bound is
// unambiguously "no bound". An interval whose Start is not strictly before
// End matches nothing.
type Range struct {
	Start string
	End   string
}

// Scan returns up to limit present key/value pairs whose keys lie in the
// half-open interval r, in ascending byte order. A limit <= 0 returns every
// matching pair.
//
// A reversed interval (Start not before End) and a range matching no keys
// both return an empty slice and nil, never an error or partial data. Hit
// semantics match Get: keys written with empty values are included; empty
// keys, deleted keys and keys never written are not.
//
// The read hits the latest committed view directly in memory and is never
// held up by a write synchronizing to disk. Scanning a closed store returns
// an error wrapping fs.ErrClosed.
func (s *Store) Scan(r Range, limit int) ([]KVPair, error) {
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	if !validInterval(r.Start, r.End) {
		return []KVPair{}, nil
	}
	return scanView(*s.current.Load(), r.Start, r.End, limit), nil
}

// ScanPrefix returns up to limit present pairs whose keys begin with prefix,
// in ascending byte order. An empty prefix matches every key. It is
// equivalent to Scan over the key interval the prefix occupies.
func (s *Store) ScanPrefix(prefix string, limit int) ([]KVPair, error) {
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	return scanView(*s.current.Load(), prefix, prefixEnd(prefix), limit), nil
}

// OpenCursor opens a cursor over the half-open interval r of the latest
// committed view. The cursor's view is fixed at open time: later writes,
// deletes, compactions, newly opened snapshots and Store.Close neither change
// nor invalidate what it pages through. The versions it references are kept
// out of compaction reclamation until it closes.
func (s *Store) OpenCursor(r Range) (*Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	if !validInterval(r.Start, r.End) {
		return newEmptyCursor(), nil
	}
	return s.newCursor(s.current.Load(), r.Start, r.End), nil
}

// OpenCursorPrefix opens a cursor over keys beginning with prefix.
func (s *Store) OpenCursorPrefix(prefix string) (*Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	return s.newCursor(s.current.Load(), prefix, prefixEnd(prefix)), nil
}

// Scan is the Snapshot variant: it scans the snapshot's fixed view. A scan
// on a closed snapshot is an ordinary empty result, never an error, just as
// Get on a closed snapshot is a miss.
func (s *Snapshot) Scan(r Range, limit int) ([]KVPair, error) {
	v := s.v.Load()
	if v == nil || !validInterval(r.Start, r.End) {
		return []KVPair{}, nil
	}
	return scanView(*v, r.Start, r.End, limit), nil
}

// ScanPrefix scans the snapshot's fixed view for keys beginning with prefix.
func (s *Snapshot) ScanPrefix(prefix string, limit int) ([]KVPair, error) {
	v := s.v.Load()
	if v == nil {
		return []KVPair{}, nil
	}
	return scanView(*v, prefix, prefixEnd(prefix), limit), nil
}

// OpenCursor opens a cursor over the snapshot's fixed view. Versions the
// cursor pages through stay out of compaction reclamation until it closes.
// Opening a cursor does not count as taking a snapshot. On a closed snapshot
// it yields a cursor over an empty view, mirroring the miss-without-error
// semantics of every other closed-snapshot read.
func (s *Snapshot) OpenCursor(r Range) (*Cursor, error) {
	v := s.v.Load()
	if v == nil {
		return newEmptyCursor(), nil
	}
	if !validInterval(r.Start, r.End) {
		return newEmptyCursor(), nil
	}
	return s.openCursor(v, r.Start, r.End)
}

// OpenCursorPrefix opens a cursor over the snapshot's fixed view restricted
// to keys beginning with prefix.
func (s *Snapshot) OpenCursorPrefix(prefix string) (*Cursor, error) {
	v := s.v.Load()
	if v == nil {
		return newEmptyCursor(), nil
	}
	return s.openCursor(v, prefix, prefixEnd(prefix))
}

func (s *Snapshot) openCursor(v *view, start, end string) (*Cursor, error) {
	st := s.store
	if st == nil || st.closed.Load() {
		// The producing store is gone: no compaction can run anymore, so
		// nothing has to pin. The cursor still serves its fixed in-memory
		// view.
		return newCursorPinned(nil, v, start, end), nil
	}
	// Register under the same lock Compact holds for its whole rewrite, so
	// every compaction while this cursor is alive includes its versions.
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed.Load() {
		return newCursorPinned(nil, v, start, end), nil
	}
	return st.newCursor(v, start, end), nil
}

// Cursor pages in stable byte order over one fixed view. It is safe for
// concurrent use; paging never takes the store's write lock and is not
// blocked by writes syncing to disk.
type Cursor struct {
	store *Store // pin accounting target; nil for an unpinned/empty cursor

	mu     sync.Mutex
	v      *view // fixed at open; nil only on a fully inert handle
	start  string
	end    string
	keys   []string // present keys in [start,end), sorted, fixed at open
	closed bool
}

// Page returns at most limit pairs of the cursor's fixed ordered sequence
// starting at offset off. The first call passes offset 0; each following call
// passes the previous offset plus the number of pairs it got back. Paged that
// way the calls reproduce the cursor's whole sequence byte-identically with
// neither duplicates nor gaps. An offset equal to the sequence length with a
// positive limit returns an empty page and nil, signalling the end.
//
// A non-positive limit, a negative offset, or an offset past the end of the
// sequence is an out-of-range or otherwise illegal continuation argument and
// returns an error wrapping fs.ErrInvalid. Paging a closed cursor is an empty
// result without error.
func (c *Cursor) Page(off, limit int) ([]KVPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return []KVPair{}, nil
	}
	if limit <= 0 || off < 0 || off > len(c.keys) {
		return nil, errCursorArgument
	}
	stop := off + limit
	if stop > len(c.keys) || stop < off {
		stop = len(c.keys) // clamp a large page; guard against int overflow
	}
	v := *c.v
	out := make([]KVPair, 0, stop-off)
	for _, k := range c.keys[off:stop] {
		n := v[k]
		if n == nil || !n.present {
			continue // defensive: keys are fixed present-at-open, so unreachable
		}
		out = append(out, KVPair{Key: k, Value: cloneBytes(n.value)})
	}
	return out, nil
}

// Close releases the cursor. Its versions become reclaimable by the next
// compaction. Close is idempotent, serializes with concurrent pages and
// other closes, and always returns nil; reads after Close are empty results
// without error.
func (c *Cursor) Close() error {
	c.mu.Lock()
	already := c.closed
	c.closed = true
	st := c.store
	c.store = nil
	c.mu.Unlock()

	if already || st == nil {
		return nil
	}
	st.unregisterCursor(c)
	return nil
}

// ---- pin registry ----

func (s *Store) registerCursor(c *Cursor) {
	s.cursorMu.Lock()
	s.cursors[c] = struct{}{}
	s.cursorMu.Unlock()
}

func (s *Store) unregisterCursor(c *Cursor) {
	s.cursorMu.Lock()
	delete(s.cursors, c)
	s.cursorMu.Unlock()
}

// newCursor materializes the cursor's ordered key list once, at open time,
// and registers it so compaction retains its versions.
func (s *Store) newCursor(v *view, start, end string) *Cursor {
	c := newCursorPinned(s, v, start, end)
	s.registerCursor(c)
	return c
}

func newCursorPinned(st *Store, v *view, start, end string) *Cursor {
	return &Cursor{
		store: st,
		v:     v,
		start: start,
		end:   end,
		keys:  presentKeys(*v, start, end),
	}
}

// newEmptyCursor returns a never-erroring cursor over nothing, used when a
// cursor is opened over an empty/closed view. It pins no versions.
func newEmptyCursor() *Cursor {
	empty := make(view)
	return &Cursor{v: &empty, keys: nil}
}

// ---- ordered reads over immutable views ----

func scanView(v view, start, end string, limit int) []KVPair {
	keys := presentKeys(v, start, end)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]KVPair, 0, len(keys))
	for _, k := range keys {
		n := v[k]
		out = append(out, KVPair{Key: k, Value: cloneBytes(n.value)})
	}
	return out
}

// presentKeys returns the present keys in [start,end) sorted by byte order.
// Go's string ordering is unsigned byte ordering on the raw contents.
func presentKeys(v view, start, end string) []string {
	keys := make([]string, 0, len(v))
	for k, n := range v {
		if n == nil || !n.present {
			continue
		}
		if start != "" && k < start {
			continue
		}
		if end != "" && k >= end {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// validInterval reports whether [start,end) can hold keys. An empty bound is
// unbounded; with both given, start must be strictly before end.
func validInterval(start, end string) bool {
	if start == "" || end == "" {
		return true
	}
	return start < end
}

// prefixEnd returns the smallest key strictly greater than every key having
// prefix: drop trailing 0xFF bytes and increment the last remaining byte. A
// prefix of all 0xFF bytes (and the empty prefix) has no upper bound.
func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}
	end := []byte(prefix)
	for len(end) > 0 && end[len(end)-1] == 0xFF {
		end = end[:len(end)-1]
	}
	if len(end) == 0 {
		return ""
	}
	end[len(end)-1]++
	return string(end)
}
