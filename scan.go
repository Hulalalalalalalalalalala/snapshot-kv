package snapshot

import (
	"errors"
	"io/fs"
	"sort"
	"sync"
)

// KVPair is one key/value pair returned by a scan or a cursor page. The
// bytes are owned by the caller.
type KVPair struct {
	Key   string
	Value []byte
}

// ScanRange returns every live key k with start <= k <= end in byte order,
// together with its value from the latest committed view. An empty start or
// end leaves that side unbounded. A reversed range (start > end) or a range
// that matches nothing yields an empty slice and no error. Hit semantics
// match Get: present keys include those carrying an empty value; deleted and
// never-written keys never appear. Scanning a closed store returns an error
// wrapping fs.ErrClosed. The returned bytes are owned by the caller and reads
// are served straight from the in-memory view, along the same path as
// snapshot reads.
func (s *Store) ScanRange(start, end string) ([]KVPair, error) {
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	return collectView(*s.current.Load(), start, end, true), nil
}

// ScanPrefix returns every live key beginning with prefix, in byte order. An
// empty prefix scans the whole view; a prefix that matches nothing yields an
// empty slice and no error. Scanning a closed store returns an error
// wrapping fs.ErrClosed.
func (s *Store) ScanPrefix(prefix string) ([]KVPair, error) {
	if s.closed.Load() {
		return nil, errStoreClosed
	}
	start, end := prefixRange(prefix)
	return collectView(*s.current.Load(), start, end, false), nil
}

// ScanRange returns the pairs of the snapshot's fixed view with start <= k <=
// end, in byte order. A reversed range or one that matches nothing is empty.
// A closed snapshot scans as an empty result, never an error.
func (s *Snapshot) ScanRange(start, end string) []KVPair {
	v := s.v.Load()
	if v == nil {
		return []KVPair{}
	}
	return collectView(*v, start, end, true)
}

// ScanPrefix returns every key of the snapshot's fixed view beginning with
// prefix, in byte order. A closed snapshot scans as an empty result.
func (s *Snapshot) ScanPrefix(prefix string) []KVPair {
	v := s.v.Load()
	if v == nil {
		return []KVPair{}
	}
	start, end := prefixRange(prefix)
	return collectView(*v, start, end, false)
}

// Cursor is a paged reader over a fixed view. Opening a cursor pins the view
// at that instant: later writes, deletes, compactions, newly opened snapshots
// and even closing the store never change what its pages return. The versions
// a cursor references are kept out of compaction reclamation until it is
// closed; the compaction after close reclaims them.
//
// A Cursor is safe for concurrent use: Next and Close serialize, with closing
// queued behind any page already in flight.
type Cursor struct {
	store *Store // store whose registry owns pin; nil only with no live store
	pin   *pin   // nil after Close, or when no store is available to pin in

	mu     sync.Mutex
	view   view     // fixed view; nil after Close
	keys   []string // keys of the fixed view within the scan bounds, in order
	offset int      // index of the next key to return
}

// Cursor opens a paged scan over the latest committed view, fixed to the view
// current the instant the call begins and bounded to the closed interval
// [start, end]. An empty start or end leaves that side unbounded. Opening a
// cursor on a closed store returns an error wrapping fs.ErrClosed.
func (s *Store) Cursor(start, end string) (*Cursor, error) {
	return s.openCursor(start, end, true)
}

// CursorPrefix opens a paged prefix scan over the latest committed view.
func (s *Store) CursorPrefix(prefix string) (*Cursor, error) {
	start, end := prefixRange(prefix)
	return s.openCursor(start, end, false)
}

func (s *Store) openCursor(start, end string, endInclusive bool) (*Cursor, error) {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return nil, errStoreClosed
	}
	v := *s.current.Load()
	// Pin registration inside the write lock is O(1): it only records the
	// immutable view, its generation and the bounds. The O(n) key filtering
	// and sort happen after unlocking, so opening a cursor is not held
	// hostage to a write's fsync, and paging never takes the store lock.
	p := s.pins.add(s.viewGen, v, start, end, endInclusive)
	s.mu.Unlock()

	return &Cursor{store: s, pin: p, view: v, keys: sortedKeysInRange(v, start, end, endInclusive)}, nil
}

// Cursor opens a paged scan over the snapshot's fixed view, bounded to the
// closed interval [start, end]. The cursor keeps serving that view even
// after the snapshot itself has been closed. A cursor opened from a closed
// snapshot is immediately exhausted: its first Next returns an empty page
// and ok=false.
func (s *Snapshot) Cursor(start, end string) *Cursor {
	return s.openCursor(start, end, true)
}

// CursorPrefix opens a paged prefix scan over the snapshot's fixed view.
func (s *Snapshot) CursorPrefix(prefix string) *Cursor {
	start, end := prefixRange(prefix)
	return s.openCursor(start, end, false)
}

func (s *Snapshot) openCursor(start, end string, endInclusive bool) *Cursor {
	vp := s.v.Load()
	c := &Cursor{}
	if vp == nil {
		return c // cursor on an already closed snapshot: immediately exhausted
	}
	// Serialize pin registration against an in-flight compaction, so a cursor
	// is either fully before one (its versions are retained in the rewrite) or
	// fully after it (its view is one the rewrite already ends in). The store
	// lock stays usable after Store.Close; then no compaction can race. The
	// O(n) key filtering and sort wait until the lock is released.
	v := *vp
	var p *pin
	if s.store != nil {
		s.store.mu.Lock()
		p = s.store.pins.add(s.gen, v, start, end, endInclusive)
		s.store.mu.Unlock()
	}
	c.store = s.store
	c.pin = p
	c.view = v
	c.keys = sortedKeysInRange(v, start, end, endInclusive)
	return c
}

// Next returns up to limit consecutive pairs in byte order and advances the
// cursor. An empty page with ok=false marks the end; calls after that keep
// returning the end marker. limit must be positive, otherwise Next returns an
// error wrapping fs.ErrInvalid, advances nothing and leaves the cursor
// usable. Reading a closed cursor is an ordinary miss: an empty page,
// ok=false and no error. The returned bytes are owned by the caller; paging
// one cursor across pages never repeats or loses a key and yields
// byte-identical values for the same key.
func (c *Cursor) Next(limit int) (pairs []KVPair, ok bool, err error) {
	if limit <= 0 {
		return nil, false, errBadLimit
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil || c.offset >= len(c.keys) {
		return nil, false, nil
	}
	n := len(c.keys) - c.offset
	if n > limit {
		n = limit
	}
	page := make([]KVPair, n)
	for i := 0; i < n; i++ {
		node := c.view[c.keys[c.offset+i]]
		page[i] = KVPair{Key: c.keys[c.offset+i], Value: cloneBytes(node.value)}
	}
	c.offset += n
	return page, true, nil
}

// Close releases the cursor's fixed view and lets the next compaction reclaim
// the versions it pinned. It is idempotent and returns nil; closing queues
// behind a Next already in flight and never interrupts it.
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return nil
	}
	c.view = nil
	c.keys = nil
	if c.pin != nil {
		c.store.pins.remove(c.pin)
		c.pin = nil
	}
	return nil
}

// collectView gathers the present pairs of v within the bounds, sorted by
// byte order. endInclusive selects [start,end] (range scans) versus
// [start,end) (prefix scans, where end is the first key past the prefix).
func collectView(v view, start, end string, endInclusive bool) []KVPair {
	keys := sortedKeysInRange(v, start, end, endInclusive)
	out := make([]KVPair, 0, len(keys))
	for _, k := range keys {
		out = append(out, KVPair{Key: k, Value: cloneBytes(v[k].value)})
	}
	return out
}

// sortedKeysInRange returns the present keys k of v with start <= k and
// (end == "" or k < end / k <= end), in byte order. Tombstones are skipped,
// so a reversed interval (non-empty end smaller than start) or one without
// hits naturally yields an empty slice. Empty bounds are open: start="" is
// the smallest possible key, end="" means no upper bound.
func sortedKeysInRange(v view, start, end string, endInclusive bool) []string {
	if end != "" && start > end {
		return nil
	}
	keys := make([]string, 0, len(v))
	for k, n := range v {
		if k == "" || n == nil || !n.present {
			// Empty keys are never a hit, matching point Get; tombstones and
			// absent versions are skipped the same way.
			continue
		}
		if k < start {
			continue
		}
		if end != "" {
			if endInclusive {
				if k > end {
					continue
				}
			} else if k >= end {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// prefixRange converts a prefix into the smallest key carrying it and the
// first key past every key carrying it. An empty prefix covers the whole key
// space. If every byte of the prefix is 0xff no finite upper bound exists, so
// end is returned empty to denote "unbounded above".
func prefixRange(prefix string) (start, end string) {
	if prefix == "" {
		return "", ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return prefix, string(b[:i+1])
		}
	}
	return prefix, ""
}

// pin records one open cursor's claim on history. The cursor's view keeps
// the versions alive in memory; the pin makes the compaction rewrite keep
// them on disk. Retaining only the bounds (not a copied key list) keeps both
// pinning and closing O(1); the compaction that actually needs the keys
// filters and sorts the pinned view, bounded by live data and open cursors
// rather than cumulative writes.
type pin struct {
	id           uint64
	gen          uint64 // generation of the pinned view: primary retention order
	view         view
	start, end   string
	endInclusive bool
}

// pinRegistry tracks open cursors. It has its own lock because cursor Close
// releases a pin without holding the store write lock, while scans and pages
// take neither lock: fsync-bound writes never stall readers.
type pinRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	pins   []*pin // append-only in opening order; removal swaps-and-shrinks
}

func (r *pinRegistry) add(gen uint64, v view, start, end string, endInclusive bool) *pin {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	p := &pin{id: r.nextID, gen: gen, view: v, start: start, end: end, endInclusive: endInclusive}
	r.pins = append(r.pins, p)
	return p
}

func (r *pinRegistry) remove(target *pin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, p := range r.pins {
		if p == target {
			r.pins[i] = r.pins[len(r.pins)-1]
			r.pins[len(r.pins)-1] = nil
			r.pins = r.pins[:len(r.pins)-1]
			return
		}
	}
}

// ordered returns the currently open pins sorted oldest-view first. Cursors
// on the same view generation tie-break by opening order; they pin identical
// content, so the tie order is immaterial. The slice is a snapshot: a cursor
// closing afterwards does not mutate it.
func (r *pinRegistry) ordered() []*pin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*pin, len(r.pins))
	copy(out, r.pins)
	sort.Slice(out, func(i, j int) bool {
		if out[i].gen != out[j].gen {
			return out[i].gen < out[j].gen
		}
		return out[i].id < out[j].id
	})
	return out
}

// errBadLimit is returned when a cursor page is requested with a non-positive
// limit; it wraps fs.ErrInvalid like every other invalid-argument error.
var errBadLimit = errors.Join(
	errors.New("snapshot: cursor limit must be positive"),
	fs.ErrInvalid,
)
