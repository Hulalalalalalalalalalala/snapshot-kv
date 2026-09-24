package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func keysOf(pairs []KVPair) []string {
	ks := make([]string, len(pairs))
	for i, p := range pairs {
		ks[i] = p.Key
	}
	return ks
}

func TestStoreScanOrderedRange(t *testing.T) {
	s := openStore(t, tempDir(t))

	// Insert out of order, including binary keys, to prove byte ordering.
	for _, k := range []string{"c", "a", "b", "a1", "a\x00", "a\xff", "\x01", "ab"} {
		if err := s.Put(k, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("deleted", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("empty", nil); err != nil { // empty value still a hit
		t.Fatal(err)
	}

	pairs, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"\x01", "a", "a\x00", "a1", "ab", "a\xff", "b", "c", "empty"}
	if got := keysOf(pairs); !sliceEq(got, want) {
		t.Fatalf("full scan order:\n got=%q\nwant=%q", got, want)
	}
	// Empty-value key is present with a zero-length value (nil, like Get).
	foundEmpty := false
	for _, p := range pairs {
		if p.Key == "empty" {
			foundEmpty = true
			if len(p.Value) != 0 {
				t.Fatalf("empty-value key has non-empty value: %v", p.Value)
			}
		}
	}
	if !foundEmpty {
		t.Fatal("empty-value key missing from scan")
	}
	// Deleted and never-written keys absent; empty key can never exist.
	for _, bad := range []string{"deleted", "never", ""} {
		if i := sort.SearchStrings(keysOf(pairs), bad); i < len(pairs) && pairs[i].Key == bad {
			t.Fatalf("scan included %q", bad)
		}
	}

	// Half-open interval ["a","b"): includes a, a\x00, a\xff, a1, ab.
	pairs, err = s.Scan(Range{Start: "a", End: "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, want[1:6]) {
		t.Fatalf("range [a,b): %q want %q", got, want[1:6])
	}

	// Open-ended Start.
	pairs, err = s.Scan(Range{Start: "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"b", "c", "empty"}) {
		t.Fatalf("range [b,): %q", got)
	}

	// End-bounded: [, "a1") excludes "a1" and everything byte-above it.
	pairs, err = s.Scan(Range{End: "a1"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"\x01", "a", "a\x00"}) {
		t.Fatalf("range [,a1): %q", got)
	}

	// Limit truncates in order; a non-positive limit means everything.
	pairs, err = s.Scan(Range{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 3 {
		t.Fatalf("limit 3 got %d pairs", len(pairs))
	}
	if got := keysOf(pairs); !sliceEq(got, want[:3]) {
		t.Fatalf("limited scan: %q", got)
	}
}

func TestScanReversedAndNonmatchingAreEmpty(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("m", []byte("1")); err != nil {
		t.Fatal(err)
	}
	for _, r := range []Range{
		{Start: "z", End: "a"}, // reversed
		{Start: "m", End: "m"}, // zero-width
		{Start: "n", End: "z"}, // in order but nothing there
	} {
		pairs, err := s.Scan(r, 0)
		if err != nil {
			t.Fatalf("range %v: unexpected err %v", r, err)
		}
		if len(pairs) != 0 {
			t.Fatalf("range %v returned partial data: %q", r, pairs)
		}
	}

	// Prefix that matches nothing: empty, no error.
	pairs, err := s.ScanPrefix("zzz", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Fatalf("nonmatching prefix: %q", pairs)
	}
}

func TestScanPrefixBoundaries(t *testing.T) {
	s := openStore(t, tempDir(t))
	keys := []string{"a", "ab", "abc", "b", "a\xff", "a\xffz", "b\x00"}
	for _, k := range keys {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := s.ScanPrefix("a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"a", "ab", "abc", "a\xff", "a\xffz"}) {
		t.Fatalf("prefix a: %q", got)
	}

	// Trailing 0xFF rolls the bound: "a\xff" covers a\xff and a\xffz, ends at "b".
	pairs, err = s.ScanPrefix("a\xff", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"a\xff", "a\xffz"}) {
		t.Fatalf("prefix a\\xff: %q", got)
	}

	// All-0xFF prefix is effectively unbounded past it.
	if err := s.Put("\xff\x01", []byte("x")); err != nil {
		t.Fatal(err)
	}
	pairs, err = s.ScanPrefix("\xff", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"\xff\x01"}) {
		t.Fatalf("prefix \\xff: %q", got)
	}

	// Empty prefix matches everything.
	pairs, err = s.ScanPrefix("", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != len(keys)+1 {
		t.Fatalf("empty prefix matched %d, want %d", len(pairs), len(keys)+1)
	}

	// Returned values are byte-correct copies.
	pairs, _ = s.ScanPrefix("ab", 0)
	if len(pairs) != 2 || pairs[0].Key != "ab" || string(pairs[0].Value) != "ab" {
		t.Fatalf("prefix ab values: %+v", pairs)
	}
}

func TestScanOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scan(Range{}, 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("scan closed store: %v", err)
	}
	if _, err := s.ScanPrefix("k", 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("scanprefix closed store: %v", err)
	}
	if _, err := s.OpenCursor(Range{}); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("opencursor closed store: %v", err)
	}
	if _, err := s.OpenCursorPrefix("k"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("opencursorprefix closed store: %v", err)
	}
}

func TestSnapshotScanFixedView(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Put("a", []byte("9")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("c", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}

	pairs, err := snap.Scan(Range{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pairs); !sliceEq(got, []string{"a", "b"}) {
		t.Fatalf("snapshot scan: %q", got)
	}
	if string(pairs[0].Value) != "1" || string(pairs[1].Value) != "2" {
		t.Fatalf("snapshot scan values: %q %q", pairs[0].Value, pairs[1].Value)
	}
	pairs, err = snap.ScanPrefix("a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 || string(pairs[0].Value) != "1" {
		t.Fatalf("snapshot prefix: %+v", pairs)
	}

	// Closed snapshot: scans are empty misses, never errors.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if pairs, err := snap.Scan(Range{}, 0); err != nil || len(pairs) != 0 {
		t.Fatalf("closed snapshot scan: %v %v", pairs, err)
	}
	if pairs, err := snap.ScanPrefix("", 0); err != nil || len(pairs) != 0 {
		t.Fatalf("closed snapshot prefix: %v %v", pairs, err)
	}
	if cur, err := snap.OpenCursor(Range{}); err != nil {
		t.Fatalf("cursor on closed snapshot: %v", err)
	} else {
		if p, err := cur.Page(0, 10); err != nil || len(p) != 0 {
			t.Fatalf("page on closed-snapshot cursor: %v %v", p, err)
		}
		if err := cur.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func drainCursor(t *testing.T, c *Cursor, pageSize int) []KVPair {
	t.Helper()
	var all []KVPair
	off := 0
	for {
		page, err := c.Page(off, pageSize)
		if err != nil {
			t.Fatalf("page off=%d: %v", off, err)
		}
		all = append(all, page...)
		if len(page) < pageSize {
			// One more confirmation page at the exact end offset: empty, nil.
			end, err := c.Page(off+len(page), 1)
			if err != nil || len(end) != 0 {
				t.Fatalf("end page: %v %v", end, err)
			}
			return all
		}
		off += len(page)
	}
}

func TestCursorPaginationByteIdentical(t *testing.T) {
	s := openStore(t, tempDir(t))
	const n = 25
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%03d", i)
		if err := s.Put(k, []byte(strings.Repeat("v", i+1))); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatal(err)
	}

	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}

	// Mutate the world while paging: overwrites, deletes, new keys, new
	// snapshots and compactions must not change the cursor's sequence.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			s.Put(fmt.Sprintf("k%03d", i%n), []byte("changed"))
			if i%3 == 0 {
				s.Delete(fmt.Sprintf("k%03d", i))
			}
			s.Put(fmt.Sprintf("new%03d", i), []byte("x"))
			s.Compact()
			snap, _ := s.Snapshot()
			snap.Close()
		}
	}()

	var all []KVPair
	off := 0
	for {
		page, perr := c.Page(off, 7)
		if perr != nil {
			t.Fatalf("page off=%d: %v", off, perr)
		}
		all = append(all, page...)
		off += len(page)
		if len(page) < 7 {
			break
		}
	}
	wg.Wait()

	if len(all) != len(expected) {
		t.Fatalf("paged %d pairs, want %d", len(all), len(expected))
	}
	for i := range expected {
		if all[i].Key != expected[i].Key || !bytes.Equal(all[i].Value, expected[i].Value) {
			t.Fatalf("page %d differs: got %q=%q want %q=%q",
				i, all[i].Key, all[i].Value, expected[i].Key, expected[i].Value)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorInvalidArguments(t *testing.T) {
	s := openStore(t, tempDir(t))
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		off, limit int
	}{
		{"zero limit", 0, 0},
		{"negative limit", 0, -1},
		{"negative offset", -1, 1},
		{"offset past end", 4, 1},
		{"far past end", 100, 1},
	} {
		if _, err := c.Page(tc.off, tc.limit); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("%s: err=%v want fs.ErrInvalid", tc.name, err)
		}
	}
	// A failed call changes nothing: the sequence still reads whole.
	all := drainCursor(t, c, 2)
	if got := keysOf(all); !sliceEq(got, []string{"a", "b", "c"}) {
		t.Fatalf("cursor after invalid args: %q", got)
	}

	// Re-reading an earlier in-range offset is allowed and gives the same
	// bytes; the canonical forward walk still has no dup or gap.
	p, err := c.Page(1, 1)
	if err != nil || len(p) != 1 || p[0].Key != "b" || string(p[0].Value) != "b" {
		t.Fatalf("re-read offset 1: %+v err=%v", p, err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Closed: reads are empty, no error; invalid limit is also just empty.
	if p, err := c.Page(0, 1); err != nil || len(p) != 0 {
		t.Fatalf("page closed cursor: %v %v", p, err)
	}
	if p, err := c.Page(0, -9); err != nil || len(p) != 0 {
		t.Fatalf("bad page closed cursor: %v %v", p, err)
	}
	if err := c.Close(); err != nil { // idempotent
		t.Fatalf("double close: %v", err)
	}
}

func TestCursorBoundsAndReversed(t *testing.T) {
	s := openStore(t, tempDir(t))
	for _, k := range []string{"a", "aa", "ab", "b"} {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	c, err := s.OpenCursor(Range{Start: "aa", End: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(drainCursor(t, c, 1)); !sliceEq(got, []string{"aa", "ab"}) {
		t.Fatalf("range cursor: %q", got)
	}
	c.Close()

	pc, err := s.OpenCursorPrefix("a")
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(drainCursor(t, pc, 10)); !sliceEq(got, []string{"a", "aa", "ab"}) {
		t.Fatalf("prefix cursor: %q", got)
	}
	pc.Close()

	rc, err := s.OpenCursor(Range{Start: "z", End: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if p, err := rc.Page(0, 10); err != nil || len(p) != 0 {
		t.Fatalf("reversed cursor: %v %v", p, err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorPinsHistoryAcrossCompaction(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	big := bytes.Repeat([]byte{'B'}, 4096)
	if err := s.Put("pinned", big); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("other", []byte("o1")); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite after the cursor: the cursor must keep the big old value.
	if err := s.Put("pinned", []byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("other", []byte("o2")); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	paged := drainCursor(t, c, 3)
	want := map[string]string{"pinned": string(big), "other": "o1"}
	if len(paged) != 2 {
		t.Fatalf("cursor after compact: %+v", paged)
	}
	for _, p := range paged {
		if string(p.Value) != want[p.Key] {
			t.Fatalf("cursor %q after compact = %q, want %q", p.Key, p.Value, want[p.Key])
		}
	}
	// Latest view moved on.
	if got, ok, _ := s.Get("pinned"); !ok || string(got) != "small" {
		t.Fatalf("latest pinned: %q %v", got, ok)
	}

	// Cursor still open: its big value is retained on disk.
	sizePinned := walSize(t, dir)

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// The very next compaction drops the cursor's history; the directory
	// must be observably smaller than while the cursor pinned it.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	sizeReclaimed := walSize(t, dir)
	if sizeReclaimed >= sizePinned {
		t.Fatalf("history not reclaimed after cursor close: pinned=%d reclaimed=%d",
			sizePinned, sizeReclaimed)
	}

	// Reopen: terminal state complete and readable.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("pinned"); !ok || string(got) != "small" {
		t.Fatalf("after reopen: %q %v", got, ok)
	}
	pairs, err := s2.Scan(Range{}, 0)
	if err != nil || len(pairs) != 2 {
		t.Fatalf("scan after reopen: %v %v", pairs, err)
	}
}

func TestCursorPinnedDeleteAcrossCompactionAndReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	big := bytes.Repeat([]byte{'D'}, 4096)
	if err := s.Put("k", big); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	// Cursor still pages the old big value even though the head is small.
	p := drainCursor(t, c, 5)
	if len(p) != 1 || !bytes.Equal(p[0].Value, big) {
		t.Fatalf("cursor lost pinned value: %+v", p)
	}

	// Abandon the cursor without closing and reopen: the pinned history is
	// present but harmless; the latest view is correct, and a compaction now
	// reclaims it with no repair step.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "new" {
		t.Fatalf("reopened value: %q %v", got, ok)
	}
	before := walSize(t, dir)
	if err := s2.Compact(); err != nil {
		t.Fatal(err)
	}
	after := walSize(t, dir)
	if after >= before {
		t.Fatalf("leftover pinned history not reclaimed: before=%d after=%d", before, after)
	}
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "new" {
		t.Fatalf("value after reclaim compact: %q %v", got, ok)
	}
	s2.Close()
}

func TestSnapshotCursorPinsToo(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	c, err := snap.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Page(0, 10); err != nil || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("snapshot cursor across compact: %+v %v", p, err)
	}
	c.Close()

	pc, err := snap.OpenCursorPrefix("k")
	if err != nil {
		t.Fatal(err)
	}
	if p, err := pc.Page(0, 10); err != nil || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("snapshot prefix cursor: %+v %v", p, err)
	}

	// Closing the producing snapshot does not invalidate an already-open
	// cursor: it holds its own fixed view and pin.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := pc.Page(0, 10); err != nil || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("cursor after snapshot close: %+v %v", p, err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if p, err := pc.Page(0, 10); err != nil || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("cursor after snapshot close + compact: %+v %v", p, err)
	}
	pc.Close()
	c.Close()

	// Snapshot closed first; cursor opened afterwards is an empty view.
	c2, err := snap.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if p, err := c2.Page(0, 1); err != nil || len(p) != 0 {
		t.Fatalf("cursor from closed snapshot: %+v %v", p, err)
	}
	c2.Close()
}

func TestCursorSurvivesStoreClose(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := c.Page(0, 10)
	if err != nil || len(p) != 1 || p[0].Key != "a" || string(p[0].Value) != "1" {
		t.Fatalf("cursor after store close: %+v %v", p, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Page(0, 10); err != nil || len(p) != 0 {
		t.Fatalf("closed cursor after store close: %v %v", p, err)
	}
}

func TestConcurrentScansAndCursorCloses(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Scanners hammer both views while writers mutate.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pairs, err := s.Scan(Range{}, 0)
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("scan: %v", err)
					return
				}
				keys := keysOf(pairs)
				if !sort.StringsAreSorted(keys) {
					t.Errorf("scan not sorted")
					return
				}
			}
		}()
	}

	// Cursors opened, paged from many goroutines, and closed repeatedly.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, err := s.OpenCursor(Range{})
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("open cursor: %v", err)
					return
				}
				var cwg sync.WaitGroup
				for p := 0; p < 3; p++ {
					cwg.Add(1)
					go func(seed int) {
						defer cwg.Done()
						off := 0
						for off < 60 {
							page, err := c.Page(off, 5)
							if err != nil {
								if errors.Is(err, fs.ErrInvalid) {
									return // another goroutine passed a different offset race
								}
								return
							}
							if len(page) == 0 {
								return
							}
							off += len(page)
						}
					}(r)
				}
				cwg.Wait()
				if err := c.Close(); err != nil {
					t.Errorf("close: %v", err)
					return
				}
				if err := c.Close(); err != nil { // duplicate close queues, nil
					t.Errorf("double close: %v", err)
					return
				}
			}
		}()
	}

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := s.Put(fmt.Sprintf("k%03d", i%50), []byte("x")); err != nil {
					return
				}
				if i%25 == 0 {
					s.Compact()
				}
			}
		}(w)
	}

	// One cursor stays open the whole time: its in-flight paging must remain
	// unaffected by all of the above, including repeated close attempts of
	// *other* cursors.
	steady, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := steady.Page(0, 1000)
	for i := 0; i < 20; i++ {
		got, err := steady.Page(0, 1000)
		if err != nil {
			t.Fatalf("steady cursor: %v", err)
		}
		if len(got) != len(expected) {
			t.Fatalf("steady cursor length changed: %d vs %d", len(got), len(expected))
		}
		for j := range expected {
			if got[j].Key != expected[j].Key || !bytes.Equal(got[j].Value, expected[j].Value) {
				t.Fatalf("steady cursor changed at %d", j)
			}
		}
	}

	close(stop)
	wg.Wait()
	if err := steady.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestScanValuesAreOwnedCopies(t *testing.T) {
	s := openStore(t, tempDir(t))
	in := []byte("hello")
	if err := s.Put("k", in); err != nil {
		t.Fatal(err)
	}
	p, err := s.Scan(Range{}, 0)
	if err != nil || len(p) != 1 {
		t.Fatal(err)
	}
	p[0].Value[0] = 'X'
	if got, ok, _ := s.Get("k"); !ok || string(got) != "hello" {
		t.Fatalf("storage aliased scan value: %q", got)
	}

	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := c.Page(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	cp[0].Value[0] = 'Y'
	cp2, _ := c.Page(0, 1)
	if string(cp2[0].Value) != "hello" {
		t.Fatalf("storage aliased cursor value: %q", cp2[0].Value)
	}
	c.Close()
}

func TestScanAfterReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, []byte(k+"!")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	pairs, err := s2.Scan(Range{Start: "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 || pairs[0].Key != "b" || string(pairs[0].Value) != "b!" ||
		pairs[1].Key != "c" || string(pairs[1].Value) != "c!" {
		t.Fatalf("scan after reopen: %+v", pairs)
	}
	c, err := s2.OpenCursorPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(drainCursor(t, c, 1)); !sliceEq(got, []string{"a", "b", "c"}) {
		t.Fatalf("cursor after reopen: %q", got)
	}
	c.Close()
}

func TestLegacyWALReopens(t *testing.T) {
	// Hand-write a log using the original, seq-less frame format; scan and
	// pagination over a reopened old-version directory must work unchanged.
	dir := tempDir(t)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	writeLegacy := func(op byte, key string, value []byte) {
		hdr := make([]byte, frameHeaderSize)
		binary.LittleEndian.PutUint32(hdr[0:4], recordMagic)
		hdr[4] = op
		binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(key)))
		binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(value)))
		h := crc32.NewIEEE()
		h.Write(hdr)
		buf.Write(hdr)
		buf.WriteString(key)
		h.Write([]byte(key))
		buf.Write(value)
		h.Write(value)
		var crcb [crcSize]byte
		binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
		buf.Write(crcb[:])
	}
	writeLegacy(opPut, "a", []byte("1"))
	writeLegacy(opPut, "b", []byte("2"))
	writeLegacy(opDelete, "b", nil)
	if err := os.WriteFile(filepath.Join(dir, walName), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pairs, err := s.Scan(Range{}, 0)
	if err != nil || len(pairs) != 1 || pairs[0].Key != "a" || string(pairs[0].Value) != "1" {
		t.Fatalf("legacy scan: %+v err=%v", pairs, err)
	}
	// New writes interleave with legacy history; overwrites + compact work.
	if err := s.Put("a", []byte("9")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Get("a"); !ok || string(got) != "9" {
		t.Fatalf("post-legacy overwrite: %q %v", got, ok)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "9" {
		t.Fatalf("legacy chain after reopen: %q %v", got, ok)
	}
}

func TestCursorPinnedAcrossMultipleCompactionsThenReclaim(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	big := bytes.Repeat([]byte{'Z'}, 8192)
	if err := s.Put("k", big); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}

	// Several overwrite+compact eras while the cursor stays open.
	for i := 0; i < 4; i++ {
		if err := s.Put("k", []byte(fmt.Sprintf("gen%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		// The pinned big value survives every era.
		p, err := c.Page(0, 10)
		if err != nil || len(p) != 1 || !bytes.Equal(p[0].Value, big) {
			t.Fatalf("era %d cursor: %+v err=%v", i, p, err)
		}
	}
	pinnedSize := walSize(t, dir)

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	reclaimedSize := walSize(t, dir)
	if reclaimedSize >= pinnedSize {
		t.Fatalf("pinned history retained after close: %d >= %d", reclaimedSize, pinnedSize)
	}

	// Reopen into the fully-reclaimed terminal state; scan agrees with Get.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "gen3" {
		t.Fatalf("terminal value: %q ok=%v", got, ok)
	}
	pairs, err := s2.Scan(Range{}, 0)
	if err != nil || len(pairs) != 1 || string(pairs[0].Value) != "gen3" {
		t.Fatalf("terminal scan: %+v err=%v", pairs, err)
	}
}

func TestScansAndCursorsDoNotCountAsSnapshots(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	before := s.Stats().Snapshots

	s.Scan(Range{}, 0)
	s.ScanPrefix("k", 0)
	c1, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.OpenCursorPrefix("k")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot() // the only thing that counts
	if err != nil {
		t.Fatal(err)
	}
	sc, err := snap.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	c1.Page(0, 1)
	c2.Page(0, 1)
	sc.Page(0, 1)
	snap.Scan(Range{}, 0)
	if got := s.Stats().Snapshots; got != before+1 {
		t.Fatalf("snapshot count = %d, want %d", got, before+1)
	}
	c1.Close()
	c2.Close()
	sc.Close()
	snap.Close()
	if got := s.Stats().Snapshots; got != before+1 {
		t.Fatalf("snapshot count after closes = %d, want %d", got, before+1)
	}
}

func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
