package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func keysOf(pairs []KVPair) []string {
	keys := make([]string, len(pairs))
	for i, p := range pairs {
		keys[i] = p.Key
	}
	return keys
}

func TestScanRangeOrderingAndBounds(t *testing.T) {
	s := openStore(t, tempDir(t))
	input := []string{"a", "a1", "a2", "b", "ba", "bb", "c", "z"}
	for _, k := range input {
		if err := s.Put(k, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ScanRange("a1", "bb")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a1", "a2", "b", "ba", "bb"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("range [a1,bb]: %v, want %v", keysOf(got), want)
	}

	// Both endpoints inclusive.
	got, err = s.ScanRange("b", "b")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"b"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("range [b,b]: %v, want %v", keysOf(got), want)
	}

	// Empty bounds are open on that side.
	got, err = s.ScanRange("", "b")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "a1", "a2", "b"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("range [,b]: %v, want %v", keysOf(got), want)
	}
	got, err = s.ScanRange("c", "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"c", "z"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("range [c,]: %v, want %v", keysOf(got), want)
	}
	got, err = s.ScanRange("", "")
	if err != nil {
		t.Fatal(err)
	}
	if want := input; !equalStrings(keysOf(got), want) {
		t.Fatalf("full range: %v, want %v", keysOf(got), want)
	}

	// Reversed range: empty, non-nil, no error.
	got, err = s.ScanRange("z", "a")
	if err != nil || len(got) != 0 {
		t.Fatalf("reversed range: pairs=%v err=%v, want empty, nil", got, err)
	}
	// Range matching nothing in the gap.
	got, err = s.ScanRange("a3", "a9")
	if err != nil || len(got) != 0 {
		t.Fatalf("empty gap: pairs=%v err=%v", got, err)
	}

	// Values are byte-correct and owned by the caller.
	got, _ = s.ScanRange("a", "a")
	if len(got) != 1 || string(got[0].Value) != "v-a" {
		t.Fatalf("value: %+v", got)
	}
	got[0].Value[0] = 'X'
	again, _ := s.ScanRange("a", "a")
	if string(again[0].Value) != "v-a" {
		t.Fatalf("scan aliased storage: %q", again[0].Value)
	}
}

func TestScanPrefix(t *testing.T) {
	s := openStore(t, tempDir(t))
	for _, k := range []string{"user:1", "user:2", "user:10", "username", "utag", "x"} {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ScanPrefix("user:")
	if err != nil {
		t.Fatal(err)
	}
	// Byte order: "user:1" < "user:10" < "user:2".
	if want := []string{"user:1", "user:10", "user:2"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("prefix user: %v, want %v", keysOf(got), want)
	}

	got, err = s.ScanPrefix("nope")
	if err != nil || len(got) != 0 {
		t.Fatalf("missing prefix: %v err=%v", got, err)
	}

	got, err = s.ScanPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 || !sort.StringsAreSorted(keysOf(got)) {
		t.Fatalf("empty prefix: %v", keysOf(got))
	}

	// Prefix of all-0xff bytes must stay correct at the upper edge.
	if err := s.Put("\xff\xff", []byte("edge")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("\xff\xff\x00", []byte("edge-child")); err != nil {
		t.Fatal(err)
	}
	got, err = s.ScanPrefix("\xff\xff")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"\xff\xff", "\xff\xff\x00"}; !equalStrings(keysOf(got), want) {
		t.Fatalf("0xff prefix: %q", keysOf(got))
	}
	// 0xff prefix must not swallow a key it does not prefix.
	if got, _ := s.ScanPrefix("\xff"); len(got) != 2 {
		t.Fatalf("0xff prefix over-matched: %q", keysOf(got))
	}
}

func TestScanHitSemantics(t *testing.T) {
	s := openStore(t, tempDir(t))
	// Empty values show up; deleted and never-written keys do not.
	if err := s.Put("e-nil", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("e-empty", []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("live", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("dead", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("dead"); err != nil {
		t.Fatal(err)
	}

	got, err := s.ScanPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"e-nil": 0, "e-empty": 0, "live": 1}
	if len(got) != len(want) {
		t.Fatalf("pairs=%v, want %d keys", keysOf(got), len(want))
	}
	for _, p := range got {
		n, ok := want[p.Key]
		if !ok {
			t.Fatalf("unexpected/deleted key %q in scan", p.Key)
		}
		if len(p.Value) != n {
			t.Fatalf("key %q value len %d, want %d", p.Key, len(p.Value), n)
		}
	}
}

func TestScanSnapshotFixedView(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("c", []byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}

	got := snap.ScanPrefix("")
	want := []KVPair{{Key: "a", Value: []byte("old")}, {Key: "b", Value: []byte("keep")}}
	if !equalPairs(got, want) {
		t.Fatalf("snapshot scan: %+v, want %+v", got, want)
	}
	if r := snap.ScanRange("z", "a"); len(r) != 0 {
		t.Fatalf("reversed snapshot range: %v", r)
	}
	if r := snap.ScanPrefix("zzz"); len(r) != 0 {
		t.Fatalf("missing snapshot prefix: %v", r)
	}

	// Closing the snapshot turns scans into empty results, never errors.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if r := snap.ScanPrefix(""); len(r) != 0 {
		t.Fatalf("closed snapshot scan: %v", r)
	}
	if r := snap.ScanRange("a", "z"); len(r) != 0 {
		t.Fatalf("closed snapshot range: %v", r)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

func TestScanClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScanRange("a", "z"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("ScanRange closed: %v, want fs.ErrClosed", err)
	}
	if _, err := s.ScanPrefix(""); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("ScanPrefix closed: %v, want fs.ErrClosed", err)
	}
	if _, err := s.Cursor("a", "z"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Cursor closed: %v, want fs.ErrClosed", err)
	}
	if _, err := s.CursorPrefix(""); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("CursorPrefix closed: %v, want fs.ErrClosed", err)
	}
}

func drainCursor(t *testing.T, c *Cursor, pageSize int) []KVPair {
	t.Helper()
	var all []KVPair
	for {
		page, ok, err := c.Next(pageSize)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return all
		}
		if len(page) == 0 || len(page) > pageSize {
			t.Fatalf("bad page size %d (limit %d)", len(page), pageSize)
		}
		all = append(all, page...)
	}
}

func TestCursorPaginationStable(t *testing.T) {
	s := openStore(t, tempDir(t))
	const n = 25
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%02d", i)
		if err := s.Put(k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Same cursor paged with different limits returns the identical sequence.
	reference := drainCursor(t, mustCursor(t, s, "", ""), 1)
	if len(reference) != n {
		t.Fatalf("reference len %d", len(reference))
	}
	for _, pageSize := range []int{1, 2, 7, n, n + 100} {
		c := mustCursor(t, s, "", "")
		got := drainCursor(t, c, pageSize)
		if !equalPairs(got, reference) {
			t.Fatalf("pageSize %d: mismatch (%d vs %d)", pageSize, len(got), len(reference))
		}
		// Once exhausted it stays exhausted.
		if page, ok, err := c.Next(1); ok || err != nil || len(page) != 0 {
			t.Fatalf("exhausted cursor returned page=%v ok=%v err=%v", page, ok, err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCursorBoundsAndPrefix(t *testing.T) {
	s := openStore(t, tempDir(t))
	for _, k := range []string{"a", "a1", "a2", "b", "b1"} {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	c, err := s.Cursor("a1", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(drainCursor(t, c, 2)); !equalStrings(got, []string{"a1", "a2", "b"}) {
		t.Fatalf("cursor range: %v", got)
	}

	cp, err := s.CursorPrefix("a")
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(drainCursor(t, cp, 10)); !equalStrings(got, []string{"a", "a1", "a2"}) {
		t.Fatalf("cursor prefix: %v", got)
	}

	// Reversed / matchless cursors end on the first page.
	for _, tc := range [][2]string{{"z", "a"}, {"zz", "zzz"}} {
		c, err := s.Cursor(tc[0], tc[1])
		if err != nil {
			t.Fatal(err)
		}
		page, ok, err := c.Next(10)
		if ok || err != nil || len(page) != 0 {
			t.Fatalf("%v: page=%v ok=%v err=%v", tc, page, ok, err)
		}
		c.Close()
	}
}

func TestCursorInvalidLimit(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	c := mustCursor(t, s, "", "")
	for _, bad := range []int{0, -1, -100} {
		page, ok, err := c.Next(bad)
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("Next(%d): err=%v, want fs.ErrInvalid", bad, err)
		}
		if ok || page != nil {
			t.Fatalf("Next(%d): page=%v ok=%v", bad, page, ok)
		}
	}
	// The invalid call advanced nothing: paging still yields the full set.
	got := drainCursor(t, c, 1)
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("cursor after bad limit: %+v", got)
	}
}

func TestCursorFixedViewAcrossMutationsAndCompaction(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cursor("", "")
	if err != nil {
		t.Fatal(err)
	}
	first, ok, err := c.Next(1)
	if err != nil || !ok || first[0].Key != "k1" || string(first[0].Value) != "v1" {
		t.Fatalf("first page: %+v ok=%v err=%v", first, ok, err)
	}

	// Mutate, delete, compact, snapshot and close-store attempts: the cursor
	// still pages its locked view and never repeats k1.
	for i := 0; i < 10; i++ {
		if err := s.Put("k1", []byte(fmt.Sprintf("new%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("k3", []byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("k2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	rest, ok, err := c.Next(10)
	if err != nil || !ok {
		t.Fatalf("rest page: ok=%v err=%v", ok, err)
	}
	if len(rest) != 1 || rest[0].Key != "k2" || string(rest[0].Value) != "v2" {
		t.Fatalf("cursor view changed: %+v", rest)
	}
	if page, ok, _ := c.Next(1); ok || len(page) != 0 {
		t.Fatalf("cursor did not end at its fixed view: %v", page)
	}

	// The cursor even outlives the store.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	page, ok, err := c.Next(5)
	if ok || err != nil || len(page) != 0 {
		t.Fatalf("cursor after store close: %v ok=%v err=%v", page, ok, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorSnapshotFixedView(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("old")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	c := snap.Cursor("", "")
	if err := s.Put("a", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	got := drainCursor(t, c, 1)
	if !equalPairs(got, []KVPair{{Key: "a", Value: []byte("old")}}) {
		t.Fatalf("snapshot cursor: %+v", got)
	}
	c.Close()

	// Cursor opened on a closed snapshot is immediately exhausted, and its
	// close is a harmless no-op.
	snap.Close()
	dead := snap.Cursor("", "")
	if page, ok, err := dead.Next(1); ok || err != nil || len(page) != 0 {
		t.Fatalf("cursor from closed snapshot: %v ok=%v err=%v", page, ok, err)
	}
	if err := dead.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorCloseIdempotentAndMisses(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	c := mustCursor(t, s, "", "")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
	// Reads after close are plain misses, no error.
	page, ok, err := c.Next(10)
	if ok || err != nil || len(page) != 0 {
		t.Fatalf("closed cursor Next: page=%v ok=%v err=%v", page, ok, err)
	}
}

func TestCursorPinsHistoryThenReclaimsOnClose(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	big := bytes.Repeat([]byte{'Z'}, 8192)
	if err := s.Put("a", []byte("first")); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cursor("", "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := s.Put("a", big); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("b", []byte("keep")); err != nil {
		t.Fatal(err)
	}

	sizeBefore := walSize(t, dir)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	sizePinned := walSize(t, dir)
	// The pinned old value physically survives the rewrite while the 20
	// overwritten big versions do not: retention follows the open cursor,
	// not the write history.
	data, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("first")) {
		t.Fatal("pinned old value missing from compacted log while cursor open")
	}
	if count := bytes.Count(data, big); count != 1 {
		t.Fatalf("overwritten history retained: big value appears %d times, want 1", count)
	}
	if sizePinned > sizeBefore {
		t.Fatalf("compaction grew the log while cursor pinned a small old value: %d -> %d", sizeBefore, sizePinned)
	}

	// Cursor still returns exactly the view it opened on: only "a", and the
	// value current then; "b" was added afterwards and must not appear.
	got := drainCursor(t, c, 3)
	if !equalPairs(got, []KVPair{{Key: "a", Value: []byte("first")}}) {
		t.Fatalf("pinned cursor view: %+v", got)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// The next compaction reclaims everything the cursor pinned; with only
	// live big values + "keep" the log drops the retained history.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	sizeAfter := walSize(t, dir)
	if sizeAfter >= sizePinned {
		t.Fatalf("history not reclaimed after cursor close: pinned=%d after=%d", sizePinned, sizeAfter)
	}

	// Post-reclaim state is intact.
	if got, ok, _ := s.Get("a"); !ok || !bytes.Equal(got, big) {
		t.Fatalf("latest a after reclaim: len=%d ok=%v", len(got), ok)
	}
	if got, ok, _ := s.Get("b"); !ok || string(got) != "keep" {
		t.Fatalf("b after reclaim: %q ok=%v", got, ok)
	}

	// Reopen: complete committed state, no temp file, ready without repair.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, compactTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp file left: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("b"); !ok || string(got) != "keep" {
		t.Fatalf("b after reopen: %q ok=%v", got, ok)
	}
}

func TestCursorPinnedHistorySurvivesReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("v-old")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("del", []byte("x")); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cursor("", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("v-new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("del"); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash right after compaction with the cursor still open.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// Replay of the compacted log ends in exactly the current view...
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "v-new" {
		t.Fatalf("latest a after reopen: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("del"); ok {
		t.Fatal("deletion lost across reopen of cursor-pinned compaction")
	}
	// ...and the pinned old version physically survived in the log while the
	// cumulative snapshot count stayed intact.
	data, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("v-old")) {
		t.Fatal("cursor-pinned version was reclaimed while cursor was open")
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count: %d, want 1", n)
	}
	c.Close()
}

func TestConcurrentScanAndCursor(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 20; i++ {
		if err := s.Put(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer mutating while scans and pages run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.Put(fmt.Sprintf("k%03d", i%20), []byte(fmt.Sprintf("w%d", i))); err != nil {
				return
			}
		}
	}()

	// Scanners: every result is sorted and internally self-consistent.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				pairs, err := s.ScanPrefix("k")
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("scan: %v", err)
					return
				}
				if !sort.StringsAreSorted(keysOf(pairs)) {
					t.Errorf("unsorted scan: %v", keysOf(pairs))
					return
				}
			}
		}()
	}

	// Cursors: full paging yields each key exactly once.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c, err := s.CursorPrefix("k")
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("open cursor: %v", err)
					return
				}
				seen := map[string]bool{}
				page, ok, err := c.Next(3)
				for ; ok; page, ok, err = c.Next(3) {
					if err != nil {
						t.Errorf("next: %v", err)
						c.Close()
						return
					}
					for _, p := range page {
						if seen[p.Key] {
							t.Errorf("key %q repeated in one cursor", p.Key)
						}
						seen[p.Key] = true
					}
				}
				if err != nil {
					t.Errorf("next end: %v", err)
				}
				if len(seen) != 20 {
					t.Errorf("cursor saw %d/20 keys", len(seen))
				}
				c.Close()
			}
		}()
	}

	// Compactors running concurrently.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				if err := s.Compact(); err != nil {
					t.Errorf("compact: %v", err)
					return
				}
			}
		}()
	}

	// Repeated close of one cursor from multiple goroutines.
	c := mustCursor(t, s, "", "")
	var closeWg sync.WaitGroup
	for i := 0; i < 8; i++ {
		closeWg.Add(1)
		go func() {
			defer closeWg.Done()
			if err := c.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
	}
	closeWg.Wait()

	close(stop)
	wg.Wait()
}

func TestScanAndSnapshotSamePath(t *testing.T) {
	// The latest-view scan must read the same in-memory state a snapshot
	// taken at the same instant reads: compare directly.
	s := openStore(t, tempDir(t))
	for _, k := range []string{"m1", "m2", "m3"} {
		if err := s.Put(k, []byte("="+k)); err != nil {
			t.Fatal(err)
		}
	}
	live, err := s.ScanPrefix("m")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	fixed := snap.ScanPrefix("m")
	if !equalPairs(live, fixed) {
		t.Fatalf("live %+v != snapshot %+v", live, fixed)
	}
	snap.Close()
}

func mustCursor(t *testing.T, s *Store, start, end string) *Cursor {
	t.Helper()
	c, err := s.Cursor(start, end)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	return c
}

func equalStrings(a, b []string) bool {
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

func equalPairs(a, b []KVPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}
