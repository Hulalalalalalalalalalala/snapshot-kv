package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	return info.Size()
}

func TestCompactReclaimsDiskHistory(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Put("a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	pinned, err := s.Snapshot() // pins v1 of "a" and live "b"
	if err != nil {
		t.Fatal(err)
	}

	big := make([]byte, 4096)
	for i := range big {
		big[i] = byte(i)
	}
	// Many overwrites and a delete: all of this is reclaimable history on
	// disk, none of it belongs to the pinned snapshot.
	for i := 0; i < 20; i++ {
		if err := s.Put("a", big); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("c", []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}

	statsBefore := s.Stats()
	sizeBefore := walSize(t, dir)

	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, compactTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temporary compaction file left behind: %v", err)
	}
	sizeAfter := walSize(t, dir)
	if sizeAfter >= sizeBefore {
		t.Fatalf("wal did not shrink: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// Stats three numbers are unchanged by compaction.
	statsAfter := s.Stats()
	if statsAfter != statsBefore {
		t.Fatalf("stats changed: before=%+v after=%+v", statsBefore, statsAfter)
	}

	// The open snapshot's fixed view is byte-identical.
	if got, ok := pinned.Get("a"); !ok || string(got) != "v1" {
		t.Fatalf("pinned a: %q ok=%v", got, ok)
	}
	if got, ok := pinned.Get("b"); !ok || string(got) != "doomed" {
		t.Fatalf("pinned deleted b: %q ok=%v", got, ok)
	}
	if _, ok := pinned.Get("c"); ok {
		t.Fatal("pinned snapshot sees key created after it")
	}

	// Latest view: miss/hit semantics unchanged.
	if got, ok, err := s.Get("a"); !ok || err != nil || len(got) != len(big) {
		t.Fatalf("latest a: len=%d ok=%v err=%v", len(got), ok, err)
	} else {
		for i := range big {
			if got[i] != big[i] {
				t.Fatalf("latest a torn at %d", i)
			}
		}
	}
	if got, ok, _ := s.Get("c"); !ok || string(got) != "kept" {
		t.Fatalf("latest c: %q ok=%v", got, ok)
	}
	if _, ok, _ := s.Get("b"); ok {
		t.Fatal("deleted key is a hit after compaction")
	}
	if _, ok, _ := s.Get("never"); ok {
		t.Fatal("unknown key is a hit after compaction")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: committed state is complete, snapshot count never reset.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("c"); !ok || string(got) != "kept" {
		t.Fatalf("after reopen c: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("b"); ok {
		t.Fatal("deletion lost across reopen")
	}
	if st := s2.Stats(); st != statsBefore {
		t.Fatalf("stats after reopen: %+v want %+v", st, statsBefore)
	}
	if n := s2.Stats().Snapshots; n != 4 {
		t.Fatalf("snapshot count after reopen: %d, want 4", n)
	}

	// New commits append to the compacted log and survive another reopen.
	if err := s2.Put("d", []byte("post")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if got, ok, _ := s3.Get("d"); !ok || string(got) != "post" {
		t.Fatalf("post-compact commit lost: %q ok=%v", got, ok)
	}
	if n := s3.Stats().Snapshots; n != 4 {
		t.Fatalf("snapshot count changed: %d", n)
	}

	// The pinned snapshot even outlives its store and still reads identically.
	if got, ok := pinned.Get("a"); !ok || string(got) != "v1" {
		t.Fatalf("pinned view changed: %q ok=%v", got, ok)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactDropsInMemoryHistory(t *testing.T) {
	s := openStore(t, tempDir(t))

	if err := s.Put("k", []byte("old1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("old2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("gone", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("gone"); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	v := s.current.Load()
	v.rangeEach(func(key string, n *node) bool {
		if n.older != nil {
			t.Fatalf("current node for %q still keeps history after compaction", key)
		}
		return true
	})
	if n := v.lookup("gone"); n != nil {
		t.Fatalf("deleted key retained a node in compacted view: %+v", n)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "new" {
		t.Fatalf("latest k: %q ok=%v", got, ok)
	}
}

func TestCompactEmptyValuesStayHits(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	for i, v := range [][]byte{nil, {}} {
		if err := s.Put(fmt.Sprintf("e%d", i), v); err != nil {
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
	for i := 0; i < 2; i++ {
		if got, ok, err := s2.Get(fmt.Sprintf("e%d", i)); !ok || err != nil || len(got) != 0 {
			t.Fatalf("empty value %d after reopen: %q ok=%v err=%v", i, got, ok, err)
		}
	}
}

func TestCompactOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Compact(); err != nil {
		t.Fatalf("compact on open store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("compact closed store: err=%v, want fs.ErrClosed", err)
	}
}

func TestCompactRepeatedAndIdempotent(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := s.Compact(); err != nil {
			t.Fatalf("compact %d: %v", i, err)
		}
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("value after repeated compaction: %q ok=%v", got, ok)
	}
}

func TestCompactConcurrent(t *testing.T) {
	s := openStore(t, tempDir(t))

	const writers = 4
	const compactors = 2
	const iterations = 100

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", w)
			for i := 0; i < iterations; i++ {
				v := []byte(fmt.Sprintf("%d-%d", w, i))
				if err := s.Put(key, v); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				if i%17 == 0 {
					if err := s.Delete(key); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
				}
			}
		}(w)
	}
	for c := 0; c < compactors; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := s.Compact(); err != nil {
					t.Errorf("compact: %v", err)
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := s.Snapshot()
			if err != nil {
				if errors.Is(err, fs.ErrClosed) {
					return
				}
				t.Errorf("snapshot: %v", err)
				return
			}
			// Two passes over the fixed view must agree exactly.
			first := map[string]string{}
			for w := 0; w < writers; w++ {
				if v, ok := snap.Get(fmt.Sprintf("k%d", w)); ok {
					first[fmt.Sprintf("k%d", w)] = string(v)
				}
			}
			for k, want := range first {
				if got, ok := snap.Get(k); !ok || string(got) != want {
					t.Errorf("snapshot view of %s changed across compaction", k)
				}
			}
			snap.Close()
		}
	}()

	wg.Wait()
	close(stop)
	readers.Wait()

	if err := s.Compact(); err != nil {
		t.Fatalf("final compact: %v", err)
	}
}

func TestCompactStaleTempAfterCrash(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("keep", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-compaction: a half-built replacement file exists
	// next to an intact wal.log. Reopen must ignore the temp, need no second
	// compaction, and recover a complete committed state.
	if err := os.WriteFile(filepath.Join(dir, compactTmpName), []byte("partial garbage"), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with stale temp: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("keep"); !ok || string(got) != "value" {
		t.Fatalf("value lost with stale temp: %q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count with stale temp: %d, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(dir, compactTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale temp not removed on open: %v", err)
	}

	// Commits and a fresh compaction work immediately, with no repair step.
	if err := s2.Put("after", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Compact(); err != nil {
		t.Fatalf("compact after crash recovery: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if got, ok, _ := s3.Get("after"); !ok || string(got) != "x" {
		t.Fatalf("post-recovery commit lost: %q ok=%v", got, ok)
	}
}

func TestCompactWithSnapshotsOpenThenReclaimAfterClose(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	old, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}

	// Snapshot open: disk is still reclaimed, pinned view still served.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, ok := old.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("pinned v1: %q ok=%v", got, ok)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("latest v2: %q ok=%v", got, ok)
	}

	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	// A later snapshot sees only v2; compaction again stays error-free.
	young, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := young.Get("k"); !ok {
		t.Fatal("young snapshot misses k")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, ok := young.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("young snapshot changed: %q ok=%v", got, ok)
	}
	young.Close()
}

func TestCompactEmptyStoreDoesNotGrow(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dir, walName)); err != nil || info.Size() != 0 {
		t.Fatalf("empty compacted wal size=%v err=%v, want 0", info.Size(), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if st := s2.Stats(); st.Keys != 0 || st.Snapshots != 0 || st.AvgBytes != 0 {
		t.Fatalf("stats on reopened compacted empty store: %+v", st)
	}
}
