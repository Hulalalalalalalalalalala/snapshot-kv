package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func TestCompactShrinksDirectoryAndKeepsViews(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Put("keep", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// Churn: many overwritten versions plus a deleted key.
	var last []byte
	for i := 0; i < 300; i++ {
		last = bytes.Repeat([]byte{'x'}, 64)
		if err := s.Put("keep", last); err != nil {
			t.Fatal(err)
		}
		if err := s.Put("churn", bytes.Repeat([]byte{'y'}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete("churn"); err != nil {
		t.Fatal(err)
	}

	statsBefore := s.Stats()
	sizeBefore := dirSize(t, dir)

	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if sizeAfter := dirSize(t, dir); sizeAfter >= sizeBefore {
		t.Fatalf("directory did not shrink: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// The open snapshot's fixed view is byte-for-byte intact.
	if got, ok := snap.Get("keep"); !ok || string(got) != "v1" {
		t.Fatalf("snapshot view changed by compaction: %q %v", got, ok)
	}
	if _, ok := snap.Get("churn"); ok {
		t.Fatal("snapshot sees key created after it")
	}

	// The live view is untouched as well.
	if got, ok, err := s.Get("keep"); !ok || err != nil || !bytes.Equal(got, last) {
		t.Fatalf("live value changed by compaction: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := s.Get("churn"); ok {
		t.Fatal("deleted key visible after compaction")
	}
	if _, ok, _ := s.Get("never"); ok {
		t.Fatal("unknown key visible after compaction")
	}
	if _, ok, _ := s.Get(""); ok {
		t.Fatal("empty key visible after compaction")
	}

	// Stats are a pure function of the live view: identical across compaction.
	if st := s.Stats(); st != statsBefore {
		t.Fatalf("stats changed by compaction: before=%+v after=%+v", statsBefore, st)
	}

	// Only the real log remains: no scratch file left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != walName {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("unexpected directory contents after compaction: %v", names)
	}
}

func TestCompactTrimsHistoryInMemory(t *testing.T) {
	s := openStore(t, tempDir(t))

	mustPut := func(k, v string) {
		t.Helper()
		if err := s.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	mustPut("k", "v1")
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	mustPut("k", "v2")
	mustPut("k", "v3")

	mustPut("doomed", "x")
	if err := s.Delete("doomed"); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	cur := *s.current.Load()

	// The chain keeps everything down to what the open snapshot pins (v1)
	// and nothing below it.
	head := cur["k"]
	if head == nil || string(head.value) != "v3" {
		t.Fatalf("head after compact: %+v", head)
	}
	n := head
	var floor *node
	for ; n != nil; n = n.older {
		floor = n
	}
	if floor == nil || string(floor.value) != "v1" {
		t.Fatalf("pinned version lost: floor=%+v", floor)
	}
	if floor.older != nil {
		t.Fatal("history below the pinned floor survived compaction")
	}

	// A deleted key no snapshot references is gone from the live view.
	if _, ok := cur["doomed"]; ok {
		t.Fatal("tombstone chain survived compaction")
	}

	// Once the snapshot closes, a second compaction releases the rest.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	head = (*s.current.Load())["k"]
	if head == nil || head.older != nil {
		t.Fatalf("history survived after snapshot close: %+v", head)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v3" {
		t.Fatalf("live value changed: %q %v", got, ok)
	}
}

func TestCompactKeepsDeletedKeyVisibleToSnapshot(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("d", []byte("alive")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if err := s.Delete("d"); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, ok := snap.Get("d"); !ok || string(got) != "alive" {
		t.Fatalf("compaction broke snapshot's deleted key: %q %v", got, ok)
	}
	if _, ok, _ := s.Get("d"); ok {
		t.Fatal("deleted key visible in live view")
	}
}

func TestCompactPersistsAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	vals := map[string]string{"a": "1", "b": "22", "c": ""}
	for k, v := range vals {
		if err := s.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ { // history to reclaim
		if err := s.Put("a", []byte(strings.Repeat("z", 32))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("gone", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		snap.Close()
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
	for k, v := range vals {
		got, ok, err := s2.Get(k)
		if !ok || err != nil || string(got) != v {
			t.Fatalf("after reopen %s: got=%q ok=%v err=%v want=%q", k, got, ok, err, v)
		}
	}
	if _, ok, _ := s2.Get("gone"); ok {
		t.Fatal("deletion lost across compacted reopen")
	}
	if n := s2.Stats().Snapshots; n != 5 {
		t.Fatalf("snapshot count reset by compaction: %d, want 5", n)
	}
	// The compacted log accepts new commits and survives another reopen.
	if err := s2.Put("post", []byte("compact")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s3.Get("post"); !ok || string(got) != "compact" {
		t.Fatalf("post-compaction commit lost: %q %v", got, ok)
	}
	if n := s3.Stats().Snapshots; n != 5 {
		t.Fatalf("snapshot count after second reopen: %d, want 5", n)
	}
	s3.Close()
}

func TestCompactOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("compact on closed store: err=%v, want fs.ErrClosed", err)
	}
}

func TestCompactConcurrentQueues(t *testing.T) {
	s := openStore(t, tempDir(t))

	const writers = 3
	const iterations = 100

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", w)
			for i := 0; i < iterations; i++ {
				if err := s.Put(key, []byte(strconv.Itoa(i))); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}

	// Repeated and concurrent compactions all succeed.
	const compactors = 3
	errs := make(chan error, compactors*10)
	var compactWg sync.WaitGroup
	for c := 0; c < compactors; c++ {
		compactWg.Add(1)
		go func() {
			defer compactWg.Done()
			for i := 0; i < 10; i++ {
				errs <- s.Compact()
			}
		}()
	}

	// Snapshots taken and read while compactions run keep consistent views.
	stop := make(chan struct{})
	var readWg sync.WaitGroup
	readWg.Add(1)
	go func() {
		defer readWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := s.Snapshot()
			if err != nil {
				t.Errorf("snapshot: %v", err)
				return
			}
			for w := 0; w < writers; w++ {
				key := fmt.Sprintf("k%d", w)
				v, ok := snap.Get(key)
				if !ok {
					continue
				}
				if _, err := strconv.Atoi(string(v)); err != nil {
					t.Errorf("malformed value %q", v)
				}
				v2, ok := snap.Get(key)
				if !ok || !bytes.Equal(v, v2) {
					t.Error("snapshot view changed across compaction")
				}
			}
			snap.Close()
		}
	}()

	wg.Wait()
	close(stop)
	readWg.Wait()
	compactWg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent compact: %v", err)
		}
	}

	// Final state is consistent and durable.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	for w := 0; w < writers; w++ {
		got, ok, _ := s.Get(fmt.Sprintf("k%d", w))
		if !ok || string(got) != strconv.Itoa(iterations-1) {
			t.Fatalf("k%d: got=%q ok=%v", w, got, ok)
		}
	}
}

func TestStaleScratchLogRemovedOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A compaction that died before its rename leaves the scratch behind.
	scratch := filepath.Join(dir, walCompactName)
	if err := os.WriteFile(scratch, []byte("partial compact"), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("open with stale scratch: %v", err)
	}
	if _, err := os.Stat(scratch); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("scratch file not cleaned up: %v", err)
	}
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("data lost: %q %v", got, ok)
	}
	if err := s2.Compact(); err != nil {
		t.Fatal(err)
	}
	s2.Close()
}

// TestCompactCrashMidway kills a child process at random points while it
// writes and compacts, then verifies the directory reopens to a complete
// committed state: every write the child reported as committed is present,
// the snapshot count never resets, and the store is usable immediately.
func TestCompactCrashMidway(t *testing.T) {
	if os.Getenv("SNAPSHOT_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}

	for round := 0; round < 4; round++ {
		dir := tempDir(t)
		progress := filepath.Join(dir, "progress")

		cmd := exec.Command(os.Args[0], "-test.run=TestCompactCrashMidway")
		cmd.Env = append(os.Environ(),
			"SNAPSHOT_CRASH_CHILD=1",
			"SNAPSHOT_CRASH_DIR="+dir,
			"SNAPSHOT_CRASH_PROGRESS="+progress,
		)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(100+round*150) * time.Millisecond)
		cmd.Process.Kill()
		cmd.Wait()

		// Highest iteration the child durably reported as committed.
		var committed int64 = -1
		if data, err := os.ReadFile(progress); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if line == "" {
					continue
				}
				n, err := strconv.ParseInt(line, 10, 64)
				if err != nil {
					t.Fatalf("corrupt progress line %q: %v", line, err)
				}
				if n > committed {
					committed = n
				}
			}
		}

		s, err := Open(dir)
		if err != nil {
			t.Fatalf("round %d: reopen after kill: %v", round, err)
		}
		for i := int64(0); i <= committed; i++ {
			key := fmt.Sprintf("k%d", i)
			got, ok, err := s.Get(key)
			if !ok || err != nil || !bytes.Equal(got, crashValue(i)) {
				t.Fatalf("round %d: committed write %s lost: ok=%v err=%v len=%d",
					round, key, ok, err, len(got))
			}
		}
		if n := s.Stats().Snapshots; committed >= 0 && n < uint64(committed+1) {
			t.Fatalf("round %d: snapshot count regressed: %d, want >= %d", round, n, committed+1)
		}
		// Reopen-and-use: compaction and new commits work right away.
		if err := s.Compact(); err != nil {
			t.Fatalf("round %d: compact after kill: %v", round, err)
		}
		if err := s.Put("after-crash", []byte("ok")); err != nil {
			t.Fatalf("round %d: put after kill: %v", round, err)
		}
		s.Close()

		s2, err := Open(dir)
		if err != nil {
			t.Fatalf("round %d: second reopen: %v", round, err)
		}
		if got, ok, _ := s2.Get("after-crash"); !ok || string(got) != "ok" {
			t.Fatalf("round %d: post-crash commit lost: %q %v", round, got, ok)
		}
		s2.Close()
	}
}

// crashValue is the deterministic value the crash child writes for iteration
// i: a readable marker plus enough bulk to give compaction real work.
func crashValue(i int64) []byte {
	v := append([]byte(fmt.Sprintf("v%d", i)), bytes.Repeat([]byte{'v'}, 4096)...)
	v[len(v)-1] = byte(i)
	return v
}

// runCrashChild hammers a store with commits, snapshots and compactions,
// logging each committed iteration to a progress file so the parent test can
// verify nothing committed was lost after the child is killed.
func runCrashChild() {
	dir := os.Getenv("SNAPSHOT_CRASH_DIR")
	progress := os.Getenv("SNAPSHOT_CRASH_PROGRESS")

	s, err := Open(dir)
	if err != nil {
		os.Exit(2)
	}
	pf, err := os.OpenFile(progress, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		os.Exit(2)
	}
	for i := int64(0); ; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), crashValue(i)); err != nil {
			os.Exit(2)
		}
		snap, err := s.Snapshot()
		if err != nil {
			os.Exit(2)
		}
		snap.Close()
		if err := s.Compact(); err != nil {
			os.Exit(2)
		}
		// Report the iteration only after everything above is durable.
		if _, err := fmt.Fprintf(pf, "%d\n", i); err != nil {
			os.Exit(2)
		}
		if err := pf.Sync(); err != nil {
			os.Exit(2)
		}
	}
}
