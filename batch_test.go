package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ---- basic atomicity ----

func TestCommitBatchAppliesAtomicallyInOrder(t *testing.T) {
	s := openStore(t, tempDir(t))

	if err := s.Put("keep", []byte("old")); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()

	ops := []BatchOp{
		{Key: "a", Value: []byte("one")},
		{Key: "b", Value: []byte("two")},
		{Key: "a", Value: []byte("three")}, // later touch wins inside the batch
		{Key: "keep", Value: []byte("new")},
		{Key: "gone", Value: []byte("x")},
		{Key: "gone", Delete: true}, // put then delete: ends deleted
		{Key: "del", Value: []byte("y")},
	}
	if err := s.Put("del", []byte("y")); err != nil {
		t.Fatal(err)
	}
	ops = append(ops, BatchOp{Key: "del", Delete: true})
	if err := s.CommitBatch(ops); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}

	after, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()

	// The fixed view taken before sees nothing of the batch.
	if _, ok := before.Get("a"); ok {
		t.Fatal("pre-batch snapshot sees batch key")
	}
	if got, ok := before.Get("keep"); !ok || string(got) != "old" {
		t.Fatalf("pre-batch snapshot changed: %q ok=%v", got, ok)
	}

	// The view after sees the whole batch, list order resolved.
	want := map[string]string{"a": "three", "b": "two", "keep": "new"}
	for k, v := range want {
		if got, ok := after.Get(k); !ok || string(got) != v {
			t.Fatalf("post-batch snapshot %s=%q ok=%v, want %q", k, got, ok, v)
		}
	}
	for _, k := range []string{"gone", "del"} {
		if _, ok := after.Get(k); ok {
			t.Fatalf("post-batch snapshot sees deleted key %s", k)
		}
	}
	if got, ok, _ := s.Get("a"); !ok || string(got) != "three" {
		t.Fatalf("latest a: %q ok=%v", got, ok)
	}
}

func TestCommitBatchDeleteThenPutWins(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	ops := []BatchOp{
		{Key: "k", Delete: true},
		{Key: "k", Value: []byte("v2")},
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("delete-then-put: %q ok=%v", got, ok)
	}
}

func TestCommitBatchEmptyAndNoOp(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	size := walSize(t, dir)
	if err := s.CommitBatch(nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if err := s.CommitBatch([]BatchOp{}); err != nil {
		t.Fatalf("empty slice batch: %v", err)
	}
	// Only idempotent deletes: commits nothing, log stays untouched.
	if err := s.CommitBatch([]BatchOp{
		{Key: "never", Delete: true},
		{Key: "gone", Delete: true},
	}); err != nil {
		t.Fatalf("noop batch: %v", err)
	}
	if got := walSize(t, dir); got != size {
		t.Fatalf("log grew on no-op batch: before=%d after=%d", size, got)
	}

	// A nil value is a stored empty value, still a hit.
	if err := s.CommitBatch([]BatchOp{{Key: "empty", Value: nil}}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.Get("empty"); !ok || err != nil || len(got) != 0 {
		t.Fatalf("nil value batch: %q ok=%v err=%v", got, ok, err)
	}
}

// ---- rejection ----

func TestCommitBatchEmptyKeyRejectsWholeBatch(t *testing.T) {
	for _, bad := range [][]BatchOp{
		{{Key: "", Value: []byte("x")}, {Key: "a", Value: []byte("y")}},
		{{Key: "a", Value: []byte("y")}, {Key: "", Value: []byte("x")}},
		{{Key: "a", Value: []byte("y")}, {Key: "", Delete: true}},
	} {
		t.Run("", func(t *testing.T) {
			dir := tempDir(t)
			s := openStore(t, dir)
			if err := s.Put("keep", []byte("v")); err != nil {
				t.Fatal(err)
			}
			before := walSize(t, dir)

			err := s.CommitBatch(bad)
			if !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("err=%v, want fs.ErrInvalid", err)
			}
			if _, ok, _ := s.Get("a"); ok {
				t.Fatal("sibling change of rejected batch became visible")
			}
			if got := walSize(t, dir); got != before {
				t.Fatalf("rejected batch wrote data: before=%d after=%d", before, got)
			}
			if got, ok, _ := s.Get("keep"); !ok || string(got) != "v" {
				t.Fatalf("state changed by rejected batch: %q ok=%v", got, ok)
			}
			s.Close()

			s2, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			if _, ok, _ := s2.Get("a"); ok {
				t.Fatal("rejected batch survived on disk")
			}
		})
	}
}

func TestCommitBatchOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err := s.CommitBatch([]BatchOp{{Key: "a", Value: []byte("x")}})
	if !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("err=%v, want fs.ErrClosed", err)
	}
}

func TestOpenRegularFileStillInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open(file) err=%v, want fs.ErrInvalid", err)
	}
}

// ---- durability and crash atomicity ----

func TestCommitBatchDurableAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("22")},
		{Key: "c", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("c", []byte("live")); err != nil {
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
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("a after reopen: %q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("b"); !ok || string(got) != "22" {
		t.Fatalf("b after reopen: %q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("c"); !ok || string(got) != "live" {
		t.Fatalf("c after reopen: %q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count after reopen: %d, want 1", n)
	}
}

// appendRawFrames writes frames straight to the wal end, bypassing the
// commit path, so a crash landing at any point of a batch can be simulated.
func appendRawFrames(t *testing.T, dir string, fn func(l *walWriter)) {
	t.Helper()
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	l := &walWriter{f: f, w: bufio.NewWriter(f)}
	fn(l)
	if err := l.w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitBatchTornTailReopensWholeOrNothing(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	// A fully committed batch that must survive.
	if err := s.CommitBatch([]BatchOp{
		{Key: "committed", Value: []byte("yes")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Killed mid-batch: begin plus one inner frame, no end marker.
	appendRawFrames(t, dir, func(l *walWriter) {
		if err := l.encodeFrame(opBatchBegin, "", nil); err != nil {
			t.Fatal(err)
		}
		if err := l.encodeFrameSeq(opPutSeq, "torn", 99, []byte("no")); err != nil {
			t.Fatal(err)
		}
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("committed"); !ok || string(got) != "yes" {
		t.Fatalf("committed batch lost: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("torn"); ok {
		t.Fatal("half-batch visible after reopen")
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count lost across torn batch: %d", n)
	}

	// The torn tail must have been truncated, so new commits append cleanly.
	if err := s2.CommitBatch([]BatchOp{{Key: "after", Value: []byte("ok")}}); err != nil {
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
	if got, ok, _ := s3.Get("after"); !ok || string(got) != "ok" {
		t.Fatalf("post-recovery batch lost: %q ok=%v", got, ok)
	}
	if _, ok, _ := s3.Get("torn"); ok {
		t.Fatal("torn batch reappeared after a later commit")
	}
}

func TestCommitBatchMiscountEndDiscardsBatch(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("old", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// begin + one inner frame + end claiming two inners: not an intact batch.
	var endVal [metaValueSize]byte
	binary.LittleEndian.PutUint64(endVal[:], 2)
	appendRawFrames(t, dir, func(l *walWriter) {
		if err := l.encodeFrame(opBatchBegin, "", nil); err != nil {
			t.Fatal(err)
		}
		if err := l.encodeFrameSeq(opPutSeq, "bad", 50, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := l.encodeFrame(opBatchEnd, "", endVal[:]); err != nil {
			t.Fatal(err)
		}
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("old"); !ok || string(got) != "v0" {
		t.Fatalf("prefix before bad batch lost: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("bad"); ok {
		t.Fatal("count-mismatched batch was applied")
	}
}

func TestCommitBatchFramesOnDisk(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.CommitBatch([]BatchOp{
		{Key: "k", Value: []byte("v")},
		{Key: "k", Value: []byte("w")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	// A frame begins with the little-endian recordMagic 50 41 4e 53 followed
	// immediately by its op byte.
	prefix := func(op byte) []byte { return []byte{0x50, 0x41, 0x4e, 0x53, op} }
	if !bytes.Contains(data, prefix(opBatchBegin)) || !bytes.Contains(data, prefix(opBatchEnd)) {
		t.Fatal("wal does not contain batch framing markers")
	}
	if bytes.Count(data, prefix(opBatchBegin)) != 1 || bytes.Count(data, prefix(opBatchEnd)) != 1 {
		t.Fatal("wal does not contain exactly one begin and one end marker")
	}
}

// ---- snapshots, cursors and compaction ----

func TestCommitBatchSnapshotCursorVisibility(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("pre", []byte("0")); err != nil {
		t.Fatal(err)
	}
	cursor, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}); err != nil {
		t.Fatal(err)
	}

	// The pre-batch cursor pages only the pre-batch view.
	pages, err := cursor.Page(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 1 || pages[0].Key != "pre" {
		t.Fatalf("pinned cursor saw the batch: %+v", pages)
	}
	if _, ok := snap.Get("a"); ok {
		t.Fatal("pre-batch snapshot saw the batch")
	}
	cursor.Close()
	snap.Close()

	// A cursor opened after sees the whole batch.
	c2, err := s.OpenCursorPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	all, err := c2.Page(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("new cursor pages %d pairs, want 3: %+v", len(all), all)
	}
}

func TestCommitBatchCompactionKeepsIntraBatchOrder(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	// This cursor pins v1; the batch then touches k three times. Its middle
	// value was never visible to any whole-batch view and is reclaimable, but
	// the retained pair (pinned v1, post-batch head v3) must survive the log
	// rewrite linked oldest-to-newest in the right order.
	cursor, err := s.OpenCursorPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "k", Value: make([]byte, 4096)},
		{Key: "k", Value: []byte("middle")},
		{Key: "k", Value: []byte("v3")},
		{Key: "other", Value: []byte("o1")},
		{Key: "other", Value: []byte("o2")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact after batch: %v", err)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v3" {
		t.Fatalf("k after compact: %q ok=%v", got, ok)
	}
	if got, ok, _ := s.Get("other"); !ok || string(got) != "o2" {
		t.Fatalf("other after compact: %q ok=%v", got, ok)
	}
	pinned, err := cursor.Page(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pinned) != 1 || string(pinned[0].Value) != "v1" {
		t.Fatalf("pinned cursor changed: %+v", pinned)
	}
	cursor.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Replay the compacted log: retained per-key histories must survive in
	// ascending sequence order, with no seq duplicated or reordered.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "v3" {
		t.Fatalf("k after reopen: %q ok=%v", got, ok)
	}
	chains, values := replayChains(t, dir)
	if seqs := chains["k"]; len(seqs) != 2 {
		t.Fatalf("k retained history after compaction: %v, want 2 versions", seqs)
	} else if seqs[0] >= seqs[1] {
		t.Fatalf("batch versions split or reordered in compacted log: %v", seqs)
	}
	if values[keySeq{"k", chains["k"][0]}] != "v1" || values[keySeq{"k", chains["k"][1]}] != "v3" {
		t.Fatalf("retained values mismatch: %+v", values)
	}
}

type keySeq struct {
	key string
	seq uint64
}

func replayChains(t *testing.T, dir string) (map[string][]uint64, map[keySeq]string) {
	t.Helper()
	chains := make(map[string][]uint64)
	values := make(map[keySeq]string)
	seen := make(map[uint64]bool)
	_, _, err := replayWAL(dir, func(op byte, key string, value []byte, seq uint64) {
		switch op {
		case opPutSeq, opDeleteSeq:
			if seen[seq] {
				t.Fatalf("duplicate seq %d on key %q during replay", seq, key)
			}
			seen[seq] = true
			chains[key] = append(chains[key], seq)
			if op == opPutSeq {
				values[keySeq{key, seq}] = string(value)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return chains, values
}

func TestCommitBatchCursorPinnedHistoryReclaimedAfterClose(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	big := bytes.Repeat([]byte("Q"), 8192)
	if err := s.Put("k", big); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursorPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "k", Value: []byte("tiny")},
		{Key: "k2", Value: big},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact while cursor pinned: %v", err)
	}
	pages, err := c.Page(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 1 || string(pages[0].Value) != string(big) {
		t.Fatalf("pinned cursor changed across compaction: %+v", pages)
	}
	pinned := dirBytes(t, dir)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact after cursor close: %v", err)
	}
	if after := dirBytes(t, dir); after >= pinned {
		t.Fatalf("pinned history not reclaimed after cursor close: %d -> %d", pinned, after)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "tiny" {
		t.Fatalf("latest k: %q ok=%v", got, ok)
	}
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() {
			total += info.Size()
		}
	}
	return total
}

// ---- concurrency ----

func queueLen(s *Store) int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.queue)
}

func walSyncs(s *Store) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return -1
	}
	return s.wal.syncs
}

func TestCommitBatchGroupCommitSharesOneSync(t *testing.T) {
	s := openStore(t, tempDir(t))

	const followers = 7
	proceed := make(chan struct{})
	var leaderWG sync.WaitGroup
	leaderWG.Add(1)
	s.leaderHook = func() {
		leaderWG.Done()
		<-proceed
	}
	defer func() { s.leaderHook = nil }()

	leaderErr := make(chan error, 1)
	go func() {
		leaderErr <- s.CommitBatch([]BatchOp{{Key: "leader", Value: []byte("L")}})
	}()
	leaderWG.Wait() // the leader is elected and parked before its first drain

	var wg sync.WaitGroup
	for i := 0; i < followers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.CommitBatch([]BatchOp{{Key: fmt.Sprintf("f%d", i), Value: []byte("F")}}); err != nil {
				t.Errorf("follower %d: %v", i, err)
			}
		}(i)
	}

	// Wait until every follower is queued behind the parked leader.
	deadline := time.Now()
	for queueLen(s) != followers+1 {
		if time.Since(deadline) > 5*time.Second {
			t.Fatalf("followers did not coalesce: queue=%d", queueLen(s))
		}
		runtime.Gosched()
	}
	before := walSyncs(s)
	close(proceed)

	if err := <-leaderErr; err != nil {
		t.Fatalf("leader: %v", err)
	}
	wg.Wait()
	after := walSyncs(s)

	// All 8 batches returned, one view per batch committed, but exactly one
	// durable force covered the whole coalesced group.
	if after-before != 1 {
		t.Fatalf("group commit used %d syncs for %d batches, want 1", after-before, followers+1)
	}
	if got, ok, _ := s.Get("leader"); !ok || string(got) != "L" {
		t.Fatalf("leader value: %q ok=%v", got, ok)
	}
	for i := 0; i < followers; i++ {
		if got, ok, _ := s.Get(fmt.Sprintf("f%d", i)); !ok || string(got) != "F" {
			t.Fatalf("follower %d value: %q ok=%v", i, got, ok)
		}
	}
}

func TestCommitBatchConcurrent(t *testing.T) {
	s := openStore(t, tempDir(t))

	const n = 16
	const rounds = 50
	var wg sync.WaitGroup
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				err := s.CommitBatch([]BatchOp{
					{Key: fmt.Sprintf("priv-%d", g), Value: []byte(fmt.Sprintf("x%d", r))},
					{Key: "shared", Value: []byte(fmt.Sprintf("%d-%d", g, r))},
					{Key: fmt.Sprintf("priv-%d", g), Value: []byte("y")},
				})
				if err != nil {
					t.Errorf("batch: %v", err)
					return
				}
			}
		}(g)
	}
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if err := s.Compact(); err != nil {
					t.Errorf("compact: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Every per-goroutine key ends at its last value; shared lands on exactly
	// one committed value.
	for g := 0; g < n; g++ {
		if got, ok, _ := s.Get(fmt.Sprintf("priv-%d", g)); !ok || string(got) != "y" {
			t.Fatalf("priv-%d: %q ok=%v", g, got, ok)
		}
	}
	if got, ok, _ := s.Get("shared"); !ok || len(got) == 0 {
		t.Fatalf("shared missing after concurrent batches: ok=%v", ok)
	}
	if st := s.Stats(); st.Keys != n+1 {
		t.Fatalf("stats keys=%d, want %d", st.Keys, n+1)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("repeated compact: %v", err)
	}
}

func TestCommitBatchConcurrentDurability(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	const n = 12
	const rounds = 40
	var wg sync.WaitGroup
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if err := s.CommitBatch([]BatchOp{
					{Key: fmt.Sprintf("k%d", g%4), Value: []byte(fmt.Sprintf("%d:%d", g, r))},
					{Key: fmt.Sprintf("u%d", g), Value: []byte(fmt.Sprintf("%d", r))},
				}); err != nil {
					t.Errorf("batch: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	memBefore := s.Stats()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Stats(); got != memBefore {
		t.Fatalf("stats differ after reopen: %+v want %+v", got, memBefore)
	}
	for g := 0; g < n; g++ {
		if got, ok, _ := s2.Get(fmt.Sprintf("u%d", g)); !ok || string(got) != fmt.Sprintf("%d", rounds-1) {
			t.Fatalf("u%d after reopen: %q ok=%v", g, got, ok)
		}
	}
	// No sequence may appear twice anywhere in the replayed history.
	chains, _ := replayChains(t, dir)
	for k, seqs := range chains {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("key %s chain out of order after reopen: %v", k, seqs)
			}
		}
	}
}

// ---- stats comparison ----

func TestCommitBatchStatsBeforeAfter(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("aaaa")); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if err := s.CommitBatch([]BatchOp{
		{Key: "b", Value: []byte("bb")},
		{Key: "c", Value: []byte("cc")},
		{Key: "a", Value: []byte("aaaaaa")},
		{Key: "d", Value: []byte("x")},
		{Key: "d", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if after.Keys != before.Keys+2 {
		t.Fatalf("keys: before=%d after=%d", before.Keys, after.Keys)
	}
	// (6+2+2)/3 = 3.333... -> 3.33
	if after.AvgBytes != 3.33 {
		t.Fatalf("avg bytes: %v, want 3.33", after.AvgBytes)
	}
	if after.Snapshots != before.Snapshots {
		t.Fatal("commit batch changed snapshot count")
	}
}
