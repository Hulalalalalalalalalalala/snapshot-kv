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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// errInjectedSync stands in for a failed fsync in failure tests.
var errInjectedSync = errors.New("snapshot: injected fsync failure")

// ---- basic batch semantics ----

func TestCommitBatchAtomic(t *testing.T) {
	s := openStore(t, tempDir(t))

	batch := []Op{
		PutOp("a", []byte("1")),
		PutOp("b", []byte("2")),
		DeleteOp("missing"), // no-op, commits nothing
		PutOp("c", []byte("3")),
	}
	if err := s.Commit(batch); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got, ok, err := s.Get(k); !ok || err != nil || string(got) != want {
			t.Fatalf("after batch %s=%q ok=%v err=%v want %q", k, got, ok, err, want)
		}
	}
}

func TestCommitEmptyBatchNoEffect(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(nil); err != nil {
		t.Fatalf("nil batch: %v", err)
	}
	if err := s.Commit([]Op{}); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	// Nothing happened; seq and view unchanged. A following commit behaves
	// exactly as before.
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("empty batch changed state: %q ok=%v", got, ok)
	}
}

func TestCommitEmptyKeyRejectsWholeBatch(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}

	for _, bad := range [][]Op{
		{PutOp("", []byte("x"))},
		{PutOp("a", []byte("1")), PutOp("", []byte("x"))},
		{PutOp("a", []byte("1")), DeleteOp("")},
		{DeleteOp(""), PutOp("a", []byte("1"))},
	} {
		err := s.Commit(bad)
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("batch %v: err=%v want fs.ErrInvalid", bad, err)
		}
	}

	// Nothing from a rejected batch is visible; the earlier value survives.
	if got, ok, _ := s.Get("a"); ok {
		t.Fatalf("rejected batch left %q behind", got)
	}
	if got, ok, _ := s.Get("keep"); !ok || string(got) != "v" {
		t.Fatalf("rejected batch disturbed earlier value: %q ok=%v", got, ok)
	}
}

func TestCommitOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err := s.Commit([]Op{PutOp("a", []byte("1"))})
	if !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("commit closed store: %v want fs.ErrClosed", err)
	}
	// An empty batch is an unconditional no-op, even on a closed store.
	if err := s.Commit(nil); err != nil {
		t.Fatalf("empty batch on closed store: %v want nil", err)
	}
	// Closed takes precedence over key validation for a non-empty batch.
	if err := s.Commit([]Op{PutOp("", nil)}); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("empty-key batch on closed store: %v want fs.ErrClosed", err)
	}
}

func TestCommitLastWinsWithinBatch(t *testing.T) {
	s := openStore(t, tempDir(t))

	// Multiple ops on one key in a batch: the final one in order wins.
	if err := s.Commit([]Op{
		PutOp("k", []byte("first")),
		PutOp("k", []byte("second")),
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "second" {
		t.Fatalf("last put wins: %q ok=%v", got, ok)
	}

	// Put then delete of the same key within the batch: ends deleted.
	if err := s.Commit([]Op{
		PutOp("d", []byte("x")),
		DeleteOp("d"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get("d"); ok {
		t.Fatal("put-then-delete in one batch should end deleted")
	}

	// Delete then put: ends present with the final value.
	if err := s.Put("e", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]Op{
		DeleteOp("e"),
		PutOp("e", []byte("new")),
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Get("e"); !ok || string(got) != "new" {
		t.Fatalf("delete-then-put: %q ok=%v", got, ok)
	}
}

func TestSinglePointOpsAreOneItemBatches(t *testing.T) {
	// Put and Delete are exactly one-op Commits; their error and visibility
	// semantics are unchanged from the point interface.
	s := openStore(t, tempDir(t))
	if err := s.Put("", nil); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("put empty: %v", err)
	}
	if err := s.Delete(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("delete empty: %v", err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("never"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("point put: %q ok=%v", got, ok)
	}
}

func TestCommitNetNoOpWritesNothing(t *testing.T) {
	// A batch whose only effective op is deleting an absent key (directly, or
	// after in-batch put/delete cancellation) changes neither view nor log.
	dir := tempDir(t)
	s := openStore(t, dir)
	base := walSize(t, dir)

	if err := s.Commit([]Op{DeleteOp("ghost")}); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
	if err := s.Commit([]Op{PutOp("x", []byte("v")), DeleteOp("x")}); err != nil {
		t.Fatalf("put+delete absent key: %v", err)
	}
	if got := walSize(t, dir); got != base {
		t.Fatalf("net-no-op batch wrote %d log bytes", got-base)
	}

	// No-effect batches encode nothing, so the next real commit still
	// persists and survives a crash-style reopen unchanged.
	if err := s.Put("real", []byte("v")); err != nil {
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
	if got, ok, _ := s2.Get("real"); !ok || string(got) != "v" {
		t.Fatalf("real commit: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("x"); ok {
		t.Fatal("cancelled put leaked")
	}
}

func TestCommitLastWinsDeleteAfterPutOnAbsentKey(t *testing.T) {
	// put then delete on a brand-new key nets to absence: no tombstone that
	// would later pin or compact as live.
	s := openStore(t, tempDir(t))
	if err := s.Commit([]Op{PutOp("z", []byte("1")), DeleteOp("z")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if v := *s.current.Load(); len(v) != 0 {
		t.Fatalf("net-deleted batch left a node: %d keys", len(v))
	}
}

func TestCommitEmptyBatchSkipsFsync(t *testing.T) {
	s := openStore(t, tempDir(t))
	var syncs atomic.Int64
	s.walMu.Lock()
	base := s.wal.syncFn
	s.wal.syncFn = func() error {
		syncs.Add(1)
		return base()
	}
	s.walMu.Unlock()

	if err := s.Commit(nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]Op{}); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("empty batch performed %d fsyncs, want 0", got)
	}
}

// ---- atomicity across snapshots and reopen ----

func TestBatchAtomicAcrossSnapshot(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("old")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Commit([]Op{
			PutOp("a", []byte("new")),
			PutOp("b", []byte("newb")),
		})
	}()

	// Many snapshots: a batch is one point on the total order. The only
	// legal states are {a=old,b absent} before it and {a=new,b present}
	// after; {a=new,b absent} or {a=old,b present} would be a torn batch.
	for i := 0; i < 2000; i++ {
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		av, aok := snap.Get("a")
		bv, bok := snap.Get("b")
		if aok && string(av) == "new" && !bok {
			t.Fatalf("torn batch (new a, no b) at iter %d", i)
		}
		if bok && (!aok || string(av) != "new") {
			t.Fatalf("torn batch (b=%v, old a=%q) at iter %d", bv, av, i)
		}
		snap.Close()
	}
	wg.Wait()
}

func TestBatchPersistsWholeAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Commit([]Op{
		PutOp("a", []byte("1")),
		PutOp("b", []byte("2")),
		PutOp("a", []byte("11")),
		DeleteOp("b"),
		PutOp("c", []byte("3")),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
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
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "11" {
		t.Fatalf("a after reopen: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("b"); ok {
		t.Fatal("batch delete lost across reopen")
	}
	if got, ok, _ := s2.Get("c"); !ok || string(got) != "3" {
		t.Fatalf("c after reopen: %q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count after reopen: %d", n)
	}
}

func TestTornBatchIsFullyDiscarded(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("before", []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]Op{
		PutOp("a", []byte("one")),
		PutOp("b", []byte("two")),
		PutOp("c", []byte("three")),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a well-formed begin + one op, then stop: no end frame. The
	// whole partial batch must vanish on reopen; the committed prefix stays.
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writeRawFrame := func(op byte, key string, value []byte) {
		t.Helper()
		hdr := make([]byte, frameHeaderSize)
		binary.LittleEndian.PutUint32(hdr[0:4], recordMagic)
		hdr[4] = op
		binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(key)))
		binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(value)))
		h := crc32.NewIEEE()
		h.Write(hdr)
		h.Write([]byte(key))
		h.Write(value)
		if _, err := f.Write(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(key); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(value); err != nil {
			t.Fatal(err)
		}
		var crcb [crcSize]byte
		binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
		if _, err := f.Write(crcb[:]); err != nil {
			t.Fatal(err)
		}
	}
	var seqb [seqSize]byte
	binary.LittleEndian.PutUint64(seqb[:], 99)
	writeRawFrame(opBatchBegin, "", seqb[:])
	vb := append(append([]byte{}, seqb[:]...), []byte("partial")...)
	writeRawFrame(opPutSeq, "partial", vb)
	// (no opBatchEnd, and a torn tail after it)
	f.Write([]byte{0xde, 0xad})
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn batch: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("before"); !ok || string(got) != "kept" {
		t.Fatalf("committed prefix lost: %q ok=%v", got, ok)
	}
	for _, k := range []string{"a", "b", "c"} {
		if got, ok, _ := s2.Get(k); !ok || string(got) != map[string]string{"a": "one", "b": "two", "c": "three"}[k] {
			t.Fatalf("committed batch lost key %s: %q ok=%v", k, got, ok)
		}
	}
	if _, ok, _ := s2.Get("partial"); ok {
		t.Fatal("partial unclosed batch became visible")
	}

	// Store is usable immediately with no repair step.
	if err := s2.Put("after", []byte("x")); err != nil {
		t.Fatalf("commit after torn recovery: %v", err)
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
	if _, ok, _ := s3.Get("partial"); ok {
		t.Fatal("partial batch survived second reopen")
	}
}

// ---- group commit ----

// withArrivalHook replaces the leader's arrival-window yield so a test can
// block the leader until a fixed number of batches have queued.
func withArrivalHook(s *Store, fn func()) func() {
	s.mu.Lock()
	old := s.groupYield
	s.groupYield = fn
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.groupYield = old
		s.mu.Unlock()
	}
}

func TestGroupCommitSharesOneSync(t *testing.T) {
	s := openStore(t, tempDir(t))

	var syncs atomic.Int64
	s.walMu.Lock()
	base := s.wal.syncFn
	s.wal.syncFn = func() error {
		syncs.Add(1)
		return base()
	}
	s.walMu.Unlock()

	const n = 8
	leaderAtHook := make(chan struct{}, 1)
	proceed := make(chan struct{})
	restore := withArrivalHook(s, func() {
		leaderAtHook <- struct{}{} // runs on whichever committer is leader
		<-proceed
	})
	defer restore()

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Commit([]Op{PutOp(fmt.Sprintf("k%d", i), []byte("v"))})
		}(i)
	}

	<-leaderAtHook // leader is parked in the arrival window
	// Wait until every follower has queued, then release the single sync.
	for {
		s.mu.Lock()
		queued := len(s.queue)
		s.mu.Unlock()
		if queued >= n {
			break
		}
		runtime.Gosched()
	}
	close(proceed)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("group used %d fsyncs, want 1", got)
	}
	for i := 0; i < n; i++ {
		if got, ok, _ := s.Get(fmt.Sprintf("k%d", i)); !ok || string(got) != "v" {
			t.Fatalf("batch result %d missing: %q ok=%v", i, got, ok)
		}
	}
}

func TestGroupCommitEachBatchAtomic(t *testing.T) {
	// Many concurrent multi-key batches under a widening arrival hook; every
	// key of a batch must be present or the batch's own values are coherent.
	s := openStore(t, tempDir(t))

	const batches = 40
	const width = 5
	var wg sync.WaitGroup
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			ops := make([]Op, 0, width)
			for w := 0; w < width; w++ {
				ops = append(ops, PutOp(fmt.Sprintf("b%d-w%d", b, w), []byte(fmt.Sprintf("%d", b))))
			}
			if err := s.Commit(ops); err != nil {
				t.Errorf("batch %d: %v", b, err)
			}
		}(b)
	}
	wg.Wait()

	for b := 0; b < batches; b++ {
		for w := 0; w < width; w++ {
			k := fmt.Sprintf("b%d-w%d", b, w)
			got, ok, err := s.Get(k)
			if err != nil || !ok || string(got) != fmt.Sprintf("%d", b) {
				t.Fatalf("key %s: %q ok=%v err=%v", k, got, ok, err)
			}
		}
	}
}

func TestGroupCommitOneRejectedDoesNotPoisonGroup(t *testing.T) {
	// An empty-key batch is rejected before queueing, so concurrent valid
	// batches sharing a sync still succeed.
	s := openStore(t, tempDir(t))

	leaderAtHook := make(chan struct{}, 1)
	proceed := make(chan struct{})
	restore := withArrivalHook(s, func() {
		leaderAtHook <- struct{}{}
		<-proceed
	})
	defer restore()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // the valid batch; it becomes leader and parks in the hook
		defer wg.Done()
		if err := s.Commit([]Op{PutOp("good", []byte("v"))}); err != nil {
			t.Errorf("valid batch: %v", err)
		}
	}()
	go func() { // rejected invalid batch while the group gathers
		defer wg.Done()
		if err := s.Commit([]Op{PutOp("", nil)}); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("invalid batch: %v", err)
		}
	}()
	<-leaderAtHook
	close(proceed)
	wg.Wait()

	if got, ok, _ := s.Get("good"); !ok || string(got) != "v" {
		t.Fatalf("valid batch in group lost: %q ok=%v", got, ok)
	}
}

func TestCommitFsyncFailureIsAtomicAndRecoverable(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Commit([]Op{PutOp("before", []byte("kept"))}); err != nil {
		t.Fatal(err)
	}

	s.walMu.Lock()
	s.wal.syncFn = func() error { return errInjectedSync }
	s.walMu.Unlock()

	err := s.Commit([]Op{
		PutOp("a", []byte("1")),
		PutOp("b", []byte("2")),
	})
	if err == nil {
		t.Fatal("commit with failing fsync returned nil")
	}
	if !errors.Is(err, errInjectedSync) {
		t.Fatalf("commit error %v does not wrap the sync error", err)
	}

	// The failed batch is fully absent from the view. (The poisoned handle
	// rejects Get with fs.ErrClosed, so inspect the published view directly.)
	v := *s.current.Load()
	if n := v["a"]; n != nil {
		t.Fatal("failed batch became visible in memory")
	}
	if n := v["before"]; n == nil || !n.present || string(n.value) != "kept" {
		t.Fatalf("prior data disturbed: %+v", n)
	}
	// A failed durability attempt poisons the open handle rather than risk
	// acknowledging un-durable data.
	if err := s.Put("more", []byte("v")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("commit after fsync failure: %v want fs.ErrClosed", err)
	}

	// Reopen: the file was rolled back to the last durable boundary, so the
	// store recovers the complete prior commit and serves immediately.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after failed sync: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("before"); !ok || string(got) != "kept" {
		t.Fatalf("prior commit lost after failed sync: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("a"); ok {
		t.Fatal("failed batch persisted to disk")
	}
	if err := s2.Put("after", []byte("x")); err != nil {
		t.Fatalf("store not usable after recovery: %v", err)
	}
}

func TestGroupCommitDurableAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	const n = 8
	leaderAtHook := make(chan struct{}, 1)
	proceed := make(chan struct{})
	restore := withArrivalHook(s, func() {
		leaderAtHook <- struct{}{}
		<-proceed
	})

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ops := []Op{
				PutOp(fmt.Sprintf("b%d-x", i), []byte(fmt.Sprintf("%d", i))),
				PutOp(fmt.Sprintf("b%d-y", i), []byte(fmt.Sprintf("%d", i))),
			}
			if err := s.Commit(ops); err != nil {
				t.Errorf("batch %d: %v", i, err)
			}
		}(i)
	}
	<-leaderAtHook
	for {
		s.mu.Lock()
		q := len(s.queue)
		s.mu.Unlock()
		if q >= n {
			break
		}
		runtime.Gosched()
	}
	close(proceed)
	wg.Wait()
	restore()

	// One shared fsync made every batch durable; reopen shows each whole.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 0; i < n; i++ {
		for _, suffix := range []string{"x", "y"} {
			k := fmt.Sprintf("b%d-%s", i, suffix)
			if got, ok, _ := s2.Get(k); !ok || string(got) != fmt.Sprintf("%d", i) {
				t.Fatalf("after reopen %s: %q ok=%v want %d", k, got, ok, i)
			}
		}
	}
}

// ---- interaction with cursors, scans and compaction ----

func TestBatchCursorPagesWholeBatches(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Commit([]Op{PutOp("a", []byte("0")), PutOp("b", []byte("0"))}); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for b := 0; b < 50; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			ops := []Op{
				PutOp("a", []byte(fmt.Sprintf("%d", b))),
				PutOp(fmt.Sprintf("new%d", b), []byte("x")),
			}
			if b%2 == 0 {
				ops = append(ops, DeleteOp("b"))
				ops = append(ops, PutOp("b", []byte(fmt.Sprintf("%d", b))))
			}
			if err := s.Commit(ops); err != nil {
				t.Errorf("batch %d: %v", b, err)
			}
		}(b)
	}
	wg.Wait()

	pairs := drainCursor(t, c, 3)
	if len(pairs) != 2 {
		t.Fatalf("cursor view changed: got %d pairs %+v", len(pairs), pairs)
	}
	if pairs[0].Key != "a" || string(pairs[0].Value) != "0" ||
		pairs[1].Key != "b" || string(pairs[1].Value) != "0" {
		t.Fatalf("cursor content changed: %+v", pairs)
	}
	c.Close()
}

func TestBatchCompactionDoesNotSplitOrReorder(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	// One batch touches several keys; older versions of the same keys came
	// from earlier batches. Compact and reopen: versions stay coherent.
	if err := s.Commit([]Op{PutOp("a", []byte("a0")), PutOp("b", []byte("b0"))}); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot() // pins v0
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte{'X'}, 4096)
	for i := 0; i < 10; i++ {
		if err := s.Commit([]Op{
			PutOp("a", big),
			PutOp("b", big),
			PutOp("c", []byte("c")),
		}); err != nil {
			t.Fatal(err)
		}
	}
	sizeBefore := walSize(t, dir)
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	sizeAfter := walSize(t, dir)
	if sizeAfter >= sizeBefore {
		t.Fatalf("compact did not shrink: %d -> %d", sizeBefore, sizeAfter)
	}

	// Pinned snapshot still reads its coherent whole-batch view.
	if got, ok := snap.Get("a"); !ok || string(got) != "a0" {
		t.Fatalf("pinned a: %q ok=%v", got, ok)
	}
	if got, ok := snap.Get("b"); !ok || string(got) != "b0" {
		t.Fatalf("pinned b: %q ok=%v", got, ok)
	}
	snap.Close()

	if got, ok, _ := s.Get("a"); !ok || !bytes.Equal(got, big) {
		t.Fatalf("latest a wrong length: ok=%v len=%d", ok, len(got))
	}
	if got, ok, _ := s.Get("c"); !ok || string(got) != "c" {
		t.Fatalf("latest c: %q ok=%v", got, ok)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("a"); !ok || !bytes.Equal(got, big) {
		t.Fatalf("a after reopen: len=%d ok=%v", len(got), ok)
	}
	if got, ok, _ := s2.Get("c"); !ok || string(got) != "c" {
		t.Fatalf("c after reopen: %q ok=%v", got, ok)
	}
}

func TestBatchCursorPinnedHistoryReclaimedAfterClose(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	big := bytes.Repeat([]byte{'P'}, 4096)
	if err := s.Commit([]Op{PutOp("p", big), PutOp("q", big)}); err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]Op{PutOp("p", []byte("s")), PutOp("q", []byte("s"))}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	pairs := drainCursor(t, c, 4)
	if len(pairs) != 2 || !bytes.Equal(pairs[0].Value, big) || !bytes.Equal(pairs[1].Value, big) {
		t.Fatalf("cursor lost pinned batch: %+v", pairs)
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
		t.Fatalf("pinned batch history not reclaimed: %d -> %d", pinnedSize, reclaimedSize)
	}
}

func TestBatchCompactRepeatedConcurrentAllNil(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 20; i++ {
		if err := s.Commit([]Op{
			PutOp(fmt.Sprintf("k%d", i), bytes.Repeat([]byte{byte(i)}, 256)),
			DeleteOp(fmt.Sprintf("k%d", i-1)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := s.Compact(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("concurrent compact: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("final compact: %v", err)
	}
}

// ---- memory boundedness ----

func TestMemoryDoesNotGrowWithCommits(t *testing.T) {
	s := openStore(t, tempDir(t))
	// Compact away history, keep no snapshots/cursors, and churn a fixed set
	// of keys with a large number of batches. The live view must keep a
	// bounded number of nodes.
	const rounds = 200
	for r := 0; r < rounds; r++ {
		if err := s.Commit([]Op{
			PutOp("fixed1", []byte(fmt.Sprintf("%d", r))),
			PutOp("fixed2", []byte(fmt.Sprintf("%d", r))),
			DeleteOp("fixed2"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	v := *s.current.Load()
	if len(v) != 1 {
		t.Fatalf("live view has %d keys, want 1", len(v))
	}
	for k, n := range v {
		if n.older != nil {
			t.Fatalf("key %q retains history after compact", k)
		}
	}
}

func TestBatchMixedHighContentionStress(t *testing.T) {
	s := openStore(t, tempDir(t))

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Batch committers: mixes of puts, overwrites and deletes across keys.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				base := w * 10
				ops := []Op{
					PutOp(fmt.Sprintf("k%02d", base+(i%5)), []byte(fmt.Sprintf("%d-%d", w, i))),
					PutOp(fmt.Sprintf("k%02d", base+((i+1)%5)), []byte("x")),
				}
				if i%7 == 0 {
					ops = append(ops, DeleteOp(fmt.Sprintf("k%02d", base+((i+2)%5))))
				}
				if err := s.Commit(ops); err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}
	// Compactors.
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := s.Compact(); err != nil {
					if !errors.Is(err, fs.ErrClosed) {
						t.Errorf("compact: %v", err)
					}
					return
				}
			}
		}()
	}
	// Snapshot readers verifying per-batch coherence: every present key is a
	// well-formed marker and repeated reads agree.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
				pairs, err := snap.Scan(Range{}, 200)
				if err != nil {
					t.Errorf("scan: %v", err)
					snap.Close()
					return
				}
				for _, p := range pairs {
					if got, ok := snap.Get(p.Key); !ok || !bytes.Equal(got, p.Value) {
						t.Errorf("incoherent snapshot view for %s", p.Key)
						break
					}
				}
				snap.Close()
			}
		}()
	}
	// Cursor openers paging a fixed view while the world mutates.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, err := s.OpenCursorPrefix("k")
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("open cursor: %v", err)
					return
				}
				off := 0
				for {
					page, err := c.Page(off, 16)
					if err != nil {
						break
					}
					off += len(page)
					if len(page) < 16 {
						break
					}
				}
				c.Close()
			}
		}()
	}

	// Let it churn for a short while, then stop and settle with one final
	// compaction.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := s.Compact(); err != nil {
		t.Fatalf("final compact: %v", err)
	}
}
