package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// writeLegacyCheckpointForTest emits a version-1 single-file checkpoint.
func writeLegacyCheckpointForTest(dir string, st capturedState) error {
	path := filepath.Join(dir, checkpointName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	head := make([]byte, checkpointHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(head[4:8], checkpointVersion1)
	binary.LittleEndian.PutUint64(head[8:16], st.seq)
	binary.LittleEndian.PutUint64(head[16:24], st.snaps)
	binary.LittleEndian.PutUint64(head[24:32], uint64(st.offset))
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(st.entries)))
	h := crc32.NewIEEE()
	w.Write(head)
	h.Write(head)
	ehdr := make([]byte, checkpointEntrySize)
	for _, e := range st.entries {
		if e.present {
			ehdr[0] = ckptFlagPut
		} else {
			ehdr[0] = ckptFlagDelete
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(e.key)))
		binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(e.value)))
		binary.LittleEndian.PutUint64(ehdr[9:17], e.seq)
		w.Write(ehdr)
		h.Write(ehdr)
		io.WriteString(w, e.key)
		h.Write([]byte(e.key))
		if len(e.value) > 0 {
			w.Write(e.value)
			h.Write(e.value)
		}
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	w.Write(crcb[:])
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// waitQueueLen spins until the group-commit queue reaches n followers.
func waitQueueLen(s *Store, n int) {
	deadline := time.Now().Add(5 * time.Second)
	for queueLen(s) != n {
		if time.Since(deadline) > 0 {
			return
		}
		runtime.Gosched()
	}
}

// sput puts a string value, for scratch tests.
func sput(s *Store, k, v string) error { return s.Put(k, []byte(v)) }

func fullReplayView(t *testing.T, dir string) []KVPair {
	t.Helper()
	os.RemoveAll(filepath.Join(dir, checkpointDirName))
	os.Remove(filepath.Join(dir, checkpointName))
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("full replay open: %v", err)
	}
	defer s.Close()
	return dumpView(t, s)
}

func TestLayeredLayeredChainEquivalence(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	want := map[string]string{}
	put := func(k, v string) {
		if err := s.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	del := func(k string) {
		if err := s.Delete(k); err != nil {
			t.Fatal(err)
		}
		delete(want, k)
	}

	put("a", "1")
	put("b", "2")
	if err := s.Checkpoint(); err != nil { // L0
		t.Fatal(err)
	}
	put("a", "3")
	put("c", "4")
	if err := s.Checkpoint(); err != nil { // L1 delta
		t.Fatal(err)
	}
	del("b")
	put("d", "5")
	put("a", "6")
	if err := s.Checkpoint(); err != nil { // L2 delta
		t.Fatal(err)
	}
	put("tail", "x")
	del("c")
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Layered reopen.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := dumpView(t, s2)
	compareWant(t, got, want)
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snaps %d", n)
	}
	s2.Close()

	// Pure WAL replay must agree byte-for-byte.
	full := fullReplayView(t, dir)
	compareWant(t, full, want)
}

func compareWant(t *testing.T, got []KVPair, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d pairs %+v, want %d %v", len(got), got, len(want), want)
	}
	for _, p := range got {
		w, ok := want[p.Key]
		if !ok {
			t.Fatalf("unexpected key %q", p.Key)
		}
		if string(p.Value) != w {
			t.Fatalf("key %q = %q want %q", p.Key, p.Value, w)
		}
	}
}

func TestLayeredCorruptMiddleLayerFallsBack(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	s.Checkpoint() // L0
	sput(s, "b", "2")
	s.Checkpoint() // L1
	sput(s, "c", "3")
	s.Checkpoint() // L2
	s.Close()

	// Corrupt L1; L2 must be rejected too (unlinked), recovery uses L0 + tail.
	p1 := filepath.Join(dir, layerFileName(1))
	b, _ := os.ReadFile(p1)
	b[len(b)-8] ^= 0xFF
	os.WriteFile(p1, b, 0o600)

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// a from L0; b,c from the WAL tail replay.
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if got, ok, _ := s2.Get(kv[0]); !ok || string(got) != kv[1] {
			t.Fatalf("%s=%q ok=%v want %q", kv[0], got, ok, kv[1])
		}
	}
	s2.Close()

	// The corrupt L1 and orphaned L2 were removed; only L0 remains.
	if layers := chainLayerFiles(t, dir); len(layers) != 1 {
		t.Fatalf("expected only L0 to remain, got %v", layers)
	}
}

func TestLayeredGapRejectsSuffix(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	s.Checkpoint()
	sput(s, "b", "2")
	s.Checkpoint()
	sput(s, "c", "3")
	s.Checkpoint()
	s.Close()

	// Delete L1 leaving L0,L2: L2 unlinked, recovery loads L0 and replays tail.
	os.Remove(filepath.Join(dir, layerFileName(1)))
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("c"); !ok || string(got) != "3" {
		t.Fatalf("c=%q ok=%v", got, ok)
	}
	s2.Close()
	if layers := chainLayerFiles(t, dir); len(layers) != 1 {
		t.Fatalf("expected only L0, got %v", layers)
	}
}

func TestLayeredWindowRebase(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	want := map[string]string{}
	const rounds = checkpointWindow*2 + 3
	for i := 0; i < rounds; i++ {
		k := fmt.Sprintf("k%d", i%5)
		v := fmt.Sprintf("v%d", i)
		sput(s, k, v)
		want[k] = v
		if i%2 == 0 {
			s.Delete("gone")
			delete(want, "gone")
		} else {
			sput(s, "gone", fmt.Sprintf("g%d", i))
			want["gone"] = fmt.Sprintf("g%d", i)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if got := len(chainLayerFiles(t, dir)); got > checkpointWindow {
			t.Fatalf("window exceeded: %d", got)
		}
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	compareWant(t, dumpView(t, s2), want)
	s2.Close()
	compareWant(t, fullReplayView(t, dir), want)
}

func TestLayeredCompactWithChain(t *testing.T) {
	for _, nLayers := range []int{1, 2, checkpointWindow, checkpointWindow + 3} {
		t.Run(fmt.Sprintf("layers=%d", nLayers), func(t *testing.T) {
			dir := tempDir(t)
			s := openStore(t, dir)
			want := map[string]string{}
			sput(s, "base", "keep")
			want["base"] = "keep"
			for l := 0; l < nLayers; l++ {
				sput(s, fmt.Sprintf("k%d", l), fmt.Sprintf("v%d", l))
				want[fmt.Sprintf("k%d", l)] = fmt.Sprintf("v%d", l)
				sput(s, "volatile", fmt.Sprintf("w%d", l))
				want["volatile"] = fmt.Sprintf("w%d", l)
				if l%2 == 0 {
					s.Delete("volatile")
					delete(want, "volatile")
				}
				if err := s.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			// A pinned snapshot holds an old value that compaction must keep.
			sput(s, "pinned", "old")
			old, _ := s.Snapshot()
			big := make([]byte, 2048)
			for i := 0; i < 10; i++ {
				s.Put("pinned", big)
			}
			want["pinned"] = string(big)
			sput(s, "after", "z")
			want["after"] = "z"

			sizeBefore := dirBytes(t, dir)
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
			sizeAfter := dirBytes(t, dir)
			_ = sizeBefore
			_ = sizeAfter

			// live layer count bounded by window
			if got := len(chainLayerFiles(t, dir)); got > checkpointWindow {
				t.Fatalf("layers after compact %d > window", got)
			}
			// A pinned snapshot holds the value visible when taken ("old");
			// compaction must keep serving that fixed view from memory.
			if got, ok := old.Get("pinned"); !ok || string(got) != "old" {
				t.Fatalf("pinned snapshot broken: %q ok=%v", got, ok)
			}
			old.Close()
			compareWant(t, dumpView(t, s), want)
			s.Close()

			s2, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			compareWant(t, dumpView(t, s2), want)
			s2.Close()
			compareWant(t, fullReplayView(t, dir), want)
		})
	}
}

func TestLayeredLayerTempSwept(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	s.Checkpoint()
	s.Close()
	// half-built next layer debris
	os.MkdirAll(filepath.Join(dir, checkpointDirName), 0o700)
	os.WriteFile(filepath.Join(dir, layerTmpName(1)), []byte("partial"), 0o600)
	os.WriteFile(filepath.Join(dir, stagedChainDir+"-x"), []byte("x"), 0o600)
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("a=%q ok=%v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, layerTmpName(1))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("layer temp not swept")
	}
	s2.Close()
}

func TestLayeredLegacyUpgrade(t *testing.T) {
	dir := tempDir(t)
	// Build a version-1 single-file checkpoint using the old writer.
	s := openStore(t, dir)
	sput(s, "a", "1")
	sput(s, "b", "2")
	off, _ := s.wal.position()
	st := capturedState{
		entries: []checkpointEntry{
			{key: "a", value: []byte("1"), present: true, seq: 1},
			{key: "b", value: []byte("2"), present: true, seq: 2},
		},
		offset: off, seq: 2, snaps: 0,
	}
	if err := writeLegacyCheckpointForTest(dir, st); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen uses the legacy checkpoint.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("b"); !ok || string(got) != "2" {
		t.Fatalf("legacy reopen b=%q ok=%v", got, ok)
	}
	// Taking a checkpoint upgrades to the layered layout and removes legacy.
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("legacy checkpoint not removed on upgrade")
	}
	if !checkpointExists(t, dir) {
		t.Fatal("layered checkpoint missing after upgrade")
	}
	sput(s2, "c", "3")
	s2.Close()

	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if got, ok, _ := s3.Get(kv[0]); !ok || string(got) != kv[1] {
			t.Fatalf("%s=%q ok=%v", kv[0], got, ok)
		}
	}
	s3.Close()
}

func TestLayeredDirtyQueueBounded(t *testing.T) {
	// Queueing many batches must not retain a full-map copy per queued batch.
	// Functional check: a large number of queued then drained commits produces
	// exact final state and per-key last-write-wins.
	s := openStore(t, tempDir(t))
	proceed := make(chan struct{})
	var wg sync.WaitGroup
	s.leaderHook = func() { wg.Done(); <-proceed }
	defer func() { s.leaderHook = nil }()

	wg.Add(1)
	go func() { sput(s, "leader", "L") }()
	wg.Wait()

	const followers = 50
	var done sync.WaitGroup
	for i := 0; i < followers; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			s.CommitBatch([]BatchOp{
				{Key: fmt.Sprintf("f%d", i), Value: []byte("F")},
				{Key: "shared", Value: []byte(fmt.Sprintf("%d", i))},
			})
		}(i)
	}
	// give followers a moment to queue
	waitQueueLen(s, followers+1)
	close(proceed)
	done.Wait()
	if got, ok, _ := s.Get("leader"); !ok || string(got) != "L" {
		t.Fatalf("leader %q", got)
	}
	if got, ok, _ := s.Get(fmt.Sprintf("f%d", followers-1)); !ok || string(got) != "F" {
		t.Fatalf("last follower missing ok=%v", ok)
	}
	if _, ok, _ := s.Get("shared"); !ok {
		t.Fatal("shared missing")
	}
}

func TestLayeredClosedStoreErrors(t *testing.T) {
	s := openStore(t, tempDir(t))
	s.Close()
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"put", func() error { return s.Put("a", []byte("x")) }},
		{"put-empty", func() error { return s.Put("", []byte("x")) }},
		{"del", func() error { return s.Delete("a") }},
		{"del-empty", func() error { return s.Delete("") }},
		{"batch", func() error { return s.CommitBatch([]BatchOp{{Key: "a"}}) }},
		{"batch-empty-key", func() error { return s.CommitBatch([]BatchOp{{Key: ""}}) }},
		{"checkpoint", func() error { return s.Checkpoint() }},
		{"compact", func() error { return s.Compact() }},
	} {
		if err := tc.fn(); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("%s on closed: %v want ErrClosed", tc.name, err)
		}
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
	if err != nil {
		t.Fatalf("copy dir: %v", err)
	}
}

func TestLayeredIntermediateLayerAfterCompact(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "0")
	s.Checkpoint()
	for i := 1; i <= 5; i++ {
		sput(s, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		s.Put("a", []byte(fmt.Sprintf("a%d", i)))
		if i == 3 {
			if _, err := s.Snapshot(); err != nil {
				t.Fatal(err)
			}
		}
		s.Checkpoint()
	}
	sput(s, "tail", "t")
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	layers := chainLayerFiles(t, dir)
	if len(layers) < 2 {
		t.Fatalf("want >=2 reanchored layers, got %d", len(layers))
	}

	want := func(t *testing.T, d string) {
		st, err := Open(d)
		if err != nil {
			t.Fatal(err)
		}
		got := dumpView(t, st)
		exp := map[string]string{"a": "a5", "tail": "t"}
		for i := 1; i <= 5; i++ {
			exp[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
		}
		compareWant(t, got, exp)
		if n := st.Stats().Snapshots; n != 1 {
			t.Fatalf("snapshots %d want 1", n)
		}
		st.Close()
	}

	// Full chain.
	d0 := t.TempDir()
	copyDir(t, dir, filepath.Join(d0, "store"))
	want(t, filepath.Join(d0, "store"))

	// Each intermediate prefix length: remove layers >= keep and reopen.
	for keep := 1; keep < len(layers); keep++ {
		d := t.TempDir()
		cp := filepath.Join(d, "store")
		copyDir(t, dir, cp)
		for idx := keep; idx < len(layers); idx++ {
			os.Remove(filepath.Join(cp, layerFileName(uint32(idx))))
		}
		want(t, cp)
	}
}

func TestLayeredEmptyCheckpointThenCompact(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	sput(s, "x", "1")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("x"); !ok || string(got) != "1" {
		t.Fatalf("x=%q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snaps %d want 1", n)
	}
	// Another checkpoint after a re-anchored compact appends a valid delta.
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s3.Get("x"); !ok || string(got) != "1" {
		t.Fatalf("x after second checkpoint=%q ok=%v", got, ok)
	}
	s3.Close()
}

func TestLayeredCompactThenCompactAgain(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	for l := 0; l < checkpointWindow+2; l++ {
		sput(s, fmt.Sprintf("k%d", l), fmt.Sprintf("v%d", l))
		s.Checkpoint()
	}
	sput(s, "tail", "z")
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	sput(s, "more", "m")
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("more"); !ok || string(got) != "m" {
		t.Fatalf("more=%q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("tail"); !ok || string(got) != "z" {
		t.Fatalf("tail=%q ok=%v", got, ok)
	}
	s2.Close()
}

func TestLayeredLegacyLiveThenCompact(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	off, _ := s.wal.position()
	st := capturedState{
		entries: []checkpointEntry{{key: "a", value: []byte("1"), present: true, seq: 1}},
		offset:  off, seq: 1, snaps: 0,
	}
	if err := writeLegacyCheckpointForTest(dir, st); err != nil {
		t.Fatal(err)
	}
	s.legacyLive = true
	sput(s, "b", "2")
	if err := s.Compact(); err != nil {
		t.Fatalf("compact with legacy checkpoint live: %v", err)
	}
	// Legacy file must be durably retired before the WAL swap.
	if _, err := os.Stat(filepath.Join(dir, checkpointName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("legacy checkpoint survived compaction: %v", err)
	}
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("a=%q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("b"); !ok || string(got) != "2" {
		t.Fatalf("b=%q ok=%v", got, ok)
	}
	s2.Close()
}

func TestLayeredCrashDebrisMidCompact(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	s.Checkpoint()
	sput(s, "b", "2")
	s.Checkpoint()
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	sput(s, "c", "3")
	s.Close()

	// Simulate kill after staging the replacement WAL and new chain but before
	// the rename: old wal.log and old ckpt/ are still the record.
	os.WriteFile(filepath.Join(dir, compactTmpName), []byte("partial"), 0o600)
	os.MkdirAll(filepath.Join(dir, stagedChainDir), 0o700)
	os.WriteFile(filepath.Join(dir, stagedChainDir, "0000000000.ckpt"), []byte("x"), 0o600)

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen amid compaction debris: %v", err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if got, ok, _ := s2.Get(kv[0]); !ok || string(got) != kv[1] {
			t.Fatalf("%s=%q ok=%v", kv[0], got, ok)
		}
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snaps %d", n)
	}
	for _, name := range []string{compactTmpName, stagedChainDir} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("debris %s not swept", name)
		}
	}
	// Existing chain intact, usable with no repair step.
	if !checkpointExists(t, dir) {
		t.Fatal("valid chain lost during debris sweep")
	}
	if err := s2.Compact(); err != nil {
		t.Fatalf("compact after recovery: %v", err)
	}
	if err := s2.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after recovery: %v", err)
	}
	s2.Close()
}

func TestLayeredDeleteAfterNewestLayerCompact(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	sput(s, "a", "1")
	sput(s, "d", "doomed")
	s.Checkpoint()
	sput(s, "b", "2")
	s.Checkpoint()
	sput(s, "c", "3")
	s.Checkpoint()
	// Delete d after the newest layer with no snapshot/cursor pinning it.
	if err := s.Delete("d"); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen seeding the newest re-anchored layer then replaying its tail:
	// d must stay deleted, never resurrected.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s2.Get("d"); ok {
		t.Fatal("key deleted after newest layer was resurrected on reopen")
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if got, ok, _ := s2.Get(kv[0]); !ok || string(got) != kv[1] {
			t.Fatalf("%s=%q ok=%v", kv[0], got, ok)
		}
	}
	// Agrees with a from-WAL reopen.
	s2.Close()
	full := fullReplayView(t, dir)
	for _, p := range full {
		if p.Key == "d" {
			t.Fatal("full replay shows deleted d")
		}
	}
}

func TestLayeredSingleOpsShareGroupCommit(t *testing.T) {
	s := openStore(t, tempDir(t))
	const followers = 6
	proceed := make(chan struct{})
	var leaderWG sync.WaitGroup
	leaderWG.Add(1)
	s.leaderHook = func() { leaderWG.Done(); <-proceed }
	defer func() { s.leaderHook = nil }()

	leaderErr := make(chan error, 1)
	go func() { leaderErr <- s.Put("leader", []byte("L")) }()
	leaderWG.Wait()

	var wg sync.WaitGroup
	for i := 0; i < followers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			switch i % 3 {
			case 0:
				err = s.Put(fmt.Sprintf("p%d", i), []byte("P"))
			case 1:
				err = s.Delete(fmt.Sprintf("absent%d", i)) // no-op, still queued
			default:
				err = s.CommitBatch([]BatchOp{{Key: fmt.Sprintf("b%d", i), Value: []byte("B")}})
			}
			if err != nil {
				t.Errorf("follower %d: %v", i, err)
			}
		}(i)
	}
	waitQueueLen(s, followers+1)
	before := walSyncs(s)
	close(proceed)
	if err := <-leaderErr; err != nil {
		t.Fatalf("leader: %v", err)
	}
	wg.Wait()
	if got := walSyncs(s) - before; got != 1 {
		t.Fatalf("mixed single/batch group used %d syncs, want 1", got)
	}
	if got, ok, _ := s.Get("leader"); !ok || string(got) != "L" {
		t.Fatalf("leader %q", got)
	}
	if got, ok, _ := s.Get("p0"); !ok || string(got) != "P" {
		t.Fatalf("p0 %q ok=%v", got, ok)
	}
	if got, ok, _ := s.Get("b2"); !ok || string(got) != "B" {
		t.Fatalf("b2 %q ok=%v", got, ok)
	}
}
