package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// writeLegacyV1Checkpoint hand-encodes the original single-file v1
// checkpoint (snapshot.ckpt) for the store's current state, so an
// old-version directory can be simulated without keeping the v1 writer in
// production code.
func writeLegacyV1Checkpoint(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	off, err := s.wal.position()
	if err != nil {
		t.Fatal(err)
	}
	v := *s.current.Load()
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	path := filepath.Join(s.dir, checkpointName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	h := crc32.NewIEEE()
	head := make([]byte, v1HeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(head[4:8], checkpointVersion1)
	binary.LittleEndian.PutUint64(head[8:16], s.nextSeq)
	binary.LittleEndian.PutUint64(head[16:24], s.snapshots.Load())
	binary.LittleEndian.PutUint64(head[24:32], uint64(off))
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(keys)))
	w.Write(head)
	h.Write(head)

	e := make([]byte, entryHdrSize)
	for _, k := range keys {
		n := v[k]
		if n.present {
			e[0] = ckptFlagPut
			binary.LittleEndian.PutUint32(e[5:9], uint32(len(n.value)))
		} else {
			e[0] = ckptFlagDelete
			binary.LittleEndian.PutUint32(e[5:9], 0)
		}
		binary.LittleEndian.PutUint32(e[1:5], uint32(len(k)))
		binary.LittleEndian.PutUint64(e[9:17], n.seq)
		w.Write(e)
		h.Write(e)
		w.WriteString(k)
		h.Write([]byte(k))
		if n.present {
			w.Write(n.value)
			h.Write(n.value)
		}
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	w.Write(crcb[:])
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLayeredUpgradeFromLegacyV1Checkpoint(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	// Hand-write a legacy checkpoint instead of calling Checkpoint, which now
	// writes the layered layout.
	writeLegacyV1Checkpoint(t, s)
	if err := s.Put("tail", []byte("t")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen honors the v1 file and replays the tail.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s2.Get("tail"); !ok || string(got) != "t" {
		t.Fatalf("v1 reopen tail: %q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("v1 reopen snapshots=%d want 1", n)
	}
	if s2.cpHasChain {
		t.Fatal("v1 directory must not report a layered chain before upgrade")
	}

	// The first new checkpoint upgrades to the layered layout and withdraws
	// the legacy file.
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("legacy v1 file not removed after upgrade: %v", err)
	}
	if countLayers(t, dir) != 1 {
		t.Fatal("upgrade did not lay exactly one base layer")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// And the layered chain reopens correctly afterwards.
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	for k, v := range map[string]string{"a": "v1", "b": "v2", "tail": "t"} {
		if got, ok, _ := s3.Get(k); !ok || string(got) != v {
			t.Fatalf("post-upgrade %s: %q ok=%v want %s", k, got, ok, v)
		}
	}
}

func TestInterruptedBaseSwapRecoveries(t *testing.T) {
	t.Run("valid staged base is promoted", func(t *testing.T) {
		dir := tempDir(t)
		s := openStore(t, dir)
		if err := s.Put("keep", []byte("v")); err != nil {
			t.Fatal(err)
		}
		// Build a legitimate base straight into ckpt.next and leave a
		// displaced chain in ckpt.old, as a kill between the two renames.
		hdr, full := captureBase(map[string]*node{
			"keep": {value: []byte("v"), present: true, seq: 1},
		}, 1, 0, 0)
		next := filepath.Join(dir, cpNextDir)
		if err := os.Mkdir(next, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := createLayerFile(next, layerName(0), hdr, full, nil); err != nil {
			t.Fatal(err)
		}
		old := filepath.Join(dir, cpOldDir)
		if err := os.Mkdir(old, 0o700); err != nil {
			t.Fatal(err)
		}
		stale := filepath.Join(old, layerName(0))
		if err := os.WriteFile(stale, []byte("old"), 0o600); err != nil {
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
		if got, ok, _ := s2.Get("keep"); !ok || string(got) != "v" {
			t.Fatalf("promoted staged base wrong: %q ok=%v", got, ok)
		}
		if _, err := os.Stat(filepath.Join(dir, cpOldDir)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("displaced chain not removed after promotion")
		}
		if _, err := os.Stat(filepath.Join(dir, cpNextDir)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("staging dir not renamed away")
		}
	})

	t.Run("partial staged build is swept not promoted", func(t *testing.T) {
		dir := tempDir(t)
		s := openStore(t, dir)
		if err := s.Put("keep", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(); err != nil { // valid live base
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		// A crash during the next consolidation: ckpt.next exists but holds a
		// half-built layer, and the live chain is untouched.
		next := filepath.Join(dir, cpNextDir)
		if err := os.Mkdir(next, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(next, layerTmp), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
		s2, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s2.Close()
		if got, ok, _ := s2.Get("keep"); !ok || string(got) != "v" {
			t.Fatalf("live chain lost to a partial stage: %q ok=%v", got, ok)
		}
		if _, err := os.Stat(next); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("partial staging dir not swept")
		}
		if countLayers(t, dir) != 1 {
			t.Fatal("live base did not survive the sweep")
		}
		// A checkpoint immediately works, no repair needed.
		if err := s2.Checkpoint(); err != nil {
			t.Fatalf("checkpoint after recovery: %v", err)
		}
	})
}

func TestDeltaTempSweptOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, cpDirName, layerTmp), []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := os.Stat(filepath.Join(dir, cpDirName, layerTmp)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("delta temp not swept")
	}
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("base state lost: %q ok=%v", got, ok)
	}
}

func TestCheckpointSpaceBoundedByWindowNotHistory(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	big := make([]byte, 2048)
	// Far more commits than the window; the same key is overwritten so live
	// data stays one value, but each delta captures the recent overwrite. The
	// layer count and checkpoint bytes must stay bounded by the window rather
	// than grow with the number of cycles.
	maxLayers := 0
	for i := 0; i < checkpointWindow*6; i++ {
		if err := s.Put("hot", big); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if n := countLayers(t, dir); n > checkpointWindow {
			t.Fatalf("iteration %d: %d layers exceed window %d", i, n, checkpointWindow)
		} else if n > maxLayers {
			maxLayers = n
		}
	}
	if maxLayers < 2 {
		t.Fatalf("expected the chain to grow deltas between consolidations, max=%d", maxLayers)
	}
	// Force a consolidation so the chain ends as a single complete base.
	if err := s.Put("hot", big); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if n := countLayers(t, dir); n != 1 {
		t.Fatalf("consolidation left %d layers, want 1", n)
	}
	entries, err := os.ReadDir(filepath.Join(dir, cpDirName))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("chain dir has %d entries, want 1", len(entries))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("hot"); !ok || len(got) != len(big) {
		t.Fatalf("latest value lost across bounded-chain reopen: ok=%v", ok)
	}
}

func TestPutDeleteShareGroupCommitSync(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("seed", []byte("x")); err != nil {
		t.Fatal(err)
	}

	proceed := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	s.leaderHook = func() {
		wg.Done()
		<-proceed
	}
	defer func() { s.leaderHook = nil }()

	lerr := make(chan error, 1)
	go func() { lerr <- s.CommitBatch([]BatchOp{{Key: "b", Value: []byte("B")}}) }()
	wg.Wait()

	var fwg sync.WaitGroup
	for i := 0; i < 3; i++ {
		fwg.Add(1)
		go func(i int) {
			defer fwg.Done()
			if err := s.Put(fmt.Sprintf("p%d", i), []byte("P")); err != nil {
				t.Errorf("put: %v", err)
			}
		}(i)
	}
	fwg.Add(1)
	go func() {
		defer fwg.Done()
		if err := s.Delete("seed"); err != nil {
			t.Errorf("delete: %v", err)
		}
	}()

	// The single writes join the parked leader's queue.
	deadline := time.Now()
	for queueLen(s) != 5 {
		if time.Since(deadline) > 5*time.Second {
			t.Fatalf("singles did not join the batch group: %d", queueLen(s))
		}
		runtime.Gosched()
	}
	before := walSyncs(s)
	close(proceed)
	if err := <-lerr; err != nil {
		t.Fatal(err)
	}
	fwg.Wait()
	after := walSyncs(s)
	if after-before != 1 {
		t.Fatalf("put/delete + batch used %d syncs, want 1", after-before)
	}
	if _, ok, _ := s.Get("seed"); ok {
		t.Fatal("grouped delete did not take effect")
	}
	for i := 0; i < 3; i++ {
		if got, ok, _ := s.Get(fmt.Sprintf("p%d", i)); !ok || string(got) != "P" {
			t.Fatalf("grouped put p%d lost: %q ok=%v", i, got, ok)
		}
	}
}

func TestRandomizedChainReopenMatchesFullReplay(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	dirFull := tempDir(t)
	dirChain := tempDir(t)
	full := openStore(t, dirFull)
	chain := openStore(t, dirChain)

	keys := []string{"a", "b", "c", "d", "e"}
	type roundPlan struct {
		ops        []BatchOp
		snapshot   bool
		checkpoint bool
	}
	// Build the whole workload once so both stores receive identical ops.
	plans := make([]roundPlan, 60)
	for r := range plans {
		p := roundPlan{}
		for _, k := range keys {
			switch rng.Intn(3) {
			case 0:
				p.ops = append(p.ops, BatchOp{Key: k, Delete: true})
			case 1:
				p.ops = append(p.ops, BatchOp{Key: k, Value: []byte(fmt.Sprintf("%d", rng.Intn(1000)))})
			}
		}
		p.snapshot = rng.Intn(4) == 0
		p.checkpoint = rng.Intn(2) == 0
		plans[r] = p
	}
	applyPlan := func(st *Store, p roundPlan) {
		if len(p.ops) > 0 {
			if err := st.CommitBatch(p.ops); err != nil {
				t.Fatal(err)
			}
		}
		if p.snapshot {
			if _, err := st.Snapshot(); err != nil {
				t.Fatal(err)
			}
		}
	}

	for round, p := range plans {
		applyPlan(full, p)
		applyPlan(chain, p)
		if p.checkpoint {
			if err := chain.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		// Periodically reopen the chain store from disk and continue.
		if round%15 == 14 {
			want := dumpView(t, chain)
			if err := chain.Close(); err != nil {
				t.Fatal(err)
			}
			chain = openStore(t, dirChain)
			got := dumpView(t, chain)
			if len(got) != len(want) {
				t.Fatalf("round %d: %d pairs, want %d", round, len(got), len(want))
			}
			for i := range want {
				if got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
					t.Fatalf("round %d mismatch: %q=%q vs %q=%q", round,
						got[i].Key, got[i].Value, want[i].Key, want[i].Value)
				}
			}
		}
	}

	if err := full.Close(); err != nil {
		t.Fatal(err)
	}
	if err := chain.Close(); err != nil {
		t.Fatal(err)
	}
	reFull := openStore(t, dirFull)
	defer reFull.Close()
	reChain, err := Open(dirChain)
	if err != nil {
		t.Fatal(err)
	}
	defer reChain.Close()
	a, b := dumpView(t, reFull), dumpView(t, reChain)
	if len(a) != len(b) {
		t.Fatalf("final: chain %d pairs vs full %d", len(b), len(a))
	}
	for i := range a {
		if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
			t.Fatalf("final mismatch %d: %q=%q vs %q=%q", i, a[i].Key, a[i].Value, b[i].Key, b[i].Value)
		}
	}
	if reFull.Stats().Snapshots != reChain.Stats().Snapshots {
		t.Fatalf("snapshot counts differ: full=%d chain=%d",
			reFull.Stats().Snapshots, reChain.Stats().Snapshots)
	}
}
