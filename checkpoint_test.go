package snapshot

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// dumpView returns every live key/value pair in the latest view, sorted, so
// two reopened directories can be compared as exact terminal state.
func dumpView(t *testing.T, s *Store) []KVPair {
	t.Helper()
	pairs, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
	return pairs
}

// exerciseStore performs a fixed mixed workload of puts, same-key rewrites,
// deletes and atomic batches (including same-key-last-wins cases).
func exerciseStore(t *testing.T, s *Store, tag string) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", tag, err)
		}
	}
	must(s.Put("alpha", []byte("one")))
	must(s.Put("beta", []byte("two")))
	must(s.Put("alpha", []byte("one-v2")))
	must(s.CommitBatch([]BatchOp{
		{Key: "g1", Value: []byte("a")},
		{Key: "g1", Value: []byte("b")}, // later op wins within the batch
		{Key: "g2", Value: []byte("c")},
		{Key: "gone", Delete: true},
	}))
	must(s.Put("nil-val", nil))
	must(s.Put("empty-val", []byte{}))
	must(s.Put("doomed", []byte("x")))
	must(s.Delete("doomed"))
	must(s.Delete("never-existed")) // no-op must not disturb anything
	must(s.CommitBatch([]BatchOp{
		{Key: "g3", Delete: true}, // absent: whole op is a no-op
		{Key: "beta", Delete: true},
		{Key: "beta", Value: []byte("resurrected")}, // delete-then-put wins
	}))
	for i := 0; i < 3; i++ {
		if _, err := s.Snapshot(); err != nil {
			t.Fatalf("%s snapshot: %v", tag, err)
		}
	}
}

// checkpointArtifacts returns the checkpoint-related names present in dir:
// the chain directory and its layer files (flattened as ckpt/<name>), the
// staging directories, and a legacy single-file checkpoint if present.
func checkpointArtifacts(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	root := filepath.Join(dir)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		switch e.Name() {
		case cpDirName, cpNextDir, cpOldDir, checkpointName:
			out = append(out, e.Name())
		}
		if e.Name() == cpDirName && e.IsDir() {
			layers, err := os.ReadDir(filepath.Join(root, cpDirName))
			if err != nil {
				t.Fatalf("read ckpt dir: %v", err)
			}
			for _, l := range layers {
				out = append(out, cpDirName+"/"+l.Name())
			}
		}
	}
	sort.Strings(out)
	return out
}

// checkpointExists reports whether a usable live checkpoint artifact exists
// (a layered chain with at least one layer, or a legacy single file).
func checkpointExists(t *testing.T, dir string) bool {
	t.Helper()
	for _, n := range checkpointArtifacts(t, dir) {
		if n == checkpointName || strings.HasPrefix(n, cpDirName+"/"+layerPrefix) {
			return true
		}
	}
	return false
}

// liveLayerPath returns the path of one layer file of the given generation.
func liveLayerPath(t *testing.T, dir string, gen uint64) string {
	t.Helper()
	return filepath.Join(dir, cpDirName, layerName(gen))
}

// countLayers reports how many layer files the live chain directory holds.
func countLayers(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, cpDirName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0
		}
		t.Fatalf("read ckpt dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if _, ok := parseLayerName(e.Name()); ok {
			n++
		}
	}
	return n
}

func TestCheckpointReopenMatchesFullReplay(t *testing.T) {
	dirFull := tempDir(t)
	dirCP := tempDir(t)
	full := openStore(t, dirFull)
	cp := openStore(t, dirCP)

	exerciseStore(t, full, "full")
	exerciseStore(t, cp, "cp")

	// Checkpoint in the middle of continued life, then commit more work that
	// must come back purely from the WAL tail replay.
	if err := cp.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !checkpointExists(t, dirCP) {
		t.Fatal("checkpoint file not created")
	}
	walAfter := walSize(t, dirCP)
	post := []BatchOp{
		{Key: "tail1", Value: []byte("t1")},
		{Key: "alpha", Value: []byte("one-v3")},
		{Key: "g1", Value: []byte("b2"), Delete: false},
		{Key: "g2", Delete: true},
	}
	if err := cp.CommitBatch(post); err != nil {
		t.Fatal(err)
	}
	if err := full.CommitBatch(post); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := cp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := full.Close(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint never modifies the WAL; the tail batch landed after it.
	if got := walSize(t, dirCP); got <= walAfter {
		t.Fatalf("wal did not grow after checkpointed commit: %d", got)
	}

	want := []KVPair{
		{Key: "alpha", Value: []byte("one-v3")},
		{Key: "beta", Value: []byte("resurrected")},
		{Key: "empty-val", Value: []byte{}},
		{Key: "g1", Value: []byte("b2")},
		{Key: "nil-val", Value: nil},
		{Key: "tail1", Value: []byte("t1")},
	}

	reopenedFull := openStore(t, dirFull)
	reopenedCP, err := Open(dirCP)
	if err != nil {
		t.Fatalf("reopen with checkpoint: %v", err)
	}
	defer reopenedCP.Close()

	gotFull := dumpView(t, reopenedFull)
	gotCP := dumpView(t, reopenedCP)
	if len(gotCP) != len(want) {
		t.Fatalf("checkpoint reopen has %d pairs, want %d: %+v", len(gotCP), len(want), gotCP)
	}
	for i := range want {
		if gotFull[i].Key != want[i].Key || string(gotFull[i].Value) != string(want[i].Value) {
			t.Fatalf("full replay mismatch at %d: got %q=%q want %q=%q", i, gotFull[i].Key, gotFull[i].Value, want[i].Key, want[i].Value)
		}
		if gotCP[i].Key != want[i].Key || string(gotCP[i].Value) != string(want[i].Value) {
			t.Fatalf("checkpoint reopen mismatch at %d: got %q=%q want %q=%q", i, gotCP[i].Key, gotCP[i].Value, want[i].Key, want[i].Value)
		}
	}
	// Cumulative snapshot count survives the checkpoint and the later one.
	if n := reopenedCP.Stats().Snapshots; n != 4 {
		t.Fatalf("snapshots after checkpoint reopen: %d want 4", n)
	}
	// Nil value is a hit with zero-length bytes.
	if got, ok, err := reopenedCP.Get("nil-val"); !ok || err != nil || len(got) != 0 {
		t.Fatalf("nil-val: got=%q ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := reopenedCP.Get("g2"); ok {
		t.Fatal("tail delete lost across checkpoint reopen")
	}
	if _, ok, _ := reopenedCP.Get("doomed"); ok {
		t.Fatal("tombstone in checkpoint became a live key")
	}
}

func TestCheckpointSeedsOnlyAndReplaysTail(t *testing.T) {
	// Proof that reopen loads the checkpoint and replays only the tail, not
	// the whole log: corrupt bytes that are fully covered by the checkpoint
	// must be invisible, while a frame appended after the checkpoint is still
	// replayed. A from-scratch replay of this WAL would lose everything.
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("covered1", []byte("c1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("covered2", []byte("c2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cut := walSize(t, dir)
	if cut == 0 {
		t.Fatal("expected nonempty WAL at cut")
	}
	if err := s.Put("tail", []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Garble the checkpoint-covered prefix only.
	path := filepath.Join(dir, walName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < cut-1; i++ {
		data[i] ^= 0xFF
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with covered-prefix corruption: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("covered1"); !ok || string(got) != "c1" {
		t.Fatalf("checkpoint-covered value lost: %q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("tail"); !ok || string(got) != "later" {
		t.Fatalf("tail value not replayed: %q ok=%v", got, ok)
	}
}

func TestCheckpointRejectsCorruptAndFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		// mutate transforms the valid checkpoint bytes into a bad artifact.
		mutate func(b []byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)/2] }},
		{"bad magic", func(b []byte) []byte { b[0] ^= 0xFF; return b }},
		{"bad version", func(b []byte) []byte {
			b[7] = 99 // version field's high byte flip => unknown version
			return b
		}},
		{"flipped payload byte", func(b []byte) []byte {
			b[len(b)-10] ^= 0x01 // breaks the CRC check
			return b
		}},
		{"trailing bytes", func(b []byte) []byte { return append(b, 0xAA) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tempDir(t)
			s := openStore(t, dir)
			if err := s.Put("keep", []byte("value")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Snapshot(); err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := s.Put("after", []byte("tail")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			cpPath := liveLayerPath(t, dir, 0)
			b, err := os.ReadFile(cpPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(cpPath, tc.mutate(b), 0o600); err != nil {
				t.Fatal(err)
			}

			// Reopen must not error and must recover entirely from the WAL.
			s2, err := Open(dir)
			if err != nil {
				t.Fatalf("open with %s checkpoint: %v", tc.name, err)
			}
			defer s2.Close()
			if got, ok, _ := s2.Get("keep"); !ok || string(got) != "value" {
				t.Fatalf("keep after rejected checkpoint: %q ok=%v", got, ok)
			}
			if got, ok, _ := s2.Get("after"); !ok || string(got) != "tail" {
				t.Fatalf("tail after rejected checkpoint: %q ok=%v", got, ok)
			}
			if n := s2.Stats().Snapshots; n != 1 {
				t.Fatalf("snapshot count via WAL: %d want 1", n)
			}
			// The rejected artifact is withdrawn, not left as debris.
			if checkpointExists(t, dir) {
				t.Fatal("rejected checkpoint not removed on open")
			}
		})
	}
}

func TestCheckpointRepeatedBuildsChainAndConsolidatesInWindow(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	// Two checkpoint calls per round: the first lays the base, subsequent
	// calls append delta layers until the retention window fills, at which
	// point a fresh base replaces the whole chain.
	for i := 0; i < checkpointWindow*2+1; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("checkpoint %d: %v", i, err)
		}
		got := checkpointArtifacts(t, dir)
		for _, n := range got {
			if n == layerTmp || n == cpDirName+"/"+layerTmp ||
				n == cpNextDir || n == cpOldDir || n == checkpointTmpName {
				t.Fatalf("iteration %d left staging debris: %v", i, got)
			}
		}
		if n := countLayers(t, dir); n < 1 || n > checkpointWindow {
			t.Fatalf("iteration %d: live layers=%d, want 1..%d", i, n, checkpointWindow)
		}
	}

	// Consolidation never leaves superseded layers beyond the window.
	if n := countLayers(t, dir); n != 1 {
		t.Fatalf("chain not consolidated to a fresh base: %d layers", n)
	}

	// The surviving base holds the complete terminal state, so reopen from
	// the chain reproduces every committed key.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 0; i < checkpointWindow*2+1; i++ {
		if got, ok, _ := s2.Get(fmt.Sprintf("k%d", i)); !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d after chain reopen: %q ok=%v", i, got, ok)
		}
	}
}

func TestCheckpointDeltaLayersAppendAndReopen(t *testing.T) {
	// Several checkpoints without forcing consolidation produce a base plus
	// delta layers; reopen loads them in generation order and replays the
	// trailing tail, byte-for-byte matching a store that only replayed its
	// WAL.
	dirFull := tempDir(t)
	dirChain := tempDir(t)
	full := openStore(t, dirFull)
	chain := openStore(t, dirChain)

	commit := func(st *Store, round int) {
		t.Helper()
		if err := st.CommitBatch([]BatchOp{
			{Key: fmt.Sprintf("key%d", round), Value: []byte(fmt.Sprintf("v%d", round))},
			{Key: "shared", Value: []byte(fmt.Sprintf("round%d", round))},
			{Key: "gone", Value: []byte("x")},
			{Key: "gone", Delete: true}, // put-then-delete ends deleted
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	for r := 0; r < checkpointWindow-1; r++ {
		commit(full, r)
		commit(chain, r)
		if err := chain.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	// Base + (window-2) deltas so far, none consolidated away.
	if got := countLayers(t, dirChain); got != checkpointWindow-1 {
		t.Fatalf("layers=%d want %d", got, checkpointWindow-1)
	}
	// One more committed batch lands strictly after the newest layer and
	// must come back solely from the WAL tail replay.
	commit(full, checkpointWindow-1)
	commit(chain, checkpointWindow-1)
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

	if a, b := dumpView(t, reFull), dumpView(t, reChain); len(a) != len(b) {
		t.Fatalf("chain reopen %d pairs, full replay %d", len(b), len(a))
	} else {
		for i := range a {
			if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
				t.Fatalf("mismatch %d: %q=%q vs %q=%q", i, a[i].Key, a[i].Value, b[i].Key, b[i].Value)
			}
		}
	}
	if got, ok, _ := reChain.Get("shared"); !ok || string(got) != fmt.Sprintf("round%d", checkpointWindow-1) {
		t.Fatalf("tail batch not replayed over chain: %q ok=%v", got, ok)
	}
	if _, ok, _ := reChain.Get("gone"); ok {
		t.Fatal("delta tombstone did not shadow base put")
	}
	if n := reChain.Stats().Snapshots; n != uint64(checkpointWindow) {
		t.Fatalf("snapshots across chain: %d want %d", n, checkpointWindow)
	}
}

func TestCheckpointBadDeltaRejectsSuffixKeepsPrefix(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil { // base gen0
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil { // delta gen1
		t.Fatal(err)
	}
	if err := s.Put("c", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil { // delta gen2
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the middle delta (gen1). Reopen must reject gen1 and gen2,
	// fall back to the intact base, and replay the WAL tail from its cut so
	// the final state is still exactly the full committed state.
	p := liveLayerPath(t, dir, 1)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-8] ^= 0x01 // break the CRC
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got, ok, _ := s2.Get(k); !ok || string(got) != v {
			t.Fatalf("fallback-to-prefix lost %s: %q ok=%v want %s", k, got, ok, v)
		}
	}
	// The rejected suffix was withdrawn; only the base layer remains.
	if got := countLayers(t, dir); got != 1 {
		t.Fatalf("rejected suffix not removed: %d layers remain", got)
	}
}

func TestCheckpointEmptyStore(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("checkpoint empty store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if st := s2.Stats(); st.Keys != 0 || st.Snapshots != 0 {
		t.Fatalf("empty checkpoint reopened wrong: %+v", st)
	}
	if err := s2.Put("later", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Checkpoint(); err != nil {
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
	if got, ok, _ := s3.Get("later"); !ok || string(got) != "x" {
		t.Fatalf("commit after empty checkpoint: %q ok=%v", got, ok)
	}
}

func TestCheckpointOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("checkpoint closed store: err=%v want fs.ErrClosed", err)
	}
}

func TestCheckpointStaleTempRemovedOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("keep", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Crash between temp build and the atomic rename: a half-built temp sits
	// beside an intact checkpoint and WAL.
	if err := os.WriteFile(filepath.Join(dir, checkpointTmpName), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with stale checkpoint temp: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("keep"); !ok || string(got) != "value" {
		t.Fatalf("value lost with stale temp: %q ok=%v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale checkpoint temp not removed: %v", err)
	}
}

func TestCheckpointStaleTempWithNoLiveCheckpoint(t *testing.T) {
	// First-ever checkpoint killed after the temp landed but before the
	// rename: there is no live checkpoint, so reopen replays the WAL in full
	// and the temp is swept without needing a second checkpoint to recover.
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("keep", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointTmpName), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("keep"); !ok || string(got) != "value" {
		t.Fatalf("value lost with only a stale temp: %q ok=%v", got, ok)
	}
	if checkpointExists(t, dir) {
		t.Fatal("no live checkpoint should exist after a crashed first checkpoint")
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale temp not swept: %v", err)
	}
	// Committing and checkpointing proceed immediately, no repair step.
	if err := s2.Put("after", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after cleanup: %v", err)
	}
}

func TestCheckpointSameViewBeforeAndAfter(t *testing.T) {
	// A snapshot fixed before a checkpoint reads identically after the
	// checkpoint and after a kill-and-reopen: the checkpoint only changes
	// how reopening recovers, never any view.
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	fixed, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("v2")); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"a": "v1", "b": "v1"}
	for k, v := range want {
		if got, ok := fixed.Get(k); !ok || string(got) != v {
			t.Fatalf("fixed view of %s before reopen: %q ok=%v want %q", k, got, ok, v)
		}
	}
	// Latest view reflects both post-snapshot writes, checkpoint in between.
	if got, ok, _ := s.Get("a"); !ok || string(got) != "v2" {
		t.Fatalf("latest a: %q ok=%v", got, ok)
	}
	if got, ok, _ := s.Get("b"); !ok || string(got) != "v2" {
		t.Fatalf("latest b: %q ok=%v", got, ok)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "v2" {
		t.Fatalf("a after reopen: %q ok=%v", got, ok)
	}
	if got, ok, _ := s2.Get("b"); !ok || string(got) != "v2" {
		t.Fatalf("b after reopen: %q ok=%v", got, ok)
	}
	// The fixed snapshot even outlives its store, unchanged.
	for k, v := range want {
		if got, ok := fixed.Get(k); !ok || string(got) != v {
			t.Fatalf("fixed view of %s after reopen: %q ok=%v want %q", k, got, ok, v)
		}
	}
	fixed.Close()
}

func TestCheckpointTornBatchInTailDiscardedWholesale(t *testing.T) {
	// A kill mid-commit after a checkpoint leaves a torn framed batch as the
	// tail: reopen seeds the checkpoint and drops the whole partial batch.
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("stable", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cut := walSize(t, dir)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append the beginning of a batch: begin marker plus one inner frame but
	// no end marker.
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	lw := &walWriter{f: f, w: bufio.NewWriter(f)}
	if err := lw.encodeFrame(opBatchBegin, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := lw.encodeFrameSeq(opPutSeq, "half", 999, []byte("nope")); err != nil {
		t.Fatal(err)
	}
	if err := lw.w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("stable"); !ok || string(got) != "yes" {
		t.Fatalf("checkpoint state lost: %q ok=%v", got, ok)
	}
	if _, ok, _ := s2.Get("half"); ok {
		t.Fatal("half of a torn batch became visible after checkpoint reopen")
	}
	if got := walSize(t, dir); got != cut {
		t.Fatalf("torn tail not truncated: %d want %d", got, cut)
	}
	// Committing continues normally, with no repair step required.
	if err := s2.Put("recovered", []byte("ok")); err != nil {
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
	if got, ok, _ := s3.Get("recovered"); !ok || string(got) != "ok" {
		t.Fatalf("post-recovery commit lost: %q ok=%v", got, ok)
	}
}

func TestCompactRetiresCheckpoint(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
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
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if !checkpointExists(t, dir) {
		t.Fatal("expected checkpoint before compaction")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if checkpointExists(t, dir) {
		t.Fatal("checkpoint survived the WAL rewrite; it could pair with the wrong log")
	}
	// Pinned snapshot still reads its view through and after compaction.
	if got, ok := old.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("pinned view: %q ok=%v", got, ok)
	}
	if got, ok, _ := s.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("latest: %q ok=%v", got, ok)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Full WAL replay after the checkpoint was retired: state intact.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("after reopen post-compact: %q ok=%v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count: %d want 1", n)
	}
	// A fresh checkpoint upgrades the compacted layout again.
	if err := s2.Put("m", []byte("n")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Checkpoint(); err != nil {
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
	if got, ok, _ := s3.Get("m"); !ok || string(got) != "n" {
		t.Fatalf("checkpoint over compacted wal: %q ok=%v", got, ok)
	}
}

func TestCheckpointDoesNotReclaimPinnedHistory(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	cur, err := s.OpenCursorPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Checkpoint is not a compaction: the fixed views are untouched.
	if got, ok := snap.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("snapshot view after checkpoint: %q ok=%v", got, ok)
	}
	pages, err := cur.Page(0, 10)
	if err != nil || len(pages) != 1 || string(pages[0].Value) != "v1" {
		t.Fatalf("cursor after checkpoint: %+v err=%v", pages, err)
	}
	// And compaction right after still honors the pins.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, ok := snap.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("snapshot view after compact: %q ok=%v", got, ok)
	}
	pages, err = cur.Page(0, 10)
	if err != nil || len(pages) != 1 || string(pages[0].Value) != "v1" {
		t.Fatalf("cursor after compact: %+v err=%v", pages, err)
	}
	cur.Close()
	snap.Close()
}

func TestCheckpointConcurrentWithEverything(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	const writers = 4
	const rounds = 80
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("k%d", w)
				if err := s.CommitBatch([]BatchOp{
					{Key: key, Value: []byte(fmt.Sprintf("%d-%d", w, i))},
					{Key: "shared", Value: []byte(fmt.Sprintf("%d", i))},
				}); err != nil {
					t.Errorf("batch: %v", err)
					return
				}
				if i%13 == 0 {
					if err := s.Delete(key); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
				}
			}
		}(w)
	}
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if err := s.Checkpoint(); err != nil {
					if errors.Is(err, fs.ErrClosed) {
						return
					}
					t.Errorf("checkpoint: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			if err := s.Compact(); err != nil {
				if errors.Is(err, fs.ErrClosed) {
					return
				}
				t.Errorf("compact: %v", err)
				return
			}
		}
	}()
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
			v, err := s.Snapshot()
			if err != nil {
				if errors.Is(err, fs.ErrClosed) {
					return
				}
				t.Errorf("snapshot: %v", err)
				return
			}
			got, _ := v.Get("shared")
			if again, ok := v.Get("shared"); ok && string(again) != string(got) {
				t.Errorf("fixed snapshot view changed: %q vs %q", again, got)
			}
			cur, err := v.OpenCursorPrefix("k")
			if err == nil {
				if _, perr := cur.Page(0, 64); perr != nil {
					t.Errorf("cursor page: %v", perr)
				}
				cur.Close()
			}
			v.Close()
		}
	}()

	wg.Wait()
	close(stop)
	readers.Wait()

	// Whatever the interleaving, a final checkpoint captures complete state
	// and a reopen reproduces it exactly whether it loads the checkpoint or
	// retires it after a last compaction.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	live := dumpView(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	reopened := dumpView(t, s2)
	if len(reopened) != len(live) {
		t.Fatalf("reopened has %d pairs, live had %d", len(reopened), len(live))
	}
	for i := range live {
		if reopened[i].Key != live[i].Key || string(reopened[i].Value) != string(live[i].Value) {
			t.Fatalf("mismatch at %d: %q=%q vs %q=%q", i,
				reopened[i].Key, reopened[i].Value, live[i].Key, live[i].Value)
		}
	}
}

func TestCheckpointStatsUnchanged(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("bbbb")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("cc")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if after := s.Stats(); after != before {
		t.Fatalf("stats changed across checkpoint: before=%+v after=%+v", before, after)
	}
}
