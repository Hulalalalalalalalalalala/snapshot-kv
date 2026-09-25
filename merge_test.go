package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// buildMultiRingChain seeds s, takes a full backup into a fresh chain and
// appends rings rings, returning the chain directory. Every ring is made
// after at least one commit so non-empty callers get real deltas.
func buildMultiRingChain(t *testing.T, s *Store, rings int) string {
	t.Helper()
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("v0")},
		{Key: "gone", Value: []byte("x")},
		{Key: "keep", Value: []byte("k0")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for r := 1; r <= rings; r++ {
		ops := []BatchOp{
			{Key: "alpha", Value: []byte(fmt.Sprintf("v%d", r))},
			{Key: fmt.Sprintf("ring-%02d", r), Value: []byte("z")},
		}
		if r%2 == 0 {
			ops = append(ops, BatchOp{Key: "gone", Delete: true})
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("ring %d: %v", r, err)
		}
	}
	return chain
}

func dirFileHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := make(map[string][32]byte)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = sha256.Sum256(data)
	}
	return out
}

func assertSameFileSet(t *testing.T, got, want map[string][32]byte, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d files, want %d", label, len(got), len(want))
	}
	for name, h := range want {
		gh, ok := got[name]
		if !ok {
			t.Fatalf("%s: file %q missing after rejected merge", label, name)
		}
		if gh != h {
			t.Fatalf("%s: file %q changed by rejected merge", label, name)
		}
	}
}

func ringFiles(t *testing.T, dir string) []uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var idx []uint32
	for _, e := range entries {
		if n, ok := parseRingIndex(e.Name()); ok {
			idx = append(idx, n)
		}
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	return idx
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += fi.Size()
	}
	return total
}

func TestMergeChainEquivalence(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 4)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots
	sizeBefore := dirSize(t, chain)
	oldCopy := copyChain(t, chain)

	// Fold the first two rings; rings 3 and 4 survive renumbered as 1 and 2.
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("surviving rings = %v, want [1 2]", got)
	}
	if size := dirSize(t, chain); size >= sizeBefore {
		t.Fatalf("chain dir did not shrink: %d >= %d", size, sizeBefore)
	}

	// The merged chain restores to exactly the pre-merge chain's tip, live and
	// reopened, with the same cumulative snapshot count.
	rs, _ := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()

	// And it matches byte-for-byte a restore of the untouched old chain.
	oldRS, err := Restore(oldCopy, filepath.Join(t.TempDir(), "old"))
	if err != nil {
		t.Fatal(err)
	}
	oldPairs := allPairs(t, oldRS)
	if n := oldRS.Stats().Snapshots; n != snaps {
		t.Fatalf("old chain snapshots = %d, want %d", n, snaps)
	}
	oldRS.Close()
	mergedRS, err := Restore(chain, filepath.Join(t.TempDir(), "new"))
	if err != nil {
		t.Fatal(err)
	}
	assertSamePairs(t, allPairs(t, mergedRS), oldPairs, "merged vs old chain")
	mergedRS.Close()
}

func TestMergeChainFoldAll(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 3)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots

	if err := s.MergeChain(chain, 3); err != nil {
		t.Fatalf("merge all: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings left after full fold: %v", got)
	}
	// A head-only artifact must restore through the head-only path unchanged.
	rs, target := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()

	// A deleted key folded into the new head never resurrects.
	chk, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer chk.Close()
	if _, ok, _ := chk.Get("gone"); ok {
		t.Fatal("deleted key resurrected by full fold")
	}
	if got, ok, _ := chk.Get("alpha"); !ok || string(got) != "v3" {
		t.Fatalf("alpha = %q ok=%v, want v3", got, ok)
	}

	// Appending onto a fully folded chain links ring 1 to the fresh head.
	if err := s.Put("after", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("append after full fold: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 || got[0] != 1 {
		t.Fatalf("rings after append = %v, want [1]", got)
	}
	rs2, _ := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs2.Close()
}

func TestMergeChainPreservesDeletedOverwrites(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("head")},
		{Key: "b", Value: []byte("head")},
		{Key: "c", Value: []byte("head")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// Ring 1 overwrites a, deletes b, leaves c.
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("ring1")},
		{Key: "b", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	// Fold ring 1 into the head: its deletes and overwrites must be terminal.
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatal(err)
	}
	rs, target := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs.Close()
	chk, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer chk.Close()
	if got, ok, _ := chk.Get("a"); !ok || string(got) != "ring1" {
		t.Fatalf("a = %q ok=%v, want ring1", got, ok)
	}
	if _, ok, _ := chk.Get("b"); ok {
		t.Fatal("b resurrected after fold")
	}
	if got, ok, _ := chk.Get("c"); !ok || string(got) != "head" {
		t.Fatalf("c = %q ok=%v, want head", got, ok)
	}
}

func TestMergeChainRepeated(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 2)
	for cycle := 0; cycle < 6; cycle++ {
		if err := s.CommitBatch([]BatchOp{
			{Key: "alpha", Value: []byte(fmt.Sprintf("c%d", cycle))},
			{Key: fmt.Sprintf("cyc-%d", cycle), Value: []byte("y")},
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("append cycle %d: %v", cycle, err)
		}
		if err := s.MergeChain(chain, 1); err != nil {
			t.Fatalf("merge cycle %d: %v", cycle, err)
		}
		if got := ringFiles(t, chain); len(got) != 2 {
			t.Fatalf("cycle %d: rings %v, want 2", cycle, got)
		}
		// The chain stays restorable and exactly equals the store after every
		// cycle.
		rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
		if err != nil {
			t.Fatalf("restore cycle %d: %v", cycle, err)
		}
		assertSamePairs(t, allPairs(t, rs), allPairs(t, s), fmt.Sprintf("cycle %d", cycle))
		rs.Close()
	}
}

func TestMergeChainMultiSegmentSurvivors(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	// A head spanning three segments.
	headOps := make([]BatchOp, backupEntriesPerSegment*2+13)
	for i := range headOps {
		headOps[i] = BatchOp{Key: fmt.Sprintf("k%06d", i), Value: []byte("h")}
	}
	if err := s.CommitBatch(headOps); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// Two large rings, each spanning multiple segments.
	mkRing := func(off int) {
		ops := make([]BatchOp, backupEntriesPerSegment+250)
		for i := range ops {
			ops[i] = BatchOp{Key: fmt.Sprintf("k%06d", (i+off)%(backupEntriesPerSegment*3)), Value: []byte(fmt.Sprintf("r%d", off))}
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}
	mkRing(0)
	mkRing(100)
	// Ring 2 (the multi-segment survivor) must be re-segmented with correct
	// cumulative counts and CRCs.
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge: %v", err)
	}
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs.Close()

	// Fold again until the chain is head-only: every segment boundary is
	// re-walked.
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("second merge: %v", err)
	}
	rs2, _ := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs2.Close()
}

func TestMergeChainEmptyRings(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil { // empty ring
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil { // empty ring advancing snaps
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("merge empty rings: %v", err)
	}
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), 1)
	rs.Close()
}

func TestMergeChainSnapshotUnaffected(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 3)
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	before := pairsFromSnapshot(t, snap)
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatal(err)
	}
	after := pairsFromSnapshot(t, snap)
	assertSamePairs(t, after, before, "snapshot view across merge")
	snap.Close()

	// The backed-up store itself is untouched: live pairs and stats match.
	if got, ok, _ := s.Get("alpha"); !ok || string(got) != "v3" {
		t.Fatalf("store changed by merge: alpha=%q ok=%v", got, ok)
	}
}

func TestMergeChainRejectsBadArguments(t *testing.T) {
	s := openStore(t, tempDir(t))
	master := buildMultiRingChain(t, s, 2)

	// A rejected merge leaves its chain byte-for-byte untouched; assert that on
	// a fresh copy each time and confirm the copy still restores afterwards.
	check := func(name string, src string, dir string, rings int) {
		t.Helper()
		before := dirFileHashes(t, src)
		err := s.MergeChain(dir, rings)
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("%s: err=%v, want fs.ErrInvalid", name, err)
		}
		if dir == src {
			assertSameFileSet(t, dirFileHashes(t, src), before, name)
		}
		rs, rerr := Restore(src, filepath.Join(t.TempDir(), "still-good"))
		if rerr != nil {
			t.Fatalf("%s: chain not restorable after rejection: %v", name, rerr)
		}
		rs.Close()
	}

	check("zero rings", master, copyChain(t, master), 0)
	check("negative rings", master, copyChain(t, master), -1)
	check("too many rings", master, copyChain(t, master), 3)
	check("missing dir", master, filepath.Join(t.TempDir(), "nope"), 1)

	filePath := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("regular file", master, filePath, 1)
	check("empty dir", master, t.TempDir(), 1)
	check("empty path", master, "", 1)
}

func TestMergeChainRejectsDamage(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < backupEntriesPerSegment+5; i++ {
		if err := s.Put(fmt.Sprintf("k%06d", i), []byte("p")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "good")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 3; r++ {
		if err := s.Put(fmt.Sprintf("k%06d", r), []byte("changed")); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	expectReject := func(t *testing.T, d string) {
		t.Helper()
		before := dirFileHashes(t, d)
		if err := s.MergeChain(d, 2); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("merge err=%v, want fs.ErrInvalid", err)
		}
		assertSameFileSet(t, dirFileHashes(t, d), before, "rejected merge")
	}

	t.Run("missing first ring", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.Remove(filepath.Join(d, ringName(1))); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("missing middle ring", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.Remove(filepath.Join(d, ringName(2))); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("truncated ring", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(1))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data[:len(data)/2], 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("flipped byte", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(2))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[incrRingHeaderSize+incrSegHeaderSize+8] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("broken link", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(2))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[12] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("discontinuous watermark", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(2))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(data[16:24], binary.LittleEndian.Uint64(data[16:24])+1)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("unknown version", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(1))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(data[4:8], backupVersion+42)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("missing manifest", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.Remove(filepath.Join(d, backupManifest)); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("foreign file", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.WriteFile(filepath.Join(d, "stray.txt"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
}

func TestMergeChainEmptyHead(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil { // empty full head
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: nil},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil { // ring adds keys
		t.Fatal(err)
	}
	want := allPairs(t, s)
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge empty head: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings left: %v", got)
	}
	rs, _ := restoreCheckpoint(t, chain, want, 0)
	rs.Close()
}

func TestMergeChainSurvivorCompactionTombstone(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 40; i++ {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// Ring 1 deletes half the head keys and compacts them out of memory.
	for i := 0; i < 40; i += 2 {
		if err := s.Delete(fmt.Sprintf("k%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	// Ring 2 survives the merge and keeps touching unrelated keys.
	for i := 1; i < 40; i += 4 {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge: %v", err)
	}
	rs, target := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs.Close()
	chk, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer chk.Close()
	for i := 0; i < 40; i += 2 {
		if _, ok, _ := chk.Get(fmt.Sprintf("k%02d", i)); ok {
			t.Fatalf("deleted k%02d resurrected after merge", i)
		}
	}
}

func TestMergeChainClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed merge: %v", err)
	}
}

func TestMergeChainDebrisSwept(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 1)
	want := allPairs(t, s)

	// A leftover staging directory from a kill is pure debris.
	stage := mergeStagePath(chain)
	if err := os.MkdirAll(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "junk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge over staging debris: %v", err)
	}
	if _, err := os.Stat(stage); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris left behind")
	}
	rs, _ := restoreCheckpoint(t, chain, want, s.Stats().Snapshots)
	rs.Close()

	// A kill between the two swap renames parks the live chain and removes the
	// chain name; the next operation heals the parked chain back.
	obs := mergeObsPath(chain)
	if err := os.Rename(chain, obs); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(chain); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("chain should be absent: %v", err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("heal through incremental: %v", err)
	}
	if _, err := os.Stat(obs); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("parked chain not cleared after heal")
	}
	rs2, err := Restore(chain, filepath.Join(t.TempDir(), "healed"))
	if err != nil {
		t.Fatalf("restored healed chain: %v", err)
	}
	rs2.Close()

	// A parked copy beside a complete new chain is discarded, including by
	// Restore.
	if err := os.MkdirAll(obs, 0o700); err != nil {
		t.Fatal(err)
	}
	rs3, err := Restore(chain, filepath.Join(t.TempDir(), "after-obs"))
	if err != nil {
		t.Fatalf("restore discards obs: %v", err)
	}
	rs3.Close()
	if _, err := os.Stat(obs); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("Restore did not clear parked copy")
	}
}

func TestMergeChainOpenHealsParkedChain(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 1)
	want := dirFileHashes(t, chain)

	// Kill between the two swap renames: the chain name is gone, parked aside
	// as its sibling <base>.merge.obs.
	obs := mergeObsPath(chain)
	if err := os.Rename(chain, obs); err != nil {
		t.Fatal(err)
	}

	// Opening the missing chain name heals the parked directory back into
	// place instead of creating an empty directory over it. (Open then seeds an
	// empty store there, so the artifact gains a wal.log; the healed chain
	// files are what matter.)
	st, err := Open(chain)
	if err != nil {
		t.Fatalf("open heals parked chain: %v", err)
	}
	st.Close()
	got := dirFileHashes(t, chain)
	for name, h := range want {
		if got[name] != h {
			t.Fatalf("healed file %q missing or changed", name)
		}
	}
	if _, err := os.Stat(obs); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("parked copy left after open heal")
	}
}

func TestMergeChainConcurrentExports(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := buildMultiRingChain(t, s, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.CommitBatch([]BatchOp{
				{Key: fmt.Sprintf("w%05d", i), Value: []byte("x")},
				{Key: "alpha", Value: []byte(fmt.Sprintf("w%d", i))},
			})
		}
	}()

	// Interleave merges and incremental exports; the mutex must keep the chain
	// whole at every step.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("concurrent incremental: %v", err)
		}
		if err := s.MergeChain(chain, 1); err != nil {
			t.Fatalf("concurrent merge: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	// One last export then a full-chain validation via Restore.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "final"))
	if err != nil {
		t.Fatalf("final chain invalid: %v", err)
	}
	assertSamePairs(t, allPairs(t, rs), allPairs(t, s), "concurrent final state")
	rs.Close()
}

func TestMergeChainReadsStayLockFree(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 200; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := buildMultiRingChain(t, s, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ops := make([]BatchOp, 1500)
			for i := range ops {
				ops[i] = BatchOp{Key: fmt.Sprintf("bulk-%05d", i), Value: []byte("pp")}
			}
			if err := s.CommitBatch(ops); err != nil {
				return
			}
			if err := s.BackupIncremental(chain); err != nil {
				return
			}
			if err := s.MergeChain(chain, 1); err != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	var reads int
	for time.Now().Before(deadline) {
		if _, ok, err := s.Get("k0001"); err != nil || !ok {
			t.Fatalf("read during merge: ok=%v err=%v", ok, err)
		}
		reads++
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("no reads completed during merges")
	}
}
