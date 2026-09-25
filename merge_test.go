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

// ringFiles lists the finished ring files in a chain directory, in order.
func ringFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var rings []string
	for _, e := range entries {
		if _, ok := parseRingIndex(e.Name()); ok {
			rings = append(rings, e.Name())
		}
	}
	return rings
}

// dirSize sums the sizes of the regular files directly inside dir.
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
		if info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total
}

// mergeDebris lists merge staging/trash siblings next to dir.
func mergeDebris(t *testing.T, dir string) []string {
	t.Helper()
	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var junk []string
	for _, e := range entries {
		name := e.Name()
		if isMergeTempName(name, base, mergeStagePrefix) ||
			isMergeTempName(name, base, mergeTrashPrefix) {
			junk = append(junk, name)
		}
	}
	return junk
}

// buildChain writes a full backup of s into chain and then rings incremental
// exports, each preceded by a batch of the given ops and one snapshot. It
// returns the live pairs and snapshot count after every stage, head first.
func buildChain(t *testing.T, s *Store, chain string, ringOps [][]BatchOp) ([]KVPair, uint64) {
	t.Helper()
	if err := s.Backup(chain); err != nil {
		t.Fatalf("backup: %v", err)
	}
	for i, ops := range ringOps {
		if len(ops) > 0 {
			if err := s.CommitBatch(ops); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("incremental %d: %v", i+1, err)
		}
	}
	return allPairs(t, s), s.Stats().Snapshots
}

func TestMergeChainRoundtrip(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one")},
		{Key: "beta", Value: []byte("b")},
		{Key: "gone", Value: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}

	chain := filepath.Join(t.TempDir(), "chain")
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{
		{
			{Key: "alpha", Value: []byte("two")},
			{Key: "gone", Delete: true},
		},
		{
			{Key: "beta", Delete: true},
			{Key: "gamma", Value: []byte("g")},
		},
		{
			{Key: "alpha", Value: []byte("three")},
			{Key: "delta", Value: []byte("d")},
		},
	})

	// Baseline: the unmerged chain restores to the live state.
	preArt := copyChain(t, chain)
	preArt2 := copyChain(t, chain)
	rs, _ := restoreCheckpoint(t, preArt, pairs, snaps)
	rs.Close()

	before, err := validateBackupChain(chain)
	if err != nil {
		t.Fatal(err)
	}
	if before.ringCount != 3 {
		t.Fatalf("ringCount = %d, want 3", before.ringCount)
	}

	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}

	// The merged chain is one head plus the last ring, renumbered from 1, and
	// validates as a whole with the same tip watermark and snapshot count.
	after, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("merged chain invalid: %v", err)
	}
	if after.ringCount != 1 {
		t.Fatalf("merged ringCount = %d, want 1", after.ringCount)
	}
	if after.endWM != before.endWM || after.snaps != before.snaps {
		t.Fatalf("tip moved: (%d,%d) -> (%d,%d)",
			before.endWM, before.snaps, after.endWM, after.snaps)
	}
	if got := ringFiles(t, chain); len(got) != 1 || got[0] != ringName(1) {
		t.Fatalf("rings after merge = %v, want [%s]", got, ringName(1))
	}
	if junk := mergeDebris(t, chain); len(junk) != 0 {
		t.Fatalf("merge debris left behind: %v", junk)
	}

	// The merged chain restores to exactly the pre-merge state and count.
	rs2, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs2.Close()

	// The pre-merge copy is untouched and still restores the same state.
	rs3, _ := restoreCheckpoint(t, preArt2, pairs, snaps)
	rs3.Close()

	// The chain keeps growing: a new incremental lands as ring 2 and the
	// extended chain restores the new live state.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("four")},
		{Key: "gamma", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after merge: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 2 {
		t.Fatalf("rings after continued export = %v, want 2", got)
	}
	rs4, _ := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs4.Close()
}

func TestMergeChainAllRings(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{
		{{Key: "a", Value: []byte("10")}},
		{{Key: "b", Delete: true}},
	})

	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings after full merge = %v, want none", got)
	}
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("merged chain invalid: %v", err)
	}
	if art.ringCount != 0 {
		t.Fatalf("ringCount = %d, want 0", art.ringCount)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()

	// A head-only merged chain accepts a fresh ring 1.
	if err := s.CommitBatch([]BatchOp{{Key: "c", Value: []byte("3")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after full merge: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 || got[0] != ringName(1) {
		t.Fatalf("rings = %v, want [%s]", got, ringName(1))
	}
	rs2, _ := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs2.Close()
}

func TestMergeChainTerminalState(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	buildChain(t, s, chain, [][]BatchOp{
		{
			{Key: "a", Value: []byte("10")},
			{Key: "b", Delete: true},
		},
		{
			{Key: "a", Delete: true},
			{Key: "c", Value: []byte("30")},
			{Key: "d", Value: []byte("40")},
		},
		{
			{Key: "e", Value: []byte("50")},
		},
	})

	// Merging the first two rings folds their terminal state into the head:
	// a and b stay deleted, c and d carry their ring-2 values, e arrives with
	// the surviving ring.
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	want := []KVPair{
		{Key: "c", Value: []byte("30")},
		{Key: "d", Value: []byte("40")},
		{Key: "e", Value: []byte("50")},
	}
	rs, dir := restoreCheckpoint(t, chain, want, s.Stats().Snapshots)
	rs.Close()
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, k := range []string{"a", "b"} {
		if _, ok, _ := reopened.Get(k); ok {
			t.Fatalf("deleted key %q resurrected after merge", k)
		}
	}
}

func TestMergeChainRejects(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{{Key: "k", Value: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
		{{Key: "k", Value: []byte("v3")}},
	})

	plainFile := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(plainFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	emptyDir := t.TempDir()
	headOnly := filepath.Join(t.TempDir(), "headonly")
	if err := s.Backup(headOnly); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		dir   string
		rings int
	}{
		{"zero rings", chain, 0},
		{"negative rings", chain, -1},
		{"too many rings", chain, 3},
		{"empty path", "", 1},
		{"missing directory", filepath.Join(t.TempDir(), "nope"), 1},
		{"not a directory", plainFile, 1},
		{"no head", emptyDir, 1},
		{"no rings on chain", headOnly, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := s.MergeChain(tc.dir, tc.rings)
			if err == nil {
				t.Fatal("MergeChain succeeded, want rejection")
			}
			if !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("error %v is not fs.ErrInvalid", err)
			}
		})
	}

	// Every rejection left the chain exactly as it was.
	if got := ringFiles(t, chain); len(got) != 2 {
		t.Fatalf("rings after rejections = %v, want 2", got)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()
	if junk := mergeDebris(t, chain); len(junk) != 0 {
		t.Fatalf("rejected merges left debris: %v", junk)
	}

	// A closed store rejects with fs.ErrClosed.
	closed := openStore(t, tempDir(t))
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.MergeChain(chain, 1); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed store merge = %v, want fs.ErrClosed", err)
	}
}

func TestMergeChainCorruption(t *testing.T) {
	newChain := func(t *testing.T) (*Store, string) {
		s := openStore(t, tempDir(t))
		if err := s.CommitBatch([]BatchOp{
			{Key: "x", Value: []byte("1")},
			{Key: "y", Value: []byte("2")},
		}); err != nil {
			t.Fatal(err)
		}
		chain := filepath.Join(t.TempDir(), "chain")
		buildChain(t, s, chain, [][]BatchOp{
			{{Key: "x", Value: []byte("10")}},
			{{Key: "y", Delete: true}},
		})
		return s, chain
	}
	chainFiles := func(t *testing.T, dir string) []string {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names
	}

	// A bit-flipped ring, a truncated ring and an unknown ring version each
	// reject the merge wholesale and leave the chain directory untouched.
	t.Run("checksum", func(t *testing.T) {
		s, chain := newChain(t)
		ring := filepath.Join(chain, ringName(1))
		data, err := os.ReadFile(ring)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)/2] ^= 0xff
		if err := os.WriteFile(ring, data, 0o600); err != nil {
			t.Fatal(err)
		}
		want := chainFiles(t, chain)
		if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("merge over corrupt ring = %v, want fs.ErrInvalid", err)
		}
		if got := chainFiles(t, chain); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("chain changed after rejection: %v -> %v", want, got)
		}
		if junk := mergeDebris(t, chain); len(junk) != 0 {
			t.Fatalf("debris left: %v", junk)
		}
	})
	t.Run("truncated", func(t *testing.T) {
		s, chain := newChain(t)
		ring := filepath.Join(chain, ringName(2))
		info, err := os.Stat(ring)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(ring, info.Size()/2); err != nil {
			t.Fatal(err)
		}
		if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("merge over truncated ring = %v, want fs.ErrInvalid", err)
		}
		if junk := mergeDebris(t, chain); len(junk) != 0 {
			t.Fatalf("debris left: %v", junk)
		}
	})
	t.Run("unknown version", func(t *testing.T) {
		s, chain := newChain(t)
		ring := filepath.Join(chain, ringName(1))
		data, err := os.ReadFile(ring)
		if err != nil {
			t.Fatal(err)
		}
		data[4] = 0x7f // version byte
		if err := os.WriteFile(ring, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("merge over unknown version = %v, want fs.ErrInvalid", err)
		}
		if junk := mergeDebris(t, chain); len(junk) != 0 {
			t.Fatalf("debris left: %v", junk)
		}
	})
	t.Run("missing ring", func(t *testing.T) {
		s, chain := newChain(t)
		if err := os.Remove(filepath.Join(chain, ringName(1))); err != nil {
			t.Fatal(err)
		}
		if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("merge over broken chain = %v, want fs.ErrInvalid", err)
		}
		if junk := mergeDebris(t, chain); len(junk) != 0 {
			t.Fatalf("debris left: %v", junk)
		}
	})
}

func TestMergeChainEmptyRings(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{{Key: "k", Value: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	// Two empty rings (snapshots but no writes) still advance the count.
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{nil, nil})
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings = %v, want none", got)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()

	// An empty head with empty rings merges to an empty head; only the
	// cumulative snapshot count carries.
	s2 := openStore(t, tempDir(t))
	chain2 := filepath.Join(t.TempDir(), "chain2")
	if err := s2.Backup(chain2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s2.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s2.BackupIncremental(chain2); err != nil {
			t.Fatal(err)
		}
	}
	if err := s2.MergeChain(chain2, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	rs2, _ := restoreCheckpoint(t, chain2, nil, s2.Stats().Snapshots)
	rs2.Close()
}

func TestMergeChainDirectoryShrinks(t *testing.T) {
	s := openStore(t, tempDir(t))
	// A wide head plus three rings that each rewrite every key: the merged
	// head holds one terminal value per key, so the chain must shrink.
	var head []BatchOp
	for i := 0; i < 300; i++ {
		head = append(head, BatchOp{Key: fmt.Sprintf("key-%04d", i), Value: []byte("head-value")})
	}
	if err := s.CommitBatch(head); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	var ringOps [][]BatchOp
	for r := 0; r < 3; r++ {
		var ops []BatchOp
		for i := 0; i < 300; i++ {
			ops = append(ops, BatchOp{
				Key:   fmt.Sprintf("key-%04d", i),
				Value: []byte(fmt.Sprintf("ring-%d-value", r)),
			})
		}
		ringOps = append(ringOps, ops)
	}
	pairs, snaps := buildChain(t, s, chain, ringOps)
	before := dirSize(t, chain)
	if err := s.MergeChain(chain, 3); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	after := dirSize(t, chain)
	if after >= before {
		t.Fatalf("chain did not shrink: %d -> %d", before, after)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()
}

func TestMergeChainDebrisSwept(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{{Key: "k", Value: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
	})

	// Staging debris from a killed merge is swept by the next merge.
	junk := filepath.Join(parent, "chain.merge-new-junk")
	if err := os.MkdirAll(junk, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(junk, "partial"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	if junk := mergeDebris(t, chain); len(junk) != 0 {
		t.Fatalf("debris not swept: %v", junk)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()

	// A kill between the two commit renames (chain missing, old chain whole
	// in the trash sibling) is repaired by the next operation on the chain.
	chain2 := filepath.Join(parent, "chain2")
	pairs2, snaps2 := buildChain(t, s, chain2, [][]BatchOp{
		{{Key: "k", Value: []byte("v3")}},
	})
	trash := filepath.Join(parent, "chain2.merge-old-dead")
	if err := os.Rename(chain2, trash); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain2, 1); err != nil {
		t.Fatalf("MergeChain after simulated kill: %v", err)
	}
	if junk := mergeDebris(t, chain2); len(junk) != 0 {
		t.Fatalf("trash not swept: %v", junk)
	}
	rs2, _ := restoreCheckpoint(t, chain2, pairs2, snaps2)
	rs2.Close()
}

func TestMergeChainEmptyHead(t *testing.T) {
	// A backup of an empty store is a legal head; rings built on it merge
	// into a head that holds exactly the ring state.
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	pairs, snaps := buildChain(t, s, chain, [][]BatchOp{
		{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2")},
		},
		{
			{Key: "a", Delete: true},
			{Key: "c", Value: []byte("3")},
		},
	})
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings = %v, want none", got)
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()
}

func TestMergeChainSegmentedHead(t *testing.T) {
	// The merged head crosses segment boundaries: more live keys than one
	// segment holds, spread across the head and several rings.
	s := openStore(t, tempDir(t))
	put := func(prefix string, from, to int) {
		var ops []BatchOp
		for i := from; i < to; i++ {
			ops = append(ops, BatchOp{
				Key:   fmt.Sprintf("%s-%05d", prefix, i),
				Value: []byte(fmt.Sprintf("%s-v%d", prefix, i)),
			})
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
	}
	put("head", 0, 5000)
	chain := filepath.Join(t.TempDir(), "chain")
	// Ring 1 rewrites half the head keys and deletes a slice of them.
	var r1 []BatchOp
	for i := 0; i < 2500; i++ {
		r1 = append(r1, BatchOp{Key: fmt.Sprintf("head-%05d", i), Value: []byte("r1")})
	}
	for i := 2500; i < 3000; i++ {
		r1 = append(r1, BatchOp{Key: fmt.Sprintf("head-%05d", i), Delete: true})
	}
	pairs, snaps := func() ([]KVPair, uint64) {
		if err := s.Backup(chain); err != nil {
			t.Fatal(err)
		}
		if err := s.CommitBatch(r1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
		put("extra", 0, 4500)
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
		return allPairs(t, s), s.Stats().Snapshots
	}()

	// Merging both rings folds the rewritten head keys, the deletes and the
	// extras into one segmented head.
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("MergeChain: %v", err)
	}
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("merged chain invalid: %v", err)
	}
	if art.ringCount != 0 {
		t.Fatalf("ringCount = %d, want 0", art.ringCount)
	}
	// 5000 - 500 deleted + 4500 extras = 9000 live keys over three segments.
	if art.head.total != 9000 {
		t.Fatalf("merged head total = %d, want 9000", art.head.total)
	}
	if len(art.head.segments) != 3 {
		t.Fatalf("merged head segments = %d, want 3", len(art.head.segments))
	}
	rs, _ := restoreCheckpoint(t, chain, pairs, snaps)
	rs.Close()
}

func TestMergeChainLongChain(t *testing.T) {
	// A long chain merges correctly end to end, both in one shot and one ring
	// at a time. The implementation streams; this guards correctness as the
	// chain grows rather than measuring memory.
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	const rings = 30
	for r := 0; r < rings; r++ {
		ops := []BatchOp{
			{Key: fmt.Sprintf("rot-%03d", r%5), Value: []byte(fmt.Sprintf("r%d", r))},
			{Key: fmt.Sprintf("uniq-%03d", r), Value: []byte("u")},
		}
		if r%3 == 0 {
			ops = append(ops, BatchOp{Key: fmt.Sprintf("uniq-%03d", r), Delete: true})
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
	pairs, snaps := allPairs(t, s), s.Stats().Snapshots

	// One shot: fold the whole chain into a lone head.
	oneShot := copyChain(t, chain)
	if err := s.MergeChain(oneShot, rings); err != nil {
		t.Fatalf("one-shot merge: %v", err)
	}
	if got := ringFiles(t, oneShot); len(got) != 0 {
		t.Fatalf("one-shot rings = %v, want none", got)
	}
	rs, _ := restoreCheckpoint(t, oneShot, pairs, snaps)
	rs.Close()

	// Piecemeal: merge the oldest ring into the head, thirty times over.
	piecemeal := copyChain(t, chain)
	for r := 0; r < rings; r++ {
		if err := s.MergeChain(piecemeal, 1); err != nil {
			t.Fatalf("piecemeal merge %d: %v", r, err)
		}
	}
	if got := ringFiles(t, piecemeal); len(got) != 0 {
		t.Fatalf("piecemeal rings = %v, want none", got)
	}
	rs2, _ := restoreCheckpoint(t, piecemeal, pairs, snaps)
	rs2.Close()

	// Halving the chain keeps the back half of the rings, renumbered.
	halved := copyChain(t, chain)
	if err := s.MergeChain(halved, rings/2); err != nil {
		t.Fatalf("halving merge: %v", err)
	}
	if got := ringFiles(t, halved); len(got) != rings/2 {
		t.Fatalf("halved rings = %d, want %d", len(got), rings/2)
	}
	rs3, _ := restoreCheckpoint(t, halved, pairs, snaps)
	rs3.Close()
}

func TestMergeChainConcurrentActivity(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.CommitBatch([]BatchOp{{Key: "seed", Value: []byte("0")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Foreground writers, snapshotters, checkpoints and compactions run
	// while exports and merges reshape the chain.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.CommitBatch([]BatchOp{
					{Key: fmt.Sprintf("w%d-%04d", w, i%64), Value: []byte(fmt.Sprintf("%d", i))},
				})
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if snap, err := s.Snapshot(); err == nil {
				snap.Close()
			}
			_ = s.Checkpoint()
			_ = s.Compact()
		}
	}()

	// Exports and merges on this store are serialized with each other but
	// never block the foreground above.
	for i := 0; i < 20; i++ {
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("incremental %d: %v", i, err)
		}
		if err := s.MergeChain(chain, 1); err != nil {
			t.Fatalf("merge %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()

	// One last export brings the chain to the live state; it must restore
	// exactly, with the cumulative snapshot count intact.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs.Close()
	if junk := mergeDebris(t, chain); len(junk) != 0 {
		t.Fatalf("debris left: %v", junk)
	}
}
