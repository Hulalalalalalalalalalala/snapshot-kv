package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// fileDigest is one regular file's name and size/content hash, for asserting
// a chain directory is byte-for-byte unchanged.
type fileDigest struct {
	name string
	size int64
	data []byte
}

func digestDir(t *testing.T, dir string) []fileDigest {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []fileDigest
	for _, e := range entries {
		if e.IsDir() {
			t.Fatalf("unexpected directory %q in %s", e.Name(), dir)
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fileDigest{name: e.Name(), size: int64(len(data)), data: append([]byte(nil), data...)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func assertDigestsEqual(t *testing.T, got, want []fileDigest, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d files, want %d", label, len(got), len(want))
	}
	for i := range want {
		if got[i].name != want[i].name || got[i].size != want[i].size ||
			string(got[i].data) != string(want[i].data) {
			t.Fatalf("%s: file %q changed", label, want[i].name)
		}
	}
}

// diagDirs returns the diagnostic siblings next to chain.
func diagDirs(t *testing.T, chain string) []string {
	t.Helper()
	parent := filepath.Dir(chain)
	base := filepath.Base(chain)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		p := base + verifyDiagPrefix
		if e.IsDir() && len(e.Name()) > len(p) && e.Name()[:len(p)] == p {
			out = append(out, filepath.Join(parent, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// buildStagedChain writes a head and rings incremental exports, returning the
// live pair/snapshot state captured after the head and after each ring.
func buildStagedChain(t *testing.T, s *Store, chain string, ringOps [][]BatchOp) ([]struct {
	pairs []KVPair
	snaps uint64
}, string) {
	t.Helper()
	if err := s.Backup(chain); err != nil {
		t.Fatalf("backup: %v", err)
	}
	stages := []struct {
		pairs []KVPair
		snaps uint64
	}{{allPairs(t, s), s.Stats().Snapshots}}
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
		stages = append(stages, struct {
			pairs []KVPair
			snaps uint64
		}{allPairs(t, s), s.Stats().Snapshots})
	}
	return stages, chain
}

// corruptRing flips one byte of the named ring in place.
func corruptRing(t *testing.T, chain string, idx uint32) {
	t.Helper()
	p := filepath.Join(chain, ringName(idx))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// restoreExpect restores chain, asserts its live state and snapshot count,
// closes it and reopens the target to assert the same again.
func restoreExpect(t *testing.T, chain string, want []KVPair, snaps uint64) {
	t.Helper()
	rs, target := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()
	re, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	re.Close()
}

func TestVerifyChainIntactUnchanged(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	headOnly := filepath.Join(t.TempDir(), "head")
	if err := s.Backup(headOnly); err != nil {
		t.Fatal(err)
	}
	for _, chain := range []string{headOnly} {
		before := digestDir(t, chain)
		if err := s.VerifyChain(chain); err != nil {
			t.Fatalf("verify head-only: %v", err)
		}
		assertDigestsEqual(t, digestDir(t, chain), before, "head-only chain after verify")
		if junk := diagDirs(t, chain); len(junk) != 0 {
			t.Fatalf("intact verify created diagnostics: %v", junk)
		}
	}

	chain := filepath.Join(t.TempDir(), "chain")
	buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
		nil, // empty ring, still advances the snapshot count
		{{Key: "other", Value: []byte("x")}},
	})
	before := digestDir(t, chain)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify chain: %v", err)
	}
	assertDigestsEqual(t, digestDir(t, chain), before, "chain after verify")
	// A second verification is an idempotent no-op.
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("second verify: %v", err)
	}
	assertDigestsEqual(t, digestDir(t, chain), before, "chain after second verify")
	if junk := diagDirs(t, chain); len(junk) != 0 {
		t.Fatalf("intact verify created diagnostics: %v", junk)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatalf("restore after verify: %v", err)
	}
	rs.Close()
}

func TestVerifyChainIsolatesCorruptRing(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("a0")},
		{Key: "b", Value: []byte("b0")},
		{Key: "c", Value: []byte("c0")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	stages, _ := buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "a", Value: []byte("a1")}},
		{{Key: "b", Delete: true}},
		{{Key: "c", Value: []byte("c3")}, {Key: "d", Value: []byte("d3")}},
		{{Key: "d", Value: []byte("d4")}},
	})

	// Snapshot the bytes of every ring before damaging ring 2.
	originals := map[string][]byte{}
	for idx := uint32(1); idx <= 4; idx++ {
		name := ringName(idx)
		b, err := os.ReadFile(filepath.Join(chain, name))
		if err != nil {
			t.Fatal(err)
		}
		originals[name] = b
	}
	corruptRing(t, chain, 2)
	// The damaged bytes are exactly what must be preserved.
	damagedRing2, err := os.ReadFile(filepath.Join(chain, ringName(2)))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify downgraded unexpectedly with error: %v", err)
	}

	// The chain now holds the head and ring 1 only.
	if got := ringFiles(t, chain); len(got) != 1 || got[0] != ringName(1) {
		t.Fatalf("rings after downgrade = %v, want only %s", got, ringName(1))
	}
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("downgraded chain does not validate: %v", err)
	}
	// It restores to exactly the state at the last intact watermark, snapshot
	// count included.
	restoreExpect(t, chain, stages[1].pairs, stages[1].snaps)

	// The isolated artifacts sit in exactly one diagnostic sibling, byte for
	// byte as they were when verification ran.
	diags := diagDirs(t, chain)
	if len(diags) != 1 {
		t.Fatalf("diagnostic dirs = %v, want one", diags)
	}
	wantIsolated := map[string]bool{ringName(2): true, ringName(3): true, ringName(4): true}
	entries, err := os.ReadDir(diags[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(wantIsolated) {
		t.Fatalf("diag holds %v, want %v", entrNames(entries), wantIsolated)
	}
	for _, e := range entries {
		if !wantIsolated[e.Name()] {
			t.Fatalf("unexpected diagnostic file %q", e.Name())
		}
		got, err := os.ReadFile(filepath.Join(diags[0], e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var want []byte
		if e.Name() == ringName(2) {
			want = damagedRing2 // the damaged ring is preserved as damaged
		} else {
			want = originals[e.Name()]
		}
		if string(got) != string(want) {
			t.Fatalf("diagnostic copy of %q differs from the isolated bytes", e.Name())
		}
	}

	// A further verification of the healed chain is a no-op.
	before := digestDir(t, chain)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify healed chain: %v", err)
	}
	assertDigestsEqual(t, digestDir(t, chain), before, "healed chain after re-verify")
}

func entrNames(es []os.DirEntry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func TestVerifyChainCorruptLastRing(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	stages, _ := buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
		{{Key: "k", Value: []byte("v3")}},
	})
	corruptRing(t, chain, 2)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 {
		t.Fatalf("rings after last-ring downgrade = %v", got)
	}
	restoreExpect(t, chain, stages[1].pairs, stages[1].snaps)
	diags := diagDirs(t, chain)
	if len(diags) != 1 {
		t.Fatalf("diag dirs = %v", diags)
	}
	entries, err := os.ReadDir(diags[0])
	if err != nil || len(entries) != 1 || entries[0].Name() != ringName(2) {
		t.Fatalf("diag contents = %v, err=%v", entrNames(entries), err)
	}
}

func TestVerifyChainGapDowngrades(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	stages, _ := buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
		{{Key: "k", Value: []byte("v3")}},
		{{Key: "k", Value: []byte("v4")}},
	})
	// Remove ring 2: ring 3 dangles past the gap and is isolated with it; the
	// intact prefix ends at ring 1.
	if err := os.Remove(filepath.Join(chain, ringName(2))); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify gap: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 || got[0] != ringName(1) {
		t.Fatalf("rings after gap downgrade = %v", got)
	}
	restoreExpect(t, chain, stages[1].pairs, stages[1].snaps)
	diags := diagDirs(t, chain)
	if len(diags) != 1 {
		t.Fatalf("diag dirs = %v", diags)
	}
	entries, _ := os.ReadDir(diags[0])
	got := entrNames(entries)
	want := []string{ringName(3)}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("isolated past gap = %v, want %v", got, want)
	}
}

func TestVerifyChainFirstRingCorruptKeepsHead(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("head")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	stages, _ := buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "a", Value: []byte("ring1")}},
		{{Key: "a", Value: []byte("ring2")}},
	})
	corruptRing(t, chain, 1)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings after first-ring downgrade = %v, want head only", got)
	}
	restoreExpect(t, chain, stages[0].pairs, stages[0].snaps)
	diags := diagDirs(t, chain)
	if len(diags) != 1 {
		t.Fatalf("diag dirs = %v", diags)
	}
	entries, _ := os.ReadDir(diags[0])
	if got := entrNames(entries); fmt.Sprint(got) != fmt.Sprint([]string{ringName(1), ringName(2)}) {
		t.Fatalf("isolated = %v", got)
	}
}

func TestVerifyChainHeadCorruptRejects(t *testing.T) {
	newChain := func(t *testing.T) (string, map[string][]byte) {
		s := openStore(t, tempDir(t))
		if err := s.Put("k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		chain := filepath.Join(t.TempDir(), "chain")
		if err := s.Backup(chain); err != nil {
			t.Fatal(err)
		}
		if err := s.Put("k", []byte("v2")); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
		files := map[string][]byte{}
		for _, d := range digestDir(t, chain) {
			files[d.name] = d.data
		}
		return chain, files
	}

	// A damaged head rejects the whole chain: it cannot be downgraded, so the
	// directory keeps its exact file membership (the damaged file stays as
	// damaged, untouched) and no diagnostic directory is created.
	check := func(t *testing.T, s *Store, chain, damaged string, damagedBytes []byte, before map[string][]byte) {
		if err := s.VerifyChain(chain); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("verify corrupt %s = %v, want fs.ErrInvalid", damaged, err)
		}
		after := digestDir(t, chain)
		if got, want := namesFromDigests(after), namesOfMap(before); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("file set changed: %v vs %v", got, want)
		}
		// Every file retains its pre-verify bytes (the damaged one is the bytes
		// the test wrote, the rest are untouched).
		for _, d := range after {
			var want []byte
			if d.name == damaged {
				want = damagedBytes
			} else {
				want = before[d.name]
			}
			if string(d.data) != string(want) {
				t.Fatalf("file %q changed during a rejected verify", d.name)
			}
		}
		if len(diagDirs(t, chain)) != 0 {
			t.Fatal("head rejection created a diagnostic directory")
		}
	}

	t.Run("corrupt segment", func(t *testing.T) {
		s := openStore(t, tempDir(t))
		chain, before := newChain(t)
		name := segmentName(0)
		data := append([]byte(nil), before[name]...)
		data[len(data)/2] ^= 0xff
		if err := os.WriteFile(filepath.Join(chain, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		check(t, s, chain, name, data, before)
	})

	t.Run("corrupt manifest", func(t *testing.T) {
		s := openStore(t, tempDir(t))
		chain, before := newChain(t)
		name := backupManifest
		data := append([]byte(nil), before[name]...)
		data[10] ^= 0xff
		if err := os.WriteFile(filepath.Join(chain, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		check(t, s, chain, name, data, before)
	})
}

func namesFromDigests(ds []fileDigest) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.name)
	}
	sort.Strings(out)
	return out
}

func namesOfMap(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func TestVerifyChainRejects(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	before := digestDir(t, chain)

	plainFile := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(plainFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	emptyDir := t.TempDir()
	foreign := filepath.Join(t.TempDir(), "foreign")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	// Copy the chain, then add a stray file.
	if err := copyChainInto(chain, foreign); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "notes.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		dir  string
	}{
		{"empty path", ""},
		{"missing directory", filepath.Join(t.TempDir(), "nope")},
		{"not a directory", plainFile},
		{"no head", emptyDir},
		{"foreign file", foreign},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.VerifyChain(tc.dir); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("VerifyChain = %v, want fs.ErrInvalid", err)
			}
		})
	}

	// A live lease holder rejects verification wholesale.
	lease, err := acquireChainLease(chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("verify under held lease = %v, want fs.ErrInvalid", err)
	}
	lease.release()

	// Every rejection left the chain whole and byte-identical.
	assertDigestsEqual(t, digestDir(t, chain), before, "chain after rejections")
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("chain no longer validates: %v", err)
	}
	if junk := diagDirs(t, chain); len(junk) != 0 {
		t.Fatalf("rejections created diagnostics: %v", junk)
	}

	// A closed store reports fs.ErrClosed.
	closed := openStore(t, tempDir(t))
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.VerifyChain(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("verify on closed store = %v, want fs.ErrClosed", err)
	}
}

// copyChainInto duplicates a finished chain directory's files into an existing
// empty directory.
func copyChainInto(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func TestVerifyChainSweepsStagingDebris(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(chain)
	base := filepath.Base(chain)

	// A leftover staging sibling from a killed downgrade is removed.
	junk := filepath.Join(parent, base+verifyStagePrefix+"junk")
	if err := os.MkdirAll(junk, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(junk, "x"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := digestDir(t, chain)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := os.Stat(junk); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging debris not swept: %v", err)
	}
	assertDigestsEqual(t, digestDir(t, chain), before, "chain after sweep")

	// Opening a related directory sweeps the debris too. Use a throwaway copy
	// of the chain so opening it as a store adds no files to the real chain.
	relParent := t.TempDir()
	relChain := filepath.Join(relParent, "chain")
	if err := os.MkdirAll(relChain, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := copyChainInto(chain, relChain); err != nil {
		t.Fatal(err)
	}
	junk2 := filepath.Join(relParent, "chain"+verifyStagePrefix+"more")
	if err := os.MkdirAll(junk2, 0o700); err != nil {
		t.Fatal(err)
	}
	relBytes := map[string][]byte{}
	for _, d := range digestDir(t, relChain) {
		relBytes[d.name] = d.data
	}
	opened, err := Open(relChain)
	if err != nil {
		t.Fatalf("open chain dir: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(junk2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open-time sweep left staging debris: %v", err)
	}
	// The complete artifact is left in place by the open-time sweep.
	for name, want := range relBytes {
		got, err := os.ReadFile(filepath.Join(relChain, name))
		if err != nil {
			t.Fatalf("artifact file %s missing after open: %v", name, err)
		}
		if string(got) != string(want) {
			t.Fatalf("artifact file %s changed by open-time sweep", name)
		}
	}
}

func TestVerifyChainCrashBetweenRenames(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	original := digestDir(t, chain)
	wantPairs := restorePairs(t, chain)

	parent := filepath.Dir(chain)
	base := filepath.Base(chain)

	// Simulate a kill between the two commit renames: the whole original
	// chain is in a trash sibling and the chain path is absent, with a
	// half-staged prefix left beside it.
	trash := filepath.Join(parent, base+verifyTrashPrefix+"dead")
	staging := filepath.Join(parent, base+verifyStagePrefix+"dead")
	if err := os.Rename(chain, trash); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify after crash: %v", err)
	}
	assertDigestsEqual(t, digestDir(t, chain), original, "chain rolled back to the original whole chain")
	if _, err := os.Stat(trash); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trash not cleared: %v", err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging not cleared: %v", err)
	}
	restoreExpect(t, chain, wantPairs.pairs, wantPairs.snaps)
}

// restorePairs restores chain and captures its pairs and snapshot count.
func restorePairs(t *testing.T, chain string) struct {
	pairs []KVPair
	snaps uint64
} {
	t.Helper()
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "probe"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	return struct {
		pairs []KVPair
		snaps uint64
	}{allPairs(t, rs), rs.Stats().Snapshots}
}

func TestVerifyChainCrashAfterSwapPreservesDiag(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	stages, _ := buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "k", Value: []byte("v2")}},
		{{Key: "k", Value: []byte("v3")}},
	})

	// Simulate a kill after the swap but before the trash is finished: the
	// healed prefix is live at chainDir and the original chain sits in trash.
	parent := filepath.Dir(chain)
	base := filepath.Base(chain)
	trash := filepath.Join(parent, base+verifyTrashPrefix+"dead")
	healed := filepath.Join(t.TempDir(), "healed")
	if err := os.MkdirAll(healed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := copyChainInto(chain, healed); err != nil {
		t.Fatal(err)
	}
	// healed currently is the full chain; remove ring 1 to model the prefix.
	if err := os.Remove(filepath.Join(healed, ringName(1))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(healed, ringName(2))); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(chain, trash); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(healed, chain); err != nil {
		t.Fatal(err)
	}

	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify finishing crash: %v", err)
	}
	restoreExpect(t, chain, stages[0].pairs, stages[0].snaps)
	if _, err := os.Stat(trash); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trash not finished: %v", err)
	}
	diags := diagDirs(t, chain)
	if len(diags) != 1 {
		t.Fatalf("diag dirs = %v, want one", diags)
	}
	entries, _ := os.ReadDir(diags[0])
	if got := entrNames(entries); fmt.Sprint(got) != fmt.Sprint([]string{ringName(1), ringName(2)}) {
		t.Fatalf("preserved isolated rings = %v", got)
	}
}

func TestVerifyChainContinuedExportAndMerge(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 30; i++ {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("v0")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	buildStagedChain(t, s, chain, [][]BatchOp{
		{{Key: "k00", Value: []byte("v1")}},
		{{Key: "k01", Delete: true}},
		{{Key: "k29", Value: []byte("v3")}},
	})
	// Damage ring 2 and verify: the chain heals to head + ring 1.
	corruptRing(t, chain, 2)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 {
		t.Fatalf("rings after downgrade = %v", got)
	}

	// A delete forgotten entirely by compaction must still enter the next
	// ring as a tombstone after the downgrade.
	if err := s.Delete("k05"); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after downgrade: %v", err)
	}
	// Numbering continues densely from 1 and watermarks meet head-to-tail.
	if got := ringFiles(t, chain); len(got) != 2 || got[1] != ringName(2) {
		t.Fatalf("rings after continued export = %v", got)
	}
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("continued chain invalid: %v", err)
	}

	// Restore: k01's delete stays effective (the store still knows it, so the
	// continued export carries it again across the downgraded prefix), and
	// k05 — deleted only after the downgrade and then compacted into
	// oblivion — is recorded as a synthesized tombstone, so it stays dead.
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r1"))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := rs.Get("k05"); ok {
		t.Fatalf("forgotten delete resurrected k05 = %q", got)
	}
	if got, ok, _ := rs.Get("k01"); ok {
		t.Fatalf("k01 delete resurrected across downgrade = %q", got)
	}
	if got, ok, _ := rs.Get("k00"); !ok || string(got) != "v1" {
		t.Fatalf("k00 = %q,%v want v1", got, ok)
	}
	rs.Close()

	// Another incremental export keeps appending densely.
	if err := s.Put("k07", []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental 3: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 3 {
		t.Fatalf("rings = %v", got)
	}
	// The full pre-merge chain tip; the merge folds the head plus its first two
	// rings, leaving the third ring renumbered as the tail at the same tip.
	full, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("pre-merge chain invalid: %v", err)
	}

	// Merge the head plus its first two rings into a new full head; the third
	// ring survives renumbered, and deletes stay dead.
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("merge after downgrade: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 1 {
		t.Fatalf("rings after merge = %v, want the surviving tail", got)
	}
	art2, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("merged chain invalid: %v", err)
	}
	if art2.endWM != full.endWM || art2.snaps != full.snaps {
		t.Fatalf("merge changed tip: %d/%d vs %d/%d", art2.endWM, art2.snaps, full.endWM, full.snaps)
	}
	rs2, err := Restore(chain, filepath.Join(t.TempDir(), "r2"))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := rs2.Get("k05"); ok {
		t.Fatalf("forgotten delete resurrected after merge: %q", got)
	}
	if got, ok, _ := rs2.Get("k07"); !ok || string(got) != "later" {
		t.Fatalf("k07 = %q,%v want later", got, ok)
	}
	rs2.Close()

	// Exports continue after the merge with continuous numbering.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after merge: %v", err)
	}
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyChainConcurrentStoreActivity(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 100; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for r := 1; r <= 3; r++ {
		if err := s.Put(fmt.Sprintf("k%04d", r), []byte("new")); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failMu sync.Mutex
	var failures []error
	note := func(err error) {
		failMu.Lock()
		failures = append(failures, err)
		failMu.Unlock()
	}

	// Writer: batches, single puts/deletes and snapshots.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.CommitBatch([]BatchOp{
				{Key: fmt.Sprintf("w%05d", i%500), Value: []byte(fmt.Sprintf("%d", i))},
				{Key: "hot", Value: []byte("x")},
			}); err != nil {
				note(err)
				return
			}
			if i%7 == 0 {
				if err := s.Delete("hot"); err != nil {
					note(err)
					return
				}
			}
			if i%11 == 0 {
				snap, err := s.Snapshot()
				if err != nil {
					note(err)
					return
				}
				snap.Get("k0001")
				snap.Close()
			}
		}
	}()
	// Reader: lock-free scans and cursor paging.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur, err := s.OpenCursor(Range{})
			if err != nil {
				note(err)
				return
			}
			if _, err := cur.Page(0, 16); err != nil {
				note(err)
				cur.Close()
				return
			}
			cur.Close()
			if _, err := s.Scan(Range{}, 8); err != nil {
				note(err)
				return
			}
		}
	}()
	// Maintaintenance: checkpoints and compactions keep running.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if err := s.Checkpoint(); err != nil {
				note(err)
				return
			}
			if err := s.Compact(); err != nil {
				note(err)
				return
			}
		}
	}()

	// Verify repeatedly against the intact chain; all calls return nil and the
	// chain stays restorable throughout.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := s.VerifyChain(chain); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("concurrent verify: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if len(failures) != 0 {
		t.Fatalf("concurrent store activity failed: %v", failures[0])
	}
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("final verify: %v", err)
	}
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("chain invalid after concurrent verify: %v", err)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatal(err)
	}
	rs.Close()
}
