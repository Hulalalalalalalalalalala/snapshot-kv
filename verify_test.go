package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// quarantineDirs returns the quarantine sibling directories next to chain.
func quarantineDirs(t *testing.T, chain string) []string {
	t.Helper()
	parent := filepath.Dir(chain)
	base := filepath.Base(chain)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() && (isMergeTempName(name, base, quarantineStagePrefix) ||
			isMergeTempName(name, base, quarantineTrashPrefix)) {
			out = append(out, filepath.Join(parent, name))
		}
	}
	return out
}

// buildRecordedChain writes a head and one ring per ops batch, recording the
// complete live state (pairs and snapshot count) after each stage, head first.
func buildRecordedChain(t *testing.T, s *Store, chain string, ringOps [][]BatchOp) ([]struct {
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

func TestVerifyChainIntact(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	ops := [][]BatchOp{
		{{Key: "a", Value: []byte("one")}, {Key: "a", Value: []byte("two")}}, // within-batch last write wins
		{{Key: "b", Value: []byte("v")}},
		nil, // an empty ring
	}
	stages, _ := buildRecordedChain(t, s, chain, ops)

	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("VerifyChain intact: %v", err)
	}
	// The intact chain is untouched: same rings, no quarantine siblings.
	if names := ringFiles(t, chain); len(names) != len(ops) {
		t.Fatalf("rings changed: %v", names)
	}
	if q := quarantineDirs(t, chain); len(q) != 0 {
		t.Fatalf("quarantine created for intact chain: %v", q)
	}
	rs, _ := restoreCheckpoint(t, chain, stages[len(stages)-1].pairs, stages[len(stages)-1].snaps)
	rs.Close()
}

func TestVerifyChainPathValidation(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.VerifyChain(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty path: %v", err)
	}
	missing := filepath.Join(t.TempDir(), "no-chain")
	if err := s.VerifyChain(missing); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing dir: %v", err)
	}
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("regular file: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(missing); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed store: %v", err)
	}
}

func TestVerifyChainHeals(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	ops := [][]BatchOp{
		{{Key: "k1", Value: []byte("v1")}, {Key: "k2", Value: []byte("v2")}},
		{{Key: "k2", Delete: true}, {Key: "k3", Value: []byte("v3")}},
		{{Key: "k3", Value: []byte("v3b")}, {Key: "k4", Value: []byte("v4")}},
	}
	stages, _ := buildRecordedChain(t, s, chain, ops)

	// Damage ring 2: rings 2 and 3 must leave the live chain together even
	// though ring 3's own bytes are fine.
	damaged := copyChain(t, chain)
	badPath := filepath.Join(damaged, ringName(2))
	badBytes, err := os.ReadFile(badPath)
	if err != nil {
		t.Fatal(err)
	}
	badBytes[incrRingHeaderSize+incrSegHeaderSize+8] ^= 0xFF
	if err := os.WriteFile(badPath, badBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	ring3Bytes, err := os.ReadFile(filepath.Join(damaged, ringName(3)))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.VerifyChain(damaged); err != nil {
		t.Fatalf("VerifyChain heal: %v", err)
	}

	// The live chain is exactly the intact prefix head + ring 1.
	art, err := validateBackupChain(damaged)
	if err != nil {
		t.Fatalf("degraded chain does not validate: %v", err)
	}
	if art.ringCount != 1 {
		t.Fatalf("rings after heal = %d, want 1", art.ringCount)
	}
	if names := ringFiles(t, damaged); len(names) != 1 || names[0] != ringName(1) {
		t.Fatalf("live rings after heal: %v", names)
	}

	// Restore from the degraded chain reproduces the ring-1 watermark state,
	// cumulative snapshot count included.
	rs, _ := restoreCheckpoint(t, damaged, stages[1].pairs, stages[1].snaps)
	rs.Close()

	// The quarantine holds the damaged ring and the orphaned suffix with
	// their bytes untouched, for diagnosis.
	qs := quarantineDirs(t, damaged)
	if len(qs) != 1 {
		t.Fatalf("quarantine dirs = %v, want 1", qs)
	}
	gotBad, err := os.ReadFile(filepath.Join(qs[0], ringName(2)))
	if err != nil {
		t.Fatalf("damaged ring not retained: %v", err)
	}
	if string(gotBad) != string(badBytes) {
		t.Fatal("quarantined ring bytes differ from the damaged original")
	}
	got3, err := os.ReadFile(filepath.Join(qs[0], ringName(3)))
	if err != nil {
		t.Fatalf("isolated suffix ring not retained: %v", err)
	}
	if string(got3) != string(ring3Bytes) {
		t.Fatal("quarantined suffix ring bytes differ")
	}

	// Verification is idempotent: the degraded chain stays stable.
	if err := s.VerifyChain(damaged); err != nil {
		t.Fatalf("second VerifyChain: %v", err)
	}
	if names := ringFiles(t, damaged); len(names) != 1 {
		t.Fatalf("rings changed on re-verify: %v", names)
	}

	// Appending resumes from the degraded tip with dense numbering.
	if err := s.Put("k5", []byte("v5")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(damaged); err != nil {
		t.Fatalf("incremental after heal: %v", err)
	}
	if names := ringFiles(t, damaged); len(names) != 2 || names[1] != ringName(2) {
		t.Fatalf("rings after append: %v", names)
	}
	want := allPairs(t, s)
	wantSnaps := s.Stats().Snapshots
	rs2, _ := restoreCheckpoint(t, damaged, want, wantSnaps)
	rs2.Close()

	// Deletes covered by the merged-away ring must not resurrect.
	if err := s.MergeChain(damaged, 1); err != nil {
		t.Fatalf("merge after heal: %v", err)
	}
	if names := ringFiles(t, damaged); len(names) != 1 {
		t.Fatalf("rings after merge: %v", names)
	}
	rs3, _ := restoreCheckpoint(t, damaged, want, wantSnaps)
	rs3.Close()
	for _, k := range []string{"k2"} {
		if _, ok, _ := rs3.Get(k); ok {
			t.Fatalf("deleted key %q resurrected after degraded merge", k)
		}
	}
}

func TestVerifyChainDamageKinds(t *testing.T) {
	s := openStore(t, tempDir(t))
	// Seed the head with live keys so head-segment corruption is exercisable.
	if err := s.CommitBatch([]BatchOp{
		{Key: "h1", Value: []byte("v")},
		{Key: "h2", Value: []byte("v")},
	}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	ops := [][]BatchOp{
		{{Key: "k1", Value: []byte("v1")}},
		{{Key: "k2", Value: []byte("v2")}},
	}
	stages, _ := buildRecordedChain(t, s, chain, ops)

	cases := map[string]func(dir string){
		"first ring bit flip": func(dir string) {
			p := filepath.Join(dir, ringName(1))
			b, _ := os.ReadFile(p)
			b[incrRingHeaderSize+incrSegHeaderSize+6] ^= 0xFF
			_ = os.WriteFile(p, b, 0o600)
		},
		"first ring truncated": func(dir string) {
			p := filepath.Join(dir, ringName(1))
			b, _ := os.ReadFile(p)
			_ = os.WriteFile(p, b[:len(b)/2], 0o600)
		},
		"missing first ring": func(dir string) {
			_ = os.Remove(filepath.Join(dir, ringName(1)))
		},
		"broken prev link": func(dir string) {
			p := filepath.Join(dir, ringName(1))
			b, _ := os.ReadFile(p)
			b[12] ^= 0xFF
			_ = os.WriteFile(p, b, 0o600)
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			d := copyChain(t, chain)
			damage(d)
			if err := s.VerifyChain(d); err != nil {
				t.Fatalf("heal: %v", err)
			}
			art, err := validateBackupChain(d)
			if err != nil {
				t.Fatalf("degraded chain invalid: %v", err)
			}
			if art.ringCount != 0 {
				t.Fatalf("rings = %d, want head-only prefix", art.ringCount)
			}
			rs, _ := restoreCheckpoint(t, d, stages[0].pairs, stages[0].snaps)
			rs.Close()
		})
	}

	// Damage to the head itself has no good prefix and rejects wholesale.
	for name, damage := range map[string]func(dir string){
		"missing manifest": func(dir string) {
			_ = os.Remove(filepath.Join(dir, backupManifest))
		},
		"corrupt head segment": func(dir string) {
			p := filepath.Join(dir, segmentName(0))
			b, _ := os.ReadFile(p)
			b[backupSegHeaderSize+6] ^= 0xFF
			_ = os.WriteFile(p, b, 0o600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := copyChain(t, chain)
			damage(d)
			before, _ := os.ReadDir(d)
			err := s.VerifyChain(d)
			if !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("bad head: err=%v, want fs.ErrInvalid", err)
			}
			if q := quarantineDirs(t, d); len(q) != 0 {
				t.Fatalf("no prefix heal must not quarantine: %v", q)
			}
			// The chain directory was not rehomed by the failed heal.
			if _, statErr := os.Stat(d); statErr != nil {
				t.Fatalf("chain directory moved on rejected heal: %v", statErr)
			}
			after, _ := os.ReadDir(d)
			if len(after) != len(before) {
				t.Fatalf("rejected heal changed directory contents")
			}
		})
	}
}

func TestVerifyChainLeaseContention(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	buildRecordedChain(t, s, chain, [][]BatchOp{{{Key: "k", Value: []byte("v")}}})

	lease, err := acquireChainLease(chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("verify under live lease: %v", err)
	}
	// The chain is untouched and restores normally.
	if _, verr := validateBackupChain(chain); verr != nil {
		t.Fatalf("chain modified under live lease: %v", verr)
	}
	lease.release()
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify after lease release: %v", err)
	}
}

func TestVerifyChainInterruptedSwap(t *testing.T) {
	s := openStore(t, tempDir(t))

	// A good chain with head + two rings and a known good prefix (head + ring1).
	template := filepath.Join(t.TempDir(), "chain")
	ops := [][]BatchOp{
		{{Key: "k1", Value: []byte("v1")}, {Key: "k2", Value: []byte("v2")}},
		{{Key: "k2", Delete: true}, {Key: "k3", Value: []byte("v3")}},
	}
	stages, _ := buildRecordedChain(t, s, template, ops)

	// materialize copies an artifact into dst (a sibling of the chain path).
	materialize := func(src, dst string) {
		t.Helper()
		if err := os.MkdirAll(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			data, err := os.ReadFile(filepath.Join(src, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("completes staged heal", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "chain")
		trash := filepath.Join(parent, "chain"+quarantineTrashPrefix+"dead")
		stage := filepath.Join(parent, "chain"+quarantineStagePrefix+"dead")
		materialize(template, trash) // whole old chain parked
		prefix := copyChain(t, template)
		_ = os.Remove(filepath.Join(prefix, ringName(2)))
		if _, err := validateBackupChain(prefix); err != nil {
			t.Fatal(err)
		}
		materialize(prefix, stage) // complete staged prefix

		if err := s.VerifyChain(dir); err != nil {
			t.Fatalf("verify after simulated kill: %v", err)
		}
		rs, _ := restoreCheckpoint(t, dir, stages[1].pairs, stages[1].snaps)
		rs.Close()
		if _, err := os.Stat(trash); err != nil {
			t.Fatalf("quarantine not retained: %v", err)
		}
		if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("staging name left behind: %v", err)
		}
	})

	t.Run("rolls back unusable stage", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "chain")
		trash := filepath.Join(parent, "chain"+quarantineTrashPrefix+"dead")
		stage := filepath.Join(parent, "chain"+quarantineStagePrefix+"dead")
		materialize(template, trash)
		if err := os.MkdirAll(stage, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stage, "partial"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := s.VerifyChain(dir); err != nil {
			t.Fatalf("verify with unusable stage: %v", err)
		}
		art, err := validateBackupChain(dir)
		if err != nil {
			t.Fatalf("rolled-back chain invalid: %v", err)
		}
		if art.ringCount != 2 {
			t.Fatalf("rings after rollback = %d, want 2", art.ringCount)
		}
		if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("staging debris left behind: %v", err)
		}
	})

	t.Run("clears precommit stage", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "chain")
		materialize(template, dir)
		stage := filepath.Join(parent, "chain"+quarantineStagePrefix+"junk")
		if err := os.MkdirAll(filepath.Join(stage, incrStageDir), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := s.VerifyChain(dir); err != nil {
			t.Fatalf("verify with precommit stage: %v", err)
		}
		if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("precommit stage not swept: %v", err)
		}
		if q := quarantineDirs(t, dir); len(q) != 0 {
			t.Fatalf("unexpected quarantine for intact chain: %v", q)
		}
	})

	t.Run("open finishes interrupted swap", func(t *testing.T) {
		parent := t.TempDir()
		dir := filepath.Join(parent, "chain")
		trash := filepath.Join(parent, "chain"+quarantineTrashPrefix+"dead")
		stage := filepath.Join(parent, "chain"+quarantineStagePrefix+"dead")
		materialize(template, trash)
		prefix := copyChain(t, template)
		_ = os.Remove(filepath.Join(prefix, ringName(2)))
		materialize(prefix, stage)

		// Opening the chain directory runs the same recovery sweep without any
		// chain operation being invoked: the staged prefix completes the swap
		// and the parked old chain stays as the quarantine.
		opened, err := Open(dir)
		if err != nil {
			t.Fatalf("Open during interrupted swap: %v", err)
		}
		if err := opened.Close(); err != nil {
			t.Fatal(err)
		}
		if names := ringFiles(t, dir); len(names) != 1 || names[0] != ringName(1) {
			t.Fatalf("live rings after open: %v", names)
		}
		if _, err := os.Stat(trash); err != nil {
			t.Fatalf("quarantine lost on open: %v", err)
		}
		// Open added store files beside the artifact; copy just the artifact
		// files elsewhere and confirm the completed chain restores.
		clean := filepath.Join(t.TempDir(), "chain")
		if err := os.MkdirAll(clean, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, e := range mustReadDir(t, dir) {
			name := e.Name()
			if e.IsDir() || (name != backupManifest && !isBackupSegmentName(name) &&
				!isBackupRingName(name)) {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(clean, name), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		rs, _ := restoreCheckpoint(t, clean, stages[1].pairs, stages[1].snaps)
		rs.Close()
	})
}

// mustReadDir lists a directory, failing the test on error.
func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestVerifyChainConcurrentActivity(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	for i := 0; i < 20; i++ {
		if err := s.Put(fmt.Sprintf("seed-%03d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Put(fmt.Sprintf("ring-%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var writes, reads int64
	var mu sync.Mutex
	wg.Add(2)
	go func() { // writer
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.CommitBatch([]BatchOp{{Key: fmt.Sprintf("live-%04d", i), Value: []byte("x")}}); err != nil {
				return
			}
			mu.Lock()
			writes++
			mu.Unlock()
		}
	}()
	go func() { // lock-free reader
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, ok, err := s.Get("seed-000"); err != nil || !ok {
				t.Errorf("read during verify failed: ok=%v err=%v", ok, err)
				return
			}
			mu.Lock()
			reads++
			mu.Unlock()
		}
	}()

	// Verify repeatedly; it must neither block the foreground nor mutate the
	// backed-up store.
	for i := 0; i < 3; i++ {
		if err := s.VerifyChain(chain); err != nil {
			t.Fatalf("verify %d: %v", i, err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if writes == 0 || reads == 0 {
		t.Fatalf("foreground stalled during verify: writes=%d reads=%d", writes, reads)
	}
}

func TestIncrementalForgottenDeleteZeroAdvanceRing(t *testing.T) {
	// Two independent stores that meet at one watermark. Store A's head holds
	// keys a..z at watermark 26. Store B reaches the same watermark with two
	// fewer keys: one it deleted and compacted into oblivion, one it never had.
	// B's continuation therefore moves no watermark yet still carries two
	// synthesized tombstones — a flat, non-empty ring.
	a := openStore(t, tempDir(t))
	for _, k := range "abcdefghijklmnopqrstuvwxyz" {
		if err := a.Put(string(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := a.Backup(chain); err != nil {
		t.Fatal(err)
	}

	b := openStore(t, tempDir(t))
	for _, k := range "abcdefghijklmnopqrstuvwx" { // a..x: 24 puts
		if err := b.Put(string(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Put("y", []byte("v")); err != nil { // seq 25
		t.Fatal(err)
	}
	if err := b.Delete("y"); err != nil { // seq 26 == head watermark
		t.Fatal(err)
	}
	if err := b.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := b.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := b.BackupIncremental(chain); err != nil {
		t.Fatalf("zero-advance forgotten-delete ring rejected: %v", err)
	}

	// The flat ring passes whole-chain validation and online verification.
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("flat ring chain invalid: %v", err)
	}
	if art.ringCount != 1 || art.endWM != art.head.watermark {
		t.Fatalf("unexpected chain tip: rings=%d endWM=%d headWM=%d",
			art.ringCount, art.endWM, art.head.watermark)
	}
	if err := b.VerifyChain(chain); err != nil {
		t.Fatalf("verify flat ring: %v", err)
	}

	// Recovered to the tip watermark the deletes stay dead and the shared
	// live keys survive.
	t2 := filepath.Join(t.TempDir(), "restored")
	r2, err := Restore(chain, t2)
	if err != nil {
		t.Fatalf("restore flat-ring chain: %v", err)
	}
	defer r2.Close()
	for _, k := range []string{"y", "z"} {
		if _, ok, _ := r2.Get(k); ok {
			t.Fatalf("forgotten delete of %q resurrected", k)
		}
	}
	if _, ok, _ := r2.Get("x"); !ok {
		t.Fatal("shared live key lost")
	}

	// A normal ring appended afterwards continues from the flat tip.
	if err := b.Put("new", []byte("n")); err != nil {
		t.Fatal(err)
	}
	if err := b.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after flat ring: %v", err)
	}
	t3 := filepath.Join(t.TempDir(), "later")
	r3, err := Restore(chain, t3)
	if err != nil {
		t.Fatalf("restore extended chain: %v", err)
	}
	defer r3.Close()
	if v, ok, _ := r3.Get("new"); !ok || string(v) != "n" {
		t.Fatalf("post-flat append missing: %q ok=%v", v, ok)
	}
	for _, k := range []string{"y", "z"} {
		if _, ok, _ := r3.Get(k); ok {
			t.Fatalf("deleted key %q back after further append", k)
		}
	}

	// Merging the flat ring away keeps its deletes dead and keeps numbering
	// dense.
	if err := b.MergeChain(chain, 2); err != nil {
		t.Fatalf("merge over flat ring: %v", err)
	}
	if art2, verr := validateBackupChain(chain); verr != nil || art2.ringCount != 0 {
		t.Fatalf("merged chain invalid: art=%+v err=%v", art2, verr)
	}
	t4 := filepath.Join(t.TempDir(), "merged")
	r4, err := Restore(chain, t4)
	if err != nil {
		t.Fatalf("restore merged: %v", err)
	}
	defer r4.Close()
	for _, k := range []string{"y", "z"} {
		if _, ok, _ := r4.Get(k); ok {
			t.Fatalf("deleted key %q resurrected by merge", k)
		}
	}
}
