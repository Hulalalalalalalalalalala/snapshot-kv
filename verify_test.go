package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// chainStage captures one chain prefix directory together with the exact
// pairs and cumulative snapshot count it restores to.
type chainStage struct {
	dir   string
	pairs []KVPair
	snaps uint64
}

// buildStagedChain writes a full head and one ring per op group (each preceded
// by one snapshot), returning the chain path and the saved chain directory
// after every stage: head first, then head plus rings 1..n.
func buildStagedChain(t *testing.T, s *Store, parent string, ringOps [][]BatchOp) (string, []chainStage) {
	t.Helper()
	chain := filepath.Join(parent, "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatalf("backup: %v", err)
	}
	stages := []chainStage{{dir: copyChain(t, chain), pairs: allPairs(t, s), snaps: s.Stats().Snapshots}}
	for _, ops := range ringOps {
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("incremental: %v", err)
		}
		stages = append(stages, chainStage{dir: copyChain(t, chain), pairs: allPairs(t, s), snaps: s.Stats().Snapshots})
	}
	return chain, stages
}

// quarantineDirs returns the quarantine sibling directories of chain.
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
		if e.IsDir() && isMergeTempName(e.Name(), base, quarantinePrefix) {
			out = append(out, filepath.Join(parent, e.Name()))
		}
	}
	return out
}

func corruptRingFile(t *testing.T, dir string, idx uint32, mutate func(data []byte)) {
	t.Helper()
	p := filepath.Join(dir, ringName(idx))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	mutate(data)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyHealthyChainIsNoop(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain, _ := buildStagedChain(t, s, t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("1")}},
		{{Key: "b", Value: []byte("2")}},
	})
	before, err := os.ReadDir(chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyBackupChain(chain); err != nil {
		t.Fatalf("verify healthy chain: %v", err)
	}
	after, err := os.ReadDir(chain)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("healthy verify changed the directory: %d -> %d entries", len(before), len(after))
	}
	if q := quarantineDirs(t, chain); len(q) != 0 {
		t.Fatalf("quarantine created for a healthy chain: %v", q)
	}
	// Idempotent: a second verification is also a no-op.
	if err := s.VerifyBackupChain(chain); err != nil {
		t.Fatalf("second verify: %v", err)
	}

	// A healthy head-only artifact verifies too.
	headOnly := filepath.Join(t.TempDir(), "head")
	if err := s.Backup(headOnly); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyBackupChain(headOnly); err != nil {
		t.Fatalf("verify head-only: %v", err)
	}
}

func TestVerifyDegradesDamagedRingSuffix(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain, stages := buildStagedChain(t, s, t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}, {Key: "only1", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r2")}, {Key: "only2", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r3")}, {Key: "only3", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r4")}, {Key: "only4", Value: []byte("x")}},
	})
	// stages: [head, ring1, ring2, ring3, ring4]; corrupt ring 3 in every
	// supported way, each on a fresh copy. The intact prefix is head+rings1-2.
	type damage struct {
		name   string
		mutate func(t *testing.T, d string)
	}
	cases := []damage{
		{"flipped entry byte", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) {
				data[incrRingHeaderSize+incrSegHeaderSize+10] ^= 0xFF
			})
		}},
		{"truncated ring", func(t *testing.T, d string) {
			p := filepath.Join(d, ringName(3))
			data, _ := os.ReadFile(p)
			if err := os.WriteFile(p, data[:len(data)/2], 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"bad file crc", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) { data[len(data)-1] ^= 0xFF })
		}},
		{"unknown version", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) {
				binary.LittleEndian.PutUint32(data[4:8], backupVersion+99)
			})
		}},
		{"broken prev link", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) { data[12] ^= 0xFF })
		}},
		{"discontinuous watermark", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) {
				binary.LittleEndian.PutUint64(data[16:24],
					binary.LittleEndian.Uint64(data[16:24])+5)
			})
		}},
		{"regressing snapshot count", func(t *testing.T, d string) {
			corruptRingFile(t, d, 3, func(data []byte) {
				binary.LittleEndian.PutUint64(data[32:40], 0)
			})
		}},
		{"missing ring file", func(t *testing.T, d string) {
			if err := os.Remove(filepath.Join(d, ringName(3))); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := copyChain(t, chain)
			tc.mutate(t, d)
			// Capture the exact damaged bytes for the as-is check below; a
			// missing ring (the "missing ring file" case) has none.
			var damaged []byte
			if data, err := os.ReadFile(filepath.Join(d, ringName(3))); err == nil {
				damaged = append([]byte(nil), data...)
			}

			if err := s.VerifyBackupChain(d); err != nil {
				t.Fatalf("verify-and-degrade: %v", err)
			}

			// The live chain is exactly the intact prefix: head plus rings 1,2.
			art, err := validateBackupChain(d)
			if err != nil {
				t.Fatalf("degraded chain does not validate: %v", err)
			}
			if art.ringCount != 2 {
				t.Fatalf("rings after degrade = %d, want 2", art.ringCount)
			}
			if _, err := os.Stat(filepath.Join(d, ringName(3))); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("damaged ring 3 still on the chain")
			}
			if _, err := os.Stat(filepath.Join(d, ringName(4))); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("ring 4 past the damage still on the chain")
			}

			// It restores byte-for-byte to the state and snapshot count at the
			// last intact watermark (the saved chain stage after ring 2).
			prefix := stages[2]
			rs, target := restoreCheckpoint(t, d, prefix.pairs, prefix.snaps)
			rs.Close()
			// The restored store is also exactly what restoring the saved
			// prefix copy produced.
			want, err := Restore(prefix.dir, filepath.Join(t.TempDir(), "want"))
			if err != nil {
				t.Fatal(err)
			}
			assertSamePairs(t, allPairs(t, want), prefix.pairs, "prefix reference")
			if n := want.Stats().Snapshots; n != prefix.snaps {
				t.Fatalf("prefix snaps = %d, want %d", n, prefix.snaps)
			}
			want.Close()
			if got, err := Open(target); err != nil {
				t.Fatal(err)
			} else {
				assertSamePairs(t, allPairs(t, got), prefix.pairs, "reopened degraded restore")
				got.Close()
			}

			// The suffix was isolated as-is, in a quarantine sibling, with a
			// self-describing marker.
			q := quarantineDirs(t, d)
			if len(q) != 1 {
				t.Fatalf("quarantine dirs = %v, want exactly one", q)
			}
			if damaged != nil {
				kept, err := os.ReadFile(filepath.Join(q[0], ringName(3)))
				if err != nil {
					t.Fatalf("damaged ring not preserved: %v", err)
				}
				if string(kept) != string(damaged) {
					t.Fatal("isolated ring bytes were modified")
				}
			}
			if _, err := os.Stat(filepath.Join(q[0], ringName(4))); err != nil {
				t.Fatalf("ring 4 not isolated: %v", err)
			}
			marker, err := os.ReadFile(filepath.Join(q[0], quarantineMarkerName))
			if err != nil {
				t.Fatalf("quarantine marker: %v", err)
			}
			if m := string(marker); m == "" ||
				!strings.Contains(m, ringName(4)) ||
				(damaged != nil && !strings.Contains(m, ringName(3))) {
				t.Fatalf("marker does not name isolated files:\n%s", m)
			}

			// Isolated rings take no part in further operations: a fresh
			// incremental appends ring 3 to the degraded tip and still links.
			if err := s.Put("after", []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := s.BackupIncremental(d); err != nil {
				t.Fatalf("append after degrade: %v", err)
			}
			art2, err := validateBackupChain(d)
			if err != nil {
				t.Fatalf("chain with appended ring invalid: %v", err)
			}
			if art2.ringCount != 3 {
				t.Fatalf("rings after append = %d, want 3", art2.ringCount)
			}
		})
	}
}

func TestVerifyFirstRingCorruptDegradesToHead(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain, stages := buildStagedChain(t, s, t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
		{{Key: "a", Value: []byte("r2")}},
	})
	d := copyChain(t, chain)
	corruptRingFile(t, d, 1, func(data []byte) { data[len(data)-1] ^= 0xFF })

	if err := s.VerifyBackupChain(d); err != nil {
		t.Fatalf("degrade to head: %v", err)
	}
	art, err := validateBackupChain(d)
	if err != nil {
		t.Fatalf("head-only degraded chain invalid: %v", err)
	}
	if art.ringCount != 0 {
		t.Fatalf("rings = %d, want 0", art.ringCount)
	}
	rs, _ := restoreCheckpoint(t, d, stages[0].pairs, stages[0].snaps)
	rs.Close()

	// Appending starts the ring order over at 1 and links to the head.
	if err := s.Put("fresh", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(d); err != nil {
		t.Fatalf("append ring 1 after head-only degrade: %v", err)
	}
	if _, err := validateBackupChain(d); err != nil {
		t.Fatalf("renumbered chain invalid: %v", err)
	}
}

func TestVerifyHeadDamageRejectsWholeAndLeavesFiles(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "seed-a", Value: []byte("payload")},
		{Key: "seed-b", Value: []byte("payload")},
	}); err != nil {
		t.Fatal(err)
	}
	chain, _ := buildStagedChain(t, s, t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
	})
	cases := map[string]func(d string){
		"corrupt head segment": func(d string) {
			p := filepath.Join(d, segmentName(0))
			data, _ := os.ReadFile(p)
			data[backupSegHeaderSize+10] ^= 0xFF
			_ = os.WriteFile(p, data, 0o600)
		},
		"missing manifest": func(d string) {
			_ = os.Remove(filepath.Join(d, backupManifest))
		},
		"foreign file": func(d string) {
			_ = os.WriteFile(filepath.Join(d, "notes.txt"), []byte("hi"), 0o600)
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			d := copyChain(t, chain)
			damage(d)
			before, _ := os.ReadDir(d)
			err := s.VerifyBackupChain(d)
			if !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("verify head damage: err=%v, want fs.ErrInvalid", err)
			}
			after, _ := os.ReadDir(d)
			if len(after) != len(before) {
				t.Fatalf("files moved despite wholesale rejection: %d -> %d",
					len(before), len(after))
			}
			if q := quarantineDirs(t, d); len(q) != 0 {
				t.Fatalf("quarantine created for head damage: %v", q)
			}
			// Un-degraded damage is still rejected wholesale by the other
			// chain operations, exactly as before.
			if err := s.BackupIncremental(d); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("incremental over head damage: %v", err)
			}
			if _, rerr := Restore(d, filepath.Join(t.TempDir(), "t")); !errors.Is(rerr, fs.ErrInvalid) {
				t.Fatalf("restore over head damage: %v", rerr)
			}
		})
	}
}

func TestVerifyMergeAndAppendContinueDegradedOrder(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain, stages := buildStagedChain(t, s, t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}, {Key: "r1k", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r2")}, {Key: "r2k", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r3")}, {Key: "r3k", Value: []byte("x")}},
		{{Key: "a", Value: []byte("r4")}, {Key: "r4k", Value: []byte("x")}},
	})
	d := copyChain(t, chain)
	corruptRingFile(t, d, 3, func(data []byte) { data[len(data)-1] ^= 0xFF })
	if err := s.VerifyBackupChain(d); err != nil {
		t.Fatal(err)
	}

	// Deletes covered by the rings merged into the new head stay deleted, and
	// the surviving ring (old ring 2) is renumbered to ring 1 behind it.
	if err := s.MergeChain(d, 1); err != nil {
		t.Fatalf("merge after degrade: %v", err)
	}
	art, err := validateBackupChain(d)
	if err != nil {
		t.Fatalf("merged degraded chain invalid: %v", err)
	}
	if art.ringCount != 1 {
		t.Fatalf("rings after merge = %d, want 1", art.ringCount)
	}
	rs, _ := restoreCheckpoint(t, d, stages[2].pairs, stages[2].snaps)
	rs.Close()

	// Further rings append in the merged order and keep the chain valid.
	if err := s.Put("post", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(d); err != nil {
		t.Fatalf("append after merge: %v", err)
	}
	if _, err := validateBackupChain(d); err != nil {
		t.Fatalf("post-merge append invalidates chain: %v", err)
	}
}

func TestVerifyBatchSemanticsSurviveDegradation(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{{Key: "k", Value: []byte("v0")}}); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// Batch with within-batch last write wins, across several rings.
	if err := s.CommitBatch([]BatchOp{
		{Key: "k", Value: []byte("v1")},
		{Key: "k", Delete: true}, // final: deleted
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "k", Value: []byte("v2")},
		{Key: "k", Value: []byte("v3")}, // final value wins
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	d := copyChain(t, chain)
	corruptRingFile(t, d, 2, func(data []byte) { data[len(data)-1] ^= 0xFF })
	if err := s.VerifyBackupChain(d); err != nil {
		t.Fatal(err)
	}
	rs, err := Restore(d, filepath.Join(t.TempDir(), "target"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if _, ok, _ := rs.Get("k"); ok {
		t.Fatal("put-then-delete resurrected after degraded restore")
	}
}

func TestVerifyPathValidationAndClosed(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.VerifyBackupChain(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty path: %v", err)
	}
	if err := s.VerifyBackupChain(filepath.Join(t.TempDir(), "nope")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing path: %v", err)
	}
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyBackupChain(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file path: %v", err)
	}
	if err := s.VerifyBackupChain(t.TempDir()); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty dir without head: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyBackupChain(t.TempDir()); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed store: %v", err)
	}
}

func TestVerifyLeaseContentionFailsWhole(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// A live holder (this process, far-future expiry) occupies the chain.
	l := &chainLease{
		path:  filepath.Join(filepath.Dir(chain), filepath.Base(chain)+chainLeaseSuffix),
		pid:   uint64(os.Getpid()),
		token: 424242,
	}
	if err := os.WriteFile(l.path, l.stamp(), 0o600); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadDir(chain)
	err := s.VerifyBackupChain(chain)
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("verify against live lease: err=%v, want fs.ErrInvalid", err)
	}
	after, _ := os.ReadDir(chain)
	if len(after) != len(before) {
		t.Fatal("chain touched despite failing to take the lease")
	}
	// Once the live lease is gone the verification proceeds.
	if err := os.Remove(l.path); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyBackupChain(chain); err != nil {
		t.Fatalf("verify after lease release: %v", err)
	}
}

func TestVerifyRunsAlongsideStoreActivity(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 50; i++ {
		_ = s.Put(fmt.Sprintf("k%03d", i), []byte("v"))
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Commits, snapshots, checkpoints, compactions and incremental exports
	// keep advancing while a verifier repeatedly scans the chain.
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.CommitBatch([]BatchOp{
				{Key: fmt.Sprintf("rot-%03d", i%5), Value: []byte(fmt.Sprintf("%d", i))},
				{Key: fmt.Sprintf("uniq-%05d", i), Value: []byte("u")},
			})
			if i%7 == 0 {
				_ = s.Checkpoint()
			}
			if i%11 == 0 {
				_ = s.Compact()
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.BackupIncremental(chain); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			err := s.VerifyBackupChain(chain)
			// The exporter goroutine may hold the chain lease at this instant:
			// that contention is a specified wholesale failure, never a wait,
			// so it is retried rather than treated as a defect.
			if err != nil && !errors.Is(err, fs.ErrInvalid) {
				t.Errorf("verify during activity: %v", err)
				return
			}
			if _, ok, gerr := s.Get("k001"); gerr != nil || !ok {
				t.Errorf("read during verify: ok=%v err=%v", ok, gerr)
				return
			}
			runtime.Gosched()
		}
	}()
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
}

func TestVerifySyntheticTombstoneWhenTipDoesNotAdvance(t *testing.T) {
	// A head whose watermark runs ahead of its live-key count (a delete before
	// the backup) restores into a store whose next sequence is below the head
	// watermark. Delete head keys until the store catches up to the tip
	// watermark without advancing past it, compact the tombstones into
	// oblivion, then export: the forgotten deletes enter the zero-span ring as
	// boundary tombstones and the delete never resurrects.
	src := openStore(t, tempDir(t))
	if err := src.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
		{Key: "d", Value: []byte("4")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.Delete("d"); err != nil { // watermark 5, three live keys
		t.Fatal(err)
	}
	headDir := filepath.Join(t.TempDir(), "head")
	if err := src.Backup(headDir); err != nil {
		t.Fatal(err)
	}
	s, err := Restore(headDir, filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.nextSeq; got != 3 {
		t.Fatalf("restored nextSeq = %d, want 3", got)
	}

	// Two deletes bring nextSeq to 5 == the head watermark without passing it.
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if s.nextSeq != 5 {
		t.Fatalf("nextSeq = %d, want 5 (tip not advanced)", s.nextSeq)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("zero-span synthetic-tombstone ring rejected: %v", err)
	}
	// The ring validates as a whole chain (the boundary tombstones pass the
	// watermark interval and zero-span checks).
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("zero-span tombstone chain invalid: %v", err)
	}
	if art.ringCount != 1 || art.endWM != art.head.watermark {
		t.Fatalf("ring tip = wm %d (head %d), rings %d", art.endWM, art.head.watermark, art.ringCount)
	}
	// Restored to the chain-tip watermark, the forgotten deletes stay deleted
	// and the surviving key reads back.
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	defer rs.Close()
	for _, k := range []string{"a", "b", "d"} {
		if _, ok, _ := rs.Get(k); ok {
			t.Fatalf("forgotten delete of %q resurrected", k)
		}
	}
	if v, ok, _ := rs.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("surviving key wrong: %q ok=%v", v, ok)
	}
	// Verification accepts the zero-span tombstone ring.
	if err := s.VerifyBackupChain(chain); err != nil {
		t.Fatalf("verify zero-span tombstone chain: %v", err)
	}
}

func TestVerifySweepCompletesInterruptedSwap(t *testing.T) {
	// Case 1: replacement chain installed, retired copy still beside it with
	// the isolated suffix (rings 2 and 3). The sweep quarantines the suffix
	// and removes the retired copy; the live degraded chain stays whole.
	parent := t.TempDir()
	live := filepath.Join(parent, "live")
	fullParent := t.TempDir()
	full, _ := buildStagedChain(t, openStore(t, tempDir(t)), fullParent, [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
		{{Key: "a", Value: []byte("r2")}},
		{{Key: "a", Value: []byte("r3")}},
	})
	degradedParent := t.TempDir()
	degraded, _ := buildStagedChain(t, openStore(t, tempDir(t)), degradedParent, [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
	})
	if err := os.Rename(degraded, live); err != nil {
		t.Fatal(err)
	}
	trash := filepath.Join(parent, "live.verify-old-killed")
	if err := os.Rename(full, trash); err != nil {
		t.Fatal(err)
	}
	sweepVerifyDebris(live, true)
	if _, err := os.Stat(trash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("retired copy left behind after sweep")
	}
	art, err := validateBackupChain(live)
	if err != nil {
		t.Fatalf("live chain not whole after sweep: %v", err)
	}
	if art.ringCount != 1 {
		t.Fatalf("live rings = %d, want 1", art.ringCount)
	}
	q := quarantineDirs(t, live)
	if len(q) != 1 {
		t.Fatalf("quarantine dirs = %v, want one", q)
	}
	for _, idx := range []uint32{2, 3} {
		if _, err := os.Stat(filepath.Join(q[0], ringName(idx))); err != nil {
			t.Fatalf("ring %d not quarantined: %v", idx, err)
		}
	}
	if _, err := os.Stat(filepath.Join(q[0], ringName(1))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("intact ring 1 was moved into the quarantine")
	}

	// Case 2: chain missing, retired copy whole: the swap is undone by moving
	// the old chain back.
	parent2 := t.TempDir()
	live2 := filepath.Join(parent2, "live2")
	full2, _ := buildStagedChain(t, openStore(t, tempDir(t)), parent2, [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
	})
	if err := os.Rename(full2, live2); err != nil {
		t.Fatal(err)
	}
	trash2 := filepath.Join(parent2, "live2.verify-old-killed")
	if err := os.Rename(live2, trash2); err != nil {
		t.Fatal(err)
	}
	sweepVerifyDebris(live2, true)
	if _, err := os.Stat(live2); err != nil {
		t.Fatalf("old chain not restored: %v", err)
	}
	if _, err := os.Stat(trash2); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("retired copy left after rollback")
	}
	if _, err := validateBackupChain(live2); err != nil {
		t.Fatalf("rolled-back chain invalid: %v", err)
	}

	// Case 3: an orphaned staging directory is pure debris.
	parent3 := t.TempDir()
	live3 := filepath.Join(parent3, "live3")
	src3, _ := buildStagedChain(t, openStore(t, tempDir(t)), parent3, nil)
	if err := os.Rename(src3, live3); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(parent3, "live3.verify-new-orphan")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "partial"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	sweepVerifyDebris(live3, true)
	if _, err := os.Stat(staging); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("orphaned staging not swept")
	}
	if _, err := validateBackupChain(live3); err != nil {
		t.Fatalf("chain damaged by staging sweep: %v", err)
	}
}

func TestVerifyInterruptedSwapRepairedOnOpen(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A retired verify copy with an isolated suffix sits next to a store dir
	// that already holds the degraded chain; opening the store finishes the
	// isolation and keeps serving its data.
	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	chain, _ := buildStagedChain(t, openStore(t, tempDir(t)), t.TempDir(), [][]BatchOp{
		{{Key: "a", Value: []byte("r1")}},
		{{Key: "a", Value: []byte("r2")}},
	})
	trash := filepath.Join(parent, base+".verify-old-dead")
	if err := os.Rename(chain, trash); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("open over interrupted verify: %v", err)
	}
	defer reopened.Close()
	if got, ok, _ := reopened.Get("keep"); !ok || string(got) != "v" {
		t.Fatalf("data lost after open: %q %v", got, ok)
	}
	if _, err := os.Stat(trash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("retired verify copy not swept on open")
	}
}
