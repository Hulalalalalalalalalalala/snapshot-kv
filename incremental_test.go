package snapshot

import (
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

// copyChain duplicates a completed backup artifact directory file by file.
// Completed artifacts hold flat files only (segments, manifest, rings).
func copyChain(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), filepath.Base(src))
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			t.Fatalf("completed artifact contains directory %q", e.Name())
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// restoreCheckpoint restores art into a fresh target, closes it, reopens and
// returns the reopened store, after asserting the live state equals want.
func restoreCheckpoint(t *testing.T, art string, want []KVPair, snaps uint64) (*Store, string) {
	t.Helper()
	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(art, target)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	assertSamePairs(t, allPairs(t, rs), want, "restored live state")
	if n := rs.Stats().Snapshots; n != snaps {
		t.Fatalf("live snapshot count = %d, want %d", n, snaps)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	assertSamePairs(t, allPairs(t, s2), want, "restored reopened state")
	if n := s2.Stats().Snapshots; n != snaps {
		t.Fatalf("reopened snapshot count = %d, want %d", n, snaps)
	}
	return s2, target
}

func TestIncrementalChainRoundtrip(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one")},
		{Key: "empty", Value: nil},
		{Key: "gone", Value: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "gone", Delete: true},
		{Key: "multi", Value: []byte("first")},
		{Key: "multi", Value: []byte("second")}, // within-batch last write wins
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}

	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	type stage struct {
		art   string
		pairs []KVPair
		snaps uint64
	}
	stages := []stage{{copyChain(t, chain), allPairs(t, s), s.Stats().Snapshots}}

	// Ring 1: update, delete, add; put-then-delete and delete-then-put batch
	// semantics straddle the ring boundary.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("two")},
		{Key: "empty", Delete: true},
		{Key: "pd", Value: []byte("v")},
		{Key: "pd", Delete: true}, // put-then-delete: absent after
		{Key: "dp", Delete: true},
		{Key: "dp", Value: []byte("back")}, // delete-then-put: present
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental 1: %v", err)
	}
	stages = append(stages, stage{copyChain(t, chain), allPairs(t, s), s.Stats().Snapshots})

	// Ring 2: overwrite one of ring 1's keys, touch a head key, add more.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("three")},
		{Key: "multi", Delete: true},
		{Key: "new", Value: []byte("n")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental 2: %v", err)
	}
	stages = append(stages, stage{copyChain(t, chain), allPairs(t, s), s.Stats().Snapshots})

	// Every prefix of the chain restores to exactly its own watermark state,
	// live and after reopen.
	var lastDir string
	for i, st := range stages {
		rs, dir := restoreCheckpoint(t, st.art, st.pairs, st.snaps)
		lastDir = dir
		if i == len(stages)-1 {
			// Counting continues from the preserved cumulative count.
			if _, err := rs.Snapshot(); err != nil {
				t.Fatal(err)
			}
			if n := rs.Stats().Snapshots; n != st.snaps+1 {
				t.Fatalf("snapshot count does not continue: %d want %d", n, st.snaps+1)
			}
		}
		rs.Close()
	}

	// Deleted keys never come back; batch outcomes straddling rings hold.
	final, err := Open(lastDir)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	for _, k := range []string{"gone", "empty", "multi", "pd"} {
		if _, ok, _ := final.Get(k); ok {
			t.Fatalf("deleted key %q resurrected after chain restore", k)
		}
	}
	if got, ok, _ := final.Get("dp"); !ok || string(got) != "back" {
		t.Fatalf("delete-then-put wrong: %q ok=%v", got, ok)
	}
	if got, ok, _ := final.Get("alpha"); !ok || string(got) != "three" {
		t.Fatalf("latest overwrite missing: %q ok=%v", got, ok)
	}

	// The source store is untouched by the exports.
	if got, ok, _ := s.Get("alpha"); !ok || string(got) != "three" {
		t.Fatalf("source store changed: %q ok=%v", got, ok)
	}
}

func TestIncrementalDeletedAcrossCompaction(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	// Delete half the head keys, then compact hard so the tombstones leave
	// memory and the log entirely.
	for i := 0; i < 50; i += 2 {
		if err := s.Delete(fmt.Sprintf("k%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after compaction: %v", err)
	}
	want := allPairs(t, s)

	rs, target := restoreCheckpoint(t, chain, want, 0)
	rs.Close()
	for i := 0; i < 50; i += 2 {
		if _, ok, _ := s.Get(fmt.Sprintf("k%02d", i)); ok {
			t.Fatalf("test setup: %d present in source", i)
		}
	}
	chk, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer chk.Close()
	for i := 0; i < 50; i += 2 {
		if _, ok, _ := chk.Get(fmt.Sprintf("k%02d", i)); ok {
			t.Fatalf("compaction-forgotten deletion of k%02d resurrected", i)
		}
	}
	for i := 1; i < 50; i += 2 {
		if _, ok, _ := chk.Get(fmt.Sprintf("k%02d", i)); !ok {
			t.Fatalf("live key k%02d missing", i)
		}
	}

	// Another ring after more deletes-and-compaction stays correct.
	for i := 1; i < 50; i += 2 {
		if err := s.Delete(fmt.Sprintf("k%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental 2: %v", err)
	}
	rs2, target2 := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs2.Close()
	chk2, err := Open(target2)
	if err != nil {
		t.Fatal(err)
	}
	defer chk2.Close()
	if st := chk2.Stats(); st.Keys != 0 {
		t.Fatalf("expected empty restored store, got %d keys", st.Keys)
	}
}

func TestIncrementalEmptyRings(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	// No commits: an empty ring.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("empty ring: %v", err)
	}
	want := allPairs(t, s)
	// A snapshot advances the cumulative count even without a commit.
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("empty ring after snapshot: %v", err)
	}
	rs, _ := restoreCheckpoint(t, chain, want, 1)
	rs.Close()

	entries, err := os.ReadDir(chain)
	if err != nil {
		t.Fatal(err)
	}
	var rings int
	for _, e := range entries {
		if _, ok := parseRingIndex(e.Name()); ok {
			rings++
		}
	}
	if rings != 2 {
		t.Fatalf("ring count = %d, want 2", rings)
	}
}

func TestIncrementalIsSegmented(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	const total = backupEntriesPerSegment*2 + 13
	ops := make([]BatchOp, total)
	for i := range ops {
		ops[i] = BatchOp{Key: fmt.Sprintf("inc-%06d", i), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental: %v", err)
	}

	// The ring is one file but carries three streamed segments.
	data, err := os.ReadFile(filepath.Join(chain, ringName(1)))
	if err != nil {
		t.Fatal(err)
	}
	segCount := binary.LittleEndian.Uint32(data[40:44])
	if segCount != 3 {
		t.Fatalf("ring segment count = %d, want 3", segCount)
	}
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	defer rs.Close()
	if st := rs.Stats(); st.Keys != total {
		t.Fatalf("restored keys = %d, want %d", st.Keys, total)
	}
}

func TestIncrementalAnchorsAtWatermark(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	// Drive an export while commits keep landing; then verify the artifact
	// restores to some internally consistent, fixed watermark state (not a
	// mixture) by restoring twice and comparing byte-for-byte.
	var wg sync.WaitGroup
	stop := make(chan struct{})
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
				{Key: "a", Value: []byte(fmt.Sprintf("%d", i))},
				{Key: fmt.Sprintf("w%05d", i), Value: []byte("x")},
			})
		}
	}()
	var rings []string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("concurrent incremental: %v", err)
		}
		rings = append(rings, copyChain(t, chain))
	}
	close(stop)
	wg.Wait()

	first, err := Restore(rings[0], filepath.Join(t.TempDir(), "a"))
	if err != nil {
		t.Fatal(err)
	}
	p1 := allPairs(t, first)
	first.Close()
	first2, err := Restore(rings[0], filepath.Join(t.TempDir(), "b"))
	if err != nil {
		t.Fatal(err)
	}
	p2 := allPairs(t, first2)
	first2.Close()
	assertSamePairs(t, p1, p2, "deterministic fixed-watermark restore")

	// Final chain is internally consistent.
	tip, err := Restore(chain, filepath.Join(t.TempDir(), "tip"))
	if err != nil {
		t.Fatal(err)
	}
	pairs := allPairs(t, tip)
	for i := 1; i < len(pairs); i++ {
		if pairs[i].Key <= pairs[i-1].Key {
			t.Fatal("restored pairs not strictly ordered")
		}
	}
	tip.Close()
}

func TestIncrementalRejectsCorruption(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < backupEntriesPerSegment+10; i++ {
		if err := s.Put(fmt.Sprintf("k%06d", i), []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "good")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := s.Put(fmt.Sprintf("k%06d", i), []byte("changed")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	for i := 20; i < 40; i++ {
		if err := s.Delete(fmt.Sprintf("k%06d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	for i := 40; i < 60; i++ {
		if err := s.Put(fmt.Sprintf("k%06d", i), []byte("again")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}

	expectReject := func(t *testing.T, artifact string) {
		t.Helper()
		target := filepath.Join(t.TempDir(), "target")
		rs, err := Restore(artifact, target)
		if err == nil {
			rs.Close()
			t.Fatal("restore of corrupt chain succeeded")
		}
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("err=%v, want fs.ErrInvalid", err)
		}
		if _, statErr := os.Stat(target); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("target created by rejected restore: %v", statErr)
		}
		// Appending onto the damaged chain is rejected too.
		if err := s.BackupIncremental(artifact); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("incremental onto damaged chain: err=%v", err)
		}
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
	t.Run("flipped entry byte", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(1))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[incrRingHeaderSize+incrSegHeaderSize+10] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("broken prev link", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(2))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[12] ^= 0xFF // prevLink field
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
	t.Run("snapshot count regresses", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(2))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(data[32:40], 0)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("unknown ring version", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(1))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(data[4:8], backupVersion+99)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("bad file crc", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, ringName(1))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-1] ^= 0xFF // terminal file CRC
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("foreign file", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.WriteFile(filepath.Join(d, "notes.txt"), []byte("hi"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("stray incremental staging", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.MkdirAll(filepath.Join(d, incrStageDir), 0o700); err != nil {
			t.Fatal(err)
		}
		// A present staging directory is debris: restore rejects it, while the
		// next incremental export into the directory sweeps it and succeeds.
		target := filepath.Join(t.TempDir(), "target")
		if rs, err := Restore(d, target); err == nil {
			rs.Close()
			t.Fatal("restore with stray staging succeeded")
		} else if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("restore with stray staging: %v", err)
		}
		if err := s.BackupIncremental(d); err != nil {
			t.Fatalf("incremental should sweep its own staging debris: %v", err)
		}
	})
	t.Run("missing manifest", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.Remove(filepath.Join(d, backupManifest)); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
	t.Run("corrupt head segment", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, segmentName(0))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[backupSegHeaderSize+10] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
}

func TestIncrementalPathValidation(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	// Empty path.
	if err := s.BackupIncremental(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty path: %v", err)
	}
	// Missing directory.
	if err := s.BackupIncremental(filepath.Join(t.TempDir(), "nope")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing dir: %v", err)
	}
	// Regular file.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file path: %v", err)
	}
	// Existing empty directory without a head.
	if err := s.BackupIncremental(t.TempDir()); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("dir without head: %v", err)
	}
	// Closed store.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed store: %v", err)
	}
	// Full backup on a closed store still reports closed.
	if err := s.Backup(t.TempDir()); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed full backup: %v", err)
	}
}

func TestIncrementalDebrisSwept(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	// Simulate a kill mid-incremental: a staging directory with a half ring.
	if err := os.MkdirAll(filepath.Join(chain, incrStageDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(chain, incrStageDir, ringName(1)),
		[]byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("more", []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental over debris: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chain, incrStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris left after a completed incremental")
	}
	if _, err := os.Stat(filepath.Join(chain, ringName(1))); err != nil {
		t.Fatalf("ring missing: %v", err)
	}

	// A store directory carrying the same debris opens cleanly and keeps data.
	storeDir := tempDir(t)
	s2 := openStore(t, storeDir)
	if err := s2.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(storeDir, incrStageDir), 0o700); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(storeDir)
	if err != nil {
		t.Fatalf("open over incremental debris: %v", err)
	}
	defer s3.Close()
	if got, ok, _ := s3.Get("keep"); !ok || string(got) != "v" {
		t.Fatalf("data lost: %q %v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(storeDir, incrStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("incremental debris not swept on open")
	}
}

func TestIncrementalRingsHaveNoResidentKeyspace(t *testing.T) {
	// A long chain restores correctly end to end. The implementation streams;
	// this guards correctness as the chain grows rather than measuring memory.
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
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("ring %d: %v", r, err)
		}
	}
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), 0)
	rs.Close()
}

func TestIncrementalReadsStayLockFree(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 200; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// One slow exporter churning large rings.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ops := make([]BatchOp, 2000)
			for i := range ops {
				ops[i] = BatchOp{Key: fmt.Sprintf("bulk-%05d", i), Value: bytes.Repeat([]byte("p"), 64)}
			}
			if err := s.CommitBatch(ops); err != nil {
				return
			}
			if err := s.BackupIncremental(chain); err != nil {
				return
			}
		}
	}()
	// Readers must keep making progress off memory.
	deadline := time.Now().Add(2 * time.Second)
	var reads int
	for time.Now().Before(deadline) {
		if _, ok, err := s.Get("k0001"); err != nil || !ok {
			t.Fatalf("read during export: ok=%v err=%v", ok, err)
		}
		reads++
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("no reads completed")
	}
}
