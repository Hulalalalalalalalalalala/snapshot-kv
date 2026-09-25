package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// chainRingDirs lists the ring directories of a chain in chain order.
func chainRingDirs(t *testing.T, chainDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		t.Fatal(err)
	}
	var rings []string
	for _, e := range entries {
		if e.IsDir() {
			if _, ok := parseRingDirName(e.Name()); !ok {
				t.Fatalf("foreign directory in chain: %s", e.Name())
			}
			rings = append(rings, e.Name())
		}
	}
	sort.Strings(rings)
	return rings
}

func TestIncrementalChainRoundtrip(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	for i := 0; i < 2; i++ {
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}

	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Ring 1: overwrite one key, delete one, add one.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one-v2")},
		{Key: "beta", Delete: true},
		{Key: "ring1", Value: []byte("r1")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatalf("BackupIncremental 1: %v", err)
	}

	// Ring 2: delete a head key, re-add a deleted one, add more.
	if err := s.CommitBatch([]BatchOp{
		{Key: "multi", Delete: true},
		{Key: "beta", Value: []byte("beta-back")},
		{Key: "ring2", Value: []byte("r2")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatalf("BackupIncremental 2: %v", err)
	}

	rings := chainRingDirs(t, chainDir)
	if len(rings) != 2 || rings[0] != ringDirName(1) || rings[1] != ringDirName(2) {
		t.Fatalf("chain rings = %v, want [%s %s]", rings, ringDirName(1), ringDirName(2))
	}

	// No commits after the last anchor: the synthesized state must equal the
	// live state, which is the state at the chain's last watermark.
	want := allPairs(t, s)
	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(chainDir, target)
	if err != nil {
		t.Fatalf("Restore chain: %v", err)
	}
	assertSamePairs(t, allPairs(t, rs), want, "synthesized state")
	if _, ok, _ := rs.Get("multi"); ok {
		t.Fatal("key deleted in ring 2 readable after synthesis")
	}
	if got, ok, _ := rs.Get("beta"); !ok || string(got) != "beta-back" {
		t.Fatalf("re-added key = %q ok=%v", got, ok)
	}
	if got, ok, _ := rs.Get("alpha"); !ok || string(got) != "one-v2" {
		t.Fatalf("overwritten key = %q ok=%v", got, ok)
	}
	if snaps := rs.Stats().Snapshots; snaps != 3 {
		t.Fatalf("synthesized snapshot count = %d, want 3", snaps)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: byte-for-byte the state at the chain's last watermark.
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertSamePairs(t, allPairs(t, s2), want, "reopened synthesized state")
	if n := s2.Stats().Snapshots; n != 3 {
		t.Fatalf("snapshot count after reopen = %d, want 3", n)
	}
}

func TestIncrementalAnchorsAtWatermark(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("alpha", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatal(err)
	}

	// Commits strictly after the ring's anchor must not enter the ring.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("v3-late")},
		{Key: "late", Value: []byte("n")},
		{Key: "nested/a", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}

	rs, err := Restore(chainDir, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if got, ok, _ := rs.Get("alpha"); !ok || string(got) != "v2" {
		t.Fatalf("post-anchor write leaked into ring: %q %v", got, ok)
	}
	if _, ok, _ := rs.Get("late"); ok {
		t.Fatal("post-anchor key present in chain")
	}
	if _, ok, _ := rs.Get("nested/a"); !ok {
		t.Fatal("post-anchor delete leaked into ring")
	}
}

func TestIncrementalEmptyRing(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}
	// No commits between the two exports: the ring covers an empty interval.
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatalf("empty BackupIncremental: %v", err)
	}
	want := allPairs(t, s)
	rs, err := Restore(chainDir, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	assertSamePairs(t, allPairs(t, rs), want, "chain with empty ring")
}

func TestIncrementalOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err := s.BackupIncremental(chainDir)
	if !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("BackupIncremental on closed: err=%v, want fs.ErrClosed", err)
	}
}

func TestIncrementalRequiresUsableChain(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Chain path missing entirely.
	missing := filepath.Join(t.TempDir(), "nope")
	if err := s.BackupIncremental(missing); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing chain: err=%v, want fs.ErrInvalid", err)
	}

	// Chain path is a regular file.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("chain is file: err=%v, want fs.ErrInvalid", err)
	}

	// Empty directory: no chain head, no usable previous watermark.
	empty := t.TempDir()
	if err := s.BackupIncremental(empty); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty chain dir: err=%v, want fs.ErrInvalid", err)
	}

	// A live store directory is not a chain.
	if err := s.BackupIncremental(s.dir); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("store dir as chain: err=%v, want fs.ErrInvalid", err)
	}

	// None of the rejections left a half-built ring behind.
	for _, dir := range []string{empty, s.dir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if _, ok := parseRingDirName(e.Name()); ok {
				t.Fatalf("half ring left in %s after rejection", dir)
			}
		}
	}
}

// corruptChain builds a good two-ring chain and applies a mutation before
// each restore attempt, which must be rejected wholesale.
func TestRestoreChainRejectsDefects(t *testing.T) {
	s := openStore(t, tempDir(t))
	ops := make([]BatchOp, backupEntriesPerSegment+10)
	for i := range ops {
		ops[i] = BatchOp{Key: fmt.Sprintf("k-%06d", i), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(good); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "k-000000", Value: []byte("w1")},
		{Key: "k-000001", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(good); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k-000002", []byte("w2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(good); err != nil {
		t.Fatal(err)
	}

	// copyChain duplicates the whole chain directory tree.
	copyChain := func(t *testing.T) string {
		t.Helper()
		dst := filepath.Join(t.TempDir(), "chain")
		if err := os.MkdirAll(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, e := range mustReadDir(t, good) {
			src := filepath.Join(good, e.Name())
			if e.IsDir() {
				ringDst := filepath.Join(dst, e.Name())
				if err := os.MkdirAll(ringDst, 0o700); err != nil {
					t.Fatal(err)
				}
				for _, re := range mustReadDir(t, src) {
					copyFile(t, filepath.Join(src, re.Name()), filepath.Join(ringDst, re.Name()))
				}
			} else {
				copyFile(t, src, filepath.Join(dst, e.Name()))
			}
		}
		return dst
	}

	expectReject := func(t *testing.T, chain string) {
		t.Helper()
		target := filepath.Join(t.TempDir(), "target")
		rs, err := Restore(chain, target)
		if err == nil {
			rs.Close()
			t.Fatal("restore of defective chain succeeded")
		}
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("restore defective chain: err=%v, want fs.ErrInvalid", err)
		}
		if _, statErr := os.Stat(target); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("target created by rejected restore: %v", statErr)
		}
	}

	t.Run("missing ring", func(t *testing.T) {
		d := copyChain(t)
		if err := os.RemoveAll(filepath.Join(d, ringDirName(1))); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("watermark discontinuity", func(t *testing.T) {
		d := copyChain(t)
		// Renumber ring 2 as ring 1: its base no longer continues the head.
		if err := os.RemoveAll(filepath.Join(d, ringDirName(1))); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(d, ringDirName(2)), filepath.Join(d, ringDirName(1))); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("truncated ring segment", func(t *testing.T) {
		d := copyChain(t)
		p := filepath.Join(d, ringDirName(1), segmentName(0))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data[:len(data)/2], 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("flipped ring segment byte", func(t *testing.T) {
		d := copyChain(t)
		p := filepath.Join(d, ringDirName(2), segmentName(0))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[backupSegHeaderSize] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("unknown ring manifest version", func(t *testing.T) {
		d := copyChain(t)
		p := filepath.Join(d, ringDirName(1), backupManifest)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(data[4:8], backupVersion+99)
		fixCRC(data)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("rebased ring manifest", func(t *testing.T) {
		d := copyChain(t)
		p := filepath.Join(d, ringDirName(2), backupManifest)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		// A forged base that does not continue ring 1's watermark, CRC fixed
		// up so only the continuity check can catch it.
		base := binary.LittleEndian.Uint64(data[24:32])
		binary.LittleEndian.PutUint64(data[24:32], base+1)
		fixCRC(data)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("ring missing manifest", func(t *testing.T) {
		d := copyChain(t)
		if err := os.Remove(filepath.Join(d, ringDirName(1), backupManifest)); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("foreign file in chain", func(t *testing.T) {
		d := copyChain(t)
		if err := os.WriteFile(filepath.Join(d, "notes.txt"), []byte("hi"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("foreign file in ring", func(t *testing.T) {
		d := copyChain(t)
		if err := os.WriteFile(filepath.Join(d, ringDirName(1), "notes.txt"), []byte("hi"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("foreign directory in chain", func(t *testing.T) {
		d := copyChain(t)
		if err := os.MkdirAll(filepath.Join(d, "incr-x"), 0o700); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
}

// fixCRC rewrites the terminal CRC-32 of a manifest or segment in place.
func fixCRC(data []byte) {
	binary.LittleEndian.PutUint32(data[len(data)-crcSize:], crc32.ChecksumIEEE(data[:len(data)-crcSize]))
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalDebrisSwept(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}

	// Simulate an incremental export killed mid-run: a staging sibling with
	// half a ring inside.
	stale := chainDir + ".incr-debris1"
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, segmentName(0)), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The chain remains restorable with the debris in place, and the next
	// incremental export sweeps it.
	if err := s.Put("after", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatalf("BackupIncremental over debris: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris not swept by BackupIncremental")
	}
	want := allPairs(t, s)
	rs, err := Restore(chainDir, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	assertSamePairs(t, allPairs(t, rs), want, "chain after debris sweep")
	rs.Close()

	// A store directory whose sibling carries staging debris opens cleanly.
	stale2 := s.dir + ".incr-debris2"
	if err := os.MkdirAll(stale2, 0o700); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatalf("open with incremental debris sibling: %v", err)
	}
	s2.Close()
	if _, err := os.Stat(stale2); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris not swept on open")
	}
}

func TestIncrementalConcurrentWithCommits(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("init-%03d", i), []byte("i")); err != nil {
			t.Fatal(err)
		}
	}
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := s.CommitBatch([]BatchOp{
					{Key: fmt.Sprintf("w%d-%05d", g, i), Value: []byte(fmt.Sprintf("%d", i))},
					{Key: "shared", Value: []byte(fmt.Sprintf("%d-%d", g, i))},
				}); err != nil {
					return
				}
			}
		}(g)
	}

	// Rings accumulate while writes flow; every export must succeed without
	// blocking the commits.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.BackupIncremental(chainDir); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("concurrent BackupIncremental: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	// The final chain synthesizes a self-consistent state.
	rs, err := Restore(chainDir, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatalf("restore chain: %v", err)
	}
	defer rs.Close()
	pairs := allPairs(t, rs)
	for i := 1; i < len(pairs); i++ {
		if pairs[i].Key <= pairs[i-1].Key {
			t.Fatalf("synthesized pairs not strictly ordered at %d", i)
		}
	}
	if _, ok, _ := rs.Get("init-000"); !ok {
		t.Fatal("head key missing from synthesized state")
	}
	if _, ok, _ := rs.Get("shared"); !ok {
		t.Fatal("ring key missing from synthesized state")
	}
}

func TestIncrementalIsSegmentedAndStreams(t *testing.T) {
	s := openStore(t, tempDir(t))
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}
	// Touch more keys than one segment holds between the two watermarks.
	const total = backupEntriesPerSegment + 17
	ops := make([]BatchOp, total)
	for i := range ops {
		ops[i] = BatchOp{Key: fmt.Sprintf("key-%06d", i), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(chainDir, ringDirName(1)))
	if err != nil {
		t.Fatal(err)
	}
	var nSeg int
	for _, e := range entries {
		if isBackupSegmentName(e.Name()) {
			nSeg++
		}
	}
	if nSeg != 2 {
		t.Fatalf("ring segment count = %d, want 2", nSeg)
	}

	rs, err := Restore(chainDir, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if st := rs.Stats(); st.Keys != total {
		t.Fatalf("synthesized keys = %d, want %d", st.Keys, total)
	}
	for _, i := range []int{0, total / 2, total - 1} {
		k := fmt.Sprintf("key-%06d", i)
		if got, ok, _ := rs.Get(k); !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %s synthesized as %q ok=%v", k, got, ok)
		}
	}
}

func TestSynthesizedStoreSupportsMaintenance(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chainDir := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chainDir); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("v2")},
		{Key: "beta", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chainDir); err != nil {
		t.Fatal(err)
	}
	want := allPairs(t, s)

	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(chainDir, target)
	if err != nil {
		t.Fatal(err)
	}
	// Every maintenance entry point works on the synthesized store.
	if err := rs.Checkpoint(); err != nil {
		t.Fatalf("checkpoint synthesized store: %v", err)
	}
	if err := rs.Compact(); err != nil {
		t.Fatalf("compact synthesized store: %v", err)
	}
	if err := rs.Put("later", []byte("L")); err != nil {
		t.Fatal(err)
	}
	chain2 := filepath.Join(t.TempDir(), "chain2")
	if err := rs.Backup(chain2); err != nil {
		t.Fatalf("re-backup synthesized store: %v", err)
	}
	if err := rs.BackupIncremental(chain2); err != nil {
		t.Fatalf("incremental on synthesized re-backup: %v", err)
	}
	if n := rs.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count on synthesized store = %d, want 1", n)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	want2 := append(append([]KVPair{}, want...), KVPair{Key: "later", Value: []byte("L")})
	sort.Slice(want2, func(i, j int) bool { return want2[i].Key < want2[j].Key })
	assertSamePairs(t, allPairs(t, s2), want2, "reopen after maintenance")

	rs2, err := Restore(chain2, filepath.Join(t.TempDir(), "r2"))
	if err != nil {
		t.Fatalf("restore second-generation chain: %v", err)
	}
	defer rs2.Close()
	assertSamePairs(t, allPairs(t, rs2), want2, "second-generation chain roundtrip")
}

func TestRestoreSingleArtifactStillWorks(t *testing.T) {
	// A plain full backup with no rings restores exactly as before, and an
	// incremental export is rejected onto a directory that is not its chain.
	s := openStore(t, tempDir(t))
	seedState(t, s)
	bd := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bd); err != nil {
		t.Fatal(err)
	}
	want := allPairs(t, s)
	rs, err := Restore(bd, filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	assertSamePairs(t, allPairs(t, rs), want, "single-artifact restore")
}
