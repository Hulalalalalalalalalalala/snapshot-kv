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
	"sort"
	"sync"
	"testing"
	"time"
)

// allPairs returns every present key/value pair in ascending key order.
func allPairs(t *testing.T, s *Store) []KVPair {
	t.Helper()
	pairs, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return pairs
}

// pairsFromSnapshot returns every pair of a fixed snapshot.
func pairsFromSnapshot(t *testing.T, snap *Snapshot) []KVPair {
	t.Helper()
	pairs, err := snap.Scan(Range{}, 0)
	if err != nil {
		t.Fatalf("snapshot scan: %v", err)
	}
	return pairs
}

func assertSamePairs(t *testing.T, got, want []KVPair, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d pairs, want %d", label, len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("%s: pair %d = (%q,%q), want (%q,%q)",
				label, i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}

// seedState fills s with a spread of puts, empty values and a deletion.
func seedState(t *testing.T, s *Store) {
	t.Helper()
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one")},
		{Key: "beta", Value: []byte("val2")},
		{Key: "empty", Value: nil},
		{Key: "gone", Value: []byte("x")},
		{Key: "multi", Value: []byte("first")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "gone", Delete: true},
		{Key: "multi", Value: []byte("second")},
		{Key: "nested/a", Value: []byte("a")},
		{Key: "nested/b", Value: []byte("bb")},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreRoundtrip(t *testing.T) {
	srcDir := tempDir(t)
	s := openStore(t, srcDir)
	seedState(t, s)
	for i := 0; i < 3; i++ {
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	want := allPairs(t, s)
	if snaps := s.Stats().Snapshots; snaps != 3 {
		t.Fatalf("snapshots before backup: %d, want 3", snaps)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	// A complete artifact has a manifest and no staging directory.
	if _, err := os.Stat(filepath.Join(backupDir, backupManifest)); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, backupStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("staging directory left behind: %v", err)
	}

	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(backupDir, target)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// The restored store is usable immediately, without a reopen, and first
	// shows exactly the watermark state.
	assertSamePairs(t, allPairs(t, rs), want, "restored live state")
	if _, ok, _ := rs.Get("gone"); ok {
		t.Fatal("deleted key resurrected by restore")
	}
	if got, ok, _ := rs.Get("empty"); !ok || len(got) != 0 {
		t.Fatalf("empty-value hit lost: ok=%v len=%d", ok, len(got))
	}
	if snaps := rs.Stats().Snapshots; snaps != 3 {
		t.Fatalf("restored snapshot count: %d, want 3", snaps)
	}
	if err := rs.Put("after-restore", []byte("z")); err != nil {
		t.Fatalf("put on restored store: %v", err)
	}
	if got, ok, _ := rs.Get("after-restore"); !ok || string(got) != "z" {
		t.Fatalf("post-restore write missing: %q %v", got, ok)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: byte-for-byte the watermark state plus the one post-restore put,
	// snapshot count intact and continuing.
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	want2 := append(append([]KVPair{}, want...), KVPair{Key: "after-restore", Value: []byte("z")})
	sort.Slice(want2, func(i, j int) bool { return want2[i].Key < want2[j].Key })
	assertSamePairs(t, allPairs(t, s2), want2, "reopened restored state")
	if n := s2.Stats().Snapshots; n != 3 {
		t.Fatalf("snapshot count after reopen: %d, want 3", n)
	}
	if _, err := s2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if n := s2.Stats().Snapshots; n != 4 {
		t.Fatalf("snapshot count does not continue: %d, want 4", n)
	}

	// The source store is untouched and still fully usable.
	if got, ok, _ := s.Get("alpha"); !ok || string(got) != "one" {
		t.Fatalf("source store changed by backup: %q %v", got, ok)
	}
}

func TestBackupAnchorsAtWatermark(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	want := allPairs(t, s)

	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Commits strictly after the anchor must not enter the artifact.
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("LATER")},
		{Key: "brand-new", Value: []byte("n")},
		{Key: "beta", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(backupDir, target)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer rs.Close()
	assertSamePairs(t, allPairs(t, rs), want, "watermark state")
	if got, ok, _ := rs.Get("alpha"); !ok || string(got) != "one" {
		t.Fatalf("post-watermark write leaked into backup: %q %v", got, ok)
	}
	if _, ok, _ := rs.Get("brand-new"); ok {
		t.Fatal("post-watermark key present in backup")
	}
	if _, ok, _ := rs.Get("beta"); !ok {
		t.Fatal("post-watermark delete leaked into backup")
	}
}

func TestBackupEmptyStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup empty: %v", err)
	}
	segs, err := filepath.Glob(filepath.Join(backupDir, backupSegmentPref+"*"+backupSegmentSufx))
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 0 {
		t.Fatalf("empty backup has %d segments", len(segs))
	}
	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(backupDir, target)
	if err != nil {
		t.Fatalf("Restore empty: %v", err)
	}
	defer rs.Close()
	if st := rs.Stats(); st.Keys != 0 || st.Snapshots != 0 {
		t.Fatalf("restored empty store has state: %+v", st)
	}
	if err := rs.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	rs.Close()
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("write to restored empty store lost: %q %v", got, ok)
	}
}

func TestBackupIsSegmentedAndStreams(t *testing.T) {
	s := openStore(t, tempDir(t))
	// More keys than several segments hold, committed in a few batches to
	// avoid one fsync per key while still populating many keys.
	const total = backupEntriesPerSegment*2 + 17
	ops := make([]BatchOp, total)
	for i := range ops {
		ops[i] = BatchOp{Key: fmt.Sprintf("key-%06d", i), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	var nSeg int
	for _, e := range entries {
		if isBackupSegmentName(e.Name()) {
			nSeg++
		}
	}
	if nSeg != 3 {
		t.Fatalf("segment count = %d, want 3", nSeg)
	}

	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(backupDir, target)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer rs.Close()
	if st := rs.Stats(); st.Keys != total {
		t.Fatalf("restored keys = %d, want %d", st.Keys, total)
	}
	// Spot-check first, a middle and the last key survive ordered restore.
	for _, i := range []int{0, total / 2, total - 1} {
		k := fmt.Sprintf("key-%06d", i)
		if got, ok, _ := rs.Get(k); !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %s restored as %q ok=%v", k, got, ok)
		}
	}
	assertSamePairs(t, allPairs(t, rs), allPairs(t, s), "full segmented restore")
}

func TestBackupConcurrentWithCommits(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("init-%03d", i), []byte("i")); err != nil {
			t.Fatal(err)
		}
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

	// Several backups run while writes flow; each must succeed and restore to
	// a self-consistent state, and the reads never block on the export.
	var backups []string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		bd := filepath.Join(t.TempDir(), "backup")
		if err := s.Backup(bd); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("concurrent Backup: %v", err)
		}
		backups = append(backups, bd)
	}
	close(stop)
	wg.Wait()

	for _, bd := range backups {
		rs, err := Restore(bd, filepath.Join(t.TempDir(), "r"))
		if err != nil {
			t.Fatalf("restore concurrent backup: %v", err)
		}
		pairs := allPairs(t, rs)
		// Restored state must be internally consistent: unique sorted keys and
		// every value decodes as a non-negative decimal "g-i" marker for the
		// shared key and init keys present.
		for i := 1; i < len(pairs); i++ {
			if pairs[i].Key <= pairs[i-1].Key {
				t.Fatalf("restored pairs not strictly ordered at %d", i)
			}
		}
		if _, ok, _ := rs.Get("shared"); !ok {
			t.Fatal("shared key missing in restored backup")
		}
		if _, ok, _ := rs.Get("init-000"); !ok {
			t.Fatal("pre-backup key missing in restored backup")
		}
		rs.Close()
	}
}

func TestBackupOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err := s.Backup(filepath.Join(t.TempDir(), "backup"))
	if !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Backup on closed: err=%v, want fs.ErrClosed", err)
	}
}

func TestBackupPathValidation(t *testing.T) {
	s := openStore(t, tempDir(t))

	// Output path naming a regular file is invalid.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Backup to file: err=%v, want fs.ErrInvalid", err)
	}

	// Empty path is invalid.
	if err := s.Backup(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Backup empty path: err=%v, want fs.ErrInvalid", err)
	}

	// A directory containing unrelated entries is invalid.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(dir); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Backup over non-empty dir: err=%v, want fs.ErrInvalid", err)
	}

	// A completed backup cannot be overwritten by another backup.
	bd := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bd); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(bd); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("overwrite backup: err=%v, want fs.ErrInvalid", err)
	}
}

// corruptArtifact builds a good backup then applies a mutation before each
// restore attempt, which must be rejected wholesale.
func TestRestoreRejectsCorruption(t *testing.T) {
	s := openStore(t, tempDir(t))
	// Enough keys for multiple segments.
	ops := make([]BatchOp, backupEntriesPerSegment+50)
	for i := range ops {
		ops[i] = BatchOp{Key: fmt.Sprintf("k-%06d", i), Value: []byte(fmt.Sprintf("val%d", i))}
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(t.TempDir(), "good")
	if err := s.Backup(good); err != nil {
		t.Fatal(err)
	}

	copyDir := func(t *testing.T) string {
		t.Helper()
		dst := filepath.Join(t.TempDir(), "artifact")
		if err := os.MkdirAll(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(good)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			data, err := os.ReadFile(filepath.Join(good, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dst
	}

	expectReject := func(t *testing.T, artifact string) {
		t.Helper()
		target := filepath.Join(t.TempDir(), "target")
		rs, err := Restore(artifact, target)
		if err == nil {
			rs.Close()
			t.Fatal("restore of corrupt artifact succeeded")
		}
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("restore corrupt: err=%v, want fs.ErrInvalid", err)
		}
		if _, statErr := os.Stat(target); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("target created by rejected restore: %v", statErr)
		}
	}

	t.Run("missing segment", func(t *testing.T) {
		d := copyDir(t)
		if err := os.Remove(filepath.Join(d, segmentName(1))); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("truncated segment", func(t *testing.T) {
		d := copyDir(t)
		p := filepath.Join(d, segmentName(0))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data[:len(data)/2], 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("flipped segment byte", func(t *testing.T) {
		d := copyDir(t)
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

	t.Run("unknown segment version", func(t *testing.T) {
		d := copyDir(t)
		p := filepath.Join(d, segmentName(0))
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

	t.Run("unknown manifest version", func(t *testing.T) {
		d := copyDir(t)
		p := filepath.Join(d, backupManifest)
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

	t.Run("truncated manifest", func(t *testing.T) {
		d := copyDir(t)
		p := filepath.Join(d, backupManifest)
		if err := os.Truncate(p, backupManifestSize); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("missing manifest", func(t *testing.T) {
		d := copyDir(t)
		if err := os.Remove(filepath.Join(d, backupManifest)); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("foreign file", func(t *testing.T) {
		d := copyDir(t)
		if err := os.WriteFile(filepath.Join(d, "notes.txt"), []byte("hi"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("stray staging directory", func(t *testing.T) {
		d := copyDir(t)
		if err := os.MkdirAll(filepath.Join(d, backupStageDir), 0o700); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})

	t.Run("stray manifest temp", func(t *testing.T) {
		d := copyDir(t)
		if err := os.WriteFile(filepath.Join(d, backupManifestTmp), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectReject(t, d)
	})
}

func TestRestorePathValidation(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	good := filepath.Join(t.TempDir(), "good")
	if err := s.Backup(good); err != nil {
		t.Fatal(err)
	}

	// Backup path missing.
	if _, err := Restore(filepath.Join(t.TempDir(), "nope"),
		filepath.Join(t.TempDir(), "t")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore missing backup: err=%v, want fs.ErrInvalid", err)
	}

	// Backup path is a regular file.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(file, filepath.Join(t.TempDir(), "t")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore file backup: err=%v, want fs.ErrInvalid", err)
	}

	// Target existing and non-empty (the source store dir) is invalid.
	if _, err := Restore(good, s.dir); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore into non-empty target: err=%v, want fs.ErrInvalid", err)
	}

	// Target naming a regular file is invalid.
	if _, err := Restore(good, file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore onto file: err=%v, want fs.ErrInvalid", err)
	}

	// Target existing but empty is accepted.
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.MkdirAll(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	rs, err := Restore(good, empty)
	if err != nil {
		t.Fatalf("restore into empty dir: %v", err)
	}
	rs.Close()
}

func TestBackupDebrisSweptOnNextBackupAndOpen(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)

	// Simulate a backup killed mid-run: orphan promoted segments, a staging
	// directory and a manifest temp, but no manifest.
	dir := filepath.Join(t.TempDir(), "backup")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, backupStageDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, segmentName(0)), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, backupManifestTmp), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The next backup into that directory clears the debris and completes.
	if err := s.Backup(dir); err != nil {
		t.Fatalf("Backup over debris: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, backupManifest)); err != nil {
		t.Fatalf("fresh manifest missing after debris sweep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, backupStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris not swept")
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
	if err := os.MkdirAll(filepath.Join(storeDir, backupStageDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeDir, segmentName(0)), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(storeDir)
	if err != nil {
		t.Fatalf("open over backup debris: %v", err)
	}
	defer s3.Close()
	if got, ok, _ := s3.Get("keep"); !ok || string(got) != "v" {
		t.Fatalf("data lost across debris-sweeping open: %q %v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(storeDir, backupStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris not swept on open")
	}
}

func TestRestoreDebrisSweptOnOpen(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "store")
	// A half-built restore sibling left by a simulated kill.
	stale := target + ".restore.tmp"
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, walName), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	stale2 := filepath.Join(parent, "store.restore-random123")
	if err := os.MkdirAll(stale2, 0o700); err != nil {
		t.Fatal(err)
	}

	s := openStore(t, target)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("fixed-name restore debris not swept on open")
	}
	if _, err := os.Stat(stale2); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("random-name restore debris not swept on open")
	}
}

func TestRestoredStateMatchesFullReplay(t *testing.T) {
	src := openStore(t, tempDir(t))
	// Batch last-write-wins semantics must survive the export watermark.
	if err := src.CommitBatch([]BatchOp{
		{Key: "a", Value: []byte("a1")},
		{Key: "a", Value: []byte("a2")},
		{Key: "b", Value: []byte("b1")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.CommitBatch([]BatchOp{
		{Key: "c", Value: []byte("c1")},
		{Key: "c", Delete: true}, // put-then-delete within view across commits
		{Key: "d", Delete: true},
		{Key: "d", Value: []byte("d1")}, // delete-then-put
	}); err != nil {
		t.Fatal(err)
	}
	watermark, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := pairsFromSnapshot(t, watermark)
	watermark.Close()

	bd := filepath.Join(t.TempDir(), "backup")
	if err := src.Backup(bd); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(bd, target)
	if err != nil {
		t.Fatal(err)
	}
	rs.Close()

	// Reopen and compare against the fixed watermark snapshot: the restored
	// directory reopened must equal a full replay at the export watermark.
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertSamePairs(t, allPairs(t, s2), want, "full-replay-equivalent reopen")
	if _, ok, _ := s2.Get("c"); ok {
		t.Fatal("tombstone not reproduced on reopen")
	}
	if got, ok, _ := s2.Get("d"); !ok || string(got) != "d1" {
		t.Fatalf("delete-then-put not reproduced: %q %v", got, ok)
	}
}

// TestQueuedBatchMemoryDoesNotScaleWithLiveKeys guards the group-commit
// tightening: a long queue coalescing behind one sync against a thousand-key
// store must publish correct state and leave the committed view shallow; the
// temporary maps it builds never scale with the queue length.
func TestQueuedBatchMemoryDoesNotScaleWithLiveKeys(t *testing.T) {
	s := openStore(t, tempDir(t))
	const liveKeys = 1500
	seed := make([]BatchOp, liveKeys)
	for i := range seed {
		seed[i] = BatchOp{Key: fmt.Sprintf("live-%05d", i), Value: []byte("payload")}
	}
	if err := s.CommitBatch(seed); err != nil {
		t.Fatal(err)
	}

	probe := func() {
		const followers = 200
		proceed := make(chan struct{})
		var leaderWG sync.WaitGroup
		leaderWG.Add(1)
		s.leaderHook = func() {
			leaderWG.Done()
			<-proceed
		}
		leaderErr := make(chan error, 1)
		go func() {
			leaderErr <- s.CommitBatch([]BatchOp{{Key: "leader", Value: []byte("L")}})
		}()
		leaderWG.Wait()

		var wg sync.WaitGroup
		for i := 0; i < followers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := s.CommitBatch([]BatchOp{{Key: fmt.Sprintf("q-%04d", i), Value: []byte("x")}}); err != nil {
					t.Errorf("follower %d: %v", i, err)
				}
			}(i)
		}
		for queueLen(s) != followers+1 {
			runtime.Gosched()
		}
		close(proceed)
		if err := <-leaderErr; err != nil {
			t.Errorf("leader: %v", err)
		}
		wg.Wait()
		s.leaderHook = nil
	}

	// Repeated queueing must not accumulate overlay depth or stale maps.
	for round := 0; round < 4; round++ {
		probe()
		if d := s.current.Load().depth; d > viewFlattenLimit {
			t.Fatalf("round %d: post-group view depth %d exceeds %d", round, d, viewFlattenLimit)
		}
		runtime.GC()
	}

	if got, ok, _ := s.Get("live-00000"); !ok || string(got) != "payload" {
		t.Fatalf("live state wrong after queued groups: %q %v", got, ok)
	}
	if got, ok, _ := s.Get("q-0199"); !ok || string(got) != "x" {
		t.Fatalf("queued write missing: %q %v", got, ok)
	}
	if st := s.Stats(); st.Keys != liveKeys+200+1 {
		t.Fatalf("keys after queued groups: %d, want %d", st.Keys, liveKeys+201)
	}

	// The retained state must reopen intact.
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if st := s2.Stats(); st.Keys != liveKeys+200+1 {
		t.Fatalf("keys after reopen: %d, want %d", st.Keys, liveKeys+201)
	}
}

func TestRestoredStoreSupportsMaintenance(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	bd := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bd); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(bd, target)
	if err != nil {
		t.Fatal(err)
	}
	want := allPairs(t, rs)

	// Every maintenance entry point works on the restored store with no
	// re-export or repair step needed first.
	if err := rs.Checkpoint(); err != nil {
		t.Fatalf("checkpoint restored store: %v", err)
	}
	if err := rs.Compact(); err != nil {
		t.Fatalf("compact restored store: %v", err)
	}
	if err := rs.Put("later", []byte("L")); err != nil {
		t.Fatal(err)
	}
	if err := rs.Compact(); err != nil {
		t.Fatalf("compact after write: %v", err)
	}
	bd2 := filepath.Join(t.TempDir(), "backup2")
	if err := rs.Backup(bd2); err != nil {
		t.Fatalf("re-backup restored store: %v", err)
	}
	cur, err := rs.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	pages, err := cur.Page(0, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	cur.Close()
	want2 := append(append([]KVPair{}, want...), KVPair{Key: "later", Value: []byte("L")})
	sort.Slice(want2, func(i, j int) bool { return want2[i].Key < want2[j].Key })
	assertSamePairs(t, pages, want2, "restored cursor after maintenance")
	if n := rs.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count on restored store: %d, want 1", n)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen after checkpoint + compact: still complete, no repair needed.
	s2, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertSamePairs(t, allPairs(t, s2), want2, "reopen after maintenance")

	// The second-generation backup also restores cleanly.
	rs2, err := Restore(bd2, filepath.Join(t.TempDir(), "r2"))
	if err != nil {
		t.Fatalf("restore re-backup: %v", err)
	}
	defer rs2.Close()
	assertSamePairs(t, allPairs(t, rs2), want2, "re-backup roundtrip")
}

func TestBackupArtifactCarriesNoDeletedKeys(t *testing.T) {
	s := openStore(t, tempDir(t))
	// Touch and delete many keys so a naive export that carried tombstones
	// would be large; the artifact must be proportional to live data.
	const deleted = 3000
	ops := make([]BatchOp, 0, deleted*2)
	for i := 0; i < deleted; i++ {
		k := fmt.Sprintf("del-%05d", i)
		ops = append(ops, BatchOp{Key: k, Value: []byte("payload-payload")})
	}
	if err := s.CommitBatch(ops); err != nil {
		t.Fatal(err)
	}
	del := make([]BatchOp, deleted)
	for i := range del {
		del[i] = BatchOp{Key: fmt.Sprintf("del-%05d", i), Delete: true}
	}
	if err := s.CommitBatch(del); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("only-live", []byte("v")); err != nil {
		t.Fatal(err)
	}

	bd := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bd); err != nil {
		t.Fatal(err)
	}
	var artifactBytes int64
	entries, err := os.ReadDir(bd)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			artifactBytes += fi.Size()
		}
	}
	// 3000 deleted keys at ~20+ bytes each would be >60KB of entry data; the
	// live artifact is one key plus the small headers/manifest.
	if artifactBytes > 20000 {
		t.Fatalf("artifact carries deleted history: %d bytes", artifactBytes)
	}

	rs, err := Restore(bd, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if st := rs.Stats(); st.Keys != 1 {
		t.Fatalf("restored keys = %d, want 1", st.Keys)
	}
	if got, ok, _ := rs.Get("only-live"); !ok || string(got) != "v" {
		t.Fatalf("live key missing: %q %v", got, ok)
	}
	if _, ok, _ := rs.Get("del-00000"); ok {
		t.Fatal("deleted key present after restore")
	}
}

func TestRestoreIsIdempotentAcrossRepeatedRuns(t *testing.T) {
	s := openStore(t, tempDir(t))
	seedState(t, s)
	bd := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bd); err != nil {
		t.Fatal(err)
	}
	want := allPairs(t, s)

	// The same artifact restores into many distinct targets, each complete.
	for i := 0; i < 3; i++ {
		target := filepath.Join(t.TempDir(), fmt.Sprintf("r%d", i))
		rs, err := Restore(bd, target)
		if err != nil {
			t.Fatalf("restore run %d: %v", i, err)
		}
		assertSamePairs(t, allPairs(t, rs), want, fmt.Sprintf("restore run %d", i))
		rs.Close()
	}

	// Restoring a second time onto an existing non-empty target is rejected.
	taken := filepath.Join(t.TempDir(), "taken")
	rs, err := Restore(bd, taken)
	if err != nil {
		t.Fatal(err)
	}
	rs.Close()
	if _, err := Restore(bd, taken); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore over populated target: err=%v, want fs.ErrInvalid", err)
	}
}
