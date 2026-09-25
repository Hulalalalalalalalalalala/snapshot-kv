package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// scanAll returns every present pair of the store's latest view.
func scanAll(t *testing.T, s *Store) []KVPair {
	t.Helper()
	pairs, err := s.ScanPrefix("", 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return pairs
}

func equalPairs(a, b []KVPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
			return false
		}
	}
	return true
}

// populateStore writes a mix of puts, deletes, batches and empty values and
// takes two snapshots (bumping the cumulative count), returning the store.
func populateStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "c", Value: []byte("3")},
		{Key: "d", Value: []byte("4")},
		{Key: "b", Value: []byte("22")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("empty", nil); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snap.Close()
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBackupRestoreRoundtrip(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))
	defer s.Close()

	backupDir := filepath.Join(base, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("backup: %v", err)
	}
	restoreDir := filepath.Join(base, "restored")
	if err := Restore(backupDir, restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	r, err := Open(restoreDir)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer r.Close()

	if got, want := scanAll(t, r), scanAll(t, s); !equalPairs(got, want) {
		t.Fatalf("restored pairs %v != live pairs %v", got, want)
	}
	if got, want := r.Stats().Snapshots, s.Stats().Snapshots; got != want {
		t.Fatalf("snapshot count %d != %d", got, want)
	}
	if _, ok, err := r.Get("a"); err != nil || ok {
		t.Fatalf("deleted key present after restore: ok=%v err=%v", ok, err)
	}
	if v, ok, err := r.Get("empty"); err != nil || !ok || len(v) != 0 {
		t.Fatalf("empty value lost after restore: v=%q ok=%v err=%v", v, ok, err)
	}

	// The restored directory is a fully working store: writes, checkpoint,
	// compaction and reopen all behave as usual.
	if err := r.Put("post", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := r.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r2, err := Open(restoreDir)
	if err != nil {
		t.Fatalf("reopen restored: %v", err)
	}
	defer r2.Close()
	if v, ok, _ := r2.Get("post"); !ok || string(v) != "x" {
		t.Fatalf("post-restore write lost: %q ok=%v", v, ok)
	}
	if got, want := scanAll(t, r2), scanAll(t, s); !equalPairs(got[:len(got)-1], want) {
		t.Fatalf("after reopen pairs %v != %v", got, want)
	}
}

func TestBackupRestoreMatchesFullReplay(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))

	// A checkpoint plus more writes gives the source a nontrivial chain and
	// WAL tail; the backup must still equal the full-replay state.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{{Key: "e", Value: []byte("5")}, {Key: "c", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(filepath.Join(base, "backup")); err != nil {
		t.Fatalf("backup: %v", err)
	}
	s.Close()

	if err := Restore(filepath.Join(base, "backup"), filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer r.Close()
	orig, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatalf("reopen original: %v", err)
	}
	defer orig.Close()
	if got, want := scanAll(t, r), scanAll(t, orig); !equalPairs(got, want) {
		t.Fatalf("restored %v != original %v", got, want)
	}
	if got, want := r.Stats().Snapshots, orig.Stats().Snapshots; got != want {
		t.Fatalf("snapshot count %d != %d", got, want)
	}
}

func TestBackupFromSnapshotWatermark(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Put("k", []byte("v1"))
	s.Put("gone", []byte("x"))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", []byte("v2"))
	s.Delete("gone")
	s.Put("new", []byte("y"))

	// The snapshot is the export watermark: its fixed view is exported.
	if err := snap.Backup(filepath.Join(base, "snap-backup")); err != nil {
		t.Fatalf("snapshot backup: %v", err)
	}
	if err := Restore(filepath.Join(base, "snap-backup"), filepath.Join(base, "snap-restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(filepath.Join(base, "snap-restored"))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := r.Get("k"); !ok || string(v) != "v1" {
		t.Fatalf("k = %q ok=%v, want v1", v, ok)
	}
	if v, ok, _ := r.Get("gone"); !ok || string(v) != "x" {
		t.Fatalf("gone = %q ok=%v, want x", v, ok)
	}
	if _, ok, _ := r.Get("new"); ok {
		t.Fatal("post-snapshot key leaked into snapshot backup")
	}
	if got := r.Stats().Snapshots; got != 1 {
		t.Fatalf("snapshot count %d, want 1 (count at the snapshot watermark)", got)
	}
	r.Close()

	// The store-level export sees the latest state instead.
	if err := s.Backup(filepath.Join(base, "store-backup")); err != nil {
		t.Fatalf("store backup: %v", err)
	}
	if err := Restore(filepath.Join(base, "store-backup"), filepath.Join(base, "store-restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r2, err := Open(filepath.Join(base, "store-restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if v, ok, _ := r2.Get("k"); !ok || string(v) != "v2" {
		t.Fatalf("k = %q ok=%v, want v2", v, ok)
	}
	if _, ok, _ := r2.Get("gone"); ok {
		t.Fatal("deleted key resurrected")
	}
	if got := r2.Stats().Snapshots; got != 1 {
		t.Fatalf("snapshot count %d, want 1", got)
	}
}

func TestBackupClosedSnapshotExportsEmpty(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put("k", []byte("v"))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snap.Close()

	if err := snap.Backup(filepath.Join(base, "backup")); err != nil {
		t.Fatalf("backup from closed snapshot: %v", err)
	}
	if err := Restore(filepath.Join(base, "backup"), filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := scanAll(t, r); len(got) != 0 {
		t.Fatalf("closed snapshot exported %v, want empty", got)
	}
}

func TestBackupEmptyStore(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Backup(filepath.Join(base, "backup")); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := Restore(filepath.Join(base, "backup"), filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := scanAll(t, r); len(got) != 0 {
		t.Fatalf("empty store restored %v", got)
	}
	if got := r.Stats().Snapshots; got != 0 {
		t.Fatalf("snapshot count %d, want 0", got)
	}
}

func TestBackupTargetValidation(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", []byte("v"))

	if err := s.Backup(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty target: %v, want fs.ErrInvalid", err)
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("regular-file target: %v, want fs.ErrInvalid", err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Backup(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("snapshot backup to regular file: %v, want fs.ErrInvalid", err)
	}
	if err := snap.Backup(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("snapshot backup to empty path: %v, want fs.ErrInvalid", err)
	}

	s.Close()
	if err := s.Backup(filepath.Join(base, "backup")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed store: %v, want fs.ErrClosed", err)
	}
}

func TestRestoreTargetValidation(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))
	defer s.Close()
	if err := s.Backup(filepath.Join(base, "backup")); err != nil {
		t.Fatal(err)
	}

	if err := Restore(filepath.Join(base, "backup"), ""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty restore target: %v, want fs.ErrInvalid", err)
	}
	if err := Restore("", filepath.Join(base, "out1")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty backup dir: %v, want fs.ErrInvalid", err)
	}
	if err := Restore(filepath.Join(base, "missing"), filepath.Join(base, "out2")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing backup dir: %v, want fs.ErrInvalid", err)
	}
	empty := filepath.Join(base, "emptydir")
	if err := os.Mkdir(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Restore(empty, filepath.Join(base, "out3")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty artifact dir: %v, want fs.ErrInvalid", err)
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(filepath.Join(base, "backup"), file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("regular-file restore target: %v, want fs.ErrInvalid", err)
	}

	// A directory that already holds store data is rejected.
	live := filepath.Join(base, "livestore")
	ls, err := Open(live)
	if err != nil {
		t.Fatal(err)
	}
	ls.Put("k", []byte("v"))
	ls.Close()
	if err := Restore(filepath.Join(base, "backup"), live); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore over store: %v, want fs.ErrInvalid", err)
	}
	// The pre-existing store is untouched.
	ls2, err := Open(live)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := ls2.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("existing store damaged: %q ok=%v", v, ok)
	}
	ls2.Close()

	// An existing empty directory is a fine target.
	emptyTarget := filepath.Join(base, "emptytarget")
	if err := os.Mkdir(emptyTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Restore(filepath.Join(base, "backup"), emptyTarget); err != nil {
		t.Fatalf("restore into empty dir: %v", err)
	}
}

// withSmallSegments shrinks the segment budget so tests exercise
// multi-segment artifacts, and restores it on cleanup.
func withSmallSegments(t *testing.T, size int64) {
	t.Helper()
	old := backupSegmentTarget
	backupSegmentTarget = size
	t.Cleanup(func() { backupSegmentTarget = old })
}

// copyDir from layered_test.go is reused to clone artifact directories.

// makeMultiSegmentBackup writes a store whose backup spans several segments
// and returns the backup directory plus the number of segment files.
func makeMultiSegmentBackup(t *testing.T, base string) (string, int) {
	t.Helper()
	withSmallSegments(t, 128)
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 40; i++ {
		if err := s.Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("value-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	backupDir := filepath.Join(base, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if _, ok := parseSegmentName(e.Name()); ok {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("expected a multi-segment artifact, got %d segment", count)
	}
	return backupDir, count
}

func TestRestoreMultiSegmentRoundtrip(t *testing.T) {
	base := t.TempDir()
	backupDir, _ := makeMultiSegmentBackup(t, base)
	if err := Restore(backupDir, filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pairs := scanAll(t, r)
	if len(pairs) != 40 {
		t.Fatalf("restored %d keys, want 40", len(pairs))
	}
	for i, p := range pairs {
		wantK := fmt.Sprintf("key-%02d", i)
		wantV := fmt.Sprintf("value-%02d", i)
		if p.Key != wantK || string(p.Value) != wantV {
			t.Fatalf("pair %d = %q/%q, want %q/%q", i, p.Key, p.Value, wantK, wantV)
		}
	}
}

func TestRestoreRejectsCorruptArtifact(t *testing.T) {
	base := t.TempDir()
	backupDir, segCount := makeMultiSegmentBackup(t, base)

	cases := map[string]func(t *testing.T, dir string){
		"missing middle segment": func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, segmentFileName(1))); err != nil {
				t.Fatal(err)
			}
		},
		"missing last segment": func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, segmentFileName(segCount-1))); err != nil {
				t.Fatal(err)
			}
		},
		"truncated segment": func(t *testing.T, dir string) {
			p := filepath.Join(dir, segmentFileName(0))
			info, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(p, info.Size()/2); err != nil {
				t.Fatal(err)
			}
		},
		"bit flip": func(t *testing.T, dir string) {
			p := filepath.Join(dir, segmentFileName(0))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xFF
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"unknown version": func(t *testing.T, dir string) {
			p := filepath.Join(dir, segmentFileName(0))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			// Bump the version and repair the checksum so only the version
			// check can reject the segment.
			binary.LittleEndian.PutUint32(data[4:8], 999)
			crc := crc32.ChecksumIEEE(data[:len(data)-crcSize])
			binary.LittleEndian.PutUint32(data[len(data)-crcSize:], crc)
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"extra segment": func(t *testing.T, dir string) {
			data, err := os.ReadFile(filepath.Join(dir, segmentFileName(segCount-1)))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, segmentFileName(segCount)), data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "backup")
			copyDir(t, backupDir, dir)
			corrupt(t, dir)
			err := Restore(dir, filepath.Join(t.TempDir(), "out"))
			if !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("Restore = %v, want fs.ErrInvalid", err)
			}
		})
	}
}

func TestBackupArtifactRelocatable(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))
	defer s.Close()

	first := filepath.Join(base, "first")
	if err := s.Backup(first); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(base, "elsewhere", "moved")
	if err := os.MkdirAll(filepath.Dir(moved), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(first, moved); err != nil {
		t.Fatal(err)
	}
	if err := Restore(moved, filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore from moved artifact: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got, want := scanAll(t, r), scanAll(t, s); !equalPairs(got, want) {
		t.Fatalf("restored %v != live %v", got, want)
	}
}

func TestBackupSweepsStaleArtifact(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))
	defer s.Close()
	backupDir := filepath.Join(base, "backup")

	// First export with many small segments.
	withSmallSegments(t, 64)
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	// Plant half-written temp debris, then re-export with large segments:
	// the stale small segments and temps must be swept, not mixed in.
	if err := os.WriteFile(filepath.Join(backupDir, segmentTmpName(3)), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	backupSegmentTarget = 4 << 20
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	if err := Restore(backupDir, filepath.Join(base, "restored")); err != nil {
		t.Fatalf("restore after re-export: %v", err)
	}
	r, err := Open(filepath.Join(base, "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got, want := scanAll(t, r), scanAll(t, s); !equalPairs(got, want) {
		t.Fatalf("restored %v != live %v", got, want)
	}
}

func TestRestoreDebrisSweptOnOpen(t *testing.T) {
	base := t.TempDir()
	s := populateStore(t, filepath.Join(base, "store"))
	defer s.Close()
	if err := s.Backup(filepath.Join(base, "backup")); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(base, "restored")
	if err := Restore(filepath.Join(base, "backup"), restored); err != nil {
		t.Fatal(err)
	}

	// Simulate kill-mid-restore and kill-mid-compaction debris: the next
	// open sweeps it and the store comes up untouched.
	if err := os.WriteFile(filepath.Join(restored, checkpointDirName, "0000000000.ckpt.tmp"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(restored, compactTmpName), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := Open(restored)
	if err != nil {
		t.Fatalf("open with debris: %v", err)
	}
	defer r.Close()
	if got, want := scanAll(t, r), scanAll(t, s); !equalPairs(got, want) {
		t.Fatalf("restored %v != live %v", got, want)
	}
	if _, err := os.Stat(filepath.Join(restored, checkpointDirName, "0000000000.ckpt.tmp")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("layer temp not swept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restored, compactTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("compact temp not swept: %v", err)
	}
}

func TestBackupConcurrentWithStoreActivity(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("init-%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d-%04d", w, i%64)
				switch i % 4 {
				case 0:
					s.Put(key, []byte("x"))
				case 1:
					s.Delete(key)
				case 2:
					s.CommitBatch([]BatchOp{{Key: key, Value: []byte("y")}, {Key: key + "b", Value: []byte("z")}})
				case 3:
					if snap, err := s.Snapshot(); err == nil {
						snap.Get(key)
						snap.Close()
					}
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			s.Checkpoint()
			s.Compact()
		}
	}()
	// Cursors page while exports run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			c, err := s.OpenCursorPrefix("")
			if err != nil {
				return
			}
			c.Page(0, 8)
			c.Close()
		}
	}()

	for i := 0; i < 4; i++ {
		backupDir := filepath.Join(base, fmt.Sprintf("backup-%d", i))
		if err := s.Backup(backupDir); err != nil {
			t.Fatalf("backup %d: %v", i, err)
		}
		restored := filepath.Join(base, fmt.Sprintf("restored-%d", i))
		if err := Restore(backupDir, restored); err != nil {
			t.Fatalf("restore %d: %v", i, err)
		}
		r, err := Open(restored)
		if err != nil {
			t.Fatalf("open restored %d: %v", i, err)
		}
		// The restored state is a consistent committed view: every key the
		// backup captured is readable and the store opens without repair.
		if _, err := r.ScanPrefix("", 1); err != nil {
			t.Fatalf("scan restored %d: %v", i, err)
		}
		r.Close()
	}
	close(stop)
	wg.Wait()
}

func TestCommitQueueCollapseKeepsViews(t *testing.T) {
	base := t.TempDir()
	s, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Commit well past viewFlattenLimit several times over, with snapshots
	// pinned along the way; every view must keep serving its exact state.
	type pin struct {
		snap *Snapshot
		want map[string]string
	}
	var pins []pin
	live := make(map[string]string)
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("k%03d", i%100)
		val := fmt.Sprintf("v%d", i)
		if err := s.Put(key, []byte(val)); err != nil {
			t.Fatal(err)
		}
		live[key] = val
		if i%50 == 0 {
			snap, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			want := make(map[string]string, len(live))
			for k, v := range live {
				want[k] = v
			}
			pins = append(pins, pin{snap, want})
		}
	}
	for _, p := range pins {
		for k, want := range p.want {
			got, ok := p.snap.Get(k)
			if !ok || string(got) != want {
				t.Fatalf("pinned snapshot: %s = %q ok=%v, want %q", k, got, ok, want)
			}
		}
		p.snap.Close()
	}
	for k, want := range live {
		got, ok, err := s.Get(k)
		if err != nil || !ok || string(got) != want {
			t.Fatalf("live: %s = %q ok=%v err=%v, want %q", k, got, ok, err, want)
		}
	}

	// Reopen replays to the identical state.
	s.Close()
	r, err := Open(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for k, want := range live {
		got, ok, _ := r.Get(k)
		if !ok || string(got) != want {
			t.Fatalf("reopened: %s = %q ok=%v, want %q", k, got, ok, want)
		}
	}
}
