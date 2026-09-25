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

// scanAll returns the full live keyspace of a store as key->value.
func scanAll(t *testing.T, s *Store) map[string]string {
	t.Helper()
	pairs, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		out[p.Key] = string(p.Value)
	}
	return out
}

func equalState(t *testing.T, want, got map[string]string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("state size mismatch: want %d keys, got %d", len(want), len(got))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q: want %q, got %q", k, v, got[k])
		}
	}
}

func TestBackupRestoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := s.Put(fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "batch-a", Value: []byte("1")},
		{Key: "batch-b", Value: []byte("2")},
		{Key: "key-0000", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("empty", nil); err != nil {
		t.Fatal(err)
	}
	// Cumulative snapshot count must survive the round trip.
	for i := 0; i < 3; i++ {
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		snap.Close()
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	want := scanAll(t, s)
	wantSnaps := s.Stats().Snapshots

	backupDir := filepath.Join(dir, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatalf("backup: %v", err)
	}

	target := filepath.Join(dir, "restored")
	if err := Restore(backupDir, target); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := Open(target)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	equalState(t, want, scanAll(t, r))
	if got := r.Stats().Snapshots; got != wantSnaps {
		t.Fatalf("snapshot count: want %d, got %d", wantSnaps, got)
	}
	// The restored store is directly usable: commits, checkpoints and
	// compactions work without any repair step, and a reopen keeps state.
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
	r2, err := Open(target)
	if err != nil {
		t.Fatalf("reopen restored: %v", err)
	}
	want["post"] = "x"
	equalState(t, want, scanAll(t, r2))
	if got := r2.Stats().Snapshots; got != wantSnaps {
		t.Fatalf("snapshot count after reopen: want %d, got %d", wantSnaps, got)
	}
	r2.Close()

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupWatermarkIsolation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	want := scanAll(t, s)

	backupDir := filepath.Join(dir, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	// Writes and deletes after the export watermark are not in the artifact.
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%02d", i), []byte("changed")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(fmt.Sprintf("new%02d", i), []byte("n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete("k00"); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "restored")
	if err := Restore(backupDir, target); err != nil {
		t.Fatal(err)
	}
	r, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	equalState(t, want, scanAll(t, r))
	s.Close()
}

func TestBackupClosedStore(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err = s.Backup(filepath.Join(dir, "backup"))
	if !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("want fs.ErrClosed, got %v", err)
	}
}

func TestBackupTargetValidation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Existing non-empty directory.
	nonEmpty := filepath.Join(dir, "nonempty")
	if err := os.MkdirAll(nonEmpty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonEmpty, "x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(nonEmpty); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("non-empty target: want fs.ErrInvalid, got %v", err)
	}
	// Existing regular file.
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file target: want fs.ErrInvalid, got %v", err)
	}
	// Existing empty directory is fine.
	empty := filepath.Join(dir, "empty")
	if err := os.MkdirAll(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(empty); err != nil {
		t.Fatalf("empty dir target: %v", err)
	}
	// The failed backups left no staging debris behind.
	for _, p := range []string{nonEmpty + backupTmpSuffix, file + backupTmpSuffix} {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("staging %s left behind", p)
		}
	}
}

func TestRestoreArgumentValidation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(dir, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Backup path does not exist.
	if err := Restore(filepath.Join(dir, "nope"), filepath.Join(dir, "t1")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("missing backup: want fs.ErrInvalid, got %v", err)
	}
	// Backup path is a regular file.
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(file, filepath.Join(dir, "t2")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file backup: want fs.ErrInvalid, got %v", err)
	}
	// Target exists and is non-empty.
	nonEmpty := filepath.Join(dir, "nonempty")
	if err := os.MkdirAll(nonEmpty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonEmpty, "x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(backupDir, nonEmpty); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("non-empty target: want fs.ErrInvalid, got %v", err)
	}
	// Target is a regular file.
	if err := Restore(backupDir, file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file target: want fs.ErrInvalid, got %v", err)
	}
	// No half-installed target appeared anywhere.
	for _, p := range []string{"t1", "t2"} {
		if _, err := os.Stat(filepath.Join(dir, p)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("target %s left behind", p)
		}
		if _, err := os.Stat(filepath.Join(dir, p) + restoreTmpSuffix); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("staging for %s left behind", p)
		}
	}
}

// corruptBackup rewrites every regular file in dir matching match with
// transform, so a test can damage exactly one artifact file.
func corruptBackup(t *testing.T, dir, match string, transform func([]byte) []byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	done := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ok := false
		switch match {
		case "manifest":
			ok = name == backupManifestName
		case "segment":
			_, ok = parseBackupSegmentName(name)
		}
		if !ok {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, transform(data), 0o600); err != nil {
			t.Fatal(err)
		}
		done = true
		break
	}
	if !done {
		t.Fatalf("no %s file found in %s", match, dir)
	}
}

func TestRestoreRejectsCorruptArtifact(t *testing.T) {
	setup := func(t *testing.T) (backupDir, target string, cleanup func()) {
		dir := t.TempDir()
		s, err := Open(filepath.Join(dir, "store"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			if err := s.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		backupDir = filepath.Join(dir, "backup")
		if err := s.Backup(backupDir); err != nil {
			t.Fatal(err)
		}
		s.Close()
		return backupDir, filepath.Join(dir, "restored"), func() {}
	}

	cases := map[string]func(t *testing.T, backupDir string){
		"missing manifest": func(t *testing.T, b string) {
			os.Remove(filepath.Join(b, backupManifestName))
		},
		"truncated manifest": func(t *testing.T, b string) {
			corruptBackup(t, b, "manifest", func(d []byte) []byte { return d[:len(d)-3] })
		},
		"manifest checksum": func(t *testing.T, b string) {
			corruptBackup(t, b, "manifest", func(d []byte) []byte {
				d[10] ^= 0xFF // watermark byte, checksum now stale
				return d
			})
		},
		"unknown version": func(t *testing.T, b string) {
			corruptBackup(t, b, "manifest", func(d []byte) []byte {
				binary.LittleEndian.PutUint32(d[4:8], 99)
				// Keep the checksum valid so only the version is wrong.
				binary.LittleEndian.PutUint32(d[len(d)-4:], crc32.ChecksumIEEE(d[:len(d)-4]))
				return d
			})
		},
		"missing segment": func(t *testing.T, b string) {
			os.Remove(filepath.Join(b, backupSegmentName(0)))
		},
		"truncated segment": func(t *testing.T, b string) {
			corruptBackup(t, b, "segment", func(d []byte) []byte { return d[:len(d)/2] })
		},
		"segment checksum": func(t *testing.T, b string) {
			corruptBackup(t, b, "segment", func(d []byte) []byte {
				d[backupHeaderSize] ^= 0xFF
				return d
			})
		},
		"segment watermark mismatch": func(t *testing.T, b string) {
			corruptBackup(t, b, "segment", func(d []byte) []byte {
				binary.LittleEndian.PutUint64(d[12:20], 12345)
				binary.LittleEndian.PutUint32(d[len(d)-4:], crc32.ChecksumIEEE(d[:len(d)-4]))
				return d
			})
		},
		"extra segment": func(t *testing.T, b string) {
			data, err := os.ReadFile(filepath.Join(b, backupSegmentName(0)))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(b, backupSegmentName(7)), data, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			backupDir, target, _ := setup(t)
			damage(t, backupDir)
			if err := Restore(backupDir, target); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("want fs.ErrInvalid, got %v", err)
			}
			// The rejection is wholesale: no target, no staging residue.
			if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("half-installed target left behind")
			}
			if _, err := os.Stat(target + restoreTmpSuffix); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("staging left behind")
			}
		})
	}
}

func TestBackupRestoreEmptyStore(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(dir, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	s.Close()

	target := filepath.Join(dir, "restored")
	if err := Restore(backupDir, target); err != nil {
		t.Fatal(err)
	}
	r, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := scanAll(t, r); len(got) != 0 {
		t.Fatalf("want empty store, got %d keys", len(got))
	}
	if err := r.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestBackupMultiSegment(t *testing.T) {
	old := backupSegmentTarget
	backupSegmentTarget = 256 // tiny segments to force several files
	defer func() { backupSegmentTarget = old }()

	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := s.Put(fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	want := scanAll(t, s)
	backupDir := filepath.Join(dir, "backup")
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	s.Close()

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	segments := 0
	for _, e := range entries {
		if _, ok := parseBackupSegmentName(e.Name()); ok {
			segments++
		}
	}
	if segments < 2 {
		t.Fatalf("want multiple segments, got %d", segments)
	}

	target := filepath.Join(dir, "restored")
	if err := Restore(backupDir, target); err != nil {
		t.Fatal(err)
	}
	r, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	equalState(t, want, scanAll(t, r))
}

func TestBackupStagingDebrisSwept(t *testing.T) {
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "store")
	s, err := Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Debris from a killed backup is swept by the next backup to that path.
	backupDir := filepath.Join(dir, "backup")
	stale := backupDir + backupTmpSuffix
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "junk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(backupDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale backup staging not swept")
	}

	// Debris next to a would-be restore target is swept by the restore
	// itself and by opening the related directory.
	target := filepath.Join(dir, "restored")
	staleRestore := target + restoreTmpSuffix
	if err := os.MkdirAll(staleRestore, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Restore(backupDir, target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staleRestore); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale restore staging not swept by restore")
	}
	if err := os.MkdirAll(storeDir+restoreTmpSuffix, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(storeDir+backupTmpSuffix, 0o700); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, p := range []string{storeDir + restoreTmpSuffix, storeDir + backupTmpSuffix} {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("staging %s not swept on open", p)
		}
	}
}

func TestBackupConcurrentWithStoreOps(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 100; i++ {
		if err := s.Put(fmt.Sprintf("pre-%03d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)
	worker := func(f func() error) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := f(); err != nil {
				errCh <- err
				return
			}
		}
	}
	i := 0
	wg.Add(4)
	go worker(func() error { i++; return s.Put(fmt.Sprintf("w-%d", i), []byte("x")) })
	go worker(func() error {
		return s.CommitBatch([]BatchOp{{Key: "b1", Value: []byte("1")}, {Key: "b2", Delete: true}})
	})
	go worker(func() error {
		snap, err := s.Snapshot()
		if err != nil {
			return err
		}
		return snap.Close()
	})
	go worker(func() error {
		if err := s.Checkpoint(); err != nil {
			return err
		}
		return s.Compact()
	})

	// Reads keep flowing while backups run.
	backupDir := filepath.Join(dir, "backup")
	for j := 0; j < 5; j++ {
		if err := s.Backup(backupDir + fmt.Sprintf("-%d", j)); err != nil {
			t.Fatalf("backup %d: %v", j, err)
		}
		if _, _, err := s.Get("pre-000"); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("concurrent op: %v", err)
	default:
	}

	// Every artifact produced is restorable and self-consistent.
	for j := 0; j < 5; j++ {
		target := filepath.Join(dir, fmt.Sprintf("restored-%d", j))
		if err := Restore(backupDir+fmt.Sprintf("-%d", j), target); err != nil {
			t.Fatalf("restore %d: %v", j, err)
		}
		r, err := Open(target)
		if err != nil {
			t.Fatalf("open %d: %v", j, err)
		}
		if got := scanAll(t, r); len(got) < 100 {
			t.Fatalf("restored %d has %d keys, want at least the 100 pre-backup keys", j, len(got))
		}
		r.Close()
	}
}

func TestBackupDoesNotDisturbStore(t *testing.T) {
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "store")
	s, err := Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if err := s.Backup(filepath.Join(dir, "backup")); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if before != after {
		t.Fatalf("backup changed store stats: %+v -> %+v", before, after)
	}
	// The store directory gained no backup files.
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := parseBackupSegmentName(e.Name()); ok || e.Name() == backupManifestName {
			t.Fatalf("backup artifact %s leaked into the store directory", e.Name())
		}
	}
	// Life goes on without a reopen.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, ok, _ := s2.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("store lost committed data")
	}
}

func TestDirtyQueueDoesNotAccumulate(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Thousands of live keys, then the same keys committed over and over:
	// the dirty record must track distinct keys, not cumulative commits.
	const keys = 2000
	for i := 0; i < keys; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 10; round++ {
		for i := 0; i < keys; i++ {
			if err := s.Put(fmt.Sprintf("k%04d", i), []byte("w")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := len(s.dirty); got != keys {
		t.Fatalf("dirty list holds %d entries, want %d distinct keys", got, keys)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := len(s.dirty); got != 0 {
		t.Fatalf("dirty list not cleared by checkpoint: %d", got)
	}
	// Repeated queuing of one key stays at one entry.
	for i := 0; i < 100; i++ {
		if err := s.Put("same", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(s.dirty); got != 1 {
		t.Fatalf("dirty list accumulated %d entries for one key", got)
	}
}
