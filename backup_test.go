package snapshot

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// backupFiles returns the final segment files of a backup directory.
func backupFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read backup dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if _, ok := parseBackupSegmentName(e.Name()); ok {
			out = append(out, e.Name())
		}
	}
	return out
}

// restoreAndCompare restores the backup in bdir to a fresh directory, opens
// it and requires its full state and snapshot count to match want's.
func restoreAndCompare(t *testing.T, bdir string, want *Store) *Store {
	t.Helper()
	rdir := filepath.Join(t.TempDir(), "restored")
	if err := Restore(bdir, rdir); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	rs := openStore(t, rdir)
	gotPairs := dumpView(t, rs)
	wantPairs := dumpView(t, want)
	if len(gotPairs) != len(wantPairs) {
		t.Fatalf("restored %d pairs, want %d", len(gotPairs), len(wantPairs))
	}
	for i := range wantPairs {
		if gotPairs[i].Key != wantPairs[i].Key ||
			string(gotPairs[i].Value) != string(wantPairs[i].Value) {
			t.Fatalf("pair %d: got %q=%q, want %q=%q",
				i, gotPairs[i].Key, gotPairs[i].Value, wantPairs[i].Key, wantPairs[i].Value)
		}
	}
	if got, want := rs.Stats().Snapshots, want.Stats().Snapshots; got != want {
		t.Fatalf("restored snapshot count %d, want %d", got, want)
	}
	return rs
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	exerciseStore(t, s, "seed")
	// More than one segment's worth of keys, plus checkpoints and a
	// compaction so the export coexists with every other facility.
	for i := 0; i < 2*backupSegmentEntries+7; i++ {
		if err := s.Put(fmt.Sprintf("bulk-%06d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}

	bdir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bdir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if got := len(backupFiles(t, bdir)); got != 3 {
		t.Fatalf("expected 3 segments, got %d", got)
	}

	rdir := filepath.Join(t.TempDir(), "restored")
	if err := Restore(bdir, rdir); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	rs := openStore(t, rdir)
	gotPairs := dumpView(t, rs)
	wantPairs := dumpView(t, s)
	if len(gotPairs) != len(wantPairs) {
		t.Fatalf("restored %d pairs, want %d", len(gotPairs), len(wantPairs))
	}
	for i := range wantPairs {
		if gotPairs[i].Key != wantPairs[i].Key ||
			string(gotPairs[i].Value) != string(wantPairs[i].Value) {
			t.Fatalf("pair %d: got %q=%q, want %q=%q",
				i, gotPairs[i].Key, gotPairs[i].Value, wantPairs[i].Key, wantPairs[i].Value)
		}
	}
	if got, want := rs.Stats().Snapshots, s.Stats().Snapshots; got != want {
		t.Fatalf("restored snapshot count %d, want %d", got, want)
	}

	// The restored store continues the exported commit sequence: new writes
	// survive a reopen alongside everything restored.
	if err := rs.Put("post-restore", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := rs.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}
	rs2 := openStore(t, rdir)
	if got, ok, _ := rs2.Get("post-restore"); !ok || string(got) != "ok" {
		t.Fatalf("post-restore write lost: got=%q ok=%v", got, ok)
	}
	if got := len(dumpView(t, rs2)); got != len(wantPairs)+1 {
		t.Fatalf("reopened restored store has %d pairs, want %d", got, len(wantPairs)+1)
	}
}

func TestBackupRestoreEmptyStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	bdir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bdir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if got := len(backupFiles(t, bdir)); got != 1 {
		t.Fatalf("empty backup should have exactly 1 segment, got %d", got)
	}
	rs := restoreAndCompare(t, bdir, s)
	if st := rs.Stats(); st.Keys != 0 || st.Snapshots != 0 {
		t.Fatalf("restored stats %+v, want zero", st)
	}
}

func TestBackupFromSnapshotWatermark(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if err := s.Put("a", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}

	// The snapshot's backup exports its fixed view, not the latest state.
	bdir := filepath.Join(t.TempDir(), "snap-backup")
	if err := snap.Backup(bdir); err != nil {
		t.Fatalf("Snapshot.Backup: %v", err)
	}
	rdir := filepath.Join(t.TempDir(), "restored")
	if err := Restore(bdir, rdir); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	rs := openStore(t, rdir)
	if got, ok, _ := rs.Get("a"); !ok || string(got) != "one" {
		t.Fatalf("restored a=%q ok=%v, want one", got, ok)
	}
	if _, ok, _ := rs.Get("b"); ok {
		t.Fatal("restored store must not see commits after the snapshot")
	}
	if got := rs.Stats().Snapshots; got != 1 {
		t.Fatalf("restored snapshot count %d, want 1", got)
	}

	// The store's own backup still exports the latest watermark.
	bdir2 := filepath.Join(t.TempDir(), "store-backup")
	if err := s.Backup(bdir2); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	restoreAndCompare(t, bdir2, s)
}

func TestBackupInvalidTarget(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Backup(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty target: %v, want fs.ErrInvalid", err)
	}
	file := filepath.Join(t.TempDir(), "file")
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
	defer snap.Close()
	if err := snap.Backup(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty target via snapshot: %v, want fs.ErrInvalid", err)
	}
	if err := snap.Backup(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("regular-file target via snapshot: %v, want fs.ErrInvalid", err)
	}
}

func TestBackupOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(filepath.Join(t.TempDir(), "b")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Backup on closed store: %v, want fs.ErrClosed", err)
	}
}

func TestBackupOnClosedSnapshot(t *testing.T) {
	s := openStore(t, tempDir(t))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Backup(filepath.Join(t.TempDir(), "b")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Backup on closed snapshot: %v, want fs.ErrClosed", err)
	}
}

// mutateSegment rewrites one segment file, applying fn to its content and
// then refreshing the trailing CRC so the mutation passes the checksum.
func mutateSegment(t *testing.T, dir, name string, fn func(b []byte)) {
	t.Helper()
	path := filepath.Join(dir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fn(b)
	crc := crc32.ChecksumIEEE(b[:len(b)-crcSize])
	b[len(b)-4] = byte(crc)
	b[len(b)-3] = byte(crc >> 8)
	b[len(b)-2] = byte(crc >> 16)
	b[len(b)-1] = byte(crc >> 24)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func makeBackup(t *testing.T, keys int) (bdir string) {
	t.Helper()
	s := openStore(t, tempDir(t))
	for i := 0; i < keys; i++ {
		if err := s.Put(fmt.Sprintf("k%06d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	bdir = filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bdir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	return bdir
}

func TestRestoreRejectsCorruptBackup(t *testing.T) {
	target := func() string { return filepath.Join(t.TempDir(), "out") }

	t.Run("MissingSegment", func(t *testing.T) {
		bdir := makeBackup(t, backupSegmentEntries+1) // two segments
		if err := os.Remove(filepath.Join(bdir, backupSegmentName(1))); err != nil {
			t.Fatal(err)
		}
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("missing segment: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("TruncatedSegment", func(t *testing.T) {
		bdir := makeBackup(t, 10)
		p := filepath.Join(bdir, backupSegmentName(0))
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(p, info.Size()-3); err != nil {
			t.Fatal(err)
		}
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("truncated segment: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("ChecksumMismatch", func(t *testing.T) {
		bdir := makeBackup(t, 10)
		p := filepath.Join(bdir, backupSegmentName(0))
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)/2] ^= 0xFF // corrupt without refreshing the CRC
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("checksum mismatch: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("UnknownVersion", func(t *testing.T) {
		bdir := makeBackup(t, 10)
		mutateSegment(t, bdir, backupSegmentName(0), func(b []byte) {
			b[4] = 99 // version field, CRC refreshed
		})
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("unknown version: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("DisagreeingSegments", func(t *testing.T) {
		bdir := makeBackup(t, backupSegmentEntries+1)
		mutateSegment(t, bdir, backupSegmentName(1), func(b []byte) {
			b[24]++ // snapshot count of one segment only, CRC refreshed
		})
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("disagreeing segments: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("StraySegment", func(t *testing.T) {
		bdir := makeBackup(t, 10)
		src, err := os.ReadFile(filepath.Join(bdir, backupSegmentName(0)))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bdir, backupSegmentName(7)), src, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := Restore(bdir, target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("stray segment: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("NoSegments", func(t *testing.T) {
		if err := Restore(t.TempDir(), target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("empty backup dir: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("MissingBackupDir", func(t *testing.T) {
		if err := Restore(filepath.Join(t.TempDir(), "nope"), target()); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("missing backup dir: %v, want fs.ErrInvalid", err)
		}
	})
}

func TestRestoreTargetValidation(t *testing.T) {
	bdir := makeBackup(t, 5)

	if err := Restore(bdir, ""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty target: %v, want fs.ErrInvalid", err)
	}
	if err := Restore("", filepath.Join(t.TempDir(), "out")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("empty backup path: %v, want fs.ErrInvalid", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(bdir, file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("regular-file target: %v, want fs.ErrInvalid", err)
	}

	// A directory that already holds store data is rejected, and the data
	// is left untouched.
	live := tempDir(t)
	s := openStore(t, live)
	if err := s.Put("keep", []byte("me")); err != nil {
		t.Fatal(err)
	}
	if err := Restore(bdir, live); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("target with store data: %v, want fs.ErrInvalid", err)
	}
	if got, ok, _ := s.Get("keep"); !ok || string(got) != "me" {
		t.Fatalf("existing store disturbed: got=%q ok=%v", got, ok)
	}
}

func TestRestoreLeavesTargetUntouchedOnRejection(t *testing.T) {
	bdir := makeBackup(t, 5)
	if err := os.Remove(filepath.Join(bdir, backupSegmentName(0))); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "out")
	if err := Restore(bdir, target); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("corrupt backup: %v, want fs.ErrInvalid", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rejected restore created %s: stat err %v", target, err)
	}
}

func TestBackupMovedAsWhole(t *testing.T) {
	s := openStore(t, tempDir(t))
	exerciseStore(t, s, "move")
	bdir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bdir); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.Rename(bdir, moved); err != nil {
		t.Fatal(err)
	}
	restoreAndCompare(t, moved, s)
}

func TestBackupDebrisSwept(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	bdir := filepath.Join(t.TempDir(), "backup")
	if err := s.Backup(bdir); err != nil {
		t.Fatal(err)
	}
	// Crash debris from a killed export: a half-built segment temp. The next
	// export sweeps it; Restore ignores it either way.
	tmp := filepath.Join(bdir, backupSegmentName(0)+backupTmpSuffix)
	if err := os.WriteFile(tmp, []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	restoreAndCompare(t, bdir, s)
	if err := s.Backup(bdir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale temp not swept: %v", err)
	}
}

func TestRestoreDebrisSweptOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A staged log from a killed restore is swept on the next open and the
	// existing store is unaffected.
	debris := filepath.Join(dir, restoreTmpName)
	if err := os.WriteFile(debris, []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir)
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("store disturbed by debris: got=%q ok=%v", got, ok)
	}
	if _, err := os.Stat(debris); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("restore debris not swept: %v", err)
	}
}

func TestBackupConcurrentWithEverything(t *testing.T) {
	s := openStore(t, tempDir(t))
	bdir := filepath.Join(t.TempDir(), "backup")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writer: atomic two-key batches whose values must stay in lockstep.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			gen := []byte(fmt.Sprintf("gen-%d", i))
			if err := s.CommitBatch([]BatchOp{
				{Key: fmt.Sprintf("A-%06d", i), Value: gen},
				{Key: fmt.Sprintf("B-%06d", i), Value: gen},
			}); err != nil {
				return
			}
		}
	}()
	// Background checkpoint/compact/snapshot/cursor churn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Checkpoint()
			s.Compact()
			if snap, err := s.Snapshot(); err == nil {
				snap.Close()
			}
			if c, err := s.OpenCursor(Range{}); err == nil {
				c.Page(0, 4)
				c.Close()
			}
		}
	}()

	// Several exports run while everything above is in flight; none may
	// block or fail, and each restored backup must be batch-atomic.
	for round := 0; round < 4; round++ {
		dir := fmt.Sprintf("%s-%d", bdir, round)
		if err := s.Backup(dir); err != nil {
			t.Fatalf("Backup round %d: %v", round, err)
		}
		rdir := filepath.Join(t.TempDir(), "restored")
		if err := Restore(dir, rdir); err != nil {
			t.Fatalf("Restore round %d: %v", round, err)
		}
		rs, err := Open(rdir)
		if err != nil {
			t.Fatalf("Open restored round %d: %v", round, err)
		}
		pairs, err := rs.Scan(Range{}, 0)
		if err != nil {
			t.Fatal(err)
		}
		vals := make(map[string]string, len(pairs))
		for _, p := range pairs {
			vals[p.Key] = string(p.Value)
		}
		for k, v := range vals {
			if k[0] != 'A' {
				continue
			}
			if bv, ok := vals["B"+k[1:]]; !ok || bv != v {
				t.Fatalf("round %d: torn batch at %q: A=%q B=%q ok=%v", round, k, v, bv, ok)
			}
		}
		rs.Close()
	}
	close(stop)
	wg.Wait()
}

func TestCommitCollapsePreservesViews(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	// Drive the overlay chain through many rounds of run merging.
	for i := 1; i <= 200; i++ {
		if err := s.Put("k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(fmt.Sprintf("k%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	// The pinned snapshot still serves its exact fixed view.
	if got, ok := snap.Get("k"); !ok || string(got) != "v0" {
		t.Fatalf("snapshot k=%q ok=%v, want v0", got, ok)
	}
	if _, ok := snap.Get("k1"); ok {
		t.Fatal("snapshot must not see later keys")
	}
	// The latest view sees everything, across a reopen.
	last := fmt.Sprintf("v%d", 200)
	if got, ok, _ := s.Get("k"); !ok || string(got) != last {
		t.Fatalf("current k=%q ok=%v, want %s", got, ok, last)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir)
	if got, ok, _ := s2.Get("k"); !ok || string(got) != last {
		t.Fatalf("reopened k=%q ok=%v, want %s", got, ok, last)
	}
	if got, ok, _ := s2.Get(fmt.Sprintf("k%d", 200)); !ok || string(got) != "x" {
		t.Fatalf("reopened churn key=%q ok=%v", got, ok)
	}
}

func TestCommitQueueTempDoesNotTrackLiveKeys(t *testing.T) {
	s := openStore(t, tempDir(t))
	// A large live keyspace, committed in batches so setup stays fast.
	const live = 20000
	for base := 0; base < live; base += 1000 {
		ops := make([]BatchOp, 0, 1000)
		for i := 0; i < 1000; i++ {
			ops = append(ops, BatchOp{Key: fmt.Sprintf("live-%06d", base+i), Value: []byte("x")})
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// Repeatedly queue single-key commits: temporary allocation must track
	// the one touched key, not the 20k live ones.
	const commits = 200
	for i := 0; i < commits; i++ {
		if err := s.Put("churn", []byte("y")); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&m1)
	if got := m1.TotalAlloc - m0.TotalAlloc; got > 4<<20 {
		t.Fatalf("queued commits allocated %d bytes against %d live keys; want churn-only", got, live)
	}
}
