package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, dir
}

func TestPutGetDelete(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	if _, ok, err := s.Get("missing"); err != nil || ok {
		t.Fatalf("Get missing = ok=%v err=%v", ok, err)
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get("a")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get a = %q,%v,%v", v, ok, err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get("a"); ok {
		t.Fatal("deleted key still visible")
	}
	if err := s.Delete("a"); err != nil {
		t.Fatalf("double delete: %v", err)
	}
	if err := s.Delete("never"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

func TestEmptyAndNilValues(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	if err := s.Put("empty", []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("nil", nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"empty", "nil"} {
		v, ok, err := s.Get(k)
		if err != nil || !ok {
			t.Fatalf("Get %q = ok=%v err=%v", k, ok, err)
		}
		if len(v) != 0 {
			t.Fatalf("Get %q = %q, want empty", k, v)
		}
	}
}

func TestEmptyKeyErrors(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	if err := s.Put("", []byte("x")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Put empty key: %v", err)
	}
	if err := s.Delete(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Delete empty key: %v", err)
	}
	if _, ok, err := s.Get(""); err != nil || ok {
		t.Fatalf("Get empty key = ok=%v err=%v, want miss", ok, err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	s.Put("keep", []byte("old"))
	s.Put("gone", []byte("x"))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.Put("keep", []byte("new"))
	s.Delete("gone")
	s.Put("fresh", []byte("y"))

	if v, ok := snap.Get("keep"); !ok || string(v) != "old" {
		t.Fatalf("snapshot keep = %q,%v", v, ok)
	}
	if v, ok := snap.Get("gone"); !ok || string(v) != "x" {
		t.Fatalf("snapshot gone = %q,%v", v, ok)
	}
	if _, ok := snap.Get("fresh"); ok {
		t.Fatal("snapshot sees key created after it")
	}
	if _, ok := snap.Get(""); ok {
		t.Fatal("snapshot sees empty key")
	}
	if v, ok, _ := s.Get("keep"); !ok || string(v) != "new" {
		t.Fatalf("store keep = %q,%v", v, ok)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Get("keep"); ok {
		t.Fatal("closed snapshot still reads")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("double close snapshot: %v", err)
	}
}

func TestSnapshotSurvivesStoreClose(t *testing.T) {
	s, _ := openTemp(t)
	s.Put("k", []byte("v"))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if v, ok := snap.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("snapshot after store close = %q,%v", v, ok)
	}
	snap.Close()
}

func TestClosedStoreErrors(t *testing.T) {
	s, _ := openTemp(t)
	s.Put("k", []byte("v"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("double close store: %v", err)
	}
	if err := s.Put("k", []byte("x")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Put on closed: %v", err)
	}
	if _, _, err := s.Get("k"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Get on closed: %v", err)
	}
	if err := s.Delete("k"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Delete on closed: %v", err)
	}
	if _, err := s.Snapshot(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Snapshot on closed: %v", err)
	}
}

func TestReopenPersistence(t *testing.T) {
	s, dir := openTemp(t)
	s.Put("a", []byte("1"))
	s.Put("b", []byte("22"))
	s.Delete("a")
	snap1, _ := s.Snapshot()
	snap1.Close()
	// No clean close: simulate a crash by closing the file behind the
	// store's back is overkill; Close is fine since commits are durable
	// per write.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, ok, _ := s2.Get("a"); ok {
		t.Fatal("deleted key resurrected after reopen")
	}
	if v, ok, _ := s2.Get("b"); !ok || string(v) != "22" {
		t.Fatalf("b after reopen = %q,%v", v, ok)
	}
	keys, snaps, total, err := Stats(dir)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 1 || snaps != 1 || total != 2 {
		t.Fatalf("Stats = %d,%d,%d", keys, snaps, total)
	}
}

func TestTornTailIgnored(t *testing.T) {
	s, dir := openTemp(t)
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	good := s.walSize
	s.Close()

	// Append garbage as if a crash tore the last record.
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, ok, _ := s2.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("b = %q,%v after torn tail", v, ok)
	}
	if s2.walSize != good {
		t.Fatalf("walSize = %d, want %d", s2.walSize, good)
	}
	// New writes must land right after the last valid record.
	if err := s2.Put("c", []byte("3")); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if v, ok, _ := s3.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("c = %q,%v", v, ok)
	}
}

func TestOpenRegularFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(p); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open(file) = %v", err)
	}
}

func TestValueOwnership(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	in := []byte("abc")
	s.Put("k", in)
	in[0] = 'X' // caller mutates after Put
	v, ok, _ := s.Get("k")
	if !ok || string(v) != "abc" {
		t.Fatalf("stored value changed with caller's slice: %q", v)
	}
	v[0] = 'Y' // caller mutates returned slice
	v2, _, _ := s.Get("k")
	if string(v2) != "abc" {
		t.Fatalf("stored value changed via returned slice: %q", v2)
	}

	snap, _ := s.Snapshot()
	sv, _ := snap.Get("k")
	sv[0] = 'Z'
	sv2, _ := snap.Get("k")
	if string(sv2) != "abc" {
		t.Fatalf("snapshot value changed via returned slice: %q", sv2)
	}
	snap.Close()
}

func TestConcurrentIsolation(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	const writers = 4
	const ops = 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i%10)
				switch i % 4 {
				case 0:
					s.Put(k, []byte(fmt.Sprintf("v%d", i)))
				case 1:
					s.Get(k)
				case 2:
					s.Delete(k)
				case 3:
					snap, err := s.Snapshot()
					if err == nil {
						snap.Get(k)
						snap.Close()
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestSnapshotPinnedView verifies a snapshot keeps serving exactly the
// view at open time while writes race with reads on it.
func TestSnapshotPinnedView(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	s.Put("stable", []byte("pinned"))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Put("stable", []byte(fmt.Sprintf("v%d", i)))
			i++
		}
	}()
	for i := 0; i < 1000; i++ {
		v, ok := snap.Get("stable")
		if !ok || string(v) != "pinned" {
			t.Fatalf("snapshot view moved: %q,%v", v, ok)
		}
	}
	close(stop)
	wg.Wait()
}

func TestStatsEmptyDir(t *testing.T) {
	dir := t.TempDir()
	keys, snaps, total, err := Stats(dir)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 0 || snaps != 0 || total != 0 {
		t.Fatalf("Stats empty = %d,%d,%d", keys, snaps, total)
	}
}

func TestStatsMissingDir(t *testing.T) {
	if _, _, _, err := Stats(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("Stats on missing dir: want error")
	}
}

func TestReplayRoundTrip(t *testing.T) {
	s, dir := openTemp(t)
	for i := 0; i < 50; i++ {
		s.Put(fmt.Sprintf("k%02d", i), bytes.Repeat([]byte{byte(i)}, i))
	}
	for i := 0; i < 50; i += 2 {
		s.Delete(fmt.Sprintf("k%02d", i))
	}
	for i := 0; i < 3; i++ {
		snap, _ := s.Snapshot()
		snap.Close()
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 1; i < 50; i += 2 {
		v, ok, _ := s2.Get(fmt.Sprintf("k%02d", i))
		if !ok || len(v) != i || v[0] != byte(i) {
			t.Fatalf("k%02d = %v,%v", i, v, ok)
		}
	}
	_, snaps, _, err := Stats(dir)
	if err != nil {
		t.Fatal(err)
	}
	if snaps != 3 {
		t.Fatalf("snapshots = %d, want 3", snaps)
	}
}
