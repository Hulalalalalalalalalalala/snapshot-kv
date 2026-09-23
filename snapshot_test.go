package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "store")
}

func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBasicCRUD(t *testing.T) {
	s := openStore(t, tempDir(t))

	if got, ok, err := s.Get("missing"); ok || err != nil || got != nil {
		t.Fatalf("missing key: got=%q ok=%v err=%v", got, ok, err)
	}
	if err := s.Put("a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.Get("a"); !ok || err != nil || string(got) != "one" {
		t.Fatalf("get a: got=%q ok=%v err=%v", got, ok, err)
	}
	if err := s.Put("a", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Get("a"); !ok || string(got) != "two" {
		t.Fatalf("overwritten a: got=%q ok=%v", got, ok)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Get("a"); ok || err != nil {
		t.Fatalf("deleted a: ok=%v err=%v", ok, err)
	}
	// Idempotent deletes: missing and already-deleted keys return nil.
	if err := s.Delete("a"); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
	if err := s.Delete("never-existed"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestEmptyKeyAndEmptyValues(t *testing.T) {
	s := openStore(t, tempDir(t))

	if err := s.Put("", []byte("x")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("put empty key err=%v, want fs.ErrInvalid", err)
	}
	if err := s.Delete(""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("delete empty key err=%v, want fs.ErrInvalid", err)
	}
	if _, ok, err := s.Get(""); ok || err != nil {
		t.Fatalf("get empty key: ok=%v err=%v", ok, err)
	}

	for i, v := range [][]byte{nil, {}} {
		k := fmt.Sprintf("empty-%d", i)
		if err := s.Put(k, v); err != nil {
			t.Fatal(err)
		}
		got, ok, err := s.Get(k)
		if !ok || err != nil {
			t.Fatalf("empty value %d: ok=%v err=%v", i, ok, err)
		}
		if len(got) != 0 {
			t.Fatalf("empty value %d: got len %d", i, len(got))
		}
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := openStore(t, tempDir(t))

	mustPut := func(k string, v string) {
		t.Helper()
		if err := s.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	mustPut("old", "v1")
	mustPut("gone", "x")
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Committed after the snapshot: invisible to it.
	mustPut("old", "v2")                     // overwritten old value stays readable
	mustPut("brand-new", "n")                // new key is a miss
	if err := s.Delete("gone"); err != nil { // deleted value stays readable
		t.Fatal(err)
	}

	if got, ok := snap.Get("old"); !ok || string(got) != "v1" {
		t.Fatalf("snap old: %q %v", got, ok)
	}
	if got, ok := snap.Get("gone"); !ok || string(got) != "x" {
		t.Fatalf("snap gone: %q %v", got, ok)
	}
	if _, ok := snap.Get("brand-new"); ok {
		t.Fatal("snap sees key created after it")
	}
	if _, ok := snap.Get("never"); ok {
		t.Fatal("snap hit on unknown key")
	}
	if _, ok := snap.Get(""); ok {
		t.Fatal("snap hit on empty key")
	}

	// The live store sees the new view.
	if got, ok, _ := s.Get("old"); !ok || string(got) != "v2" {
		t.Fatalf("live old: %q %v", got, ok)
	}
	if _, ok, _ := s.Get("gone"); ok {
		t.Fatal("live store still sees deleted key")
	}

	// A second snapshot catches the current state.
	snap2, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap2.Get("gone"); ok {
		t.Fatal("new snapshot sees deleted key")
	}
	if got, ok := snap2.Get("brand-new"); !ok || string(got) != "n" {
		t.Fatalf("new snapshot brand-new: %q %v", got, ok)
	}
	snap2.Close()

	// Closing the snapshot turns every read into a miss, without error.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Get("old"); ok {
		t.Fatal("closed snapshot reported hit")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("double snapshot close: %v", err)
	}
}

func TestSnapshotOutlivesStore(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Store closed: its snapshot still serves the fixed view.
	if got, ok := snap.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("snapshot after store close: %q %v", got, ok)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("double store close: %v", err)
	}
}

func TestClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"put", func() error { return s.Put("a", []byte("b")) }},
		{"delete", func() error { return s.Delete("k") }},
		{"snapshot", func() error { _, err := s.Snapshot(); return err }},
		{"get", func() error { _, _, err := s.Get("k"); return err }},
		{"empty-key-put", func() error { return s.Put("", nil) }},
	} {
		err := tc.call()
		if !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("%s on closed store: err=%v, want fs.ErrClosed", tc.name, err)
		}
	}
}

func TestOpenPathIsFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "notadir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open(file): err=%v, want fs.ErrInvalid", err)
	}
}

func TestBytesAreCallerOwned(t *testing.T) {
	s := openStore(t, tempDir(t))

	in := []byte("hello")
	if err := s.Put("k", in); err != nil {
		t.Fatal(err)
	}
	in[0] = 'H' // mutating the input after Put must not affect storage
	got, ok, _ := s.Get("k")
	if !ok || string(got) != "hello" {
		t.Fatalf("storage aliased input: %q", got)
	}
	got[0] = 'Y' // mutating returned bytes must not affect storage
	again, ok, _ := s.Get("k")
	if !ok || string(again) != "hello" {
		t.Fatalf("storage aliased returned bytes: %q", again)
	}

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("world")); err != nil {
		t.Fatal(err)
	}
	old, ok := snap.Get("k")
	if !ok || string(old) != "hello" {
		t.Fatalf("snap old value: %q", old)
	}
	old[0] = 'Q'
	old2, ok := snap.Get("k")
	if !ok || string(old2) != "hello" {
		t.Fatalf("snapshot aliased old value: %q", old2)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := tempDir(t)

	s := openStore(t, dir)
	vals := map[string]string{
		"k1": "value1",
		"k2": "second",
		"k3": "", // empty value is a hit
	}
	for k, v := range vals {
		if err := s.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("doomed", []byte("bye")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("doomed"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen without an "orderly shutdown": everything committed is there.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range vals {
		got, ok, err := s2.Get(k)
		if !ok || err != nil || string(got) != v {
			t.Fatalf("after reopen %s: got=%q ok=%v err=%v want=%q", k, got, ok, err, v)
		}
	}
	if _, ok, _ := s2.Get("doomed"); ok {
		t.Fatal("deletion did not persist")
	}
	if st := s2.Stats(); st.Snapshots != 3 {
		t.Fatalf("snapshot count after reopen: %d, want 3", st.Snapshots)
	}
	if _, err := s2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st := s3.Stats(); st.Snapshots != 4 {
		t.Fatalf("snapshot count after second reopen: %d, want 4", st.Snapshots)
	}
	s3.Close()
}

func TestTornTailIsDiscarded(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("good", []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: garbage bytes past the valid prefix.
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	got, ok, _ := s2.Get("good")
	if !ok || string(got) != "complete" {
		t.Fatalf("valid prefix lost: %q ok=%v", got, ok)
	}
	// A new commit must succeed and persist after the tail was truncated.
	if err := s2.Put("after", []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s3.Get("after"); !ok || string(got) != "tail" {
		t.Fatalf("post-repair commit lost: %q ok=%v", got, ok)
	}
	s3.Close()
}

func TestStats(t *testing.T) {
	s := openStore(t, tempDir(t))

	st := s.Stats()
	if st.Keys != 0 || st.Snapshots != 0 || st.AvgBytes != 0 {
		t.Fatalf("empty stats: %+v", st)
	}

	putLen := func(k string, n int) {
		t.Helper()
		if err := s.Put(k, bytes.Repeat([]byte{'x'}, n)); err != nil {
			t.Fatal(err)
		}
	}

	// Total 10 bytes over 3 keys: 3.33.
	putLen("a", 4)
	putLen("b", 4)
	putLen("c", 2) // empty-value case covered separately
	if st := s.Stats(); st.Keys != 3 || st.AvgBytes != 3.33 {
		t.Fatalf("stats: %+v", st)
	}

	// Half-up tie at the third decimal: 1/8 = 0.125 -> 0.13.
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	putLen("one", 1) // 1 byte over 8 live keys: 0.125
	for _, k := range []string{"d2", "d3", "d4", "d5", "d6", "d7", "d8"} {
		putLen(k, 0) // 0-byte live values still count as keys
	}
	st = s.Stats()
	if st.Keys != 8 || st.AvgBytes != 0.13 {
		t.Fatalf("tie stats: %+v (keys=%d avg=%v)", st, st.Keys, st.AvgBytes)
	}

	// Deleting everything: back to 0 keys and 0.00.
	for _, k := range []string{"one", "d2", "d3", "d4", "d5", "d6", "d7", "d8"} {
		if err := s.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	st = s.Stats()
	if st.Keys != 0 || st.AvgBytes != 0 {
		t.Fatalf("cleared stats: %+v", st)
	}
}

func TestStatsDoesNotCountAsSnapshot(t *testing.T) {
	s := openStore(t, tempDir(t))
	_ = s.Stats()
	_ = s.Stats()
	if n := s.Stats().Snapshots; n != 0 {
		t.Fatalf("Stats incremented snapshot count: %d", n)
	}
}

func TestConcurrentIsolation(t *testing.T) {
	s := openStore(t, tempDir(t))

	const writers = 4
	const iterations = 200

	var writersWg sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			key := fmt.Sprintf("k%d", w)
			for i := 0; i < iterations; i++ {
				v := bytes.Repeat([]byte{byte('a' + w)}, i+1)
				if err := s.Put(key, v); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var readersWg sync.WaitGroup
	readersWg.Add(1)
	go func() {
		defer readersWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := s.Snapshot()
			if err != nil {
				t.Errorf("snapshot: %v", err)
				return
			}
			for w := 0; w < writers; w++ {
				key := fmt.Sprintf("k%d", w)
				v, ok := snap.Get(key)
				if !ok {
					continue // key may not have been written yet
				}
				// Every committed value is a whole, un-torn run of one byte.
				if !allByte(v, byte('a'+w)) {
					t.Errorf("torn/aliased value for %s: %q", key, v)
				}
				// The fixed view never changes between reads of one snapshot.
				v2, ok := snap.Get(key)
				if !ok || !bytes.Equal(v, v2) {
					t.Errorf("snapshot view changed between reads")
				}
			}
			snap.Close()
		}
	}()

	writersWg.Wait()
	close(stop)
	readersWg.Wait()
}

func allByte(b []byte, c byte) bool {
	for _, x := range b {
		if x != c {
			return false
		}
	}
	return true
}

func TestDurableWithoutClose(t *testing.T) {
	dir := tempDir(t)

	// A store whose handle is abandoned without Close: every successful Put
	// was already fsynced, so reopening the same directory sees all of it.
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Put("x", []byte("durable")); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Delete("x"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Put("y", []byte("kept")); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir) // no Close of s1: simulates a crash/restart
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s2.Get("x"); ok {
		t.Fatal("delete before crash lost")
	}
	if got, ok, _ := s2.Get("y"); !ok || string(got) != "kept" {
		t.Fatalf("put before crash lost: %q %v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count lost across crash: %d", n)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil { // abandoned handle: close still idempotent
		t.Fatal(err)
	}
}

func TestConcurrentSnapshotClose(t *testing.T) {
	s := openStore(t, tempDir(t))
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, []byte("0")); err != nil {
			t.Fatal(err)
		}
	}

	const readers = 8
	var readerWg sync.WaitGroup
	var sawClosed atomic.Int64
	for r := 0; r < readers; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				snap, err := s.Snapshot()
				if err != nil {
					if errors.Is(err, fs.ErrClosed) {
						sawClosed.Add(1)
						return
					}
					t.Errorf("snapshot err: %v", err)
					return
				}
				// The fixed view must be internally consistent: two full
				// passes over the keys return identical bytes, each value a
				// well-formed decimal marker.
				first := map[string][]byte{}
				for _, k := range []string{"a", "b", "c"} {
					v, ok := snap.Get(k)
					if !ok {
						t.Error("initial key missing in snapshot")
						break
					}
					if n, err := strconv.Atoi(string(v)); err != nil || n < 0 {
						t.Errorf("malformed value %q", v)
						break
					}
					first[k] = v
				}
				for k, want := range first {
					if got, ok := snap.Get(k); !ok || !bytes.Equal(got, want) {
						t.Error("snapshot view changed mid-read")
					}
				}
				snap.Close()
			}
		}()
	}

	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 1; i <= 500; i++ {
			if err := s.Put("a", []byte(strconv.Itoa(i))); err != nil {
				return // store may close under us
			}
		}
	}()

	writerWg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	readerWg.Wait()

	if sawClosed.Load() == 0 {
		t.Fatal("no reader observed fs.ErrClosed after close")
	}

	// Snapshots already handed out survive close; verify one explicitly.
	// (Acquire-then-close ordering.)
	s2 := openStore(t, tempDir(t))
	if err := s2.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	snap, err := s2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if got, ok := snap.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("snapshot broken by concurrent close: %q %v", got, ok)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
}
