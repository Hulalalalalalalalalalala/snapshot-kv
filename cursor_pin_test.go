package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func mustReadWAL(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	return data
}

// An empty (and a nil) value pinned by a cursor must survive a compaction
// rewrite and reopen as a present, zero-length hit.
func TestCompactPinnedEmptyValues(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("nilv", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("emptv", []byte{}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cursor("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, k := range []string{"nilv", "emptv"} {
		got, ok, err := s2.Get(k)
		if !ok || err != nil || len(got) != 0 {
			t.Fatalf("%s after reopen: got=%v ok=%v err=%v", k, got, ok, err)
		}
	}
	// The cursor's own pages still carry both keys exactly once each.
	page, ok, err := c.Next(10)
	if err != nil || !ok || len(page) != 2 {
		t.Fatalf("pinned empty-value page: %+v ok=%v err=%v", page, ok, err)
	}
	c.Close()
}

// A cursor opened later in wall-clock time may pin an OLDER view than an
// already-open cursor, when it is opened on an old snapshot. Compaction must
// order retained versions by true view age, not cursor opening time;
// otherwise the rewritten log could replay into the wrong final state.
func TestCompactRetentionOrdersByViewAge(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	oldSnap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	latestCur, err := s.Cursor("", "") // opened first, pins gen with v2
	if err != nil {
		t.Fatal(err)
	}
	oldCur := oldSnap.Cursor("", "") // opened second, pins older gen with v1

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// Both fixed views stay served from memory.
	if p, ok, err := oldCur.Next(10); err != nil || !ok || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("old cursor: %+v ok=%v err=%v", p, ok, err)
	}
	if p, ok, err := latestCur.Next(10); err != nil || !ok || len(p) != 1 || string(p[0].Value) != "v2" {
		t.Fatalf("latest cursor: %+v ok=%v err=%v", p, ok, err)
	}

	// The rewritten log holds both versions and replays to the latest one.
	data := mustReadWAL(t, dir)
	if !bytes.Contains(data, []byte("v1")) || !bytes.Contains(data, []byte("v2")) {
		t.Fatalf("compaction dropped a pinned version: %q", data)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("replayed final state: %q ok=%v", got, ok)
	}

	if err := oldCur.Close(); err != nil {
		t.Fatal(err)
	}
	if err := latestCur.Close(); err != nil {
		t.Fatal(err)
	}
}

// Same age-ordering hazard, with the newer view ending in a delete. The
// retained old put must be followed by a tombstone so replay ends absent.
func TestCompactRetentionOldPutThenDelete(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	oldSnap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("k"); err != nil {
		t.Fatal(err)
	}
	latestCur, err := s.Cursor("", "") // empty latest view: pins nothing for k
	if err != nil {
		t.Fatal(err)
	}
	oldCur := oldSnap.Cursor("", "") // pins k=v1 from the older snapshot

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if p, ok, err := oldCur.Next(10); err != nil || !ok || len(p) != 1 || string(p[0].Value) != "v1" {
		t.Fatalf("old cursor after delete+compact: %+v ok=%v", p, ok)
	}
	data := mustReadWAL(t, dir)
	if !bytes.Contains(data, []byte("v1")) {
		t.Fatal("pinned old value missing while cursor open")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, ok, _ := s2.Get("k"); ok {
		t.Fatal("replay resurrected a deleted key: retained put lacked its tombstone")
	}
	latestCur.Close()
	oldCur.Close()
}

// Two cursors on the same old snapshot with disjoint ranges both pin through
// a sequence of compactions, and once both close the history is reclaimed.
func TestCompactMultiplePinsSameGeneration(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("b1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte{'Q'}, 4096)
	if err := s.Put("a", big); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", big); err != nil {
		t.Fatal(err)
	}

	ca := snap.CursorPrefix("a")
	cb := snap.CursorPrefix("b")
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if p, ok, _ := ca.Next(10); !ok || len(p) != 1 || string(p[0].Value) != "a1" {
		t.Fatalf("ca: %+v ok=%v", p, ok)
	}
	if p, ok, _ := cb.Next(10); !ok || len(p) != 1 || string(p[0].Value) != "b1" {
		t.Fatalf("cb: %+v ok=%v", p, ok)
	}

	// Repeated compactions while pinned keep both small old values.
	for i := 0; i < 3; i++ {
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	data := mustReadWAL(t, dir)
	if !bytes.Contains(data, []byte("a1")) || !bytes.Contains(data, []byte("b1")) {
		t.Fatal("a pinned value vanished during repeated compactions")
	}

	ca.Close()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	data = mustReadWAL(t, dir)
	if bytes.Contains(data, []byte("a1")) {
		t.Fatal("a1 was not reclaimed once its only cursor closed")
	}
	if !bytes.Contains(data, []byte("b1")) {
		t.Fatal("b1 must remain while cb is still open")
	}
	cb.Close()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	data = mustReadWAL(t, dir)
	if bytes.Contains(data, []byte("a1")) || bytes.Contains(data, []byte("b1")) {
		t.Fatal("pinned history not reclaimed after the last cursor closed")
	}
	// Live state still intact after reclaim.
	if got, ok, _ := s.Get("a"); !ok || !bytes.Equal(got, big) {
		t.Fatalf("live a after reclaim: len=%d ok=%v", len(got), ok)
	}
}
