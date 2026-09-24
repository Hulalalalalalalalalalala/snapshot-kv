package snapshot

import (
	"bytes"
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
)

// fullState is the complete committed state as observable through scans:
// sorted present key/value pairs, the cumulative snapshot count and stats.
type fullState struct {
	pairs     []KVPair
	snapshots uint64
	stats     Stats
}

func readFullState(t *testing.T, s *Store) fullState {
	t.Helper()
	pairs, err := s.Scan(Range{}, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return fullState{pairs: pairs, snapshots: s.snapshots.Load(), stats: s.Stats()}
}

func assertStateEqual(t *testing.T, got, want fullState, tag string) {
	t.Helper()
	if got.snapshots != want.snapshots {
		t.Fatalf("%s: snapshot count %d, want %d", tag, got.snapshots, want.snapshots)
	}
	if got.stats != want.stats {
		t.Fatalf("%s: stats %+v, want %+v", tag, got.stats, want.stats)
	}
	if len(got.pairs) != len(want.pairs) {
		t.Fatalf("%s: %d pairs, want %d", tag, len(got.pairs), len(want.pairs))
	}
	for i := range want.pairs {
		if got.pairs[i].Key != want.pairs[i].Key ||
			!bytes.Equal(got.pairs[i].Value, want.pairs[i].Value) {
			t.Fatalf("%s: pair %d = %q=%q, want %q=%q", tag, i,
				got.pairs[i].Key, got.pairs[i].Value, want.pairs[i].Key, want.pairs[i].Value)
		}
	}
}

func reopenStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func checkpointPath(dir string) string { return filepath.Join(dir, checkpointName) }

func TestCheckpointReopenMatchesFullReplay(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	// A rich history before the freeze: overwrites, deletes, empty values,
	// batches and snapshots.
	if err := s.Put("a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "c", Value: nil},
		{Key: "d", Value: []byte("first")},
		{Key: "d", Value: []byte("second")}, // last write in the batch wins
		{Key: "a", Delete: true},
		{Key: "a", Value: []byte("resurrected")}, // delete-then-put
		{Key: "gone", Value: []byte("x")},
		{Key: "gone", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte{0xAB}, 5000)
	if err := s.Put("e", big); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// History after the freeze rides only the log tail on the next reopen.
	if err := s.Put("f", []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "d", Value: []byte("tail-wins")},
		{Key: "e", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Path 1: checkpoint plus tail replay.
	s2 := reopenStore(t, dir)
	gotCheckpoint := readFullState(t, s2)
	assertStateEqual(t, gotCheckpoint, want, "checkpoint reopen")
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// Path 2: delete the checkpoint and re-run the exact same bytes through a
	// full replay. The two recoveries must agree byte-for-byte.
	if err := os.Remove(checkpointPath(dir)); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s3), want, "full replay")
	if err := s3.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointFileLayout(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{checkpointName, walName} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s on disk: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp checkpoint left behind: %v", err)
	}
}

func TestCheckpointEmptyStore(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	if st := s2.Stats(); st.Keys != 0 || st.Snapshots != 0 || st.AvgBytes != 0 {
		t.Fatalf("empty checkpoint reopened wrong: %+v", st)
	}
	if got, ok, err := s2.Get("anything"); ok || err != nil || got != nil {
		t.Fatalf("empty store hit: %q %v %v", got, ok, err)
	}
}

func TestCheckpointEmptyValuesStayHits(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	for i, v := range [][]byte{nil, {}} {
		if err := s.Put(fmt.Sprintf("e%d", i), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	for i := 0; i < 2; i++ {
		if got, ok, err := s2.Get(fmt.Sprintf("e%d", i)); !ok || err != nil || len(got) != 0 {
			t.Fatalf("empty value %d: %q ok=%v err=%v", i, got, ok, err)
		}
	}
}

func TestCheckpointBatchAtomicAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// The batch straddles nothing: it lands entirely in the tail.
	if err := s.CommitBatch([]BatchOp{
		{Key: "x", Value: []byte("1")},
		{Key: "x", Delete: true}, // put-then-delete inside one batch
		{Key: "y", Value: []byte("keep")},
	}); err != nil {
		t.Fatal(err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s2), want, "batch after checkpoint")
	if _, ok, _ := s2.Get("x"); ok {
		t.Fatal("put-then-delete batch left x present")
	}
}

// corruptCheckpoint rewrites the live checkpoint bytes via fn.
func corruptCheckpoint(t *testing.T, dir string, fn func([]byte) []byte) {
	t.Helper()
	p := checkpointPath(dir)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, fn(raw), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointRejectedFallsBackToLog(t *testing.T) {
	src := tempDir(t)
	s := openStore(t, src)
	if err := s.Put("keep", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("tail", []byte("after")); err != nil {
		t.Fatal(err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// flipCRC rewrites the last four bytes (the CRC trailer) only.
	flipCRC := func(b []byte) {
		h := crc32.NewIEEE()
		h.Write(b[:len(b)-4])
		binary.LittleEndian.PutUint32(b[len(b)-4:], h.Sum32()^0xFFFFFFFF)
	}
	// reseal recomputes a valid CRC after a structural mutation.
	reseal := func(b []byte) {
		h := crc32.NewIEEE()
		h.Write(b[:len(b)-4])
		binary.LittleEndian.PutUint32(b[len(b)-4:], h.Sum32())
	}
	cases := map[string]func([]byte){
		"truncated": func(b []byte) { /* shrink handled by the truncating opener below */
		},
		"one-byte-short":       func(b []byte) {},
		"flipped payload byte": func(b []byte) { b[checkpointHeaderSize] ^= 0xFF },
		"flipped crc byte":     func(b []byte) { flipCRC(b) },
		"trailing garbage":     func(b []byte) {},
		"bad magic": func(b []byte) {
			b[0] ^= 0xFF
			reseal(b)
		},
		"unknown version": func(b []byte) {
			binary.LittleEndian.PutUint32(b[4:8], 0xFFFFFFFF)
			reseal(b)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := copyDir(t, src)
			switch name {
			case "truncated":
				p := checkpointPath(dir)
				raw, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, raw[:len(raw)/2], 0o600); err != nil {
					t.Fatal(err)
				}
			case "one-byte-short":
				p := checkpointPath(dir)
				raw, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, raw[:len(raw)-1], 0o600); err != nil {
					t.Fatal(err)
				}
			case "trailing garbage":
				f, err := os.OpenFile(checkpointPath(dir), os.O_RDWR|os.O_APPEND, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte{0x77}); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			default:
				corruptCheckpoint(t, dir, func(raw []byte) []byte {
					mutate(raw)
					return raw
				})
			}

			s3, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen with %s checkpoint: %v", name, err)
			}
			assertStateEqual(t, readFullState(t, s3), want, name)
			if err := s3.Close(); err != nil {
				t.Fatal(err)
			}
			// A rejected checkpoint is retired; the directory is left with
			// only the log, exactly the pre-checkpoint layout.
			if _, err := os.Stat(checkpointPath(dir)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("%s: rejected checkpoint not retired: %v", name, err)
			}
			// And committing works straight away with no repair step.
			s4 := reopenStore(t, dir)
			if err := s4.Put("more", []byte("ok")); err != nil {
				t.Fatalf("commit after rejected checkpoint: %v", err)
			}
			if got, ok, _ := s4.Get("more"); !ok || string(got) != "ok" {
				t.Fatalf("post-recovery commit lost: %q %v", got, ok)
			}
			if err := s4.Checkpoint(); err != nil {
				t.Fatalf("fresh checkpoint after rejection: %v", err)
			}
			if err := s4.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// copyDir duplicates a store directory tree so each corruption case starts
// from byte-identical on-disk state.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
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

func TestCheckpointOffsetBeyondLogRejected(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Rewrite the resume offset (header offset 16) to something past EOF and
	// fix up the CRC so only the offset itself is wrong.
	p := checkpointPath(dir)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// bytes 16..23 hold walOffset (LE uint64); set a huge value.
	for i := 16; i < 24; i++ {
		raw[i] = 0xFF
	}
	body := raw[:len(raw)-4]
	binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.ChecksumIEEE(body))
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s2), want, "offset beyond log")
}

func TestCheckpointMissingOpensNormally(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A directory that never had a checkpoint reopens exactly as before.
	s2 := reopenStore(t, dir)
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("old-layout reopen: %q %v", got, ok)
	}
	if n := s2.Stats().Snapshots; n != 1 {
		t.Fatalf("snapshot count on old layout: %d", n)
	}
	// First checkpoint upgrades the directory to the new layout.
	if err := s2.Checkpoint(); err != nil {
		t.Fatalf("first checkpoint: %v", err)
	}
	if err := s2.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	if got, ok, _ := s3.Get("b"); !ok || string(got) != "2" {
		t.Fatalf("upgraded-layout tail commit lost: %q %v", got, ok)
	}
}

func TestCheckpointStaleTempSweptOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("keep", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointTmpName), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	if got, ok, _ := s2.Get("keep"); !ok || string(got) != "value" {
		t.Fatalf("state lost with stale checkpoint temp: %q %v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale checkpoint temp not swept: %v", err)
	}
}

func TestCheckpointTornTailAfterCheckpoint(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("committed", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("after", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Kill a second batch mid-frame in the tail.
	appendRawFrames(t, dir, func(l *walWriter) {
		if err := l.encodeFrame(opBatchBegin, "", nil); err != nil {
			t.Fatal(err)
		}
		if err := l.encodeFrameSeq(opPutSeq, "torn", 99, []byte("no")); err != nil {
			t.Fatal(err)
		}
	})
	s2 := reopenStore(t, dir)
	if got, ok, _ := s2.Get("committed"); !ok || string(got) != "yes" {
		t.Fatalf("checkpoint state lost: %q %v", got, ok)
	}
	if got, ok, _ := s2.Get("after"); !ok || string(got) != "ok" {
		t.Fatalf("tail commit lost: %q %v", got, ok)
	}
	if _, ok, _ := s2.Get("torn"); ok {
		t.Fatal("torn tail batch visible after reopen")
	}
	if err := s2.Put("clean", []byte("append")); err != nil {
		t.Fatalf("append after torn tail: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	if got, ok, _ := s3.Get("clean"); !ok || string(got) != "append" {
		t.Fatalf("clean append lost: %q %v", got, ok)
	}
	if _, ok, _ := s3.Get("torn"); ok {
		t.Fatal("torn batch resurfaced")
	}
}

func TestCheckpointRepeatedKeepsSingleFile(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	big := bytes.Repeat([]byte("Z"), 2048)
	for i := 0; i < 30; i++ {
		if err := s.Put("overwritten", big); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	first, err := os.Stat(checkpointPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	// Much more dead history, then another freeze: the checkpoint reflects
	// only terminal state and only one checkpoint file exists.
	for i := 0; i < 50; i++ {
		if err := s.Put("overwritten", []byte("tiny")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var cps []string
	for _, e := range entries {
		if n := e.Name(); n == checkpointName || n == checkpointTmpName {
			cps = append(cps, n)
		}
	}
	if len(cps) != 1 || cps[0] != checkpointName {
		t.Fatalf("directory checkpoint files = %v, want only %q", cps, checkpointName)
	}
	second, err := os.Stat(checkpointPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if second.Size() >= first.Size() {
		t.Fatalf("new checkpoint did not shrink with terminal state: %d >= %d",
			second.Size(), first.Size())
	}
	if got, ok, _ := s.Get("overwritten"); !ok || string(got) != "tiny" {
		t.Fatalf("terminal value wrong: %q %v", got, ok)
	}
}

func TestCheckpointRetiresAcrossCompaction(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// Compaction rewrote wal.log, so no checkpoint may survive to describe
	// the old file.
	if _, err := os.Stat(checkpointPath(dir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("checkpoint survived compaction: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("checkpoint temp left by compaction: %v", err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s2), want, "reopen after compact retired checkpoint")

	// A fresh checkpoint against the rewritten log works and accelerates again.
	if err := s2.Put("b", []byte("post")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	if got, ok, _ := s3.Get("a"); !ok || string(got) != "v2" {
		t.Fatalf("a: %q %v", got, ok)
	}
	if got, ok, _ := s3.Get("b"); !ok || string(got) != "post" {
		t.Fatalf("b: %q %v", got, ok)
	}
}

func TestCheckpointSnapshotsAndCursorsUnaffected(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	cur, err := s.OpenCursor(Range{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v3")); err != nil {
		t.Fatal(err)
	}
	// Fixed views stay exact while the freeze lands.
	if got, ok := snap.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("snapshot view changed: %q %v", got, ok)
	}
	pages, err := cur.Page(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 1 || string(pages[0].Value) != "v1" {
		t.Fatalf("cursor view changed: %+v", pages)
	}
	snap.Close()
	if err := cur.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointOnClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("checkpoint on closed store: err=%v, want fs.ErrClosed", err)
	}
}

// runCrashWindow drives Checkpoint with a hook that simulates a kill at stage,
// then reopens the directory (without an orderly close of the interrupted
// store) and checks complete state. freshWrites, when non-empty, are committed
// after the interrupted freeze and must appear through tail replay.
func runCrashWindow(t *testing.T, stage checkpointStage, priorCheckpoint bool, freshWrites []BatchOp) {
	t.Helper()
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("before", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if priorCheckpoint {
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := s.Put("between", []byte("2")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("freeze", []byte("3")); err != nil {
		t.Fatal(err)
	}

	fired := make(chan struct{})
	s.checkpointHook = func(got checkpointStage) bool {
		if got == stage {
			close(fired)
			return true
		}
		return false
	}
	err := s.Checkpoint()
	if !errors.Is(err, errCheckpointInterrupted) {
		t.Fatalf("checkpoint err=%v, want interrupted", err)
	}
	<-fired
	s.checkpointHook = nil

	// The store itself is still intact and usable after the simulated kill
	// point; commit more history so the tail is exercised on reopen.
	for i := range freshWrites {
		if err := s.CommitBatch(freshWrites[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}
	want := readFullState(t, s)

	// Simulate the process actually dying: reopen without Close.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash at stage %d: %v", stage, err)
	}
	assertStateEqual(t, readFullState(t, s2), want,
		fmt.Sprintf("crash stage=%d prior=%v", stage, priorCheckpoint))
	if got, ok, _ := s2.Get("freeze"); !ok || string(got) != "3" {
		t.Fatalf("frozen state missing after crash: %q %v", got, ok)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointCrashWindows(t *testing.T) {
	for _, prior := range []bool{false, true} {
		runCrashWindow(t, checkpointTempSynced, prior,
			[]BatchOp{{Key: "after", Value: []byte("tail")}})
		runCrashWindow(t, checkpointRenamed, prior,
			[]BatchOp{{Key: "after", Value: []byte("tail")}})
	}
}

func TestCheckpointConcurrentWithEverything(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", w)
			for i := 0; i < 100; i++ {
				if err := s.CommitBatch([]BatchOp{
					{Key: key, Value: []byte(fmt.Sprintf("%d-%d", w, i))},
				}); err != nil {
					t.Errorf("batch: %v", err)
					return
				}
				if i%25 == 0 {
					if err := s.Delete(key); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
				}
			}
			// Deterministic final value per key.
			if err := s.Put(key, []byte(fmt.Sprintf("final-%d", w))); err != nil {
				t.Errorf("final put: %v", err)
			}
		}(w)
	}
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if err := s.Checkpoint(); err != nil {
					t.Errorf("checkpoint: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			if err := s.Compact(); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
			snap, err := s.Snapshot()
			if err != nil {
				t.Errorf("snapshot: %v", err)
				return
			}
			cur, err := snap.OpenCursor(Range{})
			if err != nil {
				t.Errorf("cursor: %v", err)
				snap.Close()
				return
			}
			if _, err := cur.Page(0, 8); err != nil {
				t.Errorf("page: %v", err)
			}
			cur.Close()
			snap.Close()
		}
	}()
	wg.Wait()

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	want := readFullState(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s2), want, "concurrent checkpoint reopen")
	for w := 0; w < 4; w++ {
		got, ok, _ := s2.Get(fmt.Sprintf("k%d", w))
		if !ok || string(got) != fmt.Sprintf("final-%d", w) {
			t.Fatalf("final k%d: %q ok=%v", w, got, ok)
		}
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	// Full-replay path must reach the identical terminal state.
	if err := os.Remove(checkpointPath(dir)); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s3), want, "concurrent full replay")
}

func TestCheckpointSnapshotCountAcrossFreezes(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	if n := s2.Stats().Snapshots; n != 3 {
		t.Fatalf("snapshot count across freeze: %d, want 3", n)
	}
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, dir)
	if n := s3.Stats().Snapshots; n != 4 {
		t.Fatalf("snapshot count after second freeze: %d, want 4", n)
	}
}

func TestCheckpointKeysSortedOnDisk(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	keys := []string{"z", "a", "m", "b", "y"}
	for _, k := range keys {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	d, err := loadCheckpoint(dir, info.Size())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := make([]string, 0, len(d.entries))
	for _, e := range d.entries {
		got = append(got, e.key)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("checkpoint entries not sorted: %v", got)
	}
}

func TestCheckpointOffsetIsExactLogBoundary(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("old", []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	cp, err := loadCheckpoint(dir, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if cp.walOffset != info.Size() {
		t.Fatalf("walOffset=%d, want log size %d", cp.walOffset, info.Size())
	}
	// A commit lands entirely after the frozen boundary.
	if err := s.Put("new", []byte("tail")); err != nil {
		t.Fatal(err)
	}
	info2, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	if info2.Size() <= cp.walOffset {
		t.Fatal("log did not grow past frozen boundary")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cp2, err := loadCheckpoint(dir, info2.Size())
	if err != nil {
		t.Fatal(err)
	}
	if cp2.walOffset != info.Size() {
		t.Fatalf("stored offset drifted: %d, want %d", cp2.walOffset, info.Size())
	}
}

func TestCheckpointSameSnapshotReadingsBeforeAndAfter(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot's view stays byte-identical through a freeze, subsequent
	// writes, a crash-style reopen, and a checkpoint-accelerated reopen.
	read := func() map[string]string {
		pairs, err := snap.Scan(Range{}, 0)
		if err != nil {
			t.Fatalf("snapshot scan: %v", err)
		}
		out := make(map[string]string, len(pairs))
		for _, p := range pairs {
			out[p.Key] = string(p.Value)
		}
		return out
	}
	before := read()
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil { // not visible to snap
		t.Fatal(err)
	}
	afterFreeze := read()
	if !mapsEqual(before, afterFreeze) {
		t.Fatalf("fixed view changed at freeze: %v vs %v", before, afterFreeze)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, ok := snap.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("snapshot after second freeze: %q ok=%v", got, ok)
	}
	snap.Close()
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestCheckpointCrashReopenTerminalState(t *testing.T) {
	// Kill the process simulation after every distinct stage and compare the
	// reopened terminal state against the state at the kill instant.
	for _, stage := range []checkpointStage{checkpointTempSynced, checkpointRenamed} {
		dir := tempDir(t)
		s := openStore(t, dir)
		if err := s.Put("a", []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put("b", []byte("2")); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		s.checkpointHook = func(got checkpointStage) bool {
			if got == stage {
				close(stop)
				return true
			}
			return false
		}
		_ = s.Checkpoint() // interrupted by the hook
		<-stop
		s.checkpointHook = nil
		want := readFullState(t, s)
		// Real crash: no orderly close, just a new process opening the dir.
		s2, err := Open(dir)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		assertStateEqual(t, readFullState(t, s2), want,
			fmt.Sprintf("kill stage=%d", stage))
		if err := s2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCheckpointFastReopenEqualsFullReplayLarge(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	for i := 0; i < 3000; i++ {
		if err := s.Put(fmt.Sprintf("key%06d", i), []byte(fmt.Sprintf("value-%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Put(fmt.Sprintf("tail%02d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	want := readFullState(t, s)
	walInfo, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	cpInfo, err := os.Stat(checkpointPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if cpInfo.Size() >= walInfo.Size() {
		t.Fatalf("checkpoint %d not smaller than the log %d it lets reopen skip",
			cpInfo.Size(), walInfo.Size())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint-accelerated reopen.
	s2 := reopenStore(t, dir)
	assertStateEqual(t, readFullState(t, s2), want, "accelerated reopen")
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// Full replay of the exact same bytes must reach the identical state.
	fullDir := copyDir(t, dir)
	if err := os.Remove(checkpointPath(fullDir)); err != nil {
		t.Fatal(err)
	}
	s3 := reopenStore(t, fullDir)
	assertStateEqual(t, readFullState(t, s3), want, "full replay")
}

func TestCheckpointRetryAfterInterruptedFreeze(t *testing.T) {
	for _, stage := range []checkpointStage{checkpointTempSynced, checkpointRenamed} {
		dir := tempDir(t)
		s := openStore(t, dir)
		if err := s.Put("k", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		fired := false
		s.checkpointHook = func(got checkpointStage) bool {
			if got == stage && !fired {
				fired = true
				return true
			}
			return false
		}
		if err := s.Checkpoint(); !errors.Is(err, errCheckpointInterrupted) {
			t.Fatalf("stage %d: err=%v", stage, err)
		}
		s.checkpointHook = nil

		// No repair step needed: commits and a retry freeze both work.
		if err := s.Put("k", []byte("v2")); err != nil {
			t.Fatalf("commit after interrupt: %v", err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("retry checkpoint: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, checkpointTmpName)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stage %d: temp left behind", stage)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s2 := reopenStore(t, dir)
		if got, ok, _ := s2.Get("k"); !ok || string(got) != "v2" {
			t.Fatalf("stage %d: k=%q ok=%v", stage, got, ok)
		}
		if err := s2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
