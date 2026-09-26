package snapshot

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestChainLeaseBasics acquires a lease, confirms a second acquisition fails
// wholesale with fs.ErrInvalid, releases it, and reacquires.
func TestChainLeaseBasics(t *testing.T) {
	dir := t.TempDir()

	first, err := acquireChainLease(dir, chainOpMerge)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, chainLockName)); err != nil {
		t.Fatalf("lock file missing while held: %v", err)
	}

	second, err := acquireChainLease(dir, chainOpIncremental)
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("second acquire = %v, want fs.ErrInvalid", err)
	}
	if second != nil {
		t.Fatal("busy acquire returned a lease")
	}

	first.release()
	if _, err := os.Stat(filepath.Join(dir, chainLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lock file remains after release: %v", err)
	}

	third, err := acquireChainLease(dir, chainOpBackup)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	third.release()
}

// TestChainLeaseReclaimsExpiredHolder simulates a holder killed on a
// filesystem that retained its byte-range lock: the descriptor is still open
// and the lock still looks held, but the record's deadline has passed. The
// next acquisition fences it (unlinks the dead inode, locks a fresh one) and
// proceeds with no manual cleanup.
func TestChainLeaseReclaimsExpiredHolder(t *testing.T) {
	dir := t.TempDir()

	holder, err := acquireChainLease(dir, chainOpMerge)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite an expired record through the holder's own descriptor: the
	// inode stays write-locked, so the next acquirer cannot take its lock,
	// but the deadline marks the holder dead.
	if err := writeLeaseBody(holder.file,
		encodeLeaseRecord(holder.token, chainOpMerge, chainTime().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := holder.file.Sync(); err != nil {
		t.Fatal(err)
	}

	next, err := acquireChainLease(dir, chainOpIncremental)
	if err != nil {
		t.Fatalf("acquire over expired locked holder = %v, want reclaimed lease", err)
	}
	// Release the new holder first so it unlinks the live name; the dead
	// holder's descriptor points at the already-unlinked inode.
	next.release()
	holder.release()

	if _, err := os.Stat(filepath.Join(dir, chainLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("lock file left behind after reclaim")
	}
}

// TestChainLeaseFreeLockStaleRecordReclaims covers the ordinary Linux case:
// the killed process's descriptor is closed (lock free) and its expired
// record remains on disk.
func TestChainLeaseFreeLockStaleRecordReclaims(t *testing.T) {
	dir := t.TempDir()
	body := encodeLeaseRecord(42, chainOpMerge, time.Now().Add(-time.Hour))
	if err := os.WriteFile(filepath.Join(dir, chainLockName), body, 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := acquireChainLease(dir, chainOpIncremental)
	if err != nil {
		t.Fatalf("acquire over dead holder file = %v", err)
	}
	l.release()
}

// TestChainLeaseTornRecordReclaims plants a truncated lock file older than the
// TTL and confirms it is treated as dead-holder debris.
func TestChainLeaseTornRecordReclaims(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, chainLockName)
	if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-chainLeaseTTL - time.Second)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	l, err := acquireChainLease(dir, chainOpMerge)
	if err != nil {
		t.Fatalf("acquire over torn ancient record = %v", err)
	}
	l.release()
}

// TestChainLeaseFreshTornRecordBlocks plants a freshly truncated record, which
// a live holder could be mid-writing: it must not be stolen immediately.
func TestChainLeaseFreshTornRecordBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, chainLockName)

	// A process must actually hold the file's write lock for the record to
	// block; emulate that with a live lease whose body we then truncate.
	holder, err := acquireChainLease(dir, chainOpMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.release()
	if err := holder.file.Truncate(3); err != nil {
		t.Fatal(err)
	}
	stale, err := chainLeaseRecordStale(path)
	if err != nil {
		t.Fatal(err)
	}
	if stale {
		t.Fatal("fresh torn record reported stale")
	}
}

// TestChainOperationsRejectWhileLeaseHeld drives the public operations: while
// one registered operation holds the chain, every other backup, incremental
// export, merge and restore fails wholesale with fs.ErrInvalid.
func TestChainOperationsRejectWhileLeaseHeld(t *testing.T) {
	parent := t.TempDir()
	storeDir := filepath.Join(parent, "store")
	chain := filepath.Join(parent, "chain")

	s, err := Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}

	holder, err := acquireChainLease(chain, chainOpMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.release()

	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("incremental while held = %v, want fs.ErrInvalid", err)
	}
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("merge while held = %v, want fs.ErrInvalid", err)
	}
	if _, err := Restore(chain, filepath.Join(parent, "target")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore while held = %v, want fs.ErrInvalid", err)
	}

	// A full backup into the same chain directory is likewise rejected.
	if err := s.Backup(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("backup into held chain = %v, want fs.ErrInvalid", err)
	}

	// The chain was not modified by the rejected calls.
	if rings := ringFiles(t, chain); len(rings) != 1 {
		t.Fatalf("chain rings = %v, want exactly the original ring", rings)
	}
}

// TestChainOperationsProceedAfterRelease is the other half of the exclusion
// pair: once the holder releases, the chain is fully usable again.
func TestChainOperationsProceedAfterRelease(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")

	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	holder, err := acquireChainLease(chain, chainOpMerge)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 2); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("merge while held = %v", err)
	}
	holder.release()

	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatalf("merge after release: %v", err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after merge: %v", err)
	}
	if _, err := Restore(chain, filepath.Join(parent, "target")); err != nil {
		t.Fatalf("restore after release: %v", err)
	}
}

// TestChainLeaseReclaimOnNextExport leaves an expired lock in a completed
// chain and confirms the next export reclaims it and extends the chain.
func TestChainLeaseReclaimOnNextExport(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")

	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}

	// Debris from a holder killed at an expired deadline.
	body := encodeLeaseRecord(999, chainOpMerge, time.Now().Add(-time.Hour))
	if err := os.WriteFile(filepath.Join(chain, chainLockName), body, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental reclaims expired lock: %v", err)
	}
	if rings := ringFiles(t, chain); len(rings) != 2 {
		t.Fatalf("rings after reclaim = %d, want 2", len(rings))
	}
	if _, err := os.Stat(filepath.Join(chain, chainLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("lock file left behind after export")
	}
}

// TestChainLeaseReclaimOnOpen leaves an expired lock in a chain and confirms
// opening the directory reclaims it, after which the chain is usable.
func TestChainLeaseReclaimOnOpen(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")

	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	body := encodeLeaseRecord(999, chainOpMerge, time.Now().Add(-time.Hour))
	if err := os.WriteFile(filepath.Join(chain, chainLockName), body, 0o600); err != nil {
		t.Fatal(err)
	}

	// Opening the chain directory itself reclaims the dead lease. Opening a
	// directory also creates a store there by long-standing contract; the
	// assertion of interest is the reclaim, and the test removes the
	// observer's WAL before treating the path purely as a backup chain again.
	opened, err := Open(chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(chain, chainLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("expired lock not reclaimed on open")
	}
	if err := os.Remove(filepath.Join(chain, walName)); err != nil {
		t.Fatal(err)
	}

	// The chain is usable with no manual cleanup and still validates. Reopen
	// the originating store: a fresh store's watermark sits behind the chain
	// tip and may not extend a chain it never wrote.
	s2, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s2.BackupIncremental(chain); err != nil {
		t.Fatalf("chain unusable after reclaim: %v", err)
	}
}

// TestChainLeaseDeadProcessReclaimed runs a real helper process that holds a
// lease with a future deadline, kills it with SIGKILL, and confirms the next
// acquirer proceeds: the kernel releases the open-file-description lock as
// the descriptor closes, so no manual cleanup is needed.
func TestChainLeaseDeadProcessReclaimed(t *testing.T) {
	if os.Getenv("SNAPSHOT_CHAIN_LEASE_HELPER") == "1" {
		chainLeaseHelperProcess()
		return
	}
	if _, err := os.Stat("/proc/self"); err != nil {
		t.Skip("SIGKILL lease-reclaim test is Linux-specific")
	}

	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")
	if err := os.MkdirAll(chain, 0o700); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestChainLeaseDeadProcessReclaimed$")
	cmd.Env = append(os.Environ(), "SNAPSHOT_CHAIN_LEASE_HELPER=1", "SNAPSHOT_CHAIN_DIR="+chain)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(stdout)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("helper did not become ready: %v", err)
	}
	if strings.TrimSpace(line) != "READY" {
		t.Fatalf("helper handshake = %q", line)
	}

	// The helper is alive and holds a fresh, future-deadline lease.
	if _, err := os.Stat(filepath.Join(chain, chainLockName)); err != nil {
		t.Fatalf("helper lock missing: %v", err)
	}
	if blocked, err := acquireChainLease(chain, chainOpMerge); err == nil {
		blocked.release()
		t.Fatal("acquired a lease the live helper holds")
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	_ = cmd.Wait()

	// Give the kernel a moment to finish closing the killed descriptors.
	deadline := time.Now().Add(5 * time.Second)
	var l *chainLease
	for {
		l, err = acquireChainLease(chain, chainOpMerge)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease not reclaimed after holder kill: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	l.release()
}

// chainLeaseHelperProcess is the killed-holder side of
// TestChainLeaseDeadProcessReclaimed. It acquires a lease and signals READY,
// then blocks until the parent kills it.
func chainLeaseHelperProcess() {
	dir := os.Getenv("SNAPSHOT_CHAIN_DIR")
	l, err := acquireChainLease(dir, chainOpMerge)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper acquire: %v\n", err)
		os.Exit(2)
	}
	defer l.release()
	fmt.Println("READY")
	os.Stdout.Sync()
	time.Sleep(time.Hour)
}

// TestChainMergeSweepLeavesLiveLeaseAlone confirms debris sweep never repairs
// or deletes a sibling a running merge is still committing: a trash sibling
// carrying a live lease stays put even when the chain path is empty.
func TestChainMergeSweepLeavesLiveLeaseAlone(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")
	trash := filepath.Join(parent, "chain"+mergeTrashPrefix+"live")
	if err := os.MkdirAll(chain, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(trash, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trash, backupManifest), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	holder, err := acquireChainLease(trash, chainOpMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.release()

	// Empty chain path plus a live-leased trash sibling: nothing moves.
	sweepMergeDebris(chain)
	if _, err := os.Stat(filepath.Join(trash, backupManifest)); err != nil {
		t.Fatalf("live trash swept: %v", err)
	}
	if entries, _ := os.ReadDir(chain); len(entries) != 0 {
		t.Fatalf("live trash moved back into chain: %v", entries)
	}

	// Once the holder is gone the trash is swept and moved back.
	holder.release()
	sweepMergeDebris(chain)
	if _, err := os.Stat(filepath.Join(chain, backupManifest)); err != nil {
		t.Fatalf("dead trash not restored after release: %v", err)
	}
	if _, err := os.Stat(trash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("trash sibling remains after move-back")
	}
}

// TestChainMergeTrashMovedBackOnOpen reproduces a kill between the two merge
// commit renames: the chain path is absent, the whole old chain sits in a
// trash sibling. Opening the related directory moves it back (never deletes
// it) and the chain restores exactly.
func TestChainMergeTrashMovedBackOnOpen(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")

	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	want := allPairs(t, s)
	wantSnaps := s.Stats().Snapshots
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate the kill window: move the whole chain aside into the trash
	// sibling name and leave the chain path missing.
	trash := chain + mergeTrashPrefix + "000000000"
	if err := os.Rename(chain, trash); err != nil {
		t.Fatal(err)
	}
	if err := syncDirectory(parent); err != nil {
		t.Fatal(err)
	}

	// Open a related directory (the chain path itself): it must move the
	// whole old chain back rather than delete it. Opening a directory also
	// creates an empty store there, so drop the observer's WAL before using
	// the path purely as a backup chain again.
	opened, err := Open(chain)
	if err != nil {
		t.Fatalf("open after kill: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(chain, backupManifest)); err != nil {
		t.Fatalf("old chain not moved back: %v", err)
	}
	if _, err := os.Stat(trash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("trash sibling remains after move-back")
	}
	if err := os.Remove(filepath.Join(chain, walName)); err != nil {
		t.Fatal(err)
	}

	rs, err := Restore(chain, filepath.Join(parent, "target"))
	if err != nil {
		t.Fatalf("restored chain invalid after move-back: %v", err)
	}
	assertSamePairs(t, allPairs(t, rs), want, "moved-back chain")
	if n := rs.Stats().Snapshots; n != wantSnaps {
		t.Fatalf("snapshots = %d, want %d", n, wantSnaps)
	}
	rs.Close()
}

// TestChainOperationsConcurrentExclusion fires many incremental exports,
// merges, restores and full backups at one chain concurrently. Exactly one
// operation reshapes the chain at a time; the losers all return fs.ErrInvalid
// without changing anything. The chain validates when the storm ends.
func TestChainOperationsConcurrentExclusion(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")

	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	const goroutines = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	var busyCount, okCount int
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			var rerr error
			target := filepath.Join(parent, fmt.Sprintf("target-%d", g))
			switch g % 3 {
			case 0:
				rerr = s.MergeChain(chain, 1)
			case 1:
				rerr = s.BackupIncremental(chain)
			default:
				_, rerr = Restore(chain, target)
			}
			mu.Lock()
			defer mu.Unlock()
			if rerr == nil {
				okCount++
			} else if errors.Is(rerr, fs.ErrInvalid) {
				busyCount++
			} else {
				t.Errorf("goroutine %d: %v", g, rerr)
			}
		}(g)
	}
	close(start)
	wg.Wait()

	if okCount < 1 {
		t.Fatal("no operation won the chain")
	}
	if okCount+busyCount != goroutines {
		t.Fatalf("outcomes: %d ok, %d busy of %d", okCount, busyCount, goroutines)
	}

	// The storm left exactly one complete chain or the other at every step;
	// the chain at rest must validate as one whole.
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("chain invalid after storm: %v", err)
	}
	if _, err := Restore(chain, filepath.Join(parent, "final-target")); err != nil {
		t.Fatalf("restore after storm: %v", err)
	}
	if rings := ringFiles(t, chain); len(rings) != int(art.ringCount) {
		t.Fatalf("ring files %d disagree with validated count %d", len(rings), art.ringCount)
	}
}

// TestChainConcurrentRestoresOneWins fires two restores at one chain. The
// loser fails with fs.ErrInvalid before it builds anything, so its target is
// never created; the winner installs one complete store.
func TestChainConcurrentRestoresOneWins(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")
	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	t1 := filepath.Join(parent, "t1")
	t2 := filepath.Join(parent, "t2")
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, target := range []string{t1, t2} {
		wg.Add(1)
		go func(i int, target string) {
			defer wg.Done()
			<-start
			_, errs[i] = Restore(chain, target)
		}(i, target)
	}
	close(start)
	wg.Wait()

	wins := 0
	for i, rerr := range errs {
		switch {
		case rerr == nil:
			wins++
		case errors.Is(rerr, fs.ErrInvalid):
		default:
			t.Fatalf("restore %d: %v", i, rerr)
		}
	}
	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1", wins)
	}
	for _, target := range []string{t1, t2} {
		_, serr := os.Stat(target)
		if serr == nil {
			// A present target must be the winner's complete store.
			st, err := Open(target)
			if err != nil {
				t.Fatalf("target %s present but unopenable: %v", target, err)
			}
			if v, ok, _ := st.Get("a"); !ok || string(v) != "1" {
				t.Fatalf("target %s not the complete chain state", target)
			}
			st.Close()
		} else if !errors.Is(serr, fs.ErrNotExist) {
			t.Fatal(serr)
		}
	}
	// No coordination file is left in the restored winner or the chain.
	if _, err := os.Stat(filepath.Join(chain, chainLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("chain lock left behind after restore")
	}
}

// TestClosedStoreOperationsDoNotLease confirms the closed-store error still
// wins: no lease is acquired and the error is fs.ErrClosed.
func TestClosedStoreOperationsDoNotLease(t *testing.T) {
	parent := t.TempDir()
	chain := filepath.Join(parent, "chain")
	s, err := Open(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Backup(filepath.Join(parent, "out")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("backup closed = %v, want fs.ErrClosed", err)
	}
	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("incremental closed = %v, want fs.ErrClosed", err)
	}
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("merge closed = %v, want fs.ErrClosed", err)
	}
}
