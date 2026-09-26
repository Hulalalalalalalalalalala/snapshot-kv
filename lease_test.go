package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLeaseChildHelper is the re-executed child used by the cross-process
// test. It is a no-op in a normal test run; it acts only when LEASE_HELPER is
// set. Mode "hold" acquires the chain lease and waits to be killed; mode
// "contend" tries to acquire it and records OK or BUSY in a result file.
func TestLeaseChildHelper(t *testing.T) {
	mode := os.Getenv("LEASE_HELPER")
	if mode == "" {
		return
	}
	if ms := os.Getenv("LEASE_TTL_MS"); ms != "" {
		if n, err := time.ParseDuration(ms + "ms"); err == nil {
			leaseTTL = n
		}
	}
	dir := os.Getenv("LEASE_CHAIN_DIR")
	kind := leaseKindIncremental
	if os.Getenv("LEASE_KIND") == "merge" {
		kind = leaseKindMerge
	}
	tok, err := acquireLease(dir, kind)
	if err != nil {
		outcome := "BUSY"
		if !errors.Is(err, fs.ErrInvalid) {
			outcome = "ERR:" + err.Error()
		}
		_ = os.WriteFile(os.Getenv("LEASE_RESULT"), []byte(outcome), 0o600)
		return
	}
	if mode == "contend" {
		tok.release()
		_ = os.WriteFile(os.Getenv("LEASE_RESULT"), []byte("OK"), 0o600)
		return
	}
	_ = os.WriteFile(os.Getenv("LEASE_HELD_PATH"), []byte("1"), 0o600)
	time.Sleep(30 * time.Second)
	tok.release()
}

// TestLeaseExcludesSeparateProcesses verifies the claim works across real
// processes via the hard-link protocol, not just goroutines in one process.
func TestLeaseExcludesSeparateProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("re-execs the test binary")
	}
	dir := t.TempDir()
	const ttlMS = "500"
	heldPath := filepath.Join(t.TempDir(), "held")
	holder := exec.Command(os.Args[0], "-test.run", "TestLeaseChildHelper")
	holder.Env = append(os.Environ(),
		"LEASE_HELPER=hold", "LEASE_CHAIN_DIR="+dir,
		"LEASE_HELD_PATH="+heldPath, "LEASE_TTL_MS="+ttlMS)
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	holderDone := make(chan error, 1)
	go func() { holderDone <- holder.Wait() }()
	var killOnce sync.Once
	killHolder := func() {
		killOnce.Do(func() {
			holder.Process.Kill()
			<-holderDone
		})
	}
	defer killHolder()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(heldPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("holder process never acquired the lease")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, kind := range []string{"", "merge"} {
		resultPath := filepath.Join(t.TempDir(), "result")
		contender := exec.Command(os.Args[0], "-test.run", "TestLeaseChildHelper")
		contender.Env = append(os.Environ(),
			"LEASE_HELPER=contend", "LEASE_CHAIN_DIR="+dir,
			"LEASE_KIND="+kind, "LEASE_RESULT="+resultPath)
		if out, err := contender.CombinedOutput(); err != nil {
			t.Fatalf("contender %q process failed: %v\n%s", kind, err, out)
		}
		out, err := os.ReadFile(resultPath)
		if err != nil {
			t.Fatalf("contender %q wrote no result", kind)
		}
		if string(out) != "BUSY" {
			t.Fatalf("contender %q outcome = %q, want BUSY", kind, string(out))
		}
	}

	// The authoritative sibling lease is present and self-describing.
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(dir), "*.chain-lease"))
	if len(matches) != 1 {
		t.Fatalf("authoritative lease files = %v, want exactly one", matches)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parseLease(data); !ok {
		t.Fatal("authoritative lease not self-describing")
	}

	// Hard-kill the holder mid-operation (SIGKILL: no release, no renewal),
	// wait past the expiry, and a fresh process must reclaim the dead lease and
	// take over rather than deadlock.
	holder.Process.Kill()
	<-holderDone
	killOnce.Do(func() {}) // mark the holder already reaped; defer is a no-op
	time.Sleep(900 * time.Millisecond)
	resultPath := filepath.Join(t.TempDir(), "result")
	reclaimer := exec.Command(os.Args[0], "-test.run", "TestLeaseChildHelper")
	reclaimer.Env = append(os.Environ(),
		"LEASE_HELPER=contend", "LEASE_CHAIN_DIR="+dir,
		"LEASE_RESULT="+resultPath, "LEASE_TTL_MS="+ttlMS)
	if out, err := reclaimer.CombinedOutput(); err != nil {
		t.Fatalf("reclaimer process failed: %v\n%s", err, out)
	}
	out, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal("reclaimer wrote no result")
	}
	if string(out) != "OK" {
		t.Fatalf("after hard kill and expiry, reclaimer outcome = %q, want OK", string(out))
	}
}

// withFakeClock replaces the lease clock and TTL for the duration of one
// test: the clock starts at a fixed instant and only moves when the test moves
// it, so lease expiry is deterministic without sleeping. A renewal gap in fake
// units maps to a real-time ticker far longer than the test, so holders do not
// silently renew while the clock is held.
func withFakeClock(t *testing.T) (now *atomic.Int64, advance func(time.Duration)) {
	t.Helper()
	base := time.Unix(1_700_000_000, 0)
	var clk atomic.Int64
	clk.Store(0)
	oldNow := leaseNow
	oldTTL := leaseTTL
	leaseNow = func() time.Time { return base.Add(time.Duration(clk.Load())) }
	leaseTTL = 1000 * time.Second // fake units; renewals do not fire in real time
	t.Cleanup(func() {
		leaseNow = oldNow
		leaseTTL = oldTTL
	})
	return &clk, func(d time.Duration) { clk.Add(int64(d)) }
}

// killLease simulates a holder process being killed: the background renewer is
// stopped, but the lease record is deliberately left on disk exactly as a
// killed process would leave it.
func killLease(tok *leaseToken) {
	close(tok.stop)
	<-tok.done
}

// leaseChain builds a one-ring chain in a fresh directory and returns the
// store and chain path.
func leaseChain(t *testing.T) (*Store, string) {
	t.Helper()
	s := openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one")},
		{Key: "gone", Value: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("two")},
		{Key: "gone", Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	return s, chain
}

func TestLeaseFileIsSelfDescribing(t *testing.T) {
	withFakeClock(t)
	dir := t.TempDir()
	tok, err := acquireLease(dir, leaseKindIncremental)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, leaseFileName))
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := parseLease(data)
	if !ok {
		t.Fatal("lease file does not parse back")
	}
	if rec.kind != leaseKindIncremental || rec.pid != uint64(os.Getpid()) ||
		rec.host == "" || rec.nonce == [8]byte{} || rec.expires <= rec.issued {
		t.Fatalf("lease record not self-describing: %+v", rec)
	}
	if !rec.sameHolder(tok.rec) {
		t.Fatal("parsed record differs from token identity")
	}
	tok.release()
	if _, err := os.Stat(filepath.Join(dir, leaseFileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lease file left after release: %v", err)
	}
}

func TestLeaseCorruptRecordIsDeadDebris(t *testing.T) {
	withFakeClock(t)
	dir := t.TempDir()
	path := filepath.Join(dir, leaseFileName)
	if err := os.WriteFile(path, []byte("not a lease"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok, err := acquireLease(dir, leaseKindFull)
	if err != nil {
		t.Fatalf("corrupt lease should be reclaimed, got %v", err)
	}
	tok.release()
}

func TestLeaseLiveHolderBlocksAllKinds(t *testing.T) {
	withFakeClock(t)
	dir := t.TempDir()

	for _, holderKind := range []byte{leaseKindFull, leaseKindIncremental, leaseKindMerge, leaseKindRestore} {
		tok, err := acquireLease(dir, holderKind)
		if err != nil {
			t.Fatalf("acquire kind %d: %v", holderKind, err)
		}
		for _, contenderKind := range []byte{leaseKindFull, leaseKindIncremental, leaseKindMerge, leaseKindRestore} {
			if _, err := acquireLease(dir, contenderKind); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("kind %d contended by kind %d: err=%v, want fs.ErrInvalid",
					holderKind, contenderKind, err)
			}
		}
		tok.release()
		// After release the next contender of any kind takes over immediately.
		next, err := acquireLease(dir, leaseKindIncremental)
		if err != nil {
			t.Fatalf("reacquire after release: %v", err)
		}
		next.release()
	}
}

func TestLeaseUnexpiredTakeoverRejected(t *testing.T) {
	now, advance := withFakeClock(t)
	_ = now
	s, chain := leaseChain(t)

	tok, err := acquireLease(chain, leaseKindIncremental)
	if err != nil {
		t.Fatal(err)
	}
	// Move most of the way to expiry, but not across it.
	advance(leaseTTL - time.Second)

	for _, fn := range []func() error{
		func() error { return s.BackupIncremental(chain) },
		func() error {
			_, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
			return err
		},
	} {
		if err := fn(); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("takeover before expiry: err=%v, want fs.ErrInvalid", err)
		}
	}
	// Exactly the original one ring is present; nothing was appended.
	if got := ringFiles(t, chain); len(got) != 1 {
		t.Fatalf("chain modified despite live lease: %v", got)
	}
	// The live holder itself can still finish normally.
	killLease(tok)
}

func TestLeaseExpiredKilledHolderIsReclaimed(t *testing.T) {
	_, advance := withFakeClock(t)
	s, chain := leaseChain(t)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots

	tok, err := acquireLease(chain, leaseKindIncremental)
	if err != nil {
		t.Fatal(err)
	}
	killLease(tok) // holder process killed mid-export; record left behind

	// Before expiry the chain is still locked.
	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("unexpired dead-looking lease: %v", err)
	}
	advance(leaseTTL + time.Second)

	// The next export recognizes the stale lease, reclaims it and appends.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 2 {
		t.Fatalf("rings after reclaim = %v, want 2", got)
	}
	// The lease was released at the end and no temp files linger.
	if _, err := os.Stat(filepath.Join(chain, leaseFileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("lease file left behind")
	}
	// The chain is complete and restores to the live state and count.
	rs, _ := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()
}

func TestLeaseExpiredMergeHolderReclaimed(t *testing.T) {
	_, advance := withFakeClock(t)
	s, chain := leaseChain(t)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots

	tok, err := acquireLease(chain, leaseKindMerge)
	if err != nil {
		t.Fatal(err)
	}
	killLease(tok)
	advance(leaseTTL + time.Second)

	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge after dead merge lease: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 0 {
		t.Fatalf("rings after merge = %v, want none", got)
	}
	rs, _ := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()
}

func TestLeaseKilledIncrementalDebrisSwept(t *testing.T) {
	_, advance := withFakeClock(t)
	s, chain := leaseChain(t)

	// A killed holder left its expired lease plus a half-built staging ring.
	tok, err := acquireLease(chain, leaseKindIncremental)
	if err != nil {
		t.Fatal(err)
	}
	killLease(tok)
	if err := os.MkdirAll(filepath.Join(chain, incrStageDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(chain, incrStageDir, ringName(2)),
		[]byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	advance(leaseTTL + time.Second)

	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("export over killed holder debris: %v", err)
	}
	if got := ringFiles(t, chain); len(got) != 2 {
		t.Fatalf("rings = %v, want 2", got)
	}
	if _, err := os.Stat(filepath.Join(chain, incrStageDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging debris left behind")
	}
}

func TestLeaseOpenReclaimsDeadButKeepsChain(t *testing.T) {
	_, advance := withFakeClock(t)
	s, chain := leaseChain(t)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots

	// Expired inside lease from a killed export.
	tok, err := acquireLease(chain, leaseKindIncremental)
	if err != nil {
		t.Fatal(err)
	}
	killLease(tok)
	advance(leaseTTL + time.Second)

	// A request that "opens the related directory" runs coordination recovery
	// (the same hook snapshot.Open invokes): it sweeps only the dead lease and
	// leaves the chain itself intact.
	recoverChainCoordination(chain)
	if _, err := os.Stat(filepath.Join(chain, leaseFileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("dead lease not reclaimed on open recovery")
	}
	art, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("chain damaged by recovery: %v", err)
	}
	if art.ringCount != 1 {
		t.Fatalf("ringCount = %d, want 1", art.ringCount)
	}
	rs, _ := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()
}

func TestLeaseMergeSwapRepairAfterKill(t *testing.T) {
	_, advance := withFakeClock(t)
	s, chain := leaseChain(t)
	want := allPairs(t, s)
	snaps := s.Stats().Snapshots
	parent := filepath.Dir(chain)

	// Simulate a merge killed between its two commit renames: the chain name
	// is gone, the whole old chain sits in a trash sibling, and its lease has
	// since expired.
	tok, err := acquireLease(chain, leaseKindMerge)
	if err != nil {
		t.Fatal(err)
	}
	trash := filepath.Join(parent, filepath.Base(chain)+".merge-old-dead")
	if err := os.Rename(chain, trash); err != nil {
		t.Fatal(err)
	}
	killLease(tok)
	advance(leaseTTL + time.Second)

	// Recovery must move the parked old chain back, never delete it.
	recoverChainCoordination(chain)
	if _, err := os.Stat(chain); err != nil {
		t.Fatalf("old chain not moved back into place: %v", err)
	}
	if _, err := os.Stat(trash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("trash sibling left after recovery moved the chain back")
	}
	if _, err := os.Stat(chainLeasePath(chain)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("dead merge lease left behind")
	}
	// The restored chain is the complete pre-merge chain, state and count
	// intact; the next merge then succeeds on it.
	rs, _ := restoreCheckpoint(t, chain, want, snaps)
	rs.Close()
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge after recovery: %v", err)
	}
	rs2, _ := restoreCheckpoint(t, chain, want, snaps)
	rs2.Close()
}

func TestLeaseLiveMergeProtectsSwapDebris(t *testing.T) {
	withFakeClock(t)
	s, chain := leaseChain(t)
	parent := filepath.Dir(chain)

	// A live merge with a staging sibling in progress.
	tok, err := acquireLease(chain, leaseKindMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer tok.release()
	stage := filepath.Join(parent, filepath.Base(chain)+".merge-new-work")
	if err := os.MkdirAll(stage, 0o700); err != nil {
		t.Fatal(err)
	}

	// Recovery by another open/operation must not touch the live merge's
	// staging directory or its lease.
	recoverChainCoordination(chain)
	if _, err := os.Stat(stage); err != nil {
		t.Fatal("live merge staging swept under a live lease")
	}
	if _, err := os.Stat(chainLeasePath(chain)); err != nil {
		t.Fatal("live merge lease removed")
	}
	// A contender cannot start another merge either.
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("merge under live lease: %v", err)
	}
}

func TestLeaseRenewalKeepsLongOperationLive(t *testing.T) {
	// Real clock, short TTL: the renewer must keep extending the lease while
	// the simulated long holder is alive.
	oldTTL := leaseTTL
	leaseTTL = 100 * time.Millisecond
	t.Cleanup(func() { leaseTTL = oldTTL })

	dir := t.TempDir()
	tok, err := acquireLease(dir, leaseKindIncremental)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * leaseTTL) // well past one TTL; the renewer keeps it alive
	if !tok.stillOurs() {
		t.Fatal("renewer let a live holder's lease expire")
	}
	if _, err := acquireLease(dir, leaseKindMerge); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("renewed lease failed to exclude: %v", err)
	}
	tok.release()
}

func TestLeaseClosedStoreReportsClosed(t *testing.T) {
	s, chain := leaseChain(t)
	// Hold a live lease with another holder; closed status still wins and no
	// third error type appears.
	tok, err := acquireLease(chain, leaseKindMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer tok.release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{
		func() error { return s.Backup(t.TempDir()) },
		func() error { return s.BackupIncremental(chain) },
		func() error { return s.MergeChain(chain, 1) },
	} {
		if err := fn(); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("closed-store op: err=%v, want fs.ErrClosed", err)
		}
	}
}

func TestLeaseConcurrentMutatorsAndRestores(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Foreground writes, snapshots, checkpoints and compactions keep running
	// while the chain is reshaped; none may block on chain coordination.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.CommitBatch([]BatchOp{
					{Key: fmt.Sprintf("w%d-%04d", w, i%32), Value: []byte(fmt.Sprintf("%d", i))},
				})
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if sn, err := s.Snapshot(); err == nil {
				sn.Close()
			}
			_ = s.Checkpoint()
			_ = s.Compact()
		}
	}()

	// Mutators on the one authoritative store serialize through backupMu; the
	// restores take the same directory lease independently, so they genuinely
	// contend with the mutators and either win or fail wholesale.
	var restores, busyOK int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Retry on lease contention, as a real backup client would: the goal is
		// to prove that a contender sometimes wins the directory outright and
		// completes a whole restore while the mutators keep advancing it.
		for i := 0; i < 60; i++ {
			target := filepath.Join(t.TempDir(), fmt.Sprintf("r%d", i))
			rs, err := Restore(chain, target)
			switch {
			case err == nil:
				if st := rs.Stats(); st.Keys < 0 {
					t.Errorf("bad restore stats")
				}
				rs.Close()
				atomic.AddInt64(&restores, 1)
			case errors.Is(err, errLeaseBusy):
				atomic.AddInt64(&busyOK, 1)
			default:
				t.Errorf("unexpected restore error: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// retryLeased reruns an advancing op when it loses the directory lease to
	// the concurrent restorer; any other (corruption) error is fatal.
	retryLeased := func(fn func() error) {
		t.Helper()
		for tries := 0; ; tries++ {
			err := fn()
			if err == nil {
				return
			}
			if errors.Is(err, errLeaseBusy) && tries < 200 {
				time.Sleep(time.Millisecond)
				continue
			}
			t.Fatalf("chain mutation failed after retries: %v", err)
		}
	}
	for i := 0; i < 20; i++ {
		if err := s.CommitBatch([]BatchOp{{Key: fmt.Sprintf("tick-%03d", i), Value: []byte("v")}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		retryLeased(func() error { return s.BackupIncremental(chain) })
		// The chain must be whole at every observed point.
		if _, err := validateBackupChain(chain); err != nil {
			t.Fatalf("chain broken at iteration %d: %v", i, err)
		}
		if i%3 == 2 {
			retryLeased(func() error { return s.MergeChain(chain, 1) })
			if _, err := validateBackupChain(chain); err != nil {
				t.Fatalf("chain broken after merge %d: %v", i, err)
			}
		}
	}
	close(stop)
	wg.Wait()

	retryLeased(func() error { return s.BackupIncremental(chain) })
	final, err := validateBackupChain(chain)
	if err != nil {
		t.Fatalf("final chain invalid: %v", err)
	}
	// No coordination file or temp leaks into the completed chain directory.
	entries, err := os.ReadDir(chain)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if isLeaseCoordinationName(e.Name()) && e.Name() != leaseFileName {
			t.Fatalf("lease temp leaked: %s", e.Name())
		}
	}
	if _, live, _, _ := leaseStateAtPath(filepath.Join(chain, leaseFileName)); live {
		t.Fatal("live lease left after all operations completed")
	}
	// Final terminal state and cumulative snapshot count match the store.
	rs, _ := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs.Close()
	if final.snaps != s.Stats().Snapshots {
		t.Fatalf("chain snaps %d != store snaps %d", final.snaps, s.Stats().Snapshots)
	}
	if atomic.LoadInt64(&restores) == 0 {
		t.Fatal("no concurrent restore ever won the lease")
	}
	if atomic.LoadInt64(&restores)+atomic.LoadInt64(&busyOK) != 60 {
		t.Fatal("not every concurrent restore was an outright success or lease rejection")
	}
}

func TestLeaseChainRestoresEqualFullReplay(t *testing.T) {
	// End-to-end wording of the requirement: after interleaved exports and
	// merges the restored key/value set, batch visibility and cumulative
	// snapshot count equal a full replay at the last ring watermark.
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := s.CommitBatch([]BatchOp{
			{Key: fmt.Sprintf("k%02d", i), Value: []byte(fmt.Sprintf("v%d", i))},
			{Key: "del", Value: []byte("temp")},
			{Key: "del", Delete: true}, // put-then-delete inside one batch
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MergeChain(chain, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 2); err != nil {
		t.Fatal(err)
	}
	rs, target := restoreCheckpoint(t, chain, allPairs(t, s), s.Stats().Snapshots)
	rs.Close()
	reopened, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok, _ := reopened.Get("del"); ok {
		t.Fatal("batched deletion resurrected after restore")
	}
	for i := 0; i < 6; i++ {
		if got, ok, _ := reopened.Get(fmt.Sprintf("k%02d", i)); !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key k%02d wrong after restore", i)
		}
	}
	if n := reopened.Stats().Snapshots; n != s.Stats().Snapshots {
		t.Fatalf("snapshot count %d want %d", n, s.Stats().Snapshots)
	}
}
