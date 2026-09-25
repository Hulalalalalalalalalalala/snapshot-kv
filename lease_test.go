package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// leasePathFor returns the sibling lease file coordinating operations on
// chainDir.
func leasePathFor(chainDir string) string {
	return filepath.Join(filepath.Dir(chainDir), filepath.Base(chainDir)+chainLeaseSuffix)
}

// writeLeaseFile installs a lease file for chainDir with an explicit holder
// and expiry, simulating a holder that can no longer release it itself.
func writeLeaseFile(t *testing.T, chainDir string, pid, token uint64, expiry time.Time) {
	t.Helper()
	buf := make([]byte, leaseSize+crcSize)
	binary.LittleEndian.PutUint32(buf[0:4], leaseMagic)
	binary.LittleEndian.PutUint32(buf[4:8], leaseVersion)
	binary.LittleEndian.PutUint64(buf[8:16], pid)
	binary.LittleEndian.PutUint64(buf[16:24], token)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(expiry.UnixNano()))
	binary.LittleEndian.PutUint32(buf[32:36], crc32.ChecksumIEEE(buf[:leaseSize]))
	if err := os.WriteFile(leasePathFor(chainDir), buf, 0o600); err != nil {
		t.Fatal(err)
	}
}

// deadPID returns a process id that is certainly not alive: a child process
// that has already exited and been reaped.
func deadPID(t *testing.T) uint64 {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return uint64(cmd.Process.Pid)
}

// TestChainLeaseMutualExclusion drives every chain operation while another
// holder owns the lease: each must fail wholesale with fs.ErrInvalid and
// leave the chain untouched, then succeed once the lease is released.
func TestChainLeaseMutualExclusion(t *testing.T) {
	s := openStore(t, tempDir(t))
	for i := 0; i < 8; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k9", []byte("v9")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k10", []byte("v10")); err != nil {
		t.Fatal(err)
	}

	// A live holder owns the chain: every operation on it is rejected as
	// invalid and changes nothing.
	lease, err := acquireChainLease(chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("incremental under held lease: %v", err)
	}
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("merge under held lease: %v", err)
	}
	if err := s.Backup(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("backup under held lease: %v", err)
	}
	if _, err := Restore(chain, filepath.Join(t.TempDir(), "r0")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("restore under held lease: %v", err)
	}
	// The rejected operations left the chain exactly as it was: one ring,
	// and the chain still validates and restores.
	if got := ringFiles(t, chain); len(got) != 1 {
		t.Fatalf("rings after rejected ops = %v, want one", got)
	}
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("chain after rejected ops: %v", err)
	}
	lease.release()

	// With the lease released every operation proceeds again.
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental after release: %v", err)
	}
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge after release: %v", err)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r1"))
	if err != nil {
		t.Fatalf("restore after release: %v", err)
	}
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k9", "k10"} {
		want := "v" + k[1:]
		if got, ok, err := rs.Get(k); err != nil || !ok || string(got) != want {
			t.Fatalf("restored %s = %q,%v,%v want %q", k, got, ok, err, want)
		}
	}
	if _, ok, _ := rs.Get("k8"); ok {
		t.Fatal("k8 was never written and must not restore")
	}
	rs.Close()
}

// TestChainLeaseReclaimedAfterKill leaves stale leases behind — a dead
// holder's, an expired holder's — and checks the next operation on the chain
// reclaims them and proceeds, with no manual cleanup.
func TestChainLeaseReclaimedAfterKill(t *testing.T) {
	s := openStore(t, tempDir(t))
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}

	// A lease whose holder process is gone is reclaimed even though its
	// expiry still lies in the future.
	writeLeaseFile(t, chain, deadPID(t), 7, time.Now().Add(time.Hour))
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental over dead holder's lease: %v", err)
	}
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease file left behind after run: %v", err)
	}

	// A lease whose expiry has passed is reclaimed even though its holder
	// pid is alive (this very process).
	writeLeaseFile(t, chain, uint64(os.Getpid()), 8, time.Now().Add(-time.Hour))
	if err := s.Put("c", []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatalf("incremental over expired lease: %v", err)
	}

	// A merge and a restore reclaim just the same.
	writeLeaseFile(t, chain, deadPID(t), 9, time.Now().Add(time.Hour))
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatalf("merge over dead holder's lease: %v", err)
	}
	writeLeaseFile(t, chain, deadPID(t), 10, time.Now().Add(time.Hour))
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatalf("restore over dead holder's lease: %v", err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got, ok, _ := rs.Get(k); !ok || string(got) != want {
			t.Fatalf("restored %s = %q,%v want %q", k, got, ok, want)
		}
	}
	rs.Close()
}

// TestChainLeaseReclaimedOnOpen checks that opening a directory related to a
// killed chain operation retires the dead holder's lease.
func TestChainLeaseReclaimedOnOpen(t *testing.T) {
	dir := tempDir(t)
	s := openStore(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	writeLeaseFile(t, dir, deadPID(t), 11, time.Now().Add(time.Hour))
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leasePathFor(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale lease not reclaimed on open: %v", err)
	}
	if got, ok, _ := s2.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("reopened store lost data: %q,%v", got, ok)
	}
	s2.Close()
}

// TestChainLeaseConcurrentStores hammers one chain from two stores at once:
// failures are always fs.ErrInvalid, the chain stays whole throughout, and
// the surviving chain restores to a consistent state.
func TestChainLeaseConcurrentStores(t *testing.T) {
	s1 := openStore(t, tempDir(t))
	s2 := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s1.Put("seed", []byte("0")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Backup(chain); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for w, s := range []*Store{s1, s2} {
		wg.Add(1)
		go func(w int, s *Store) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				if err := s.Put(fmt.Sprintf("w%d-%03d", w, i), []byte("x")); err != nil {
					errs <- fmt.Errorf("put: %w", err)
					return
				}
				err := s.BackupIncremental(chain)
				if err == nil {
					continue
				}
				if !errors.Is(err, fs.ErrInvalid) {
					errs <- fmt.Errorf("incremental: %w", err)
					return
				}
			}
		}(w, s)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// However the exports interleaved, the chain is one whole, valid chain
	// and restores to the terminal state its rings record.
	if _, err := validateBackupChain(chain); err != nil {
		t.Fatalf("chain after concurrent exports: %v", err)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatal(err)
	}
	pairs := allPairs(t, rs)
	for i := 1; i < len(pairs); i++ {
		if pairs[i].Key <= pairs[i-1].Key {
			t.Fatal("restored pairs not strictly ordered")
		}
	}
	rs.Close()
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease file left behind: %v", err)
	}
}

// TestChainLeaseNotLeftBehind checks that completed operations never leave
// their lease file next to the chain.
func TestChainLeaseNotLeftBehind(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.MergeChain(chain, 1); err != nil {
		t.Fatal(err)
	}
	rs, err := Restore(chain, filepath.Join(t.TempDir(), "r"))
	if err != nil {
		t.Fatal(err)
	}
	rs.Close()
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease file left behind: %v", err)
	}
}

// TestChainLeaseClosedStore confirms closed-store errors stay distinguishable
// from coordination failures: fs.ErrClosed, never fs.ErrInvalid-only.
func TestChainLeaseClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if err := s.Backup(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("backup on closed store: %v", err)
	}
	if err := s.BackupIncremental(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("incremental on closed store: %v", err)
	}
	if err := s.MergeChain(chain, 1); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("merge on closed store: %v", err)
	}
	// A closed store must not grab the chain's lease on its way out.
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed store left a lease: %v", err)
	}
}
