package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// restoreState restores art into a fresh target and returns its live pairs
// and cumulative snapshot count, then closes it.
func restoreState(t *testing.T, art string) ([]KVPair, uint64) {
	t.Helper()
	target := filepath.Join(t.TempDir(), "restored")
	rs, err := Restore(art, target)
	if err != nil {
		t.Fatalf("Restore(%s): %v", art, err)
	}
	pairs := allPairs(t, rs)
	snaps := rs.Stats().Snapshots
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}
	return pairs, snaps
}

// chainFingerprint hashes every regular file in dir by name.
func chainFingerprint(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	fp := make(map[string][32]byte)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fp[e.Name()] = sha256.Sum256(data)
	}
	return fp
}

func sameFingerprint(a, b map[string][32]byte) bool {
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

// findVerifyDiag returns the diagnostic sibling directory next to chainDir,
// or "" when none exists.
func findVerifyDiag(t *testing.T, chainDir string) string {
	t.Helper()
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	prefix := base + verifyDiagPrefix
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) > len(prefix) && e.Name()[:len(prefix)] == prefix {
			return filepath.Join(parent, e.Name())
		}
	}
	return ""
}

// verifyDiagDir returns the diagnostic sibling, failing if it is absent.
func verifyDiagDir(t *testing.T, chainDir string) string {
	t.Helper()
	if d := findVerifyDiag(t, chainDir); d != "" {
		return d
	}
	t.Fatal("no diagnostic directory next to chain")
	return ""
}

// buildVerifiedChain writes a head and n rings, one snapshot per stage, and
// returns a copy of every prefix (prefixArt[k] is head plus rings 1..k), the
// live pairs and snapshot count at every prefix, and the live store.
func buildVerifiedChain(t *testing.T, n int) (s *Store, chain string, prefixArt []string, prefixPairs [][]KVPair, prefixSnaps []uint64) {
	t.Helper()
	s = openStore(t, tempDir(t))
	if err := s.CommitBatch([]BatchOp{
		{Key: "alpha", Value: []byte("one")},
		{Key: "beta", Value: []byte("b0")},
		{Key: "gone", Value: []byte("x")},
		{Key: "empty", Value: nil},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	chain = filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	prefixArt = []string{copyChain(t, chain)}
	prefixPairs = [][]KVPair{allPairs(t, s)}
	prefixSnaps = []uint64{s.Stats().Snapshots}

	for r := 1; r <= n; r++ {
		ops := []BatchOp{
			{Key: "alpha", Value: []byte(fmt.Sprintf("a%d", r))},
			{Key: fmt.Sprintf("ring%02d", r), Value: []byte("v")},
		}
		switch r % 3 {
		case 0:
			ops = append(ops, BatchOp{Key: "gone", Delete: true})
		case 1:
			ops = append(ops, BatchOp{Key: "beta", Delete: true}, BatchOp{Key: "beta", Value: []byte(fmt.Sprintf("b%d", r))})
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatalf("ring %d: %v", r, err)
		}
		prefixArt = append(prefixArt, copyChain(t, chain))
		prefixPairs = append(prefixPairs, allPairs(t, s))
		prefixSnaps = append(prefixSnaps, s.Stats().Snapshots)
	}
	return s, chain, prefixArt, prefixPairs, prefixSnaps
}

// flipLastByte flips the final byte of a file.
func flipLastByte(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyIntactChainUnchanged(t *testing.T) {
	s, chain, _, _, _ := buildVerifiedChain(t, 4)
	defer s.Close()

	before := chainFingerprint(t, chain)
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify intact chain: %v", err)
	}
	if after := chainFingerprint(t, chain); !sameFingerprint(before, after) {
		t.Fatal("intact chain bytes changed during verification")
	}
	// No diagnostic or staging siblings are left behind.
	if d := findVerifyDiag(t, chain); d != "" {
		t.Fatalf("diagnostic directory created for an intact chain: %s", d)
	}

	// A head-only chain verifies the same way and stays byte-identical.
	headOnly := filepath.Join(t.TempDir(), "head")
	s2 := openStore(t, tempDir(t))
	if err := s2.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Backup(headOnly); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	before = chainFingerprint(t, headOnly)
	if err := s.VerifyChain(headOnly); err != nil {
		t.Fatalf("verify head-only chain: %v", err)
	}
	if after := chainFingerprint(t, headOnly); !sameFingerprint(before, after) {
		t.Fatal("intact head-only chain bytes changed during verification")
	}

	// Verifying twice is idempotent.
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("second verify: %v", err)
	}
}

func TestVerifyDegradesDamagedRings(t *testing.T) {
	// damage mutates ring k of a fresh chain copy; good is the intact prefix
	// length the degradation must reach.
	type damage struct {
		name  string
		k     uint32
		apply func(t *testing.T, dir string, k uint32)
	}
	mutations := []damage{
		{"first ring bad file crc", 1, func(t *testing.T, d string, k uint32) {
			flipLastByte(t, filepath.Join(d, ringName(k)))
		}},
		{"middle ring flipped entry", 2, func(t *testing.T, d string, k uint32) {
			p := filepath.Join(d, ringName(k))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			data[incrRingHeaderSize+incrSegHeaderSize+10] ^= 0xFF
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"last ring bad predecessor link", 4, func(t *testing.T, d string, k uint32) {
			p := filepath.Join(d, ringName(k))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			data[12] ^= 0xFF
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"middle ring missing", 3, func(t *testing.T, d string, k uint32) {
			if err := os.Remove(filepath.Join(d, ringName(k))); err != nil {
				t.Fatal(err)
			}
		}},
		{"middle ring truncated", 2, func(t *testing.T, d string, k uint32) {
			p := filepath.Join(d, ringName(k))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, data[:len(data)/2], 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"middle ring unknown version", 2, func(t *testing.T, d string, k uint32) {
			p := filepath.Join(d, ringName(k))
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint32(data[4:8], backupVersion+99)
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}

	s, chain, prefixArt, prefixPairs, prefixSnaps := buildVerifiedChain(t, 4)
	defer s.Close()

	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			d := copyChain(t, chain)
			m.apply(t, d, m.k)
			// The isolated products are preserved exactly as they are on disk
			// at isolation time — damaged bytes included.
			wantIsolated := make(map[string][32]byte)
			for idx := m.k; idx <= 4; idx++ {
				p := filepath.Join(d, ringName(idx))
				if _, err := os.Stat(p); err != nil {
					continue // the removed ring itself has no bytes to preserve
				}
				data, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				wantIsolated[ringName(idx)] = sha256.Sum256(data)
			}

			if err := s.VerifyChain(d); err != nil {
				t.Fatalf("verify degraded chain: %v", err)
			}

			good := m.k - 1
			// The chain holds exactly the dense intact prefix.
			got := ringFiles(t, d)
			if uint32(len(got)) != good {
				t.Fatalf("rings after degrade = %v, want %d", got, good)
			}
			for i := range got {
				if got[i] != ringName(uint32(i+1)) {
					t.Fatalf("ring %d named %q, want dense renumbering", i, got[i])
				}
			}
			// The degraded chain validates and restores like the saved prefix.
			if _, err := validateBackupChain(d); err != nil {
				t.Fatalf("degraded chain does not validate: %v", err)
			}
			pairs, snaps := restoreState(t, d)
			assertSamePairs(t, pairs, prefixPairs[good], "degraded chain restores to last good watermark")
			if snaps != prefixSnaps[good] {
				t.Fatalf("degraded chain snapshots = %d, want %d", snaps, prefixSnaps[good])
			}
			refPairs, refSnaps := restoreState(t, prefixArt[good])
			assertSamePairs(t, pairs, refPairs, "degraded restore matches a full replay of the prefix")
			if snaps != refSnaps {
				t.Fatalf("snapshot count %d != prefix full replay %d", snaps, refSnaps)
			}

			// Isolated rings survive byte-for-byte in the diagnostic sibling.
			diag := verifyDiagDir(t, d)
			diagEntries, err := os.ReadDir(diag)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]bool)
			for _, e := range diagEntries {
				data, err := os.ReadFile(filepath.Join(diag, e.Name()))
				if err != nil {
					t.Fatal(err)
				}
				want, ok := wantIsolated[e.Name()]
				if !ok {
					t.Fatalf("unexpected diagnostic file %q", e.Name())
				}
				if sha256.Sum256(data) != want {
					t.Fatalf("isolated ring %q bytes altered", e.Name())
				}
				seen[e.Name()] = true
			}
			for name := range wantIsolated {
				if !seen[name] {
					t.Fatalf("isolated ring %q missing from diagnostic directory", name)
				}
			}

			// A second verification is a no-op and keeps the diagnostic.
			if err := s.VerifyChain(d); err != nil {
				t.Fatalf("re-verify degraded chain: %v", err)
			}
			if _, err := os.Stat(diag); err != nil {
				t.Fatalf("diagnostic directory lost on re-verify: %v", err)
			}
			if names := ringFiles(t, d); uint32(len(names)) != good {
				t.Fatalf("re-verify changed the ring set: %v", names)
			}
		})
	}
}

func TestVerifyRejectsDamagedHead(t *testing.T) {
	s, chain, _, _, _ := buildVerifiedChain(t, 2)
	defer s.Close()

	t.Run("missing manifest", func(t *testing.T) {
		d := copyChain(t, chain)
		if err := os.Remove(filepath.Join(d, backupManifest)); err != nil {
			t.Fatal(err)
		}
		before := chainFingerprint(t, d)
		if err := s.VerifyChain(d); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("verify head without manifest: %v", err)
		}
		if after := chainFingerprint(t, d); !sameFingerprint(before, after) {
			t.Fatal("rejected verification modified the chain directory")
		}
	})
	t.Run("corrupt head segment", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, segmentName(0))
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[backupSegHeaderSize+10] ^= 0xFF
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		before := chainFingerprint(t, d)
		if err := s.VerifyChain(d); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("verify corrupt head: %v", err)
		}
		if after := chainFingerprint(t, d); !sameFingerprint(before, after) {
			t.Fatal("rejected verification modified the chain directory")
		}
	})
	t.Run("bad manifest crc", func(t *testing.T) {
		d := copyChain(t, chain)
		p := filepath.Join(d, backupManifest)
		flipLastByte(t, p)
		before := chainFingerprint(t, d)
		if err := s.VerifyChain(d); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("verify corrupt manifest: %v", err)
		}
		if after := chainFingerprint(t, d); !sameFingerprint(before, after) {
			t.Fatal("rejected verification modified the chain directory")
		}
		if findVerifyDiag(t, d) != "" {
			t.Fatal("diagnostic directory created for a head defect")
		}
	})
}

func TestVerifyPathValidation(t *testing.T) {
	s := openStore(t, tempDir(t))
	defer s.Close()
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}

	check := func(t *testing.T, dir string) {
		t.Helper()
		if err := s.VerifyChain(dir); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("VerifyChain(%q) = %v, want fs.ErrInvalid", dir, err)
		}
	}

	check(t, "")
	check(t, filepath.Join(t.TempDir(), "missing"))

	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	check(t, file)
	check(t, t.TempDir()) // existing empty directory: no usable head

	// A foreign file in an otherwise intact chain rejects wholesale.
	d := copyChain(t, chain)
	if err := os.WriteFile(filepath.Join(d, "notes.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	check(t, d)

	// A live lease holder rejects the verification and leaves the chain.
	lease, err := acquireChainLease(chain)
	if err != nil {
		t.Fatal(err)
	}
	before := ringFiles(t, chain)
	if err := s.VerifyChain(chain); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("verify under held lease: %v", err)
	}
	if after := ringFiles(t, chain); len(after) != len(before) {
		t.Fatal("verify under a held lease changed the chain")
	}
	lease.release()
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify after lease release: %v", err)
	}
}

func TestVerifyClosedStore(t *testing.T) {
	s := openStore(t, tempDir(t))
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(chain); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("verify on closed store: %v", err)
	}
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("closed store left a lease behind")
	}
}

func TestVerifyReclaimsStaleLease(t *testing.T) {
	s := openStore(t, tempDir(t))
	defer s.Close()
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(chain); err != nil {
		t.Fatal(err)
	}

	// A dead holder's lease with an unexpired TTL is reclaimed, not waited on.
	writeLeaseFile(t, chain, deadPID(t), 42, time.Now().Add(time.Hour))
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify over a dead holder's lease: %v", err)
	}
	if _, err := os.Stat(leasePathFor(chain)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reclaimed lease file left behind")
	}

	// An expired lease held by a live pid is reclaimed just the same.
	writeLeaseFile(t, chain, uint64(os.Getpid()), 43, time.Now().Add(-time.Hour))
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify over an expired lease: %v", err)
	}
}

func TestVerifyContinuesAfterDegrade(t *testing.T) {
	s, chain, _, prefixPairs, prefixSnaps := buildVerifiedChain(t, 3)
	defer s.Close()

	// Corrupt ring 2: the chain degrades to head + ring 1, while the source
	// store is already ahead of the surviving tip.
	d := copyChain(t, chain)
	flipLastByte(t, filepath.Join(d, ringName(2)))
	if err := s.VerifyChain(d); err != nil {
		t.Fatalf("degrade: %v", err)
	}
	if got := ringFiles(t, d); len(got) != 1 {
		t.Fatalf("rings after degrade = %v, want one", got)
	}
	// The degraded chain restores to exactly the saved head+ring1 prefix.
	degPairs, degSnaps := restoreState(t, d)
	assertSamePairs(t, degPairs, prefixPairs[1], "degraded chain matches the last good prefix")
	if degSnaps != prefixSnaps[1] {
		t.Fatalf("degraded snapshots %d != prefix %d", degSnaps, prefixSnaps[1])
	}

	// A further incremental export appends as ring 2 on the degraded chain,
	// numbered continuously, and the chain restores to the store's current
	// state: the gap is bridged, deletes stay deleted.
	if err := s.Put("fresh", []byte("f")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(d); err != nil {
		t.Fatalf("append after degrade: %v", err)
	}
	if got := ringFiles(t, d); len(got) != 2 || got[1] != ringName(2) {
		t.Fatalf("rings after append = %v, want dense ring 2", got)
	}
	pairs, snaps := restoreState(t, d)
	assertSamePairs(t, pairs, allPairs(t, s), "chain continued past a degradation")
	if snaps != s.Stats().Snapshots {
		t.Fatalf("snapshot count %d != store %d", snaps, s.Stats().Snapshots)
	}
	// "gone" was deleted in ring 3 of the original chain; its delete must not
	// resurrect across the degradation and bridging export.
	for _, k := range []string{"gone"} {
		if _, ok, _ := s.Get(k); ok {
			t.Fatalf("test setup: %q present in source", k)
		}
		has := false
		for _, p := range pairs {
			if p.Key == k {
				has = true
			}
		}
		if has {
			t.Fatalf("deleted key %q resurrected after degrade + export", k)
		}
	}

	// Merging the surviving rings works and keeps the same terminal state.
	if err := s.MergeChain(d, 2); err != nil {
		t.Fatalf("merge after degrade: %v", err)
	}
	merged, mergedSnaps := restoreState(t, d)
	assertSamePairs(t, merged, pairs, "merged degraded chain terminal state")
	if mergedSnaps != snaps {
		t.Fatalf("merged snapshots %d != %d", mergedSnaps, snaps)
	}

	// Exports continue onto the merged chain with continuous numbering.
	if err := s.Put("after-merge", []byte("m")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.BackupIncremental(d); err != nil {
		t.Fatalf("append after merge: %v", err)
	}
	again, againSnaps := restoreState(t, d)
	assertSamePairs(t, again, allPairs(t, s), "chain continued past a merge")
	if againSnaps != s.Stats().Snapshots {
		t.Fatalf("snapshot count %d != store %d", againSnaps, s.Stats().Snapshots)
	}
}

func TestVerifyDebrisSwept(t *testing.T) {
	s, chain, _, _, _ := buildVerifiedChain(t, 2)
	defer s.Close()

	// Kill between the two commit renames: the chain path is gone and the
	// whole old chain sits in a verify-trash sibling. Verification moves it
	// back whole and, finding it intact, leaves it in place.
	d1 := copyChain(t, chain)
	d1Parent := filepath.Dir(d1)
	d1Base := filepath.Base(d1)
	trash, err := os.MkdirTemp(d1Parent, d1Base+verifyTrashPrefix)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(d1)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, rerr := os.ReadFile(filepath.Join(d1, e.Name()))
		if rerr != nil {
			t.Fatal(rerr)
		}
		if werr := os.WriteFile(filepath.Join(trash, e.Name()), data, 0o600); werr != nil {
			t.Fatal(werr)
		}
	}
	if err := os.RemoveAll(d1); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(d1); err != nil {
		t.Fatalf("verify after kill-between-renames: %v", err)
	}
	if got := ringFiles(t, d1); len(got) != 2 {
		t.Fatalf("old chain not restored whole: %v", got)
	}
	if _, err := validateBackupChain(d1); err != nil {
		t.Fatalf("restored chain invalid: %v", err)
	}

	// A leftover staging sibling is swept; a finished diagnostic directory is
	// evidence and survives both verification and an Open of the directory.
	d2 := copyChain(t, chain)
	stage, err := os.MkdirTemp(filepath.Dir(d2), filepath.Base(d2)+verifyStagePrefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "partial"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	diag, err := os.MkdirTemp(filepath.Dir(d2), filepath.Base(d2)+verifyDiagPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diag, ringName(9)), []byte("evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(d2); err != nil {
		t.Fatalf("verify over debris: %v", err)
	}
	if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("verify staging debris not swept")
	}
	evidence := filepath.Join(diag, ringName(9))
	if data, err := os.ReadFile(evidence); err != nil || string(data) != "evidence" {
		t.Fatalf("diagnostic evidence lost: data=%q err=%v", data, err)
	}
	// Opening a related directory runs the same sweep: staging debris goes,
	// the diagnostic evidence stays.
	stage2, err := os.MkdirTemp(filepath.Dir(d2), filepath.Base(d2)+verifyStagePrefix)
	if err != nil {
		t.Fatal(err)
	}
	sweepVerifyDebris(d2)
	if _, err := os.Stat(stage2); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("open-time sweep left verify staging debris")
	}
	if _, err := os.Stat(evidence); err != nil {
		t.Fatalf("diagnostic evidence swept as debris: %v", err)
	}
}

func TestVerifyLongChainStreamed(t *testing.T) {
	s := openStore(t, tempDir(t))
	defer s.Close()
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	const rings = 40
	for r := 0; r < rings; r++ {
		ops := []BatchOp{
			{Key: fmt.Sprintf("rot-%03d", r%7), Value: []byte(fmt.Sprintf("r%d", r))},
			{Key: fmt.Sprintf("uniq-%03d", r), Value: []byte("u")},
		}
		if r%4 == 0 {
			ops = append(ops, BatchOp{Key: fmt.Sprintf("uniq-%03d", r), Delete: true})
		}
		if err := s.CommitBatch(ops); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.VerifyChain(chain); err != nil {
		t.Fatalf("verify long chain: %v", err)
	}
	pairs, _ := restoreState(t, chain)
	assertSamePairs(t, pairs, allPairs(t, s), "long chain after verify")

	// Degrading a long chain at a late ring keeps the whole long prefix.
	d := copyChain(t, chain)
	flipLastByte(t, filepath.Join(d, ringName(rings-2)))
	if err := s.VerifyChain(d); err != nil {
		t.Fatalf("degrade long chain: %v", err)
	}
	if got := ringFiles(t, d); uint32(len(got)) != rings-3 {
		t.Fatalf("rings = %d, want %d", len(got), rings-3)
	}
	diagNames, err := os.ReadDir(verifyDiagDir(t, d))
	if err != nil {
		t.Fatal(err)
	}
	if len(diagNames) != 3 {
		t.Fatalf("isolated rings = %d, want 3", len(diagNames))
	}
	names := make([]string, 0, len(diagNames))
	for _, e := range diagNames {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if names[0] != ringName(rings-2) {
		t.Fatalf("first isolated ring = %q", names[0])
	}
}

func TestVerifyReadsStayLockFree(t *testing.T) {
	s := openStore(t, tempDir(t))
	defer s.Close()
	for i := 0; i < 200; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	chain := filepath.Join(t.TempDir(), "chain")
	if err := s.Backup(chain); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := s.Put(fmt.Sprintf("k%04d", i), []byte("updated")); err != nil {
			t.Fatal(err)
		}
		if err := s.BackupIncremental(chain); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// A verifier repeatedly streaming the chain.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.VerifyChain(chain); err != nil {
				return
			}
		}
	}()
	// Foreground writes and lock-free reads keep making progress.
	deadline := time.Now().Add(2 * time.Second)
	var reads int
	for time.Now().Before(deadline) {
		if err := s.Put("live", []byte("now")); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := s.Get("k0001"); err != nil || !ok {
			t.Fatalf("read during verify: ok=%v err=%v", ok, err)
		}
		if _, err := s.Snapshot(); err != nil {
			t.Fatal(err)
		}
		reads++
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("no reads completed during verification")
	}
}
