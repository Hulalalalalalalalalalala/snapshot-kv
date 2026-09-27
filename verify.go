package snapshot

import (
	"errors"
	"os"
	"path/filepath"
)

// Online chain verification and self-healing.
//
// VerifyChain inspects one backup chain directory segment by segment: it
// compares the chain head manifest against the ring order, checks the
// watermark continuity of every artifact, verifies every checksum and the
// format version, all streaming in bounded reads. A chain that is whole gets
// no change — VerifyChain returns nil and not one byte of the chain
// directory moves.
//
// A damaged incremental ring is isolated at ring granularity: the chain is
// downgraded to its longest intact prefix (the head plus the rings before
// the first defect) and VerifyChain still returns nil. The isolated rings
// are moved, byte for byte, into a diagnostic sibling directory next to the
// chain ("<chain>.diag-*"); the chain directory then holds only intact
// rings. The downgraded chain restores byte-for-byte to the state a full
// replay reaches at the last intact watermark — keys, batch atomicity and
// the cumulative snapshot count all agree.
//
// A damaged head cannot be downgraded past: with no intact prefix the whole
// directory is rejected (an error wrapping fs.ErrInvalid) and the chain
// directory is left exactly as it was. A missing or non-directory path, a
// directory with no usable head, a foreign file inside it or an unavailable
// lease reject the whole directory the same way, again without touching it.
//
// Like every chain operation, VerifyChain first takes the chain directory's
// existing lease — it is mutually exclusive with backup, incremental export,
// merge and restore, and the lease file stays outside the chain. It holds no
// store lock: writes, batch commits, snapshot open/close, cursor paging,
// checkpoints and compactions proceed while it runs, and reads keep hitting
// memory.
//
// The downgrade is one atomic directory swap assembled in a sibling staging
// directory ("<chain>.verify-new-*"): the old chain moves aside to a trash
// sibling ("<chain>.verify-old-*"), the intact prefix is renamed onto the
// chain path and only then is the old chain's isolated suffix preserved in a
// diagnostic directory and the trash removed. A kill at any instant
// therefore leaves either the chain as it was before verification or the
// fully downgraded chain — at every moment a complete chain. Staging or
// trash siblings left behind are swept at the next backup, merge,
// verification or open of a related directory, which is immediately usable.
const (
	verifyStagePrefix = ".verify-new-"
	verifyTrashPrefix = ".verify-old-"
	verifyDiagPrefix  = ".diag-"
)

// VerifyChain verifies the backup chain in chainDir and heals it in place.
// A whole chain is left byte-for-byte unchanged; a chain whose incremental
// suffix is damaged is downgraded to the longest intact prefix, with the
// isolated rings preserved byte-for-byte in a diagnostic sibling directory;
// both outcomes return nil. A damaged head, an unusable chain directory, a
// foreign file or an unavailable lease rejects the whole chain with an error
// wrapping fs.ErrInvalid and leaves the chain directory exactly as it was.
//
// Verification is streamed segment by segment and never reads the whole
// chain or the whole keyspace into memory; its peak memory does not grow with
// chain length. It runs under the chain lease and never blocks store
// activity or lock-free reads. VerifyChain on a closed store returns an error
// wrapping fs.ErrClosed.
func (s *Store) VerifyChain(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize with exports and merges driven by this store so they never
	// reshape one chain directory concurrently. Independent of mu: held across
	// the verification's disk I/O, never blocking commits or reads.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" {
		return errBadBackup
	}
	// Coordinate with every other operation on this chain directory, in this
	// process or another: only the lease holder inspects and reshapes the
	// chain. A live holder fails the whole verification; a killed holder's
	// lease is reclaimed.
	lease, err := acquireChainLease(chainDir)
	if err != nil {
		return err
	}
	defer lease.release()

	// Repair or clear debris from a merge or a verification killed mid-commit
	// before inspecting the chain, exactly as the next backup or open would.
	sweepChainDebris(chainDir)

	info, err := os.Stat(chainDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errBadBackup
		}
		return err
	}
	if !info.IsDir() {
		return errBadBackup
	}
	// Staging debris from an export (full or incremental) killed mid-run is
	// never part of a finished chain and is swept before inspection; a
	// manifest temp is pure debris as well. Any other foreign entry below
	// rejects the whole directory.
	if err := sweepBackupDebris(chainDir); err != nil {
		return err
	}

	// Inspect the layout first: the chain must hold only the manifest,
	// well-formed head segment files and well-formed ring files. Any foreign
	// file rejects the whole chain before a single byte of its content is
	// trusted. Only the highest ring index is retained; the streaming walk
	// below detects a missing ring (a gap) at ring granularity, so no set
	// proportional to chain length is held.
	layout, err := inspectChainLayout(chainDir)
	if err != nil {
		return err
	}

	// Deep-validate the head, streaming it. The head is the irreducible
	// prefix: a damaged head cannot be downgraded around, so it rejects the
	// whole chain and leaves it untouched.
	head, err := loadBackupManifest(chainDir)
	if err != nil {
		return err
	}
	if err := checkHeadLayout(chainDir, head); err != nil {
		return err
	}
	if err := streamHeadSegments(chainDir, head, nil); err != nil {
		return err
	}

	// Walk the rings in order, streaming and fully validating each one
	// against its predecessor. The first defect fixes the intact prefix:
	// every ring from that one on is isolated and the chain downgraded.
	good := uint32(0)
	prevWM := head.watermark
	prevSnaps := head.snaps
	prevLink := head.crc
	for idx := uint32(1); idx <= layout.ringCount; idx++ {
		f, oerr := os.Open(filepath.Join(chainDir, ringName(idx)))
		if oerr != nil {
			// A missing ring (a gap in the sequence) is a damaged ring: the
			// intact prefix ends at idx-1 and later rings are isolated too.
			// Any other open failure passes through unchanged.
			if errors.Is(oerr, os.ErrNotExist) {
				return s.downgradeChain(chainDir, good)
			}
			return oerr
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, nil)
		f.Close()
		if serr != nil || sum.snaps < prevSnaps {
			// Defect at ring idx (or a non-monotonic snapshot count): keep
			// rings 1..idx-1 as the intact prefix and isolate the rest.
			return s.downgradeChain(chainDir, good)
		}
		good = idx
		prevWM = sum.endWM
		prevSnaps = sum.snaps
		prevLink = sum.fileCRC
	}

	// The whole chain is intact; the directory was not modified.
	return nil
}

// chainLayout is the accepted ring inventory of a chain directory. Segment
// and manifest names are shape-accepted during the listing but not retained:
// the intact-chain path streams without holding every directory entry, and a
// downgrade re-lists the directory when it actually stages files.
type chainLayout struct {
	ringCount uint32 // highest ring index present (0 = head only)
}

// inspectChainLayout lists chainDir and accepts only the head manifest,
// well-formed segment file names and well-formed ring file names. Any
// directory, temp or foreign name rejects the whole chain. It records just
// the highest ring index: a missing ring (a gap) is detected by the streaming
// walk when it cannot be opened, so the listing holds no ring set.
func inspectChainLayout(chainDir string) (chainLayout, error) {
	var layout chainLayout
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return layout, err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			return layout, errBadBackup // no staging or foreign directory in a chain
		}
		switch {
		case name == backupManifest, name == backupManifestTmp:
			if name == backupManifestTmp {
				return layout, errBadBackup // unfinished manifest means unfinished work
			}
		default:
			if _, ok := parseSegmentIndex(name); ok {
				break
			}
			if idx, ok := parseRingIndex(name); ok {
				if idx > layout.ringCount {
					layout.ringCount = idx
				}
				break
			}
			return layout, errBadBackup // foreign file
		}
	}
	return layout, nil
}

// checkHeadLayout verifies the head segments on disk are exactly the dense
// set the manifest names — none missing, none extra, no duplicates.
func checkHeadLayout(chainDir string, head manifestInfo) error {
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return err
	}
	want := make(map[uint32]bool, len(head.segments))
	for _, seg := range head.segments {
		if seg.index != uint32(len(want)) {
			return errBadBackup // dense, ordered 0..n-1
		}
		want[seg.index] = true
	}
	got := make(map[uint32]bool, len(want))
	for _, e := range entries {
		name := e.Name()
		if name == backupManifest {
			continue
		}
		if _, ok := parseRingIndex(name); ok {
			continue
		}
		idx, ok := parseSegmentIndex(name)
		if !ok || !want[idx] || got[idx] {
			return errBadBackup // a foreign/extra segment or a duplicate
		}
		got[idx] = true
	}
	if len(got) != len(want) {
		return errBadBackup // a manifest-named segment is absent
	}
	return nil
}

// downgradeChain isolates the rings strictly after good of the chain in
// chainDir into a diagnostic sibling and leaves the intact prefix (the head
// plus rings 1..good) at chainDir. It returns nil on success; before the
// atomic commit any failure leaves the chain directory exactly as it was.
// The isolated rings are preserved byte-for-byte.
//
// The whole switch is staged as siblings and committed by directory renames,
// so a kill at any instant leaves either the original chain or the complete
// downgraded chain; sweepVerifyDebris finishes or rolls back a killed commit.
func (s *Store) downgradeChain(chainDir string, good uint32) error {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	// Re-list at commit time and stage exactly the intact prefix: the
	// manifest and every head segment, plus rings 1..good.
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return err
	}

	// Stage the intact prefix in a sibling directory. Hard links share the
	// artifact bytes, so the staged files are byte-identical to the originals.
	stage, err := os.MkdirTemp(parent, base+verifyStagePrefix)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case name == backupManifest:
			if err := linkInto(stage, chainDir, name); err != nil {
				return err
			}
		default:
			if _, ok := parseSegmentIndex(name); ok {
				if err := linkInto(stage, chainDir, name); err != nil {
					return err
				}
				continue
			}
			if idx, ok := parseRingIndex(name); ok && idx <= good {
				if err := linkInto(stage, chainDir, name); err != nil {
					return err
				}
			}
		}
	}
	if err := syncDirectory(stage); err != nil {
		return err
	}
	// The staged prefix must itself validate as one whole chain.
	staged, err := validateBackupChain(stage)
	if err != nil {
		return err
	}
	if staged.ringCount != good {
		return errBadBackup // the intact prefix did not stage as a dense chain
	}

	// Commit by moving the old chain aside to a trash sibling and the staged
	// prefix onto the chain path. A kill before the first rename leaves the
	// original chain; a kill between the renames leaves the whole old chain in
	// the trash sibling and is rolled back by the next sweep.
	trash, err := mkRemovedTemp(parent, base+verifyTrashPrefix)
	if err != nil {
		return err
	}
	if err := os.Rename(chainDir, trash); err != nil {
		return err
	}
	if err := os.Rename(stage, chainDir); err != nil {
		if rb := os.Rename(trash, chainDir); rb == nil {
			_ = syncDirectory(parent)
		}
		return err
	}
	committed = true
	if err := syncDirectory(parent); err != nil {
		return err
	}

	// The downgraded chain is live and durable. Preserving the isolated
	// suffix in a diagnostic directory and dropping the trash is best-effort:
	// a failure here leaves the isolated bytes safe in the trash sibling,
	// which the next backup, merge, verification or open finishes — it never
	// undoes the committed heal, so VerifyChain still reports success.
	_ = finishVerifyTrash(chainDir, trash)
	_ = syncDirectory(parent)
	return nil
}

// mkRemovedTemp creates an empty temporary directory with a name beginning
// with prefix in parent, removes its directory entry and returns the path,
// reserving a unique name the caller can rename onto.
func mkRemovedTemp(parent, prefix string) (string, error) {
	dir, err := os.MkdirTemp(parent, prefix)
	if err != nil {
		return "", err
	}
	if err := os.Remove(dir); err != nil {
		return "", err
	}
	return dir, nil
}

// linkInto hard-links name from srcDir into dstDir, sharing the source bytes.
func linkInto(dstDir, srcDir, name string) error {
	return os.Link(filepath.Join(srcDir, name), filepath.Join(dstDir, name))
}

// finishVerifyTrash completes a committed downgrade whose retired original
// chain still sits at trash: it preserves in a fresh diagnostic sibling the
// rings that are no longer part of the live chain at chainDir, then removes
// the trash. It never deletes the trash before every isolated ring is safely
// linked into a diagnostic directory, so preserved bytes are never lost. It
// is safe to run again against a trash left by a kill.
func finishVerifyTrash(chainDir, trash string) error {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	live := make(map[string]bool)
	if entries, err := os.ReadDir(chainDir); err == nil {
		for _, e := range entries {
			live[e.Name()] = true
		}
	}
	trashEntries, err := os.ReadDir(trash)
	if err != nil {
		return err
	}
	var isolated []string
	for _, e := range trashEntries {
		name := e.Name()
		if _, ok := parseRingIndex(name); ok && !live[name] {
			isolated = append(isolated, name)
		}
	}

	if len(isolated) > 0 {
		diag, err := os.MkdirTemp(parent, base+verifyDiagPrefix)
		if err != nil {
			return err
		}
		ok := false
		for _, name := range isolated {
			if err := os.Link(filepath.Join(trash, name), filepath.Join(diag, name)); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				_ = os.RemoveAll(diag)
				return err
			}
			ok = true
		}
		if !ok {
			_ = os.Remove(diag)
		} else if err := syncDirectory(diag); err != nil {
			_ = os.RemoveAll(diag)
			return err
		}
	}
	if err := os.RemoveAll(trash); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// sweepVerifyDebris clears the sibling directories a verification can leave
// next to chainDir when killed: a staging prefix ("<base>.verify-new-*") and
// a retired old chain ("<base>.verify-old-*"). If the chain path is missing
// but a retired chain exists, the verification was killed between its two
// commit renames and the whole original chain is moved back into place
// before anything is cleared. If the healed chain is already live, a leftover
// trash is finished (isolated rings preserved) and removed. Best-effort, like
// the other open-time sweeps. Diagnostic directories are kept, never swept.
func sweepVerifyDebris(chainDir string) {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	var staging, trash []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if isMergeTempName(name, base, verifyStagePrefix) {
			staging = append(staging, name)
		}
		if isMergeTempName(name, base, verifyTrashPrefix) {
			trash = append(trash, name)
		}
	}

	_, statErr := os.Stat(chainDir)
	chainMissing := errors.Is(statErr, os.ErrNotExist)

	if chainMissing && len(trash) > 0 {
		// Killed between the two commit renames: the original whole chain is
		// in the trash; restore it and discard the unused staged prefix.
		if err := os.Rename(filepath.Join(parent, trash[0]), chainDir); err == nil {
			trash = trash[1:]
			chainMissing = false
			_ = syncDirectory(parent)
		}
	}
	for _, name := range staging {
		_ = os.RemoveAll(filepath.Join(parent, name))
	}
	for _, name := range trash {
		if chainMissing {
			// Chain is still absent and this trash could not be restored: do
			// not destroy evidence unilaterally; leave it for a later pass.
			continue
		}
		// A healed (or restored) chain is live: finish preserving any isolated
		// suffix and remove the retired chain. After a rollback the live chain
		// is the whole original, so nothing is isolated and the trash is simply
		// removed.
		_ = finishVerifyTrash(chainDir, filepath.Join(parent, name))
	}
}

// sweepChainDebris clears sibling staging/trash debris a merge or a
// verification can leave next to chainDir, the repair/clear step every chain
// operation and every Open of a related directory performs first.
func sweepChainDebris(chainDir string) {
	sweepMergeDebris(chainDir)
	sweepVerifyDebris(chainDir)
}
