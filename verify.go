package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Online backup-chain verification and self-healing.
//
// VerifyBackupChain reads the chain in chain order as a stream — the head
// manifest, the head segments one at a time, then the dense rings one at a
// time — and checks every link in the chain: the head artifact itself, the
// dense 1-based ring order, each ring's predecessor content-CRC link, the
// head/ring watermark continuity, the per-segment and whole-file checksums,
// and the format version every artifact carries. Files are streamed exactly
// as Restore streams them: one bounded segment at a time, never the whole
// chain or the whole keyspace at once, and no entry content is held after its
// segment passes.
//
// A healthy chain verifies and is left untouched. When a ring (or a suffix of
// rings) is damaged — missing, truncated, checksum-bad, of a bad version,
// unlinked, with a discontinuous watermark or with a regressing snapshot
// count — the chain is degraded to its longest intact prefix:
//
//   - the damaged suffix is isolated at ring granularity and moved, exactly
//     as it is, into a "<chain>.quarantine-*" sibling directory kept for
//     diagnosis, together with a self-describing marker naming every isolated
//     file;
//   - the chain directory then holds the head plus exactly rings 1..n, dense,
//     linked and watermark-continuous, so it passes the same whole-chain
//     validation Restore performs;
//   - isolated rings take no further part in restores, merges or incremental
//     exports: the next BackupIncremental appends ring n+1 to the degraded
//     tip, MergeChain merges the degraded ring order, and a restore of the
//     degraded chain reproduces the state at the last intact watermark.
//
// A damaged head has no intact prefix in front of it (the head is the first
// artifact of the chain), so such a chain is rejected wholesale with
// fs.ErrInvalid, exactly as before; nothing is moved.
//
// The degradation is all-or-nothing. The replacement chain is assembled as a
// sibling directory of hard links ("<chain>.verify-new-*") and validated as a
// whole; the commit renames the chain aside ("<chain>.verify-old-*") and the
// replacement onto the chain path, exactly the two-rename swap MergeChain
// uses, so a kill at any instant leaves either the old chain or the complete
// new one, never a half chain at the chain path. The quarantine is extracted
// from the retired copy only after the replacement is installed; a kill
// before or during the extraction leaves the replacement chain whole and
// usable, and the next operation (or open of a related directory) finishes or
// undoes the swap. The chain directory's lease is held throughout, so a
// verification never runs concurrently with an export, merge or restore on
// the same directory, and store writes, batch commits, snapshots, cursor
// paging, checkpoints, compactions and lock-free reads proceed while it runs.
const (
	verifyStagePrefix    = ".verify-new-"
	verifyTrashPrefix    = ".verify-old-"
	quarantinePrefix     = ".quarantine-"
	quarantineMarkerName = "QUARANTINE.txt"
)

// chainPrefix is the longest intact head-plus-rings prefix a verification
// found, plus the ring files that must be isolated for the chain to match it.
type chainPrefix struct {
	art      artifactInfo // validated prefix (head plus rings 1..rings)
	rings    uint32       // number of intact dense rings
	isolated []uint32     // ring indices present beyond the intact prefix
}

// VerifyBackupChain streams the whole backup chain in chainDir, verifying the
// head artifact and every ring (ring order, predecessor links, watermark
// continuity, checksums and format version). A healthy chain is returned
// untouched with nil. When a ring suffix is damaged the chain is atomically
// degraded to its longest intact prefix and the damaged artifacts are moved,
// unchanged, into a sibling quarantine directory for diagnosis; afterwards
// the chain validates whole, restores to the last intact watermark, merges and
// accepts further incremental rings exactly as before.
//
// Verification only ever reads chain artifacts and swaps chain directories;
// it takes no store lock, so writes, batch commits, snapshot open/close,
// cursor paging, checkpoints, compactions, incremental exports and lock-free
// reads proceed while it runs. It takes the chain directory's lease before
// touching anything: a request that cannot take it fails wholesale with an
// error wrapping fs.ErrInvalid. A chain directory that is missing, not a
// directory, without a usable head, or damaged at the head, is rejected with
// an error wrapping fs.ErrInvalid and is left in place. VerifyBackupChain on
// a closed store returns an error wrapping fs.ErrClosed.
func (s *Store) VerifyBackupChain(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Reshape of the chain directory: serialize with this store's exports and
	// merges, never with commits or lock-free reads.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" {
		return errBadBackup
	}
	lease, err := acquireChainLease(chainDir)
	if err != nil {
		return err
	}
	defer lease.release()

	// Finish or undo any degradation a kill interrupted, and clear merge and
	// export debris, before inspecting the chain.
	sweepMergeDebris(chainDir)
	sweepVerifyDebris(chainDir, false)
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
	for _, stage := range []string{incrStageDir, backupStageDir} {
		if err := os.RemoveAll(filepath.Join(chainDir, stage)); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(chainDir, backupManifestTmp)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}

	// A whole, healthy chain needs no work; a damaged ring suffix is isolated
	// and the chain degraded to its intact prefix in one atomic swap.
	_, err = validateOrHealChain(chainDir)
	return err
}

// validateOrHealChain returns the chain's validated content, degrading it
// first when a damaged ring suffix is all that stands in the way of a whole
// chain: the suffix is isolated and the chain is atomically replaced by its
// longest intact prefix, which is then validated and returned. Damage at the
// head, a foreign entry or a missing path rejects wholesale with an error
// wrapping fs.ErrInvalid and leaves the directory in place. The caller must
// already hold the chain lease and have swept export/merge debris.
func validateOrHealChain(chainDir string) (artifactInfo, error) {
	if art, err := validateBackupChain(chainDir); err == nil {
		return art, nil
	}
	prefix, ierr := inspectIntactPrefix(chainDir)
	if ierr != nil || len(prefix.isolated) == 0 {
		if ierr != nil {
			return artifactInfo{}, ierr
		}
		return artifactInfo{}, errBadBackup
	}
	if err := installDegradedChain(chainDir, prefix); err != nil {
		return artifactInfo{}, err
	}
	art, err := validateBackupChain(chainDir)
	if err != nil {
		// A degraded chain must validate whole; anything else is a wholesale
		// failure, never a half-installed chain.
		return artifactInfo{}, errBadBackup
	}
	return art, nil
}

// inspectIntactPrefix streams the head artifact and then the dense rings in
// chain order until the first defect, returning the validated intact prefix
// and every ring file index sitting beyond it (a missing ring's later files
// included). It holds no entry content: each artifact is streamed and
// discarded in turn. A defect in the head itself is reported as an error,
// since no chain prefix exists without the head.
func inspectIntactPrefix(chainDir string) (chainPrefix, error) {
	var zero chainPrefix

	head, err := loadBackupManifest(chainDir)
	if err != nil {
		return zero, err
	}
	if err := streamHeadSegments(chainDir, head, nil); err != nil {
		return zero, err
	}

	present, maxIndex, err := chainRingFiles(chainDir, head)
	if err != nil {
		// A foreign entry, an unfinished manifest or a head segment the
		// manifest names but the directory lacks: the defect is not an
		// isolatable ring suffix, so the whole directory is rejected.
		return zero, err
	}

	art := artifactInfo{head: head, tipCRC: head.crc, endWM: head.watermark, snaps: head.snaps}
	prevWM := head.watermark
	prevSnaps := head.snaps
	prevLink := head.crc
	var idx uint32
	for idx = 1; idx <= maxIndex; idx++ {
		if !present[idx] {
			break // gap in the dense sequence: the prefix ends before it
		}
		f, oerr := os.Open(filepath.Join(chainDir, ringName(idx)))
		if oerr != nil {
			break
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, nil)
		f.Close()
		if serr != nil || sum.snaps < prevSnaps {
			break
		}
		art.ringSums = append(art.ringSums, sum)
		prevWM = sum.endWM
		prevSnaps = sum.snaps
		prevLink = sum.fileCRC
		art.tipCRC = sum.fileCRC
		art.endWM = sum.endWM
		art.snaps = sum.snaps
	}
	art.ringCount = uint32(len(art.ringSums))

	var isolated []uint32
	for n := range present {
		if n > art.ringCount {
			isolated = append(isolated, n)
		}
	}
	sort.Slice(isolated, func(i, j int) bool { return isolated[i] < isolated[j] })
	return chainPrefix{art: art, rings: art.ringCount, isolated: isolated}, nil
}

// chainRingFiles lists the finished ring file indices directly in chainDir
// and enforces the artifact file set: the only entries allowed are the
// manifest, exactly the dense segment set the head manifest names, and ring
// files. A foreign file, a directory, an unfinished manifest temp or a
// manifest-named segment that is absent is reported as errBadBackup, since
// none of those is an isolatable ring suffix. It returns the ring set and its
// highest index (zero when the chain has no rings).
func chainRingFiles(chainDir string, head manifestInfo) (map[uint32]bool, uint32, error) {
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return nil, 0, err
	}
	wantSeg := make(map[uint32]bool, len(head.segments))
	for _, s := range head.segments {
		wantSeg[s.index] = true
	}
	present := make(map[uint32]bool)
	var max uint32
	sawManifest := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			return nil, 0, errBadBackup
		}
		switch {
		case name == backupManifest:
			sawManifest = true
		case name == backupManifestTmp:
			return nil, 0, errBadBackup
		default:
			if idx, ok := parseSegmentIndex(name); ok {
				if !wantSeg[idx] {
					return nil, 0, errBadBackup // extra segment
				}
				wantSeg[idx] = false // mark present
				continue
			}
			if idx, ok := parseRingIndex(name); ok {
				present[idx] = true
				if idx > max {
					max = idx
				}
				continue
			}
			return nil, 0, errBadBackup // foreign file
		}
	}
	if !sawManifest {
		return nil, 0, errBadBackup
	}
	for missing := range wantSeg {
		if wantSeg[missing] {
			return nil, 0, errBadBackup // a manifest-named segment is absent
		}
	}
	return present, max, nil
}

// installDegradedChain atomically replaces chainDir with the validated intact
// prefix and moves the damaged suffix into a sibling quarantine directory,
// byte for byte unchanged. The new chain is assembled with hard links and
// validated whole before the two-rename commit; neither the store nor any
// artifact content is rewritten.
func installDegradedChain(chainDir string, prefix chainPrefix) error {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	// Assemble the replacement as a sibling so every link and the final swap
	// stays on one filesystem.
	stage, err := os.MkdirTemp(parent, base+verifyStagePrefix)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			os.RemoveAll(stage)
		}
	}()

	// Head manifest and exactly the segments it names.
	if err := os.Link(filepath.Join(chainDir, backupManifest),
		filepath.Join(stage, backupManifest)); err != nil {
		return err
	}
	for _, seg := range prefix.art.head.segments {
		name := segmentName(seg.index)
		if err := os.Link(filepath.Join(chainDir, name), filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	// The intact dense rings keep their names, links, watermarks and CRCs:
	// the prefix already validated as a chain on its own.
	for idx := uint32(1); idx <= prefix.rings; idx++ {
		name := ringName(idx)
		if err := os.Link(filepath.Join(chainDir, name), filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	// The replacement must pass the same whole-chain validation Restore runs.
	if newArt, err := validateBackupChain(stage); err != nil {
		return err
	} else if newArt.ringCount != prefix.rings ||
		newArt.tipCRC != prefix.art.tipCRC ||
		newArt.endWM != prefix.art.endWM ||
		newArt.snaps != prefix.art.snaps {
		return errBadBackup
	}

	// Commit with the merge's two-rename swap: old chain aside, replacement
	// onto the chain path. A kill between the renames is repaired by moving
	// the whole old chain back; a kill afterwards leaves the complete new
	// chain and only the suffix-extraction left to finish.
	trash, err := os.MkdirTemp(parent, base+verifyTrashPrefix)
	if err != nil {
		return err
	}
	if err := os.Remove(trash); err != nil {
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

	// Extract the damaged suffix, unchanged, into its own quarantine area.
	// Failure here leaves the replacement chain whole and usable; the retired
	// copy stays a sibling the next sweep quarantines again.
	if err := quarantineRings(parent, base, trash, prefix.isolated, prefix.art); err != nil {
		return err
	}
	if err := os.RemoveAll(trash); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// quarantineRings moves the named ring files (by their index) out of the
// retired chain copy trash into a fresh "<base>.quarantine-*" sibling and
// drops a self-describing marker beside them. The ring bytes are never
// rewritten.
func quarantineRings(parent, base, trash string, indices []uint32, prefix artifactInfo) error {
	qdir, err := os.MkdirTemp(parent, base+quarantinePrefix)
	if err != nil {
		return err
	}
	var moved []string
	for _, idx := range indices {
		name := ringName(idx)
		if err := os.Rename(filepath.Join(trash, name), filepath.Join(qdir, name)); err != nil {
			// A missing isolated file (e.g. a gap ring) simply has nothing to
			// preserve; anything else fails the extraction for a retry later.
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		moved = append(moved, name)
	}
	var body []byte
	body = append(body,
		"backup chain quarantine\n"[:]...)
	body = append(body, fmt.Sprintf("chain: %s\n", base)...)
	body = append(body, fmt.Sprintf("intact rings: %d\n", prefix.ringCount)...)
	body = append(body, fmt.Sprintf("last intact watermark: %d\n", prefix.endWM)...)
	body = append(body, fmt.Sprintf("snapshot count: %d\n", prefix.snaps)...)
	body = append(body, "reason: ring failed whole-chain verification (missing, truncated, checksum, link, watermark or version)\n"...)
	for _, name := range moved {
		body = append(body, "isolated: "+name+"\n"...)
	}
	if err := os.WriteFile(filepath.Join(qdir, quarantineMarkerName), body, 0o600); err != nil {
		return err
	}
	if err := syncDirectory(qdir); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// sweepVerifyDebris completes or undoes a degradation a kill interrupted, the
// open-time counterpart of installDegradedChain's commit:
//
//   - chain missing, retired copy present: the kill landed between the two
//     commit renames; move the whole old chain back (the swap is undone).
//   - chain present, retired copy present: the replacement chain is installed;
//     move the damaged suffix out of the retired copy into a quarantine
//     directory and remove the retired copy, finishing the isolation.
//   - orphaned staging directories are pure debris and are removed.
//
// It is best-effort, like the other open-time sweeps, and stands down while a
// live lease holder operates on the chain, so it never takes apart a swap an
// in-flight operation is about to finish.
// guardLease makes the sweep stand down while a live lease holder operates on
// the chain (Open holds no lease itself and must never take apart another
// process's in-flight swap); the lease-holding chain operations pass false.
func sweepVerifyDebris(chainDir string, guardLease bool) {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	if guardLease {
		if stale, err := leaseIsStale(filepath.Join(parent, base+chainLeaseSuffix)); err == nil && !stale {
			return // a live chain operation may be mid-swap
		}
	}

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
	if len(staging) == 0 && len(trash) == 0 {
		return
	}

	changed := false
	if _, err := os.Stat(chainDir); errors.Is(err, os.ErrNotExist) && len(trash) > 0 {
		// Killed between the two commit renames: the old chain is whole in the
		// retired copy; undo the swap by putting it back.
		if err := os.Rename(filepath.Join(parent, trash[0]), chainDir); err == nil {
			changed = true
			trash = trash[1:]
		}
	}
	if _, err := os.Stat(chainDir); err == nil {
		// The replacement chain is installed. Every ring past its dense tip is
		// a suffix the committed degradation isolated; extract it from each
		// retired copy. The chain on the path already validated whole, so its
		// ring files are exactly 1..n.
		entries, lerr := os.ReadDir(chainDir)
		if lerr != nil {
			return
		}
		var intact uint32
		present := make(map[uint32]bool)
		for _, e := range entries {
			if !e.IsDir() {
				if idx, ok := parseRingIndex(e.Name()); ok {
					present[idx] = true
				}
			}
		}
		for present[intact+1] {
			intact++
		}
		for _, t := range trash {
			trashPath := filepath.Join(parent, t)
			tfiles, terr := os.ReadDir(trashPath)
			if terr != nil {
				continue
			}
			var suffix []uint32
			for _, e := range tfiles {
				if e.IsDir() {
					continue
				}
				if idx, ok := parseRingIndex(e.Name()); ok && idx > intact {
					suffix = append(suffix, idx)
				}
			}
			sort.Slice(suffix, func(i, j int) bool { return suffix[i] < suffix[j] })
			if len(suffix) > 0 {
				qdir, qerr := os.MkdirTemp(parent, base+quarantinePrefix)
				if qerr != nil {
					continue // leave the retired copy for the next sweep
				}
				kept := false
				for _, idx := range suffix {
					name := ringName(idx)
					if rerr := os.Rename(filepath.Join(trashPath, name), filepath.Join(qdir, name)); rerr == nil {
						kept = true
					}
				}
				marker := fmt.Sprintf(
					"backup chain quarantine\nchain: %s\nintact rings: %d\nreason: ring failed whole-chain verification\n",
					base, intact)
				if werr := os.WriteFile(filepath.Join(qdir, quarantineMarkerName), []byte(marker), 0o600); werr == nil {
					_ = syncDirectory(qdir)
					changed = true
				}
				if !kept {
					_ = os.RemoveAll(qdir)
				}
			}
			if err := os.RemoveAll(trashPath); err == nil {
				changed = true
			}
		}
	} else {
		// No chain to install into and nothing to move back: retire every
		// leftover copy rather than wedge the directory.
		for _, name := range trash {
			if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
				changed = true
			}
		}
	}
	for _, name := range staging {
		// An orphaned staging directory is discardable; if the chain path is
		// absent and a retired copy exists it was moved back above, and any
		// other staging copy never became state of record.
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	if changed {
		_ = syncDirectory(parent)
	}
}
