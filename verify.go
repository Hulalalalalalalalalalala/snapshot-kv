package snapshot

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Online chain verification and self-healing.
//
// VerifyChain inspects the backup chain in chainDir as one whole before any
// restore, merge or further export trusts it: the head manifest and segments
// are checked first, then the dense ring sequence is streamed one ring at a
// time, verifying every ring's predecessor content link, its start/end
// watermark continuity, its cumulative snapshot count, its checksums and its
// format version.
//
// Three outcomes:
//
//   - The chain is intact: VerifyChain returns nil after changing not one byte
//     of the chain directory.
//   - One incremental ring (or a ring suffix) is damaged but the head and the
//     rings before the damage are whole: the chain is degraded to its longest
//     intact prefix. Every ring at and past the first damaged one is isolated,
//     byte for byte, in a diagnostic sibling directory and the chain keeps
//     only the intact rings. VerifyChain still returns nil: the chain at
//     chainDir is once again a complete chain that restores, merges and
//     accepts further incremental exports exactly as if it had never grown
//     past the last good watermark.
//   - The head itself is damaged, or the directory is unusable as a chain
//     (missing, not a directory, no usable head, a foreign file, or the lease
//     cannot be taken): VerifyChain rejects the whole directory with an error
//     wrapping fs.ErrInvalid and leaves the chain exactly as it was. A ring
//     suffix can always be dropped; a damaged head has no prefix to fall back
//     to, so it is never partially accepted.
//
// Verification is segmented and streamed: head segments and rings are read a
// bounded buffer at a time and discarded as validation proceeds, so neither
// the whole chain nor the keyspace it describes is ever held in memory and
// peak resident state does not grow with chain length.
//
// The check takes the chain directory's lease before touching anything, so it
// is mutually exclusive with backups, incremental exports, merges and
// restores on the same directory, in this process or another; it never blocks
// the store's own writes, batch commits, snapshot open/close, cursor paging,
// checkpoints, compactions or lock-free reads. A lease it cannot take fails
// the whole call with an error wrapping fs.ErrInvalid; a dead holder's lease
// is reclaimed.
//
// Degradation commits exactly like a merge: the intact prefix is assembled in
// a sibling staging directory (every kept file is a hard link, the isolated
// rings are hard-linked into a diagnostic sibling), the old chain is renamed
// aside and the staged chain is renamed onto chainDir in one two-rename
// commit. A kill at any instant therefore leaves either the chain exactly as
// it was before the check or the complete degraded chain; staging or trash
// siblings are swept at the next backup, merge, verification or open of a
// related directory, and a directory reopened afterwards is usable at once.
//
// VerifyChain on a closed store returns an error wrapping fs.ErrClosed.
const (
	verifyStagePrefix = ".verify-new-"
	verifyTrashPrefix = ".verify-old-"
	verifyDiagPrefix  = ".verify-diag-"
)

// VerifyChain validates the backup chain in chainDir and, when only an
// incremental suffix is damaged, degrades the chain to its longest intact
// prefix — isolating the damaged rings in a diagnostic sibling directory —
// leaving a complete chain at chainDir and returning nil. A damaged head or an
// unusable chain directory rejects the whole operation with an error wrapping
// fs.ErrInvalid and leaves the chain untouched. See the file comment for the
// full contract.
func (s *Store) VerifyChain(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize with the other chain-advancing operations driven by this
	// store. The chain lease below is the cross-object, cross-process gate;
	// this lock only keeps this store's own contenders from racing over their
	// sibling staging directories and is independent of mu.
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

	// Repair debris a merge or a previous verification killed mid-commit
	// before looking at the chain, exactly as the next backup or open would.
	sweepMergeDebris(chainDir)
	sweepVerifyDebris(chainDir)

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
	// Residual staging from an export killed mid-run is debris, never a
	// foreign entry: clear it before classifying the chain.
	if err := sweepBackupDebris(chainDir); err != nil {
		return err
	}

	goodRings, ok, err := classifyIntactPrefix(chainDir)
	if err != nil {
		return err
	}
	if !ok {
		// The head itself is unusable: there is no intact prefix to fall back
		// to. Reject wholesale and change nothing.
		return errBadBackup
	}
	total, err := countRingFiles(chainDir)
	if err != nil {
		return err
	}
	if goodRings == total {
		// Intact chain: the directory bytes are left exactly as found.
		return nil
	}
	return commitDegradedChain(chainDir, goodRings)
}

// classifyIntactPrefix streams the whole chain and returns the number of its
// leading rings that are intact. ok is false when the chain cannot be trusted
// even as a head-only chain (a foreign entry, an unusable head or damaged head
// segments), which the caller must reject without degrading.
//
// It streams every file with bounded buffers and retains no entry content. A
// genuine I/O failure passes through unchanged; every structural or content
// defect maps to ok == false (head) or a shorter intact prefix (rings).
func classifyIntactPrefix(chainDir string) (goodRings uint32, ok bool, err error) {
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return 0, false, err
	}

	// File layout first: any foreign entry rejects the whole directory,
	// independent of where a content defect might sit.
	manifestPresent := false
	segFiles := make(map[uint32]bool)
	ringFiles := make(map[uint32]bool)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			return 0, false, nil // no staging or foreign directory in a chain
		}
		switch {
		case name == backupManifest:
			manifestPresent = true
		case name == backupManifestTmp:
			return 0, false, nil // an unfinished manifest is foreign debris here
		default:
			if idx, isSeg := parseSegmentIndex(name); isSeg {
				segFiles[idx] = true
				continue
			}
			if idx, isRing := parseRingIndex(name); isRing {
				ringFiles[idx] = true
				continue
			}
			return 0, false, nil // foreign file
		}
	}
	if !manifestPresent {
		return 0, false, nil // no usable chain head
	}

	// The head manifest must parse, and the segment files on disk must be
	// exactly the dense set it names.
	head, err := loadBackupManifest(chainDir)
	if err != nil {
		if errors.Is(err, fs.ErrInvalid) {
			return 0, false, nil
		}
		return 0, false, err
	}
	named := make(map[uint32]bool, len(head.segments))
	for i, seg := range head.segments {
		if seg.index != uint32(i) || !segFiles[seg.index] {
			return 0, false, nil // gap, reorder, or a manifest-named segment absent
		}
		named[seg.index] = true
	}
	for idx := range segFiles {
		if !named[idx] {
			return 0, false, nil // an extra segment the manifest does not name
		}
	}

	// Ring files must be dense from 1; the first missing position bounds how
	// far a prefix can be intact before any byte is streamed. Rings at or past
	// a gap never stream and join the isolated suffix on degradation.
	var structuralBound uint32 = 1
	for ringFiles[structuralBound] {
		structuralBound++
	}

	// Deep-validate every head byte. A damaged head cannot be degraded around.
	if err := streamHeadSegments(chainDir, head, nil); err != nil {
		if errors.Is(err, fs.ErrInvalid) {
			return 0, false, nil
		}
		return 0, false, err
	}

	// Stream rings in chain order up to the structural bound; the first ring
	// that fails any linkage, watermark, snapshot-count, checksum or version
	// check ends the intact prefix.
	prevWM := head.watermark
	prevSnaps := head.snaps
	prevLink := head.crc
	var good uint32
	for idx := uint32(1); idx < structuralBound; idx++ {
		f, oerr := os.Open(filepath.Join(chainDir, ringName(idx)))
		if oerr != nil {
			return 0, false, oerr
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, nil)
		f.Close()
		if serr != nil {
			if errors.Is(serr, fs.ErrInvalid) {
				return good, true, nil
			}
			return 0, false, serr
		}
		if sum.snaps < prevSnaps {
			return good, true, nil
		}
		good = idx
		prevWM = sum.endWM
		prevSnaps = sum.snaps
		prevLink = sum.fileCRC
	}
	return good, true, nil
}

// countRingFiles returns the number of finished ring files present in
// chainDir.
func countRingFiles(chainDir string) (uint32, error) {
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return 0, err
	}
	var n uint32
	for _, e := range entries {
		if !e.IsDir() {
			if _, ok := parseRingIndex(e.Name()); ok {
				n++
			}
		}
	}
	return n, nil
}

// commitDegradedChain swaps chainDir onto a chain containing only the head
// and its first goodRings rings, preserving every other ring byte for byte in
// a diagnostic sibling. The intact prefix is assembled as hard links in a
// staging directory and validated as a whole before the two-rename commit; a
// kill at any point leaves either the old chain or the complete degraded
// chain. The chain directory itself is not modified until the commit.
func commitDegradedChain(chainDir string, goodRings uint32) error {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return err
	}
	var kept, isolated []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if idx, ok := parseRingIndex(name); ok {
			if idx <= goodRings {
				kept = append(kept, name)
			} else {
				isolated = append(isolated, name)
			}
			continue
		}
		// Everything else (manifest and head segments) is part of the intact
		// head and survives unchanged.
		kept = append(kept, name)
	}

	stage, err := os.MkdirTemp(parent, base+verifyStagePrefix)
	if err != nil {
		return err
	}
	diag, err := os.MkdirTemp(parent, base+verifyDiagPrefix)
	if err != nil {
		_ = os.RemoveAll(stage)
		return err
	}
	abort := func(cause error) error {
		_ = os.RemoveAll(stage)
		_ = os.RemoveAll(diag)
		return cause
	}

	// Hard-link the intact prefix into staging: its bytes are the exact files
	// already validated, copied without being read.
	for _, name := range kept {
		if err := os.Link(filepath.Join(chainDir, name), filepath.Join(stage, name)); err != nil {
			return abort(err)
		}
	}
	// Pin the isolated rings' bytes in the diagnostic directory before the
	// commit, so removing the old chain afterwards loses none of them. A hard
	// link preserves the bytes exactly; after the swap the diagnostic entry is
	// the only name left for each.
	for _, name := range isolated {
		if err := os.Link(filepath.Join(chainDir, name), filepath.Join(diag, name)); err != nil {
			return abort(err)
		}
	}
	if err := syncDirectory(stage); err != nil {
		return abort(err)
	}
	if err := syncDirectory(diag); err != nil {
		return abort(err)
	}
	if err := syncDirectory(parent); err != nil {
		return abort(err)
	}

	// The staged prefix must itself be one complete, validating chain.
	newArt, err := validateBackupChain(stage)
	if err != nil {
		return abort(err)
	}
	if newArt.ringCount != goodRings {
		return abort(errBadBackup)
	}

	// Commit: old chain aside, staged chain onto the path. A kill between the
	// renames leaves the whole old chain in the trash sibling, which the next
	// chain operation or open moves back into place.
	trash, err := os.MkdirTemp(parent, base+verifyTrashPrefix)
	if err != nil {
		return abort(err)
	}
	if err := os.Remove(trash); err != nil {
		return abort(err)
	}
	if err := os.Rename(chainDir, trash); err != nil {
		return abort(err)
	}
	if err := os.Rename(stage, chainDir); err != nil {
		// Restore the untouched old chain from the trash sibling; the staged
		// prefix and diagnostic extra links are then pure debris.
		if rb := os.Rename(trash, chainDir); rb == nil {
			_ = syncDirectory(parent)
		}
		_ = os.RemoveAll(stage)
		_ = os.RemoveAll(diag)
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	// The new chain is live and complete; the diagnostic stays. Only the old
	// chain directory is reclaimed (its damaged rings survive via diag).
	if err := os.RemoveAll(trash); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// sweepVerifyDebris clears the sibling directories a verification can leave
// next to chainDir when killed: a staged intact prefix ("<base>.verify-new-*")
// and a retired old chain ("<base>.verify-old-*"). If the chain path itself is
// missing but a retired old chain exists, the verification was killed between
// its two commit renames and the whole old chain is moved back first. A
// finished diagnostic directory ("<base>.verify-diag-*") is kept: it holds
// isolated evidence, not temporary state. Best-effort, like the other
// open-time sweeps.
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
	if len(staging) == 0 && len(trash) == 0 {
		return
	}
	changed := false
	if _, err := os.Stat(chainDir); errors.Is(err, os.ErrNotExist) && len(trash) > 0 {
		if err := os.Rename(filepath.Join(parent, trash[0]), chainDir); err == nil {
			changed = true
			trash = trash[1:]
		}
	}
	for _, name := range staging {
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	for _, name := range trash {
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	if changed {
		_ = syncDirectory(parent)
	}
}
