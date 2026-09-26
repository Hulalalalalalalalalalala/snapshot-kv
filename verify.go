package snapshot

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Online backup-chain verification and ring-granularity self-healing.
//
// VerifyChain streams the chain in chain order — head manifest, the dense head
// segment set, then ring 1, ring 2, … — and accepts the longest prefix that is
// intact as one whole: the head artifact, dense ring order, predecessor content
// links, continuous watermarks, per-segment and whole-file checksums, and the
// format version. Each artifact is read and checksummed in bounded streaming
// passes (the same scanRing/streamHeadSegments paths Restore and MergeChain
// use); neither the whole chain nor the keyspace is read into memory.
//
// An intact chain is left exactly as it was. When the head itself is unusable
// there is no good prefix to fall back to, so the call rejects the directory
// wholesale with an error wrapping fs.ErrInvalid. When one ring (or a suffix
// beginning at one ring) is missing, truncated, discontinuous, unlinked,
// checksum-bad or of an unknown version, the chain degrades to its most recent
// intact prefix: the damaged rings are kept byte-for-byte in a quarantine
// sibling for diagnosis and stop taking part in restores, merges and later
// incremental exports; the degraded chain is one complete chain that passes
// the same whole-chain validation Restore performs.
//
// Degradation is all-or-nothing, in the same shape a merge uses: the
// replacement chain is copied file by file into a "<base>.quar-new-*"
// sibling, validated as a whole, and only then committed by moving the old
// chain aside to "<base>.quar-old-*" (the quarantine) and renaming the
// replacement onto the chain path. A kill at any instant leaves either the
// old chain or the complete new chain at the chain path; reopening a related
// directory (or the next backup, export, merge or restore) finishes an
// interrupted swap — completing it from a validated staged chain, or moving
// the parked old chain back when no complete replacement exists — and clears
// staging debris. The quarantine itself is retained.
//
// Verification runs entirely on chain artifacts and never modifies the
// backed-up store: it coordinates with the other chain operations through the
// chain lease (a holder it cannot take fails the call wholesale with
// fs.ErrInvalid), while writes, batch commits, snapshot open/close, cursor
// paging, checkpoints, compactions, incremental exports and lock-free reads
// proceed and are never blocked by the verification's disk reads.
// VerifyChain on a closed store returns an error wrapping fs.ErrClosed; the
// feature introduces no other error class.
const (
	quarantineStagePrefix = ".quar-new-"
	quarantineTrashPrefix = ".quar-old-"
)

// chainInspection is the streamed verdict over a chain directory: the longest
// intact head-plus-rings prefix and the bookkeeping needed to rebuild that
// prefix as a standalone chain.
type chainInspection struct {
	headOK bool // manifest present, well-formed, segment set exact and whole
	head   manifestInfo

	// totalRings is the number of finished ring files present, regardless of
	// gaps or validity; goodRings is the length of the intact dense prefix
	// (rings 1..goodRings all present and valid).
	totalRings uint32
	goodRings  uint32

	// Context at the good prefix's tip. Initialized to the head and advanced
	// one ring at a time.
	tipCRC uint32
	endWM  uint64
	snaps  uint64

	// foreign records an entry that is no artifact and no recognized debris.
	foreign bool
	// dense reports the ring files on disk are exactly 1..totalRings.
	dense bool
}

// VerifyChain verifies the backup chain in chainDir online and, when a ring
// suffix is damaged, isolates that suffix and degrades the chain to its most
// recent intact prefix. An already-intact chain is validated and left
// untouched. The chain is streamed in chain order in bounded reads and never
// pulls the whole chain or keyspace into memory.
//
// The call takes the chain directory's lease before touching anything, so it
// is mutually exclusive with backups, exports, merges and restores on the
// same directory; a lease it cannot take fails the call wholesale with an
// error wrapping fs.ErrInvalid. It never blocks store writes, batch commits,
// snapshot open/close, cursor paging, checkpoints, compactions or lock-free
// reads, and never modifies the backed-up store.
//
// A chain directory that is missing, not a directory, or whose head artifact
// is unusable is rejected wholesale with an error wrapping fs.ErrInvalid; a
// damaged ring suffix instead degrades the chain and then returns nil. A kill
// during verification or degradation leaves either the old chain or one
// complete new chain; reopening a related directory finishes the swap.
// VerifyChain on a closed store returns an error wrapping fs.ErrClosed.
func (s *Store) VerifyChain(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize with this store's other chain-mutating operations (merge and
	// incremental export) so a verification never swaps a chain another
	// operation is mid-commit on. The lease below coordinates across stores
	// and processes. Neither lock is s.mu: store commits and reads proceed.
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

	// Finish or roll back an interrupted degradation before inspecting, and
	// clear debris from an interrupted merge or export exactly as the other
	// chain operations do.
	sweepQuarantineDebris(chainDir)
	sweepMergeDebris(chainDir)
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
	if err := sweepBackupDebris(chainDir); err != nil {
		return err
	}

	// Fast path: the whole chain validates, nothing to heal.
	if _, err := validateBackupChain(chainDir); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrInvalid) {
		// A genuine read failure passes through rather than being mislabeled.
		return err
	}

	inspection, err := inspectChainPrefix(chainDir)
	if err != nil {
		return err
	}
	if !inspection.headOK {
		// No intact prefix exists: reject the directory like Restore does.
		return errBadBackup
	}
	if inspection.foreign {
		// A non-artifact entry inside the chain directory is never ring
		// damage: the chain operations reject it wholesale rather than guess
		// whether it may be deleted, and verification keeps that contract.
		return errBadBackup
	}
	if inspection.complete() {
		// The earlier failure was debris-only and already swept.
		return nil
	}
	return commitDegradedChain(chainDir, inspection)
}

// complete reports whether the inspection describes one whole valid chain.
func (in chainInspection) complete() bool {
	return in.headOK && in.goodRings == in.totalRings && in.dense && !in.foreign
}

// inspectChainPrefix streams chainDir in chain order and returns its longest
// intact prefix. A malformed head or a bad ring records the prefix length;
// only a genuine I/O failure returns an error.
func inspectChainPrefix(chainDir string) (chainInspection, error) {
	var ins chainInspection

	head, err := loadBackupManifest(chainDir)
	if err != nil {
		if errors.Is(err, fs.ErrInvalid) {
			return ins, nil // head unusable: headOK stays false
		}
		return ins, err
	}
	ins.headOK = true
	ins.head = head
	ins.tipCRC = head.crc
	ins.endWM = head.watermark
	ins.snaps = head.snaps

	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return ins, err
	}
	wantSeg := make(map[uint32]bool, len(head.segments))
	for _, seg := range head.segments {
		wantSeg[seg.index] = true
	}
	presentSeg := make(map[uint32]bool)
	ringSet := make(map[uint32]bool)
	sawManifest := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			// Staging directories are debris the caller sweeps first; any
			// other directory is foreign to the artifact.
			if name != backupStageDir && name != incrStageDir {
				ins.foreign = true
			}
			continue
		}
		switch {
		case name == backupManifest:
			sawManifest = true
		case name == backupManifestTmp:
			// Staging debris the caller sweeps first; ignore if still seen.
		default:
			if idx, ok := parseSegmentIndex(name); ok {
				if !wantSeg[idx] {
					ins.foreign = true
				}
				presentSeg[idx] = true
				continue
			}
			if idx, ok := parseRingIndex(name); ok {
				ringSet[idx] = true
				continue
			}
			ins.foreign = true
		}
	}
	if !sawManifest {
		ins.headOK = false
		return ins, nil
	}
	for idx := range wantSeg {
		if !presentSeg[idx] {
			// A manifest-named segment is absent: the head is incomplete.
			ins.headOK = false
			return ins, nil
		}
	}

	// Deep-validate the head content (every segment CRC and manifest match).
	if err := streamHeadSegments(chainDir, head, nil); err != nil {
		if errors.Is(err, fs.ErrInvalid) {
			ins.headOK = false
			return ins, nil
		}
		return ins, err
	}

	ins.totalRings = uint32(len(ringSet))
	ins.dense = true
	for idx := uint32(1); idx <= ins.totalRings; idx++ {
		if !ringSet[idx] {
			ins.dense = false
			break
		}
	}

	// Stream the dense prefix ring by ring. The first missing (gap),
	// malformed, unlinked, non-continuous or checksum-bad ring ends the
	// intact prefix; it and everything after it are quarantined.
	prevWM := head.watermark
	prevSnaps := head.snaps
	prevLink := head.crc
	for idx := uint32(1); idx <= ins.totalRings; idx++ {
		if !ringSet[idx] {
			break // gap in the dense sequence
		}
		f, oerr := os.Open(filepath.Join(chainDir, ringName(idx)))
		if oerr != nil {
			if errors.Is(oerr, os.ErrNotExist) {
				break
			}
			return ins, oerr
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, nil)
		f.Close()
		if serr != nil {
			// Content defects end the intact prefix and trigger a heal; a
			// genuine read failure passes through unchanged.
			if errors.Is(serr, fs.ErrInvalid) {
				break
			}
			return ins, serr
		}
		if sum.snaps < prevSnaps {
			break
		}
		ins.goodRings = idx
		prevWM = sum.endWM
		prevSnaps = sum.snaps
		prevLink = sum.fileCRC
		ins.tipCRC = sum.fileCRC
		ins.endWM = sum.endWM
		ins.snaps = sum.snaps
	}
	return ins, nil
}

// commitDegradedChain builds the intact prefix described by ins as a complete
// chain in a staging sibling, validates it, then swaps it into place and parks
// the old chain — with its damaged rings byte-for-byte intact — in a
// quarantine sibling. A crash at any instant leaves the old chain or the
// complete new chain at chainDir.
func commitDegradedChain(chainDir string, ins chainInspection) error {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)

	stage, err := os.MkdirTemp(parent, base+quarantineStagePrefix)
	if err != nil {
		return err
	}
	staged := false
	defer func() {
		if !staged {
			_ = os.RemoveAll(stage)
		}
	}()

	// Copy the head and the good rings byte-for-byte: content CRCs and links
	// therefore stay valid without re-encoding anything. One file is open at a
	// time; entry content is never parsed here.
	if err := copyChainFile(filepath.Join(chainDir, backupManifest), filepath.Join(stage, backupManifest)); err != nil {
		return err
	}
	for _, seg := range ins.head.segments {
		name := segmentName(seg.index)
		if err := copyChainFile(filepath.Join(chainDir, name), filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	for idx := uint32(1); idx <= ins.goodRings; idx++ {
		name := ringName(idx)
		if err := copyChainFile(filepath.Join(chainDir, name), filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	if err := syncDirectory(stage); err != nil {
		return err
	}

	// The replacement must itself be one complete valid chain and end exactly
	// where the inspected good prefix ends.
	newArt, err := validateBackupChain(stage)
	if err != nil {
		return err
	}
	if newArt.ringCount != ins.goodRings ||
		newArt.tipCRC != ins.tipCRC || newArt.endWM != ins.endWM ||
		newArt.snaps != ins.snaps {
		return errBadBackup
	}

	// Commit: park the old chain whole (this is the quarantine holding the
	// damaged rings unchanged), then rename the replacement onto the chain
	// path. A kill between the renames leaves chainDir missing with the whole
	// old chain parked; sweepQuarantineDebris finishes or rolls back.
	trash, err := os.MkdirTemp(parent, base+quarantineTrashPrefix)
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
	staged = true
	if err := syncDirectory(parent); err != nil {
		return err
	}
	// The parked old chain is deliberately retained as the quarantine.
	return nil
}

// copyChainFile copies src to dst byte-for-byte with a restricted mode,
// fsyncing the destination before it is closed.
func copyChainFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	return out.Close()
}

// sweepQuarantineDebris finishes or rolls back an interrupted chain
// degradation and removes staging directories a killed verification left next
// to chainDir. It is the open-time counterpart of the lease-held sweep in
// VerifyChain and follows the same all-or-nothing rules as sweepMergeDebris.
//
// The sequence of a degradation commit is: old chain -> "<base>.quar-old-*",
// staged chain -> chainDir. A kill before the first rename leaves a removable
// stage and the chain untouched; a kill between the renames leaves chainDir
// absent, in which case a validated staged chain completes the swap and any
// other case rolls the parked old chain back; a kill after the swap leaves the
// quarantine sibling in place deliberately, where the damaged rings are kept
// for diagnosis.
func sweepQuarantineDebris(chainDir string) {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	var stages, trashes []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if isMergeTempName(name, base, quarantineStagePrefix) {
			stages = append(stages, name)
		}
		if isMergeTempName(name, base, quarantineTrashPrefix) {
			trashes = append(trashes, name)
		}
	}
	if len(stages) == 0 && len(trashes) == 0 {
		return
	}

	changed := false
	if _, err := os.Stat(chainDir); errors.Is(err, os.ErrNotExist) {
		// Killed between the two commit renames. Prefer completing the heal
		// from a fully validated staged chain; the parked old chain then stays
		// as the quarantine holding the damaged rings. With no usable stage,
		// roll the whole parked old chain back into place instead.
		completed := false
		for _, name := range stages {
			sp := filepath.Join(parent, name)
			if _, verr := validateBackupChain(sp); verr != nil {
				continue
			}
			if rerr := os.Rename(sp, chainDir); rerr == nil {
				completed = true
				changed = true
				break
			}
		}
		if !completed && len(trashes) > 0 {
			if err := os.Rename(filepath.Join(parent, trashes[0]), chainDir); err == nil {
				changed = true
				trashes = trashes[1:]
			}
		}
		if !completed {
			// The chain is still absent: every parked sibling is recovery
			// debris, not a quarantine (no degrade committed). Clear them.
			for _, name := range trashes {
				if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
					changed = true
				}
			}
		}
	}

	// Pre-commit staging directories are always debris by now: a stage that
	// completed the swap lives at chainDir, not under its staging name.
	// Trash directories are retained quarantines whenever the chain exists and
	// are left untouched.
	for _, name := range stages {
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	if changed {
		_ = syncDirectory(parent)
	}
}
