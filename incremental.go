package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Incremental backup chains.
//
// A full Backup artifact is the head of a backup chain: its manifest fixes the
// head watermark and the head content hash. BackupIncremental appends one
// self-describing ring to the chain directory, covering only the terminal
// state of keys touched between the previous ring's watermark and a fresh
// fixed anchor; Restore synthesizes the rings over the head in chain order to
// rebuild the state at the last ring's watermark.
//
// Chain layout, inside the chain directory (alongside the head files):
//
//	incr.tmp/                  staging area while an incremental export runs
//	incr-0000000001.dat        first ring, appended after the head
//	incr-0000000002.dat        second ring, and so on
//
// Rings are indexed from 1, dense, and link backwards by content hash: ring 1
// links to the head manifest's terminal CRC; ring k links to ring k-1's
// terminal file CRC. A ring also names the exact watermark it starts from
// (the previous ring's end watermark; the head watermark for ring 1). The
// chain is therefore complete if and only if the head artifact is intact and
// every dense ring is present, whole, checksum-good, of a known version,
// linked to its predecessor and watermark-continuous with it. Restore
// validates the whole chain before building anything and rejects the entire
// directory with fs.ErrInvalid on the first defect; half a chain is never
// installed.
//
// A ring is promoted with a hard link from its staging file: link(2) fails if
// the final name already exists, so a ring is either wholly present under its
// final name or absent — a kill at any instant never leaves a truncated ring
// and two concurrent exports can never clobber one ring.
//
// Ring file layout (all integers little-endian):
//
//	header:
//	magic      uint32   backupMagic
//	version    uint32   backupVersion (one format with the head)
//	ringIndex  uint32   1-based position on the chain
//	prevLink   uint32   content CRC of the previous artifact (manifest/ring)
//	startWM    uint64   watermark this ring starts from (exclusive)
//	endWM      uint64   anchor watermark this ring ends at (inclusive)
//	snapshots  uint64   cumulative snapshot count at endWM
//	segCount   uint32   number of segments that follow (0 for an empty ring)
//
//	segments[segCount]:
//	  magic      uint32   backupMagic
//	  startWM    uint64
//	  endWM      uint64
//	  ringIndex  uint32
//	  segIndex   uint32   0-based within the ring
//	  count      uint32   entries in this segment (always > 0)
//	  cumulative uint64   cumulative live-entry count through this segment
//	  entries    [count] entry, same encoding as head segments
//	  segCRC     uint32   IEEE CRC-32 of every preceding byte of the segment
//
//	footer:
//	total       uint64   total entry count across segments
//	segCount    uint32   repeated for cross-checking
//	segChainCRC uint32   chained CRC-32 over the segments' terminal CRCs
//	fileCRC     uint32   IEEE CRC-32 of every preceding byte of the file
//
// Entries are strictly key-ascending and unique inside one ring. Every entry
// sequence lies in (startWM, endWM]: it is the terminal commit sequence the
// anchored view shows. The sole exception is a tombstone synthesized for a
// key the chain once held but a compaction has since forgotten entirely: its
// delete cannot be read back from memory, so the ring records the key with
// the anchor watermark endWM as its ordering marker. Synthesis re-sequences
// every frame densely anyway, so the terminal state is exact regardless.
//
// An empty ring (no touched keys, segCount 0, endWM == startWM) is legal: it
// still moves the chain's snapshot count forward. Any non-empty ring has
// endWM > startWM.
const (
	incrStageDir    = "incr.tmp"
	incrPrefix      = "incr-"
	incrSuffix      = ".dat"
	incrIndexDigits = 10

	incrRingHeaderSize = 4 + 4 + 4 + 4 + 8 + 8 + 8 + 4
	incrSegHeaderSize  = 4 + 8 + 8 + 4 + 4 + 4 + 8
	incrFooterSize     = 8 + 4 + 4 + 4
)

// ringExpect pins what a ring must link to when it is read as part of a
// specific chain.
type ringExpect struct {
	index    uint32 // required ring index
	prevLink uint32 // required predecessor content CRC
	startWM  uint64 // required start watermark
}

// ringSummary is what a validated ring establishes for the chain after it.
type ringSummary struct {
	index    uint32
	endWM    uint64
	snaps    uint64
	total    uint64
	fileCRC  uint32
	segCount uint32
}

// ringName renders a 1-based ring index as a fixed-width file name.
func ringName(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, incrIndexDigits)
	n := index
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[n%10]
		n /= 10
	}
	return incrPrefix + string(buf) + incrSuffix
}

// parseRingIndex is the inverse of ringName. ok is false for names that are
// not well-formed finished ring files.
func parseRingIndex(name string) (uint32, bool) {
	mid := len(incrPrefix) + incrIndexDigits
	if len(name) != mid+len(incrSuffix) ||
		name[:len(incrPrefix)] != incrPrefix ||
		name[mid:] != incrSuffix {
		return 0, false
	}
	var n uint32
	for i := len(incrPrefix); i < mid; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	if n == 0 {
		return 0, false // rings are 1-based
	}
	return n, true
}

// ringEntryCallback observes one entry while a ring is streamed.
type ringEntryCallback func(flag byte, key string, value []byte, seq uint64) error

// scanRing streams one open ring file, validating every byte of it and
// checking linkage against exp, and hands each entry to cb (which may be
// nil). Nothing beyond the entry currently being read is held in memory.
func scanRing(f *os.File, exp ringExpect, cb ringEntryCallback) (ringSummary, error) {
	var zero ringSummary
	r := bufio.NewReader(f)
	hf := crc32.NewIEEE()

	read := func(buf []byte) error {
		if err := readFull(r, buf); err != nil {
			return err
		}
		hf.Write(buf)
		return nil
	}

	head := make([]byte, incrRingHeaderSize)
	if err := read(head); err != nil {
		return zero, err
	}
	if binary.LittleEndian.Uint32(head[0:4]) != backupMagic ||
		binary.LittleEndian.Uint32(head[4:8]) != backupVersion {
		return zero, errBadBackup
	}
	index := binary.LittleEndian.Uint32(head[8:12])
	prevLink := binary.LittleEndian.Uint32(head[12:16])
	startWM := binary.LittleEndian.Uint64(head[16:24])
	endWM := binary.LittleEndian.Uint64(head[24:32])
	snaps := binary.LittleEndian.Uint64(head[32:40])
	segCount := binary.LittleEndian.Uint32(head[40:44])
	if index != exp.index || prevLink != exp.prevLink || startWM != exp.startWM ||
		endWM < startWM || segCount > backupMaxSegments {
		return zero, errBadBackup
	}

	ehdr := make([]byte, checkpointEntrySize)
	shdr := make([]byte, incrSegHeaderSize)
	var crcb [crcSize]byte
	var cumulative, total uint64
	var segChain uint32
	var lastKey string
	haveKey := false

	for seg := uint32(0); seg < segCount; seg++ {
		if err := read(shdr); err != nil {
			return zero, err
		}
		hs := crc32.NewIEEE()
		hs.Write(shdr)
		if binary.LittleEndian.Uint32(shdr[0:4]) != backupMagic ||
			binary.LittleEndian.Uint64(shdr[4:12]) != startWM ||
			binary.LittleEndian.Uint64(shdr[12:20]) != endWM ||
			binary.LittleEndian.Uint32(shdr[20:24]) != index ||
			binary.LittleEndian.Uint32(shdr[24:28]) != seg {
			return zero, errBadBackup
		}
		count := binary.LittleEndian.Uint32(shdr[28:32])
		segCum := binary.LittleEndian.Uint64(shdr[32:40])
		if count == 0 || count > maxRecord {
			return zero, errBadBackup
		}
		for i := uint32(0); i < count; i++ {
			if err := read(ehdr); err != nil {
				return zero, err
			}
			hs.Write(ehdr)
			flag := ehdr[0]
			keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
			valLen := binary.LittleEndian.Uint32(ehdr[5:9])
			seq := binary.LittleEndian.Uint64(ehdr[9:17])
			if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
				keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
				(flag == ckptFlagDelete && valLen != 0) ||
				seq <= startWM || seq > endWM {
				return zero, errBadBackup
			}
			body := make([]byte, int(keyLen)+int(valLen))
			if err := read(body); err != nil {
				return zero, err
			}
			hs.Write(body)
			key := string(body[:keyLen])
			if haveKey && key <= lastKey {
				return zero, errBadBackup
			}
			lastKey, haveKey = key, true
			if cb != nil {
				if err := cb(flag, key, body[keyLen:], seq); err != nil {
					return zero, err
				}
			}
		}
		if err := read(crcb[:]); err != nil {
			return zero, err
		}
		if binary.LittleEndian.Uint32(crcb[:]) != hs.Sum32() {
			return zero, errBadBackup
		}
		segChain = crc32.Update(segChain, crc32.IEEETable, crcb[:])
		cumulative += uint64(count)
		if segCum != cumulative {
			return zero, errBadBackup
		}
		total = cumulative
	}

	foot := make([]byte, incrFooterSize)
	if err := read(foot); err != nil {
		return zero, err
	}
	if binary.LittleEndian.Uint64(foot[0:8]) != total ||
		binary.LittleEndian.Uint32(foot[8:12]) != segCount ||
		binary.LittleEndian.Uint32(foot[12:16]) != segChain {
		return zero, errBadBackup
	}
	// The terminal file CRC covers every preceding byte; it is not part of its
	// own input.
	var terminal [crcSize]byte
	if err := readFull(r, terminal[:]); err != nil {
		return zero, err
	}
	if binary.LittleEndian.Uint32(terminal[:]) != hf.Sum32() {
		return zero, errBadBackup
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return zero, errBadBackup
	}

	if total > 0 && endWM == startWM {
		return zero, errBadBackup
	}
	return ringSummary{
		index:    index,
		endWM:    endWM,
		snaps:    snaps,
		total:    total,
		fileCRC:  binary.LittleEndian.Uint32(terminal[:]),
		segCount: segCount,
	}, nil
}

// streamHeadSegments validates and streams every head segment named by the
// manifest, in order, handing each put entry to cb. The head manifest is
// trusted to already have been validated.
func streamHeadSegments(backupDir string, head manifestInfo, cb ringEntryCallback) error {
	var lastKey string
	haveKey := false
	var seen uint64
	for _, seg := range head.segments {
		f, err := os.Open(filepath.Join(backupDir, segmentName(seg.index)))
		if err != nil {
			return err
		}
		r := bufio.NewReader(f)
		hdr := make([]byte, backupSegHeaderSize)
		if err := readFull(r, hdr); err != nil {
			f.Close()
			return err
		}
		h := crc32.NewIEEE()
		h.Write(hdr)
		magic := binary.LittleEndian.Uint32(hdr[0:4])
		version := binary.LittleEndian.Uint32(hdr[4:8])
		watermark := binary.LittleEndian.Uint64(hdr[8:16])
		snaps := binary.LittleEndian.Uint64(hdr[16:24])
		index := binary.LittleEndian.Uint32(hdr[24:28])
		count := binary.LittleEndian.Uint32(hdr[28:32])
		cumulative := binary.LittleEndian.Uint64(hdr[32:40])
		if magic != backupMagic || version != backupVersion ||
			watermark != head.watermark || snaps != head.snaps ||
			index != seg.index || count != seg.count || cumulative != seg.cumulative {
			f.Close()
			return errBadBackup
		}
		ehdr := make([]byte, checkpointEntrySize)
		for i := uint32(0); i < count; i++ {
			if err := readFull(r, ehdr); err != nil {
				f.Close()
				return err
			}
			h.Write(ehdr)
			flag := ehdr[0]
			keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
			valLen := binary.LittleEndian.Uint32(ehdr[5:9])
			seq := binary.LittleEndian.Uint64(ehdr[9:17])
			if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
				keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
				(flag == ckptFlagDelete && valLen != 0) ||
				seq == 0 || seq > watermark {
				f.Close()
				return errBadBackup
			}
			body := make([]byte, int(keyLen)+int(valLen))
			if err := readFull(r, body); err != nil {
				f.Close()
				return err
			}
			h.Write(body)
			key := string(body[:keyLen])
			if haveKey && key <= lastKey {
				f.Close()
				return errBadBackup
			}
			lastKey, haveKey = key, true
			if cb != nil {
				if err := cb(flag, key, body[keyLen:], seq); err != nil {
					f.Close()
					return err
				}
			}
		}
		var crcb [crcSize]byte
		if err := readFull(r, crcb[:]); err != nil {
			f.Close()
			return err
		}
		if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() ||
			binary.LittleEndian.Uint32(crcb[:]) != seg.crc {
			f.Close()
			return errBadBackup
		}
		if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
			f.Close()
			return errBadBackup
		}
		f.Close()
		seen += uint64(count)
	}
	if seen != head.total {
		return errBadBackup
	}
	return nil
}

// artifactInfo is a fully validated backup chain directory.
type artifactInfo struct {
	head      manifestInfo
	ringCount uint32 // number of rings dense after the head
	tipCRC    uint32 // content CRC the next ring must link to
	endWM     uint64 // watermark of the chain tip
	snaps     uint64 // cumulative snapshot count at the chain tip
}

// validateBackupChain inspects backupDir as one whole chain: the head
// manifest, an exact dense head segment set and dense linked rings with
// continuous watermarks. It streams every file and holds no entry content.
// Any defect returns an error wrapping fs.ErrInvalid; genuine read failures
// pass through unchanged.
func validateBackupChain(backupDir string) (artifactInfo, error) {
	var zero artifactInfo
	head, err := loadBackupManifest(backupDir)
	if err != nil {
		return zero, err
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return zero, err
	}
	headSegs := make(map[uint32]bool, len(head.segments))
	for _, s := range head.segments {
		headSegs[s.index] = true
	}
	ringSet := make(map[uint32]bool)
	sawManifest := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			return zero, errBadBackup // no staging or foreign directory in an artifact
		}
		switch {
		case name == backupManifest:
			sawManifest = true
		case name == backupManifestTmp:
			return zero, errBadBackup
		default:
			if idx, ok := parseSegmentIndex(name); ok {
				if !headSegs[idx] {
					return zero, errBadBackup
				}
				headSegs[idx] = false // mark present
				continue
			}
			if idx, ok := parseRingIndex(name); ok {
				if ringSet[idx] {
					return zero, errBadBackup
				}
				ringSet[idx] = true
				continue
			}
			return zero, errBadBackup // foreign file
		}
	}
	if !sawManifest {
		return zero, errBadBackup
	}
	for idx, missing := range headSegs {
		if missing {
			_ = idx
			return zero, errBadBackup // a manifest-named segment is absent
		}
	}

	// Deep-validate the head segments before trusting any of the chain.
	if err := streamHeadSegments(backupDir, head, nil); err != nil {
		return zero, err
	}

	// Validate the dense ring sequence and its linkage.
	ringCount := uint32(len(ringSet))
	for idx := uint32(1); idx <= ringCount; idx++ {
		if !ringSet[idx] {
			return zero, errBadBackup // gap in the rings
		}
	}
	prevWM := head.watermark
	prevSnaps := head.snaps
	prevLink := head.crc
	tipCRC := head.crc
	for idx := uint32(1); idx <= ringCount; idx++ {
		f, oerr := os.Open(filepath.Join(backupDir, ringName(idx)))
		if oerr != nil {
			return zero, oerr
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, nil)
		f.Close()
		if serr != nil {
			return zero, serr
		}
		if sum.snaps < prevSnaps {
			return zero, errBadBackup
		}
		prevWM = sum.endWM
		prevSnaps = sum.snaps
		prevLink = sum.fileCRC
		tipCRC = sum.fileCRC
	}
	return artifactInfo{
		head:      head,
		ringCount: ringCount,
		tipCRC:    tipCRC,
		endWM:     prevWM,
		snaps:     prevSnaps,
	}, nil
}

// dirHasRings reports whether backupDir names at least one finished ring
// file. Its only read error passes through from the directory listing.
func dirHasRings(backupDir string) (bool, error) {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			if _, ok := parseRingIndex(e.Name()); ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// buildChainRestoredStore streams a validated head-plus-rings chain into a
// fresh wal.log in the staging directory, applying the artifacts in chain
// order (each ring overriding the one before) so the result is exactly the
// terminal state at the chain tip. It streams one entry at a time and keeps
// no keyspace in memory.
//
// Frames are re-sequenced densely in application order: a key's own frames
// are encountered head-before-ring and in ring order, so its per-key sequence
// keeps increasing; replay therefore links the same terminal the chain
// computes. The snapshot count at the tip is appended as the closing meta,
// the same compacted-log shape buildRestoredStore writes. Nothing is
// installed until this whole step succeeds.
func buildChainRestoredStore(stage, backupDir string, art artifactInfo) error {
	wf, err := os.OpenFile(filepath.Join(stage, walName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		wf.Close()
		return err
	}
	l := &walWriter{f: wf, w: bufio.NewWriter(wf)}
	var seq uint64

	apply := func(flag byte, key string, value []byte, _ uint64) error {
		seq++
		if flag == ckptFlagPut {
			return l.encodeFrameSeq(opPutSeq, key, seq, value)
		}
		return l.encodeFrameSeq(opDeleteSeq, key, seq, nil)
	}

	if err := streamHeadSegments(backupDir, art.head, apply); err != nil {
		return fail(err)
	}
	prevWM := art.head.watermark
	prevLink := art.head.crc
	for idx := uint32(1); idx <= art.ringCount; idx++ {
		f, oerr := os.Open(filepath.Join(backupDir, ringName(idx)))
		if oerr != nil {
			return fail(oerr)
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, apply)
		f.Close()
		if serr != nil {
			return fail(serr)
		}
		prevWM = sum.endWM
		prevLink = sum.fileCRC
	}

	if art.snaps > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], art.snaps)
		if err := l.encodeFrame(opMeta, "", v[:]); err != nil {
			return fail(err)
		}
	}
	if err := l.w.Flush(); err != nil {
		return fail(err)
	}
	if err := wf.Sync(); err != nil {
		return fail(err)
	}
	if err := wf.Close(); err != nil {
		return err
	}
	return syncDirectory(stage)
}

// presentSetAtTip streams the whole validated chain and returns the set of
// keys present at its tip. It is one set of live keys (bounded by live data,
// never by the chain length), not a copy of any value: the next incremental
// export needs exactly this set to tell an untouched key from one touched,
// deleted and then forgotten by compaction.
func presentSetAtTip(backupDir string, art artifactInfo) (map[string]struct{}, error) {
	present := make(map[string]struct{})
	add := func(flag byte, key string, _ []byte, _ uint64) error {
		if flag == ckptFlagPut {
			present[key] = struct{}{}
		} else {
			delete(present, key)
		}
		return nil
	}
	if err := streamHeadSegments(backupDir, art.head, add); err != nil {
		return nil, err
	}
	prevWM := art.head.watermark
	prevLink := art.head.crc
	for idx := uint32(1); idx <= art.ringCount; idx++ {
		f, err := os.Open(filepath.Join(backupDir, ringName(idx)))
		if err != nil {
			return nil, err
		}
		sum, serr := scanRing(f, ringExpect{
			index:    idx,
			prevLink: prevLink,
			startWM:  prevWM,
		}, add)
		f.Close()
		if serr != nil {
			return nil, serr
		}
		prevWM = sum.endWM
		prevLink = sum.fileCRC
	}
	return present, nil
}

// ringEntryKind marks one touched key's terminal state for a new ring.
type ringEntryKind struct {
	key    string
	seq    uint64
	delete bool
}

// BackupIncremental appends one incremental ring to the backup chain in
// chainDir, anchored at the commit watermark current when it anchors. The
// chain directory must already hold a complete full backup (the chain head);
// writes and deletes committed after the anchor do not enter the ring, while
// the foreground keeps committing.
//
// The ring holds only the terminal state of keys touched between the chain
// tip's watermark and the anchor: later puts overwrite earlier ones, and keys
// deleted in the interval are recorded as tombstones and read back as misses
// after synthesis. Keys the compaction has forgotten entirely but the chain
// once held are recorded as tombstones too, so a delete is never resurrected
// by a later compaction of the backed-up store.
//
// The ring is streamed in bounded chunks and installed all-or-nothing under
// a staging name: a kill at any instant leaves the chain exactly as it was
// (staging debris is swept at the next export into, or Open of, the
// directory), and the backed-up store is never modified.
//
// A chain directory that is missing, not a directory, incomplete or damaged
// (a missing ring, a broken link, a non-continuous watermark, truncation,
// checksum failure or unknown version), or a chain advanced concurrently
// during the export, rejects the whole operation with an error wrapping
// fs.ErrInvalid and leaves no partial ring. BackupIncremental on a closed
// store returns an error wrapping fs.ErrClosed.
func (s *Store) BackupIncremental(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize exports driven by one store so they can never share a staging
	// directory or race to append the same ring index. This does not serialize
	// with commits or reads.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" {
		return errBadBackup
	}
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
	// Clear staging debris from an export (full or incremental) killed mid-run
	// before inspecting the chain; a manifest temp is pure debris as well.
	for _, stage := range []string{incrStageDir, backupStageDir} {
		if err := os.RemoveAll(filepath.Join(chainDir, stage)); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(chainDir, backupManifestTmp)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}

	art, err := validateBackupChain(chainDir)
	if err != nil {
		return err
	}
	// The chain tip's present-key set is the baseline the touched set is
	// diffed against. Streamed from immutable artifacts, it needs no lock.
	prevPresent, err := presentSetAtTip(chainDir, art)
	if err != nil {
		return err
	}

	// Anchor at one fixed watermark. This is the only moment the export takes
	// the write lock; entry resolution and streaming run on the immutable
	// anchored view without it.
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return errStoreClosed
	}
	anchor := backupAnchor{
		view:      s.current.Load(),
		watermark: s.nextSeq,
		snaps:     s.snapshots.Load(),
	}
	s.mu.Unlock()

	startWM := art.endWM
	// The anchor must be a continuation of the chain: a watermark or snapshot
	// count behind the tip means this store cannot extend that chain, so the
	// export is rejected wholesale before any ring file is staged.
	if anchor.watermark < startWM || anchor.snaps < art.snaps {
		return errBadBackup
	}
	touched := collectTouchedKeys(anchor.view, prevPresent, startWM, anchor.watermark)

	index := art.ringCount + 1
	return writeIncrementalRing(chainDir, ringHeader{
		index:    index,
		prevLink: art.tipCRC,
		startWM:  startWM,
		endWM:    anchor.watermark,
		snaps:    anchor.snaps,
	}, anchor, touched)
}

// ringHeader carries the fixed fields of a ring being written.
type ringHeader struct {
	index    uint32
	prevLink uint32
	startWM  uint64
	endWM    uint64
	snaps    uint64
}

// collectTouchedKeys diffs the anchored view against the present-key set the
// chain held at startWM and returns the touched keys' terminal state, sorted
// by key. A key present in the chain but absent from the view was deleted and
// forgotten by compaction; it still enters the ring as a tombstone, stamped
// at the anchor watermark.
func collectTouchedKeys(v *view, prevPresent map[string]struct{}, startWM, endWM uint64) []ringEntryKind {
	var touched []ringEntryKind
	seen := make(map[string]struct{}, len(prevPresent))
	v.rangeEach(func(k string, n *node) bool {
		seen[k] = struct{}{}
		_, was := prevPresent[k]
		if n.present {
			if !was || n.seq > startWM {
				touched = append(touched, ringEntryKind{key: k, seq: n.seq})
			}
		} else if was {
			touched = append(touched, ringEntryKind{key: k, seq: n.seq, delete: true})
		}
		return true
	})
	// Keys the chain knew but the view no longer even mentions: deleted and
	// dropped by a compaction. Stamp the tombstone at the anchor.
	for k := range prevPresent {
		if _, ok := seen[k]; ok {
			continue
		}
		touched = append(touched, ringEntryKind{key: k, seq: endWM, delete: true})
	}
	sort.Slice(touched, func(i, j int) bool { return touched[i].key < touched[j].key })
	return touched
}

// writeIncrementalRing streams one ring in bounded chunks, fully syncs it in
// the staging directory and links it into place atomically (link fails
// rather than overwrite when another export advanced the chain). The chain
// directory is forced afterwards.
func writeIncrementalRing(chainDir string, hdr ringHeader, anchor backupAnchor, touched []ringEntryKind) error {
	final := ringName(hdr.index)

	// Refuse to build before staging anything if another ring of this index
	// already exists: a concurrent export appended first.
	if _, err := os.Stat(filepath.Join(chainDir, final)); err == nil {
		return errBadBackup
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	stage := filepath.Join(chainDir, incrStageDir)
	if err := os.MkdirAll(stage, 0o700); err != nil {
		if errors.Is(err, os.ErrExist) {
			// A non-directory entry occupies the staging name: foreign debris,
			// never a usable chain.
			return errBadBackup
		}
		return err
	}
	tmpPath := filepath.Join(stage, final)
	cleanup := func() { os.RemoveAll(stage) }

	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Another export is already staging this ring: this store cannot
			// also append it.
			cleanup()
			return errBadBackup
		}
		cleanup()
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		cleanup()
		return err
	}

	w := bufio.NewWriter(tmp)
	hf := crc32.NewIEEE()
	// write bytes to the ring, advancing the whole-file CRC for every byte.
	write := func(b []byte) error {
		if _, err := w.Write(b); err != nil {
			return err
		}
		hf.Write(b)
		return nil
	}

	segCount := uint32(0)
	if len(touched) > 0 {
		segCount = uint32((len(touched) + backupEntriesPerSegment - 1) / backupEntriesPerSegment)
	}

	head := make([]byte, incrRingHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint32(head[8:12], hdr.index)
	binary.LittleEndian.PutUint32(head[12:16], hdr.prevLink)
	binary.LittleEndian.PutUint64(head[16:24], hdr.startWM)
	binary.LittleEndian.PutUint64(head[24:32], hdr.endWM)
	binary.LittleEndian.PutUint64(head[32:40], hdr.snaps)
	binary.LittleEndian.PutUint32(head[40:44], segCount)
	if err := write(head); err != nil {
		return fail(err)
	}

	ehdr := make([]byte, checkpointEntrySize)
	var segChain uint32
	var cumulative uint64
	for seg := uint32(0); seg < segCount; seg++ {
		start := int(seg) * backupEntriesPerSegment
		stop := start + backupEntriesPerSegment
		if stop > len(touched) {
			stop = len(touched)
		}
		chunk := touched[start:stop]

		hs := crc32.NewIEEE()
		sh := make([]byte, incrSegHeaderSize)
		binary.LittleEndian.PutUint32(sh[0:4], backupMagic)
		binary.LittleEndian.PutUint64(sh[4:12], hdr.startWM)
		binary.LittleEndian.PutUint64(sh[12:20], hdr.endWM)
		binary.LittleEndian.PutUint32(sh[20:24], hdr.index)
		binary.LittleEndian.PutUint32(sh[24:28], seg)
		// cumulative is filled after the chunk is encoded; entries cannot be
		// dropped, so the count equals the chunk length.
		cumulative += uint64(len(chunk))
		binary.LittleEndian.PutUint32(sh[28:32], uint32(len(chunk)))
		binary.LittleEndian.PutUint64(sh[32:40], cumulative)
		if err := write(sh); err != nil {
			return fail(err)
		}
		hs.Write(sh)

		writeEntry := func(flag byte, key string, value []byte, seq uint64) error {
			if len(key) > maxRecord || len(value) > maxRecord {
				return errors.New("snapshot: incremental entry too large")
			}
			if flag == ckptFlagPut {
				ehdr[0] = ckptFlagPut
			} else {
				ehdr[0] = ckptFlagDelete
			}
			binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(key)))
			binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(value)))
			binary.LittleEndian.PutUint64(ehdr[9:17], seq)
			if err := write(ehdr); err != nil {
				return err
			}
			hs.Write(ehdr)
			if _, err := io.WriteString(w, key); err != nil {
				return err
			}
			hf.Write([]byte(key))
			hs.Write([]byte(key))
			if len(value) > 0 {
				if err := write(value); err != nil {
					return err
				}
				hs.Write(value)
			}
			return nil
		}

		for _, e := range chunk {
			if e.delete {
				if err := writeEntry(ckptFlagDelete, e.key, nil, e.seq); err != nil {
					return fail(err)
				}
			} else {
				n := anchor.view.lookup(e.key)
				if n == nil || !n.present || n.seq != e.seq {
					return fail(errBadBackup)
				}
				if err := writeEntry(ckptFlagPut, e.key, cloneBytes(n.value), e.seq); err != nil {
					return fail(err)
				}
			}
		}

		var sc [crcSize]byte
		binary.LittleEndian.PutUint32(sc[:], hs.Sum32())
		if err := write(sc[:]); err != nil {
			return fail(err)
		}
		segChain = crc32.Update(segChain, crc32.IEEETable, sc[:])
	}

	foot := make([]byte, incrFooterSize)
	binary.LittleEndian.PutUint64(foot[0:8], cumulative)
	binary.LittleEndian.PutUint32(foot[8:12], segCount)
	binary.LittleEndian.PutUint32(foot[12:16], segChain)
	if err := write(foot); err != nil {
		return fail(err)
	}
	var fc [crcSize]byte
	binary.LittleEndian.PutUint32(fc[:], hf.Sum32())
	// Flush every buffered byte before appending the terminal CRC: writing it
	// through the raw file while the bufio writer still held content would
	// place the CRC at offset 0 and the buffered body after it.
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if _, err := tmp.Write(fc[:]); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}

	// Promote with link(2): it never overwrites, so a concurrent export cannot
	// be clobbered, and the ring appears whole or not at all.
	if err := os.Link(tmpPath, filepath.Join(chainDir, final)); err != nil {
		if errors.Is(err, os.ErrExist) {
			cleanup()
			return errBadBackup
		}
		cleanup()
		return err
	}
	if err := syncDirectory(chainDir); err != nil {
		return err
	}
	cleanup()
	return syncDirectory(chainDir)
}
