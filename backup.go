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

// Hot backup and restore.
//
// A backup exports the store's complete committed state at one fixed commit
// watermark as a portable, segmented artifact. The export anchors to the
// current view and then streams its terminal entries into self-describing,
// checksummed segments without holding the store's write lock: writes,
// deletes, batch commits, snapshots, cursor paging, checkpoints and
// compactions proceed concurrently and never block on the disk writes the
// backup performs. Reads keep hitting memory exactly as before.
//
// Artifact layout, inside the output directory:
//
//	backup.tmp/                staging area while an export runs
//	segment-XXXXX.dat          finished segments, promoted one at a time
//	manifest.dat               present only when the whole export finished
//
// The artifact is complete if and only if manifest.dat is present, intact and
// agrees exactly with the segment files that exist; Restore rejects anything
// short of that wholesale with fs.ErrInvalid. A kill at any instant therefore
// leaves either the previous contents of the directory or one complete
// artifact; staging debris and an orphaned manifest temp are swept at the
// start of the next backup into that directory or the next Open of it, and
// the store being backed up is never touched.
//
// Segment file layout (all integers little-endian):
//
//	magic      uint32   backupMagic
//	version    uint32   backupVersion
//	watermark  uint64   highest commit sequence exported
//	snapshots  uint64   cumulative snapshot count at the export watermark
//	index      uint32   segment position, starting at 0
//	count      uint32   number of entries in this segment
//	cumulative uint64   cumulative live-entry count through this segment
//	entries    [count]entry
//	crc        uint32   IEEE CRC-32 of every preceding byte of the file
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the entry's commit sequence (uint64), the key, then
// the value. Entries are written in ascending key order; a put carries its
// value bytes, a tombstone carries none.
//
// The manifest repeats the artifact-level fields (magic, version, watermark,
// snapshots, total entry count, segment count) and one record per segment
// (its index, entry count, cumulative count and content CRC), terminated by
// an overall CRC. A segment is accepted only when its own CRC verifies and
// every field the manifest records about it matches: a missing segment, a
// truncated segment, a checksum mismatch, an unknown version or a manifest
// that disagrees with the files rejects the entire artifact.
const (
	backupStageDir    = "backup.tmp"
	backupManifest    = "manifest.dat"
	backupManifestTmp = "manifest.dat.tmp"
	backupSegmentPref = "segment-"
	backupSegmentSufx = ".dat"

	backupMagic   uint32 = 0x42414b53 // bytes "SKAB"
	backupVersion uint32 = 1

	// backupSegHeaderSize is magic, version, watermark, snapshots, index,
	// count, cumulative.
	backupSegHeaderSize = 4 + 4 + 8 + 8 + 4 + 4 + 8
	// backupManifestSize is magic, version, watermark, snapshots, total
	// entries and segment count; the terminal overall CRC follows the
	// per-segment records.
	backupManifestSize = 4 + 4 + 8 + 8 + 8 + 4
	// backupSegRecSize is one manifest per-segment record: index, count,
	// cumulative, content CRC.
	backupSegRecSize = 4 + 4 + 8 + 4
	// backupEntriesPerSegment bounds how many entries one segment holds. The
	// export streams entries a chunk this size and flushes a segment per
	// chunk, so the whole keyspace is never copied out before hitting disk;
	// only one chunk of values is resident at a time. It is a write-chunking
	// target, not a format limit.
	backupEntriesPerSegment = 4096
	// backupMaxSegments is a sanity ceiling on the segment count a manifest
	// may declare, bounding pre-allocation when reading an untrusted file.
	backupMaxSegments = 1 << 20
)

// backupAnchor is the fixed point-in-time state an export streams. It is
// captured under the store lock and is immutable afterwards: published views
// are never mutated, so the streaming that follows takes no lock and cannot
// observe commits made after the anchor.
type backupAnchor struct {
	view      *view
	watermark uint64 // highest commit sequence covered
	snaps     uint64 // cumulative snapshot count covered
}

// backupEntry is one terminal key state in export order.
type backupEntry struct {
	key     string
	value   []byte
	seq     uint64
	present bool
}

// Backup exports the store's complete committed state into outDir as a
// portable, segmented backup anchored at the commit watermark current when
// Backup anchors. Writes and deletes committed after the anchor are not
// included and proceed concurrently without blocking; every lock-free read
// keeps hitting memory and is never held up by the export's disk writes.
//
// The export streams the keyspace in fixed-size segments and never copies the
// whole keyspace out before writing it. It is all-or-nothing: segment files
// are built and synced in a staging directory and promoted one at a time; the
// manifest is written last and only becomes visible once every segment it
// names is durable. A process killed at any point leaves no half artifact in
// outDir — staging debris is swept at the start of the next Backup there (or
// the next Open of that directory) and never affects the backed-up store.
//
// A path that does not exist or is not a directory, or a directory that
// already holds a complete or partial artifact or any unrelated entry, is
// rejected with an error wrapping fs.ErrInvalid. Backup on a closed store
// returns an error wrapping fs.ErrClosed.
func (s *Store) Backup(outDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	if outDir == "" {
		return errBadBackup
	}

	// Reconcile a merge killed mid-commit next to this directory, create the
	// output directory if needed, and take the chain directory lease for the
	// whole export: a concurrent incremental export, merge or restore on this
	// same directory fails wholesale with fs.ErrInvalid rather than observing
	// a half-written artifact. The lease is released (and its lock file
	// removed) on every return path.
	lease, err := coordinateChain(outDir, chainOpBackup, true)
	if err != nil {
		return err
	}
	defer lease.release()
	if err := prepareBackupDir(outDir); err != nil {
		return err
	}

	// Anchor at one fixed watermark. This is the only moment the export takes
	// the write lock; everything after it streams from an immutable view.
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

	return writeBackup(outDir, anchor)
}

// prepareBackupDir makes outDir an empty target for a fresh artifact: it
// removes staging debris and orphaned files from an interrupted export and
// refuses to overwrite a finished artifact or any unrelated entry.
func prepareBackupDir(outDir string) error {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case e.IsDir() && (name == backupStageDir || name == incrStageDir):
			// staging debris, removed below
		case !e.IsDir() && (isBackupSegmentName(name) || isBackupRingName(name) ||
			name == backupManifestTmp || name == chainLockName):
			// orphaned by a kill (or the live chain lease), removed below
		default:
			return errBadBackup
		}
	}
	for _, stage := range []string{backupStageDir, incrStageDir} {
		if err := os.RemoveAll(filepath.Join(outDir, stage)); err != nil {
			return err
		}
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if isBackupSegmentName(name) || isBackupRingName(name) || name == backupManifestTmp {
			if err := os.Remove(filepath.Join(outDir, name)); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return syncDirectory(outDir)
}

// isBackupRingName reports whether name is a finished incremental ring file.
func isBackupRingName(name string) bool {
	_, ok := parseRingIndex(name)
	return ok
}

// isBackupSegmentName reports whether name is a finished segment file name.
func isBackupSegmentName(name string) bool {
	return len(name) > len(backupSegmentPref)+len(backupSegmentSufx) &&
		name[:len(backupSegmentPref)] == backupSegmentPref &&
		name[len(name)-len(backupSegmentSufx):] == backupSegmentSufx
}

// parseSegmentIndex is the inverse of segmentName. ok is false for names that
// are not well-formed finished segment files.
func parseSegmentIndex(name string) (uint32, bool) {
	const body = "0000000000"
	mid := len(backupSegmentPref) + len(body)
	if len(name) != mid+len(backupSegmentSufx) ||
		name[:len(backupSegmentPref)] != backupSegmentPref ||
		name[mid:] != backupSegmentSufx {
		return 0, false
	}
	var n uint32
	for i := len(backupSegmentPref); i < mid; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	return n, true
}

// segmentName renders a segment index as a fixed-width, zero-padded name.
func segmentName(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, 10)
	n := index
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[n%10]
		n /= 10
	}
	return backupSegmentPref + string(buf) + backupSegmentSufx
}

// writeBackup streams the anchor's terminal entries into segments and finishes
// with the manifest. It holds no store lock: the anchored view is immutable.
func writeBackup(outDir string, anchor backupAnchor) error {
	stage := filepath.Join(outDir, backupStageDir)
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	cleanup := func() {
		os.RemoveAll(stage)
		if entries, rerr := os.ReadDir(outDir); rerr == nil {
			for _, e := range entries {
				if !e.IsDir() && isBackupSegmentName(e.Name()) {
					os.Remove(filepath.Join(outDir, e.Name()))
				}
			}
			os.Remove(filepath.Join(outDir, backupManifestTmp))
		}
		_ = syncDirectory(outDir)
	}

	// One index pass over the immutable view yields the sorted live keys.
	// Only present keys are exported: a restore rebuilds a fresh log with no
	// older history to mask, so deleted keys are simply absent — exactly as a
	// compaction drops them. This keeps the artifact proportional to live
	// data, not the cumulative set of every key ever touched. Values are not
	// collected here; they are read and written a chunk at a time below, so the
	// whole keyspace is never resident before hitting disk.
	var keys []string
	anchor.view.rangeEach(func(k string, n *node) bool {
		if n.present {
			keys = append(keys, k)
		}
		return true
	})
	sort.Strings(keys)

	var segments []manifestSegment
	var total uint64
	var index uint32

	for start := 0; start < len(keys); start += backupEntriesPerSegment {
		stop := start + backupEntriesPerSegment
		if stop > len(keys) {
			stop = len(keys)
		}
		entries := make([]backupEntry, 0, stop-start)
		for _, k := range keys[start:stop] {
			n := anchor.view.lookup(k)
			if n == nil || !n.present {
				continue
			}
			entries = append(entries, backupEntry{
				key:     k,
				seq:     n.seq,
				present: true,
				value:   cloneBytes(n.value),
			})
		}
		cumulative := total + uint64(len(entries))
		crc, err := writeSegment(stage, index, anchor, cumulative, entries)
		if err != nil {
			cleanup()
			return err
		}
		// Promote the fully synced segment out of staging. A crash exposes at
		// worst a finished segment without the manifest, which is pure debris;
		// it never exposes a partial file under a finished name.
		if err := os.Rename(
			filepath.Join(stage, segmentName(index)),
			filepath.Join(outDir, segmentName(index)),
		); err != nil {
			cleanup()
			return err
		}
		if err := syncDirectory(outDir); err != nil {
			cleanup()
			return err
		}
		segments = append(segments, manifestSegment{
			index:      index,
			count:      uint32(len(entries)),
			cumulative: cumulative,
			crc:        crc,
		})
		total = cumulative
		index++
	}

	// The manifest is the commit point: it names exactly the durable segment
	// set and carries the artifact metadata. Building it last makes the whole
	// export all-or-nothing.
	if err := writeManifest(outDir, stage, anchor, total, segments); err != nil {
		cleanup()
		return err
	}
	if err := os.RemoveAll(stage); err != nil {
		return err
	}
	return syncDirectory(outDir)
}

// writeSegment serializes one fully synced segment file in dir and returns the
// content CRC written into its terminal checksum field.
func writeSegment(dir string, index uint32, anchor backupAnchor, cumulative uint64, entries []backupEntry) (uint32, error) {
	path := filepath.Join(dir, segmentName(index))
	tmp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (uint32, error) {
		tmp.Close()
		os.Remove(path)
		return 0, err
	}

	w := bufio.NewWriter(tmp)
	head := make([]byte, backupSegHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint64(head[8:16], anchor.watermark)
	binary.LittleEndian.PutUint64(head[16:24], anchor.snaps)
	binary.LittleEndian.PutUint32(head[24:28], index)
	binary.LittleEndian.PutUint32(head[28:32], uint32(len(entries)))
	binary.LittleEndian.PutUint64(head[32:40], cumulative)

	h := crc32.NewIEEE()
	if _, err := w.Write(head); err != nil {
		return fail(err)
	}
	h.Write(head)

	ehdr := make([]byte, checkpointEntrySize)
	for _, e := range entries {
		if len(e.key) > maxRecord || len(e.value) > maxRecord {
			return fail(errors.New("snapshot: backup entry too large"))
		}
		if e.present {
			ehdr[0] = ckptFlagPut
		} else {
			ehdr[0] = ckptFlagDelete
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(e.key)))
		binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(e.value)))
		binary.LittleEndian.PutUint64(ehdr[9:17], e.seq)
		if _, err := w.Write(ehdr); err != nil {
			return fail(err)
		}
		h.Write(ehdr)
		if _, err := io.WriteString(w, e.key); err != nil {
			return fail(err)
		}
		h.Write([]byte(e.key))
		if len(e.value) > 0 {
			if _, err := w.Write(e.value); err != nil {
				return fail(err)
			}
			h.Write(e.value)
		}
	}

	crc := h.Sum32()
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], crc)
	if _, err := w.Write(crcb[:]); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path)
		return 0, err
	}
	return crc, nil
}

// manifestInfo is the validated content of manifest.dat.
type manifestInfo struct {
	watermark uint64
	snaps     uint64
	total     uint64
	crc       uint32 // manifest terminal content CRC; the first ring links to it
	segments  []manifestSegment
}

// manifestSegment describes one segment the manifest names.
type manifestSegment struct {
	index      uint32
	count      uint32
	cumulative uint64
	crc        uint32
}

// writeManifest builds and durably installs the manifest, the export's commit
// point. It is written under a temp name in the staging directory, synced,
// renamed into outDir and followed by a directory force.
func writeManifest(outDir, stage string, anchor backupAnchor, total uint64, segs []manifestSegment) error {
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	tmpPath := filepath.Join(stage, backupManifestTmp)
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}

	w := bufio.NewWriter(tmp)
	head := make([]byte, backupManifestSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint64(head[8:16], anchor.watermark)
	binary.LittleEndian.PutUint64(head[16:24], anchor.snaps)
	binary.LittleEndian.PutUint64(head[24:32], total)
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(segs)))
	h := crc32.NewIEEE()
	if _, err := w.Write(head); err != nil {
		return fail(err)
	}
	h.Write(head)

	rec := make([]byte, backupSegRecSize)
	for _, s := range segs {
		binary.LittleEndian.PutUint32(rec[0:4], s.index)
		binary.LittleEndian.PutUint32(rec[4:8], s.count)
		binary.LittleEndian.PutUint64(rec[8:16], s.cumulative)
		binary.LittleEndian.PutUint32(rec[16:20], s.crc)
		if _, err := w.Write(rec); err != nil {
			return fail(err)
		}
		h.Write(rec)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	if _, err := w.Write(crcb[:]); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(outDir, backupManifest)); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDirectory(outDir)
}

// sweepBackupDebris removes temporary files an export killed before its commit
// point can leave in a directory: the full and incremental staging
// directories and a manifest temp. Finished segment and ring files are left
// here (only the next backup into that directory reclaims them); the manifest,
// when present, is never touched.
func sweepBackupDebris(dir string) error {
	for _, stage := range []string{backupStageDir, incrStageDir} {
		if err := os.RemoveAll(filepath.Join(dir, stage)); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Remove(filepath.Join(dir, backupManifestTmp)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
