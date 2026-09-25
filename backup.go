package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// On-disk backup artifact.
//
// Backup exports the complete committed state at one fixed commit watermark
// into a caller-named directory while the store keeps running: the exported
// view is immutable, so the segments stream out without blocking writes,
// batch commits, snapshots, cursors, checkpoints or compaction, and without
// copying the keyspace — peak memory tracks only the keys being encoded.
//
// The artifact is a sequence of segment files (seg-000000.bak,
// seg-000001.bak, ...), each fully self-describing and independently
// checksummed (all integers little-endian):
//
//	magic     uint32   backupMagic
//	version   uint32   backupVersion1
//	index     uint32   this segment's position, 0-based
//	segments  uint32   total number of segments in the artifact
//	seq       uint64   export watermark: highest commit sequence covered
//	snapshots uint64   cumulative snapshot count at the watermark
//	count     uint32   number of entries that follow
//	entries   [count]entry
//	crc       uint32   IEEE CRC-32 of every preceding byte of this segment
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the entry's commit sequence (uint64), the key, then
// the value — the same terminal-state encoding a checkpoint layer uses.
// Entries are sorted by key across the whole artifact and each key appears
// exactly once, tombstones included, so the artifact reproduces the exported
// view faithfully.
//
// Every segment carries the same watermark, snapshot count and segment
// total, so the artifact is complete only when indices 0..segments-1 are all
// present and agree. Restore rejects the whole artifact with an error
// wrapping fs.ErrInvalid when any segment is missing, truncated,
// checksum-bad, of an unknown version or in disagreement with the rest. The
// artifact holds no paths and no links outside itself, so it can be moved
// elsewhere as a whole.
//
// Restore materializes the artifact as a brand-new store directory: the
// exported state becomes the base layer of a fresh checkpoint chain, so the
// directory opens directly — byte-for-byte the state a full record-by-record
// replay at the export watermark would produce, snapshot count included —
// and never needs a second export, compaction or any other repair step. A
// restore killed midway leaves only the usual checkpoint staging debris,
// which the next open sweeps; a backup killed midway leaves segment temps
// that the next backup into the same directory sweeps.
const (
	backupMagic    uint32 = 0x504b4253 // bytes "SBKP"
	backupVersion1 uint32 = 1

	backupHeaderSize = 4 + 4 + 4 + 4 + 8 + 8 + 4

	segmentNamePrefix = "seg-"
	segmentNameSuffix = ".bak"
	segmentTmpSuffix  = ".tmp"
)

// backupSegmentTarget is the encoded-entry byte budget per segment. It is a
// var so tests can shrink it and exercise multi-segment artifacts.
var backupSegmentTarget int64 = 4 << 20

// Backup exports the store's complete committed state at the current commit
// watermark into dir, creating the directory when it is missing. The
// exported view is fixed the instant the watermark is taken: commits,
// deletes, batch commits, snapshots, cursors, checkpoints and compactions
// proceed while the segments stream out and are never blocked by the export,
// and lock-free reads keep hitting memory throughout.
//
// The artifact is a set of self-describing, checksummed segment files that
// can be moved elsewhere as a whole and materialized with Restore. An empty
// target path or a target that names a regular file returns an error
// wrapping fs.ErrInvalid; exporting from a closed store returns an error
// wrapping fs.ErrClosed.
func (s *Store) Backup(dir string) error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return errStoreClosed
	}
	v := s.current.Load()
	seq := s.nextSeq
	snaps := s.snapshots.Load()
	s.mu.Unlock()
	return exportBackup(dir, v, seq, snaps)
}

// Backup exports the snapshot's fixed view as a backup artifact in dir. The
// export watermark is the commit sequence the snapshot was acquired at, and
// the exported content is exactly the key/value state that view shows,
// regardless of later writes, deletes, compactions or Store.Close. A closed
// snapshot exports an empty state, mirroring the miss semantics of its
// reads. Target validation matches Store.Backup.
func (s *Snapshot) Backup(dir string) error {
	v := s.v.Load()
	if v == nil {
		v = newView(nil) // closed snapshot: an ordinary empty view
	}
	return exportBackup(dir, v, s.seq, s.snaps)
}

// exportBackup streams the terminal state of v (tombstones included) into
// dir as a sequence of segment files. Each segment is built under a
// temporary name, forced whole with one fsync and atomically renamed into
// place, so a kill at any instant leaves only complete segments plus sweepable
// temps; the store the view came from is never touched.
func exportBackup(dir string, v *view, seq, snaps uint64) error {
	if dir == "" {
		return errBackupTarget
	}
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return errNotDir
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// Drop stale segments and temps from an earlier export so the directory
	// afterwards holds exactly this artifact and nothing half-written.
	if err := sweepBackupDebris(dir); err != nil {
		return err
	}

	keys := sortedViewKeys(v)
	counts := packSegments(v, keys)
	off := 0
	for index, n := range counts {
		if err := writeBackupSegment(dir, index, len(counts), seq, snaps, v, keys[off:off+int(n)]); err != nil {
			return err
		}
		off += int(n)
	}
	return syncDirectory(dir)
}

// sortedViewKeys returns every key the view knows, present or deleted, in
// ascending byte order.
func sortedViewKeys(v *view) []string {
	var keys []string
	v.rangeEach(func(k string, _ *node) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	return keys
}

// packSegments simulates the segment packing over the sorted keys and
// returns the entry count of each segment. Packing is deterministic, so the
// write pass reproduces exactly these boundaries. The artifact always has at
// least one segment, even for an empty state.
func packSegments(v *view, keys []string) []uint32 {
	counts := []uint32{0}
	var cur int64
	for _, k := range keys {
		n := v.lookup(k)
		sz := int64(checkpointEntrySize) + int64(len(k))
		if n != nil && n.present {
			sz += int64(len(n.value))
		}
		if counts[len(counts)-1] > 0 && cur+sz > backupSegmentTarget {
			counts = append(counts, 0)
			cur = 0
		}
		counts[len(counts)-1]++
		cur += sz
	}
	return counts
}

// segmentFileName renders a segment index as a fixed-width name so the
// on-disk listing is already in artifact order.
func segmentFileName(index int) string {
	const digits = "0123456789"
	buf := make([]byte, 6)
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[index%10]
		index /= 10
	}
	return segmentNamePrefix + string(buf) + segmentNameSuffix
}

func segmentTmpName(index int) string {
	return segmentFileName(index) + segmentTmpSuffix
}

// parseSegmentName is the inverse of segmentFileName. ok is false for names
// that are not final segment files.
func parseSegmentName(name string) (int, bool) {
	const want = len("seg-000000.bak")
	if len(name) != want ||
		name[:len(segmentNamePrefix)] != segmentNamePrefix ||
		name[want-len(segmentNameSuffix):] != segmentNameSuffix {
		return 0, false
	}
	var n int
	for i := len(segmentNamePrefix); i < want-len(segmentNameSuffix); i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// writeBackupSegment serializes one segment freshly (O_EXCL) under its temp
// name, forces the whole file with one fsync and atomically renames it into
// place. keys is this segment's slice of the artifact's sorted key list.
func writeBackupSegment(dir string, index, segCount int, seq, snaps uint64, v *view, keys []string) (err error) {
	tmp := filepath.Join(dir, segmentTmpName(index))
	final := filepath.Join(dir, segmentFileName(index))
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	w := bufio.NewWriter(f)
	h := crc32.NewIEEE()

	head := make([]byte, backupHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion1)
	binary.LittleEndian.PutUint32(head[8:12], uint32(index))
	binary.LittleEndian.PutUint32(head[12:16], uint32(segCount))
	binary.LittleEndian.PutUint64(head[16:24], seq)
	binary.LittleEndian.PutUint64(head[24:32], snaps)
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(keys)))
	if _, err = w.Write(head); err != nil {
		return err
	}
	h.Write(head)

	ehdr := make([]byte, checkpointEntrySize)
	for _, k := range keys {
		n := v.lookup(k)
		if n == nil {
			continue // defensive: keys come from this same immutable view
		}
		if n.present {
			ehdr[0] = ckptFlagPut
			binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(n.value)))
		} else {
			ehdr[0] = ckptFlagDelete
			binary.LittleEndian.PutUint32(ehdr[5:9], 0)
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(k)))
		binary.LittleEndian.PutUint64(ehdr[9:17], n.seq)
		if _, err = w.Write(ehdr); err != nil {
			return err
		}
		h.Write(ehdr)
		if _, err = io.WriteString(w, k); err != nil {
			return err
		}
		h.Write([]byte(k))
		if n.present && len(n.value) > 0 {
			if _, err = w.Write(n.value); err != nil {
				return err
			}
			h.Write(n.value)
		}
	}

	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	if _, err = w.Write(crcb[:]); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// sweepBackupDebris removes segment files and segment temps left in dir by
// earlier exports, so a fresh export never mixes with a stale one. Only
// names matching the artifact's own pattern are touched.
func sweepBackupDebris(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if _, ok := parseSegmentName(name); ok {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		if len(name) > len(segmentTmpSuffix) && name[len(name)-len(segmentTmpSuffix):] == segmentTmpSuffix {
			if _, ok := parseSegmentName(name[:len(name)-len(segmentTmpSuffix)]); ok {
				if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
			}
		}
	}
	return nil
}

// backupSegment is the decoded content of one validated segment.
type backupSegment struct {
	entries  []checkpointEntry
	seq      uint64
	snaps    uint64
	segments int // total segment count every segment of the artifact carries
}

// loadBackupSegment reads and fully validates one segment file. Any defect —
// truncation, bad magic, unknown version, malformed field, checksum
// mismatch, trailing bytes, an index that does not match the file name — is
// a plain error; the caller maps it to fs.ErrInvalid.
func loadBackupSegment(path string, wantIndex int) (backupSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return backupSegment{}, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	h := crc32.NewIEEE()

	head := make([]byte, backupHeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return backupSegment{}, err
	}
	h.Write(head)

	if magic := binary.LittleEndian.Uint32(head[0:4]); magic != backupMagic {
		return backupSegment{}, errors.New("snapshot: unknown backup format")
	}
	if version := binary.LittleEndian.Uint32(head[4:8]); version != backupVersion1 {
		return backupSegment{}, errors.New("snapshot: unknown backup format version")
	}
	var st backupSegment
	index := binary.LittleEndian.Uint32(head[8:12])
	segments := binary.LittleEndian.Uint32(head[12:16])
	if int(index) != wantIndex || segments == 0 || index >= segments {
		return backupSegment{}, errors.New("snapshot: backup segment index out of order")
	}
	st.segments = int(segments)
	st.seq = binary.LittleEndian.Uint64(head[16:24])
	st.snaps = binary.LittleEndian.Uint64(head[24:32])
	count := binary.LittleEndian.Uint32(head[32:36])
	if count > maxRecord {
		return backupSegment{}, errors.New("snapshot: backup entry count out of range")
	}

	st.entries = make([]checkpointEntry, 0, count)
	ehdr := make([]byte, checkpointEntrySize)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, ehdr); err != nil {
			return backupSegment{}, err
		}
		h.Write(ehdr)
		flag := ehdr[0]
		keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
		valLen := binary.LittleEndian.Uint32(ehdr[5:9])
		seq := binary.LittleEndian.Uint64(ehdr[9:17])
		if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
			keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
			(flag == ckptFlagDelete && valLen != 0) {
			return backupSegment{}, errors.New("snapshot: malformed backup entry")
		}
		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			return backupSegment{}, err
		}
		h.Write(body)
		e := checkpointEntry{key: string(body[:keyLen]), seq: seq, present: flag == ckptFlagPut}
		if e.present {
			e.value = cloneBytes(body[keyLen:])
		}
		st.entries = append(st.entries, e)
	}

	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return backupSegment{}, err
	}
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return backupSegment{}, errors.New("snapshot: backup checksum mismatch")
	}
	if b, err := r.ReadByte(); err == nil {
		_ = b
		return backupSegment{}, errors.New("snapshot: backup segment has trailing bytes")
	} else if !errors.Is(err, io.EOF) {
		return backupSegment{}, err
	}
	return st, nil
}

// backupContent is a fully validated artifact: the exported terminal state
// in artifact order plus the watermark metadata every segment agreed on.
type backupContent struct {
	entries []checkpointEntry
	seq     uint64
	snaps   uint64
}

// loadBackup discovers and validates the complete segment set in dir. The
// artifact is all-or-nothing: any missing, truncated, corrupt,
// unknown-version or disagreeing segment rejects the whole backup with an
// error wrapping fs.ErrInvalid.
func loadBackup(dir string) (backupContent, error) {
	invalid := func(err error) (backupContent, error) {
		return backupContent{}, errors.Join(errBackupInvalid, err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return invalid(err)
	}
	found := make(map[int]string)
	for _, e := range files {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseSegmentName(e.Name()); ok {
			found[idx] = filepath.Join(dir, e.Name())
		}
	}
	first, ok := found[0]
	if !ok {
		return invalid(errors.New("snapshot: backup has no segments"))
	}
	seg0, err := loadBackupSegment(first, 0)
	if err != nil {
		return invalid(err)
	}
	segCount := seg0.segments
	if len(found) != segCount {
		return invalid(errors.New("snapshot: backup segment count mismatch"))
	}

	var content backupContent
	content.seq = seg0.seq
	content.snaps = seg0.snaps
	seen := make(map[string]struct{})
	for i := 0; i < segCount; i++ {
		path, ok := found[i]
		if !ok {
			return invalid(errors.New("snapshot: backup segment missing"))
		}
		seg := seg0
		if i > 0 {
			if seg, err = loadBackupSegment(path, i); err != nil {
				return invalid(err)
			}
			if seg.segments != segCount || seg.seq != content.seq || seg.snaps != content.snaps {
				return invalid(errors.New("snapshot: backup segments disagree"))
			}
		}
		for _, e := range seg.entries {
			if _, dup := seen[e.key]; dup {
				return invalid(errors.New("snapshot: duplicate key in backup"))
			}
			seen[e.key] = struct{}{}
			content.entries = append(content.entries, e)
		}
	}
	return content, nil
}

// holdsStoreData reports whether dir contains any file or directory the
// store treats as its own: a log, a checkpoint chain or their staging
// artifacts.
func holdsStoreData(dir string) bool {
	for _, name := range []string{
		walName, compactTmpName,
		checkpointDirName, stagedChainDir,
		checkpointName, checkpointTmpName,
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// Restore materializes the backup artifact in backupDir as a brand-new store
// directory at storeDir, created when missing. The restored directory opens
// directly with Open and replays to exactly the state a full record-by-record
// replay at the export watermark would produce: every key and value, the
// commit sequences behind them and the cumulative snapshot count are all
// preserved, and no repair step is ever needed before first use.
//
// The artifact is validated as a whole before anything is written: any
// segment that is missing, truncated, checksum-bad, of an unknown format
// version or in disagreement with the rest rejects the entire restore with
// an error wrapping fs.ErrInvalid, as does an empty or missing backup
// directory. A restore target that names a regular file, or a directory that
// already holds store data, is likewise rejected with fs.ErrInvalid. A
// restore killed midway leaves only staging debris that the next open of the
// target sweeps; the store the backup was taken from is never touched.
func Restore(backupDir, storeDir string) error {
	content, err := loadBackup(backupDir)
	if err != nil {
		return err
	}

	if storeDir == "" {
		return errRestoreTarget
	}
	info, statErr := os.Stat(storeDir)
	switch {
	case statErr == nil:
		if !info.IsDir() {
			return errNotDir
		}
		if holdsStoreData(storeDir) {
			return errRestoreTarget
		}
	case errors.Is(statErr, fs.ErrNotExist):
		// Created below.
	default:
		return statErr
	}
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		return err
	}

	// The whole exported state becomes the base layer of a fresh checkpoint
	// chain at WAL cut 0. installLayer stages, fsyncs and atomically renames
	// it, so a kill leaves either nothing or the complete layer, and the next
	// open sweeps any half-built temp.
	layer := capturedLayer{
		index:   0,
		entries: content.entries,
		offset:  0,
		seq:     content.seq,
		snaps:   content.snaps,
	}
	if err := installLayer(storeDir, layer); err != nil {
		return err
	}
	return syncDirectory(storeDir)
}
