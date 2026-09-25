package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// On-disk backup format.
//
// Backup exports the complete committed state at one fixed commit watermark
// as a set of segment files inside a caller-chosen directory. The export is
// segmented so a large state is written as bounded pieces, and the whole
// directory can be moved elsewhere as a unit: every segment is
// self-describing and nothing references absolute paths or the source store.
//
// Segment layout (all integers little-endian):
//
//	magic     uint32   backupMagic
//	version   uint32   backupVersion1
//	index     uint32   this segment's position; 0-based
//	segments  uint32   total number of segments in the backup
//	seq       uint64   export watermark: highest commit sequence covered
//	snapshots uint64   cumulative snapshot count at the watermark
//	count     uint32   number of entries that follow
//	entries   [count]entry
//	crc       uint32   IEEE CRC-32 of every preceding byte of this file
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the entry's commit sequence (uint64), the key, then
// the value — the same shape a checkpoint layer entry uses. Tombstones are
// exported exactly like live values so the restored store's commit sequence
// continues from the precise watermark. Entries are sorted by key across the
// whole backup, segment by segment, for a canonical encoding.
//
// Every segment carries the same watermark, snapshot count and segment total,
// so any single segment identifies the backup it belongs to. Restore accepts
// a backup only as a whole: a missing, truncated, checksum-bad,
// unknown-version or disagreeing segment rejects the entire backup with an
// error wrapping fs.ErrInvalid.
//
// A segment is an all-or-nothing artifact: built under a temporary name,
// forced whole with one fsync, then atomically renamed into place. A process
// killed mid-export leaves only complete segments of the previous backup
// (if any), complete segments of the new one, and temporary debris; the
// debris is swept on the next Backup (and ignored by Restore), and a backup
// whose segments disagree is rejected as a whole rather than half-restored.
const (
	backupMagic    uint32 = 0x50554b42 // bytes "BKUP"
	backupVersion1 uint32 = 1

	// backupSegmentEntries bounds the entries one segment carries, so a
	// segment's build buffer stays proportional to the keys it touches
	// rather than to the whole exported keyspace.
	backupSegmentEntries = 1024

	backupHeaderSize = 4 + 4 + 4 + 4 + 8 + 8 + 4

	// backupTmpSuffix stages a segment while it is built; it never appears
	// under a final segment name. A file carrying it is crash debris swept
	// on the next export.
	backupTmpSuffix = ".tmp"

	// restoreTmpName stages the rebuilt log inside the restore target; it is
	// atomically renamed over wal.log only once complete and synced. A file
	// by this name left behind by a kill mid-restore is stale and removed on
	// the next open (and by the next Restore).
	restoreTmpName = "wal.log.restore"
)

// backupSegmentName renders a segment index as a fixed-width, zero-padded
// name so the on-disk listing is already in export order.
func backupSegmentName(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, len("backup-0000000000.seg"))
	copy(buf, "backup-")
	for i := len("backup-") + 9; i >= len("backup-"); i-- {
		buf[i] = digits[index%10]
		index /= 10
	}
	copy(buf[len(buf)-len(".seg"):], ".seg")
	return string(buf)
}

// parseBackupSegmentName is the inverse of backupSegmentName. ok is false
// for names that are not final segment files.
func parseBackupSegmentName(name string) (uint32, bool) {
	const prefix = "backup-"
	const suffix = ".seg"
	if len(name) != len(prefix)+10+len(suffix) ||
		name[:len(prefix)] != prefix ||
		name[len(name)-len(suffix):] != suffix {
		return 0, false
	}
	var n uint32
	for i := len(prefix); i < len(prefix)+10; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	return n, true
}

// Backup exports the store's complete committed state at one fixed commit
// watermark into dir, as a set of self-describing segment files that
// Restore can later materialize into a brand-new store directory. The
// exported view covers every key and tombstone committed up to the instant
// the watermark is captured — including everything committed through
// CommitBatch — together with the cumulative snapshot count.
//
// The watermark is captured under the same lock a snapshot acquisition
// takes, and the export then runs off the immutable view without holding
// any store lock: writes, deletes, batch commits, snapshot open/close,
// cursor paging, checkpoints and compactions all proceed concurrently and
// never block on the export, and reads keep hitting memory directly. The
// export walks the live view in place — it never copies the whole keyspace —
// so its peak memory tracks only the keys it touches, not the store's
// cumulative state.
//
// Segments are built under temporary names, forced whole and atomically
// renamed into place; a process killed at any point leaves only complete
// segments plus temporary debris that the next export sweeps and Restore
// ignores, and the store being exported is never touched. Taking a backup
// does not count as taking a snapshot.
//
// An empty target path, or a target that names a regular file, returns an
// error wrapping fs.ErrInvalid; a missing directory is created. Backing up
// a closed store returns an error wrapping fs.ErrClosed.
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

// Backup exports the snapshot's fixed view, exactly as Store.Backup exports
// the latest one: the snapshot's commit watermark becomes the export
// watermark, so the restored store replays to the precise state this
// snapshot observes, no matter what has been committed since. Exporting a
// closed snapshot returns an error wrapping fs.ErrClosed; the target-path
// rules match Store.Backup.
func (s *Snapshot) Backup(dir string) error {
	v := s.v.Load()
	if v == nil {
		return errSnapshotClosed
	}
	return exportBackup(dir, v, s.seq, s.snaps)
}

// exportBackup writes the complete terminal state of v (tombstones
// included) as backup segments under dir. It never mutates v and holds no
// store lock, so the store keeps operating while the export runs.
func exportBackup(dir string, v *view, seq, snaps uint64) error {
	if dir == "" {
		return errEmptyPath
	}
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return errNotDir
		}
	} else {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	if err := sweepBackupDebris(dir); err != nil {
		return err
	}

	// Collect the key order once. The strings are shared with the view's
	// maps, so this allocates only the slice of references for the keys the
	// export touches; values are streamed straight out of the immutable
	// nodes, segment by segment, and never cloned in bulk.
	var keys []string
	v.rangeEach(func(k string, _ *node) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)

	segments := uint32(1)
	if len(keys) > 0 {
		segments = uint32((len(keys) + backupSegmentEntries - 1) / backupSegmentEntries)
	}

	var written []string
	fail := func(err error) error {
		// Only the segments this export completed are removed: a previous
		// intact backup in the same directory is never destroyed by a
		// failed new export.
		for _, name := range written {
			os.Remove(filepath.Join(dir, name))
		}
		return err
	}
	for index := uint32(0); index < segments; index++ {
		lo := int(index) * backupSegmentEntries
		hi := lo + backupSegmentEntries
		if hi > len(keys) {
			hi = len(keys)
		}
		name, werr := writeBackupSegment(dir, index, segments, seq, snaps, v, keys[lo:hi])
		if werr != nil {
			return fail(werr)
		}
		written = append(written, name)
	}
	// Retire stale finals of an older, larger backup so the directory holds
	// exactly this export's segment set.
	pruneStaleBackupSegments(dir, segments)
	return syncDirectory(dir)
}

// writeBackupSegment builds one complete segment in its temp name, forces
// the whole file with one fsync and atomically renames it into place. It
// returns the final segment name. Values are written directly out of the
// immutable view; nothing here retains them.
func writeBackupSegment(dir string, index, segments uint32, seq, snaps uint64, v *view, keys []string) (string, error) {
	final := backupSegmentName(index)
	tmp := final + backupTmpSuffix
	path := filepath.Join(dir, tmp)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	fail := func(err error) (string, error) {
		f.Close()
		os.Remove(path)
		return "", err
	}

	w := bufio.NewWriter(f)
	head := make([]byte, backupHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion1)
	binary.LittleEndian.PutUint32(head[8:12], index)
	binary.LittleEndian.PutUint32(head[12:16], segments)
	binary.LittleEndian.PutUint64(head[16:24], seq)
	binary.LittleEndian.PutUint64(head[24:32], snaps)
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(keys)))

	h := crc32.NewIEEE()
	if _, err := w.Write(head); err != nil {
		return fail(err)
	}
	h.Write(head)

	ehdr := make([]byte, checkpointEntrySize)
	for _, k := range keys {
		n := v.lookup(k)
		if n == nil {
			return fail(errors.New("snapshot: backup key missing from immutable view"))
		}
		if len(k) > maxRecord || len(n.value) > maxRecord {
			return fail(errors.New("snapshot: backup entry too large"))
		}
		if n.present {
			ehdr[0] = ckptFlagPut
		} else {
			ehdr[0] = ckptFlagDelete
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(k)))
		binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(n.value)))
		binary.LittleEndian.PutUint64(ehdr[9:17], n.seq)
		if _, err := w.Write(ehdr); err != nil {
			return fail(err)
		}
		h.Write(ehdr)
		if _, err := io.WriteString(w, k); err != nil {
			return fail(err)
		}
		h.Write([]byte(k))
		if len(n.value) > 0 {
			if _, err := w.Write(n.value); err != nil {
				return fail(err)
			}
			h.Write(n.value)
		}
	}

	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	if _, err := w.Write(crcb[:]); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(path, filepath.Join(dir, final)); err != nil {
		os.Remove(path)
		return "", err
	}
	return final, nil
}

// backupSegment is the decoded content of one validated segment.
type backupSegment struct {
	index    uint32
	segments uint32
	seq      uint64
	snaps    uint64
	entries  []checkpointEntry
}

// loadBackupSegment reads and fully validates one segment file. Any defect
// (truncation, bad magic/version, index mismatch, bad checksum, malformed
// field, duplicated key, trailing bytes) yields a non-nil error.
func loadBackupSegment(dir, name string, wantIndex uint32) (backupSegment, error) {
	f, err := os.Open(filepath.Join(dir, name))
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

	magic := binary.LittleEndian.Uint32(head[0:4])
	version := binary.LittleEndian.Uint32(head[4:8])
	if magic != backupMagic || version != backupVersion1 {
		return backupSegment{}, errors.New("snapshot: unknown backup format")
	}
	var seg backupSegment
	seg.index = binary.LittleEndian.Uint32(head[8:12])
	seg.segments = binary.LittleEndian.Uint32(head[12:16])
	seg.seq = binary.LittleEndian.Uint64(head[16:24])
	seg.snaps = binary.LittleEndian.Uint64(head[24:32])
	count := binary.LittleEndian.Uint32(head[32:36])
	if seg.index != wantIndex {
		return backupSegment{}, errors.New("snapshot: backup segment index out of order")
	}
	if seg.segments == 0 {
		return backupSegment{}, errors.New("snapshot: backup segment count out of range")
	}
	if count > maxRecord {
		return backupSegment{}, errors.New("snapshot: backup entry count out of range")
	}

	seg.entries = make([]checkpointEntry, 0, count)
	ehdr := make([]byte, checkpointEntrySize)
	seen := make(map[string]bool, count)
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
		key := string(body[:keyLen])
		if seen[key] {
			return backupSegment{}, errors.New("snapshot: duplicate key in backup segment")
		}
		seen[key] = true
		e := checkpointEntry{key: key, seq: seq, present: flag == ckptFlagPut}
		if e.present {
			e.value = cloneBytes(body[keyLen:])
		}
		seg.entries = append(seg.entries, e)
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
	return seg, nil
}

// loadBackup discovers, validates and merges a whole backup. The backup is
// accepted only as a whole: every segment 0..segments-1 must be present,
// intact and agreeing on the total, the export watermark and the snapshot
// count, with no stray segments and no key appearing twice. Any defect
// returns an error wrapping fs.ErrInvalid. The merged entries come back
// sorted by key, ready to be replayed into a fresh log.
func loadBackup(dir string) (snaps uint64, entries []checkpointEntry, err error) {
	reject := func(cause error) (uint64, []checkpointEntry, error) {
		return 0, nil, errors.Join(errBackupInvalid, cause)
	}
	listing, rerr := os.ReadDir(dir)
	if rerr != nil {
		return reject(rerr)
	}
	have := make(map[uint32]string)
	for _, e := range listing {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseBackupSegmentName(e.Name()); ok {
			have[idx] = e.Name()
		}
	}
	if len(have) == 0 {
		return reject(errors.New("snapshot: no backup segments found"))
	}

	var total uint32
	var seq uint64
	seen := make(map[string]struct{})
	for index := uint32(0); ; index++ {
		name, ok := have[index]
		if !ok {
			return reject(fmt.Errorf("snapshot: backup segment %d is missing", index))
		}
		seg, lerr := loadBackupSegment(dir, name, index)
		if lerr != nil {
			return reject(lerr)
		}
		if index == 0 {
			total = seg.segments
			seq = seg.seq
			snaps = seg.snaps
		} else if seg.segments != total || seg.seq != seq || seg.snaps != snaps {
			return reject(errors.New("snapshot: backup segments disagree"))
		}
		for _, e := range seg.entries {
			if _, dup := seen[e.key]; dup {
				return reject(errors.New("snapshot: duplicate key across backup segments"))
			}
			seen[e.key] = struct{}{}
			entries = append(entries, e)
		}
		if index+1 == total {
			break
		}
	}
	if uint32(len(have)) != total {
		return reject(errors.New("snapshot: stray backup segments"))
	}
	sortEntries(entries)
	return snaps, entries, nil
}

// Restore materializes a backup produced by Store.Backup or Snapshot.Backup
// into targetDir as a brand-new store directory that Open can open
// directly — no further export, checkpoint or compaction is needed before
// the directory is usable. Reopening the restored directory replays to
// exactly the exported watermark: every key/value, every batch-committed
// change and the cumulative snapshot count are preserved, and new commits
// continue the exported commit sequence.
//
// The backup is validated as a whole before anything is written: a missing,
// truncated, checksum-bad or unknown-version segment rejects the entire
// restore with an error wrapping fs.ErrInvalid and leaves the target
// untouched. An empty path for either directory, a target that names a
// regular file, or a target directory that already contains store data also
// returns an error wrapping fs.ErrInvalid; a missing target directory is
// created.
//
// The rebuilt log is staged under a temporary name, forced whole with one
// fsync and atomically renamed into place. A process killed at any point
// leaves either no store data in the target or the complete restored store;
// a leftover temporary file is swept on the next open and never needs a
// repair step.
func Restore(backupDir, targetDir string) error {
	if backupDir == "" || targetDir == "" {
		return errEmptyPath
	}
	snaps, entries, err := loadBackup(backupDir)
	if err != nil {
		return err
	}

	info, serr := os.Stat(targetDir)
	if serr == nil {
		if !info.IsDir() {
			return errNotDir
		}
	} else {
		if !errors.Is(serr, fs.ErrNotExist) {
			return serr
		}
		if err := os.MkdirAll(targetDir, 0o700); err != nil {
			return err
		}
	}
	if err := sweepRestoreDebris(targetDir); err != nil {
		return err
	}
	if dirContainsStoreData(targetDir) {
		return errRestoreTarget
	}
	return writeRestoredWAL(targetDir, entries, snaps)
}

// writeRestoredWAL stages the restored store's log under a temporary name
// (one self-contained frame per exported entry, then the snapshot-count
// meta), forces it whole and atomically renames it over wal.log. Replaying
// the result reconstructs the exported watermark state exactly.
func writeRestoredWAL(dir string, entries []checkpointEntry, snaps uint64) error {
	tmpPath := filepath.Join(dir, restoreTmpName)
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	l := &walWriter{f: f, w: bufio.NewWriter(f)}
	for _, e := range entries {
		var encErr error
		if e.present {
			encErr = l.encodeFrameSeq(opPutSeq, e.key, e.seq, e.value)
		} else {
			encErr = l.encodeFrameSeq(opDeleteSeq, e.key, e.seq, nil)
		}
		if encErr != nil {
			return fail(encErr)
		}
	}
	// A zero count needs no record: replay of an absent meta yields 0.
	if snaps > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], snaps)
		if err := l.encodeFrame(opMeta, "", v[:]); err != nil {
			return fail(err)
		}
	}
	if err := l.w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, walName)); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDirectory(dir)
}

// dirContainsStoreData reports whether dir holds any file the store treats
// as state: a log, a checkpoint (layered or legacy) or staging for either.
func dirContainsStoreData(dir string) bool {
	names := []string{
		walName, compactTmpName,
		checkpointName, checkpointTmpName,
		checkpointDirName, stagedChainDir,
	}
	for _, n := range names {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			return true
		}
	}
	return false
}

// sweepBackupDebris removes half-built segment temps from a killed export.
// Final segment files and unrelated files are untouched.
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
		if len(name) <= len(backupTmpSuffix) || name[len(name)-len(backupTmpSuffix):] != backupTmpSuffix {
			continue
		}
		if _, ok := parseBackupSegmentName(name[:len(name)-len(backupTmpSuffix)]); !ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// pruneStaleBackupSegments best-effort deletes final segments at or beyond
// keep: leftovers of an older, larger backup in the same directory.
func pruneStaleBackupSegments(dir string, keep uint32) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseBackupSegmentName(e.Name()); ok && idx >= keep {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// sweepRestoreDebris deletes a rebuilt log staged by a restore that was
// killed before its rename. Its absence is not an error.
func sweepRestoreDebris(dir string) error {
	err := os.Remove(filepath.Join(dir, restoreTmpName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
