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

// Portable hot backup.
//
// Backup exports the store's complete committed state at one fixed commit
// watermark into a portable artifact: a directory of self-describing,
// checksummed segments. The watermark is captured under the write lock (the
// current view, the highest commit sequence and the cumulative snapshot
// count, taken together), then the lock is released and the immutable view
// is streamed to disk without further synchronization — writes, deletes,
// batch commits, snapshots, cursors, checkpoints and compactions all proceed
// while the export runs, and lock-free reads never wait on its disk syncs.
//
// The export is streamed: entries are encoded into one bounded in-memory
// segment buffer at a time and each filled segment is forced to stable
// storage before the next begins, so neither peak memory nor the artifact
// tracks cumulative commits — only live data at the watermark. The whole
// keyspace is never copied out as one piece.
//
// Artifact layout inside the output directory (all integers little-endian):
//
//	manifest.bak
//	    magic     uint32   backupMagic
//	    version   uint32   backupVersion1
//	    watermark uint64   highest commit sequence covered by the export
//	    snapshots uint64   cumulative snapshot count at the watermark
//	    total     uint32   number of segment files
//	    crc       uint32   IEEE CRC-32 of every preceding byte
//
//	seg-0000000000.bak ... seg-<total-1>.bak
//	    magic     uint32   backupMagic
//	    version   uint32   backupVersion1
//	    index     uint32   this segment's position, 0-based
//	    watermark uint64   same export watermark in every segment
//	    snapshots uint64   same cumulative snapshot count in every segment
//	    count     uint32   number of entries that follow
//	    entries   [count]entry
//	    crc       uint32   IEEE CRC-32 of every preceding byte of this file
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the entry's commit sequence (uint64), the key, then
// the value — the same shape a checkpoint layer entry has. Only keys live at
// the watermark are exported, so the artifact tracks live data.
//
// The manifest is written last, after every segment is durable, so a
// complete manifest implies a complete artifact. The whole artifact is
// staged in a sibling directory (backupTmpSuffix) and atomically renamed
// into place; a process killed at any point leaves either no target or the
// complete artifact, never a half-installed one, and staging debris is swept
// by the next backup to the same path or the next open of the related
// directory. Restore validates every segment (presence, format version,
// watermark and snapshot-count agreement with the manifest, whole-file
// checksum) and rejects the artifact as a whole — with an error wrapping
// fs.ErrInvalid — on any defect.
const (
	backupMagic    uint32 = 0x504b4142 // bytes "BAKP"
	backupVersion1 uint32 = 1

	backupManifestName = "manifest.bak"

	// backupTmpSuffix stages the artifact next to its target while it is
	// built; restoreTmpSuffix stages a restored store next to its target.
	// Either, left behind by a kill before the atomic rename, is pure debris.
	backupTmpSuffix  = ".tmp-backup"
	restoreTmpSuffix = ".tmp-restore"

	backupManifestSize = 4 + 4 + 8 + 8 + 4
	backupHeaderSize   = 4 + 4 + 4 + 8 + 8 + 4
)

// backupSegmentTarget bounds the encoded-entry payload buffered per segment.
// It is a var so tests can force multi-segment artifacts with little data.
var backupSegmentTarget = 2 << 20 // 2 MiB

// Backup exports the store's complete committed state at a fixed commit
// watermark — the state exactly as of one instant — into dir as a portable,
// segmented artifact that Restore can install elsewhere. Writes and deletes
// committed after the watermark are not part of the artifact; they, and
// every other operation (batch commits, snapshots, cursors, checkpoints,
// compactions), proceed normally while the export runs. Reads keep hitting
// memory and are never blocked by the export's disk syncs.
//
// The artifact is staged under a temporary sibling and atomically renamed
// into place, so dir only ever appears complete. dir must not exist yet, or
// be an empty directory; an existing non-directory or non-empty directory is
// rejected with an error wrapping fs.ErrInvalid. Backing up a closed store
// returns an error wrapping fs.ErrClosed.
func (s *Store) Backup(dir string) error {
	s.backupMu.Lock()
	defer s.backupMu.Unlock()

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return errStoreClosed
	}
	// Pin the watermark: the immutable current view plus the commit sequence
	// and snapshot count that belong to it. Everything after this point is
	// outside the export. The lock is released before any disk I/O.
	v := s.current.Load()
	watermark := s.nextSeq
	snaps := s.snapshots.Load()
	s.mu.Unlock()

	staging := dir + backupTmpSuffix
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return err
	}

	w := &backupWriter{dir: staging, watermark: watermark, snaps: snaps}
	v.rangeEach(func(k string, n *node) bool {
		if !n.present {
			return true // only live keys are exported
		}
		w.add(k, n)
		return w.err == nil
	})
	if w.err == nil {
		w.err = w.finish()
	}
	if w.err == nil {
		w.err = syncDirectory(staging)
	}
	if w.err != nil {
		os.RemoveAll(staging)
		return w.err
	}

	// Install atomically. The target must be absent or an empty directory;
	// anything else is rejected before the rename so a complete artifact
	// never lands on top of unrelated content.
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			os.RemoveAll(staging)
			return errBackupTarget
		}
		names, rerr := dirNames(dir)
		if rerr != nil {
			os.RemoveAll(staging)
			return rerr
		}
		if len(names) > 0 {
			os.RemoveAll(staging)
			return errBackupTarget
		}
		if err := os.Remove(dir); err != nil {
			os.RemoveAll(staging)
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(staging, dir); err != nil {
		os.RemoveAll(staging)
		return err
	}
	return syncDirectory(filepath.Dir(dir))
}

// backupWriter streams entries into segment files under dir.
type backupWriter struct {
	dir       string
	watermark uint64
	snaps     uint64

	buf   []byte // encoded entries of the segment being built
	count uint32 // entries in buf
	next  uint32 // index of the next segment to write
	total uint32 // segments written
	err   error
}

// add encodes one live key's terminal entry, flushing the segment when the
// buffer reaches the target size.
func (w *backupWriter) add(key string, n *node) {
	if w.err != nil {
		return
	}
	var ehdr [checkpointEntrySize]byte
	ehdr[0] = ckptFlagPut
	binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(n.value)))
	binary.LittleEndian.PutUint64(ehdr[9:17], n.seq)
	w.buf = append(w.buf, ehdr[:]...)
	w.buf = append(w.buf, key...)
	w.buf = append(w.buf, n.value...)
	w.count++
	if len(w.buf) >= backupSegmentTarget {
		w.err = w.flush()
	}
}

// flush writes the buffered entries as one complete segment file, forces it
// to stable storage and closes it.
func (w *backupWriter) flush() error {
	head := make([]byte, backupHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion1)
	binary.LittleEndian.PutUint32(head[8:12], w.next)
	binary.LittleEndian.PutUint64(head[12:20], w.watermark)
	binary.LittleEndian.PutUint64(head[20:28], w.snaps)
	binary.LittleEndian.PutUint32(head[28:32], w.count)

	h := crc32.NewIEEE()
	h.Write(head)
	h.Write(w.buf)
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())

	path := filepath.Join(w.dir, backupSegmentName(w.next))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(head); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(w.buf); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(crcb[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	w.next++
	w.total++
	w.buf = w.buf[:0]
	w.count = 0
	return nil
}

// finish flushes any partial segment and writes the manifest, which being
// written last marks the artifact complete.
func (w *backupWriter) finish() error {
	if w.count > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	man := make([]byte, backupManifestSize)
	binary.LittleEndian.PutUint32(man[0:4], backupMagic)
	binary.LittleEndian.PutUint32(man[4:8], backupVersion1)
	binary.LittleEndian.PutUint64(man[8:16], w.watermark)
	binary.LittleEndian.PutUint64(man[16:24], w.snaps)
	binary.LittleEndian.PutUint32(man[24:28], w.total)
	h := crc32.NewIEEE()
	h.Write(man)
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())

	path := filepath.Join(w.dir, backupManifestName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(man); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(crcb[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// backupSegmentName renders a segment index as a fixed-width file name so
// the directory listing is already in export order.
func backupSegmentName(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, 10)
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[index%10]
		index /= 10
	}
	return "seg-" + string(buf) + ".bak"
}

// parseBackupSegmentName is the inverse of backupSegmentName. ok is false
// for names that are not segment files.
func parseBackupSegmentName(name string) (uint32, bool) {
	const want = "seg-0000000000.bak"
	if len(name) != len(want) || name[:4] != "seg-" || name[len(name)-4:] != ".bak" {
		return 0, false
	}
	var n uint32
	for i := 4; i < 14; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	return n, true
}

// backupManifest is the decoded, validated content of manifest.bak.
type backupManifest struct {
	watermark uint64
	snaps     uint64
	total     uint32
}

// Restore installs the backup artifact in backupDir as a ready-to-open store
// at targetDir. The restored directory reopens to exactly the committed
// state at the export watermark — every key and value, with batch atomicity
// semantics and the cumulative snapshot count preserved — and needs no
// further export, checkpoint or compaction before use.
//
// The artifact is validated and materialized as a whole: a missing segment,
// a truncated or checksum-bad file, an unknown format version, or a segment
// disagreeing with the manifest rejects the entire backup with an error
// wrapping fs.ErrInvalid, as do a missing or non-directory backupDir and a
// targetDir that already exists with content. The target is staged under a
// temporary sibling and atomically renamed into place, so a process killed
// at any point never leaves a half-installed target directory; staging
// debris is swept by the next restore or backup to the same path or the next
// open of the related directory.
func Restore(backupDir, targetDir string) error {
	info, err := os.Stat(backupDir)
	if err != nil || !info.IsDir() {
		// Missing path, unreadable path component or a non-directory: the
		// artifact is not there to restore from.
		return errBackupInvalid
	}
	if info, err := os.Stat(targetDir); err == nil {
		if !info.IsDir() {
			return errBackupTarget
		}
		names, rerr := dirNames(targetDir)
		if rerr != nil {
			return rerr
		}
		if len(names) > 0 {
			return errBackupTarget
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	staging := targetDir + restoreTmpSuffix
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return err
	}
	if err := buildStoreFromBackup(backupDir, staging); err != nil {
		os.RemoveAll(staging)
		return err
	}
	if err := syncDirectory(staging); err != nil {
		os.RemoveAll(staging)
		return err
	}
	// Install atomically; an existing target is known to be an empty dir.
	if err := os.Remove(targetDir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(staging, targetDir); err != nil {
		os.RemoveAll(staging)
		return err
	}
	return syncDirectory(filepath.Dir(targetDir))
}

// buildStoreFromBackup validates the artifact in backupDir segment by
// segment and streams the live entries into a fresh write-ahead log inside
// staging, which on success holds a complete store directory. Any defect
// aborts with an error wrapping fs.ErrInvalid; the caller removes staging.
func buildStoreFromBackup(backupDir, staging string) error {
	man, err := readBackupManifest(backupDir)
	if err != nil {
		return err
	}
	if err := checkNoExtraSegments(backupDir, man.total); err != nil {
		return err
	}

	f, err := os.OpenFile(filepath.Join(staging, walName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	w := &walWriter{f: f, w: bufio.NewWriter(f)}
	fail := func(err error) error {
		f.Close()
		return err
	}
	for i := uint32(0); i < man.total; i++ {
		if err := restoreSegment(backupDir, i, man, w); err != nil {
			return fail(err)
		}
	}
	// Preserve the cumulative snapshot count exactly as the export saw it.
	if man.snaps > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], man.snaps)
		if err := w.encodeFrame(opMeta, "", v[:]); err != nil {
			return fail(err)
		}
	}
	if err := w.w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	return f.Close()
}

// readBackupManifest reads and fully validates manifest.bak. Any defect —
// missing file, truncation, bad magic, unknown version, checksum mismatch,
// trailing bytes — rejects the artifact with fs.ErrInvalid.
func readBackupManifest(dir string) (backupManifest, error) {
	f, err := os.Open(filepath.Join(dir, backupManifestName))
	if err != nil {
		return backupManifest{}, errBackupInvalid
	}
	defer f.Close()

	r := bufio.NewReader(f)
	man := make([]byte, backupManifestSize)
	if _, err := io.ReadFull(r, man); err != nil {
		return backupManifest{}, errBackupInvalid
	}
	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return backupManifest{}, errBackupInvalid
	}
	h := crc32.NewIEEE()
	h.Write(man)
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return backupManifest{}, errBackupInvalid
	}
	if magic, version := binary.LittleEndian.Uint32(man[0:4]), binary.LittleEndian.Uint32(man[4:8]); magic != backupMagic || version != backupVersion1 {
		return backupManifest{}, errBackupInvalid
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return backupManifest{}, errBackupInvalid // trailing bytes
	}
	return backupManifest{
		watermark: binary.LittleEndian.Uint64(man[8:16]),
		snaps:     binary.LittleEndian.Uint64(man[16:24]),
		total:     binary.LittleEndian.Uint32(man[24:28]),
	}, nil
}

// checkNoExtraSegments rejects the artifact when a segment file sits outside
// the manifest's declared range: the directory then does not describe one
// whole export.
func checkNoExtraSegments(dir string, total uint32) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseBackupSegmentName(e.Name()); ok && idx >= total {
			return errBackupInvalid
		}
	}
	return nil
}

// restoreSegment reads and validates segment index, which must agree with
// man on format version, watermark and snapshot count, and streams its
// entries into w as sequence-tagged log frames. A missing, truncated,
// malformed, checksum-bad or disagreeing segment rejects the whole artifact.
func restoreSegment(dir string, index uint32, man backupManifest, w *walWriter) error {
	f, err := os.Open(filepath.Join(dir, backupSegmentName(index)))
	if err != nil {
		return errBackupInvalid // missing segment
	}
	defer f.Close()

	r := bufio.NewReader(f)
	h := crc32.NewIEEE()

	head := make([]byte, backupHeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return errBackupInvalid
	}
	h.Write(head)
	if magic := binary.LittleEndian.Uint32(head[0:4]); magic != backupMagic {
		return errBackupInvalid
	}
	if version := binary.LittleEndian.Uint32(head[4:8]); version != backupVersion1 {
		return errBackupInvalid // unknown format version
	}
	if binary.LittleEndian.Uint32(head[8:12]) != index ||
		binary.LittleEndian.Uint64(head[12:20]) != man.watermark ||
		binary.LittleEndian.Uint64(head[20:28]) != man.snaps {
		return errBackupInvalid // segment disagrees with the manifest
	}
	count := binary.LittleEndian.Uint32(head[28:32])
	if count > maxRecord {
		return errBackupInvalid
	}

	ehdr := make([]byte, checkpointEntrySize)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, ehdr); err != nil {
			return errBackupInvalid
		}
		h.Write(ehdr)
		flag := ehdr[0]
		keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
		valLen := binary.LittleEndian.Uint32(ehdr[5:9])
		seq := binary.LittleEndian.Uint64(ehdr[9:17])
		if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
			keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
			(flag == ckptFlagDelete && valLen != 0) {
			return errBackupInvalid
		}
		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			return errBackupInvalid
		}
		h.Write(body)
		key := string(body[:keyLen])
		if flag == ckptFlagDelete {
			if err := w.encodeFrameSeq(opDeleteSeq, key, seq, nil); err != nil {
				return err
			}
		} else {
			if err := w.encodeFrameSeq(opPutSeq, key, seq, body[keyLen:]); err != nil {
				return err
			}
		}
	}

	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return errBackupInvalid
	}
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return errBackupInvalid // checksum mismatch
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return errBackupInvalid // trailing bytes
	}
	return nil
}

// dirNames lists the entries of an existing directory.
func dirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// sweepBackupStaging removes backup and restore staging debris left next to
// dir by a kill before the atomic rename. The finished artifact or restored
// store always appears under the target name itself, so a staging sibling is
// never live state. Best effort, like the other open-time sweeps.
func sweepBackupStaging(dir string) error {
	if err := os.RemoveAll(dir + backupTmpSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.RemoveAll(dir + restoreTmpSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
