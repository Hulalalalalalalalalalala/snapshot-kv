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

// Restore loads a backup artifact produced by Store.Backup from backupDir and
// installs a ready-to-open store into targetDir, returning it already open.
// The restored directory, once reopened, is byte-for-byte the complete
// committed state at the export watermark: a full replay from it yields the
// same terminal state the backup captured, and the cumulative snapshot count
// is preserved exactly.
//
// The artifact is validated as one whole before anything is installed: a
// missing segment, a truncated segment, a checksum mismatch, an unknown format
// version or a manifest that disagrees with the files rejects the entire
// backup with an error wrapping fs.ErrInvalid, and no target is created.
//
// Installation is all-or-nothing: the new store is fully built and synced in a
// temporary sibling directory and only then renamed onto targetDir. A process
// killed at any instant leaves either the previous target or one complete
// store; a half-built temporary directory is swept by the next Restore or the
// next Open of the related directory.
//
// A backup path that does not exist or is not a directory is rejected with an
// error wrapping fs.ErrInvalid. A target path that already exists and is not
// empty (including a regular file) is rejected the same way.
func Restore(backupDir, targetDir string) (*Store, error) {
	if backupDir == "" || targetDir == "" {
		return nil, errBadBackup
	}

	// The artifact itself must be an existing directory whose only entries are
	// the manifest and the segments it names. A merge killed mid-commit next
	// to the chain is repaired or swept as part of taking the chain lease.
	binfo, err := os.Stat(backupDir)
	if err != nil {
		return nil, errBadBackup
	}
	if !binfo.IsDir() {
		return nil, errBadBackup
	}
	// A restore synthesizes the chain exclusively: a concurrent full export,
	// incremental export or merge in this directory fails wholesale rather
	// than racing the files the restore reads, and a second restore fails
	// rather than joining it.
	lease, lerr := coordinateChain(backupDir, chainOpRestore, false)
	if lerr != nil {
		return nil, lerr
	}
	defer lease.release()

	// When incremental rings extend the head artifact, the whole chain is
	// validated up front (missing rings, broken links, discontinuous
	// watermarks, truncation, checksum failures and unknown versions reject
	// the entire directory); a head-only artifact keeps its original path.
	hasRings, err := dirHasRings(backupDir)
	if err != nil {
		return nil, err
	}
	var chain artifactInfo
	var manifest manifestInfo
	if hasRings {
		chain, err = validateBackupChain(backupDir)
		if err != nil {
			return nil, err
		}
		manifest = chain.head
	} else {
		manifest, err = loadBackupManifest(backupDir)
		if err != nil {
			// Content defects already wrap fs.ErrInvalid; an underlying read
			// failure passes through unchanged.
			return nil, err
		}
		if err := verifyBackupFileSet(backupDir, manifest.segments); err != nil {
			return nil, err
		}
	}

	// The target must be absent or an empty directory.
	if err := checkRestoreTarget(targetDir); err != nil {
		return nil, err
	}

	// Build the complete store in a temporary sibling so a kill never exposes
	// a half-installed target. The sibling lives next to the target so the
	// final rename is atomic on one filesystem.
	parent := filepath.Dir(targetDir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, err
	}
	stage, err := os.MkdirTemp(parent, filepath.Base(targetDir)+".restore-*")
	if err != nil {
		return nil, err
	}
	installed := false
	defer func() {
		if !installed {
			_ = os.RemoveAll(stage)
		}
	}()

	if hasRings {
		err = buildChainRestoredStore(stage, backupDir, chain)
	} else {
		err = buildRestoredStore(stage, backupDir, manifest)
	}
	if err != nil {
		// Artifact validation failures already wrap fs.ErrInvalid; a genuine
		// write failure is reported as-is rather than mislabeled invalid.
		return nil, err
	}

	if err := installRestoreTarget(stage, targetDir); err != nil {
		return nil, err
	}
	installed = true

	return Open(targetDir)
}

// checkRestoreTarget accepts an absent path or an existing empty directory;
// anything else (a regular file, symlink, or non-empty directory) is invalid.
func checkRestoreTarget(target string) error {
	info, err := os.Stat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return errBadBackup
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errBadBackup
	}
	return nil
}

// verifyBackupFileSet rejects any entry besides the manifest and exactly the
// dense segment set the manifest names, and rejects gaps or extras.
func verifyBackupFileSet(backupDir string, segs []manifestSegment) error {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return err
	}
	want := make(map[uint32]bool, len(segs))
	for _, s := range segs {
		if s.index != uint32(len(want)) {
			return errBadBackup // dense, ordered 0..n-1
		}
		want[s.index] = true
	}
	got := make(map[uint32]bool, len(segs))
	for _, e := range entries {
		if e.IsDir() {
			return errBadBackup // no staging or foreign directory in a valid artifact
		}
		name := e.Name()
		if name == backupManifest || name == backupManifestTmp {
			if name == backupManifestTmp {
				return errBadBackup // an unfinished manifest means an unfinished export
			}
			continue
		}
		if name == chainLockName {
			continue // coordination bookkeeping, never chain data
		}
		idx, ok := parseSegmentIndex(name)
		if !ok {
			return errBadBackup // foreign file
		}
		if !want[idx] {
			return errBadBackup // extra segment
		}
		if got[idx] {
			return errBadBackup
		}
		got[idx] = true
	}
	if len(got) != len(want) {
		return errBadBackup // a named segment is missing
	}
	return nil
}

// readFull reads exactly len(buf) bytes from r; a short read (a truncated
// artifact) is reported as errBadBackup rather than io.ErrUnexpectedEOF,
// while any other read failure passes through.
func readFull(r io.Reader, buf []byte) error {
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errBadBackup
		}
		return err
	}
	return nil
}

// loadBackupManifest reads and fully validates manifest.dat.
func loadBackupManifest(backupDir string) (manifestInfo, error) {
	var zero manifestInfo
	f, err := os.Open(filepath.Join(backupDir, backupManifest))
	if err != nil {
		// A missing manifest means the export never reached its commit point:
		// the artifact is incomplete, not a transient I/O failure.
		if errors.Is(err, os.ErrNotExist) {
			return zero, errBadBackup
		}
		return zero, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	head := make([]byte, backupManifestSize)
	if err := readFull(r, head); err != nil {
		return zero, err
	}
	h := crc32.NewIEEE()
	h.Write(head)

	magic := binary.LittleEndian.Uint32(head[0:4])
	version := binary.LittleEndian.Uint32(head[4:8])
	if magic != backupMagic || version != backupVersion {
		return zero, errBadBackup
	}
	info := manifestInfo{
		watermark: binary.LittleEndian.Uint64(head[8:16]),
		snaps:     binary.LittleEndian.Uint64(head[16:24]),
		total:     binary.LittleEndian.Uint64(head[24:32]),
	}
	n := binary.LittleEndian.Uint32(head[32:36])
	// Every segment carries at least one entry, so the segment count never
	// exceeds the entry total; an empty artifact has n == 0 and total == 0.
	// The absolute cap only guards the pre-allocation against a forged count;
	// the record loop rejects an artifact that does not actually contain them.
	if uint64(n) > info.total || n > backupMaxSegments {
		return zero, errBadBackup
	}

	rec := make([]byte, backupSegRecSize)
	var cumulative uint64
	info.segments = make([]manifestSegment, 0, n)
	for i := uint32(0); i < n; i++ {
		if err := readFull(r, rec); err != nil {
			return zero, err
		}
		h.Write(rec)
		s := manifestSegment{
			index:      binary.LittleEndian.Uint32(rec[0:4]),
			count:      binary.LittleEndian.Uint32(rec[4:8]),
			cumulative: binary.LittleEndian.Uint64(rec[8:16]),
			crc:        binary.LittleEndian.Uint32(rec[16:20]),
		}
		if s.index != i {
			return zero, errBadBackup
		}
		cumulative += uint64(s.count)
		if s.cumulative != cumulative || s.count == 0 {
			return zero, errBadBackup
		}
		info.segments = append(info.segments, s)
	}
	if cumulative != info.total {
		return zero, errBadBackup
	}

	var crcb [crcSize]byte
	if err := readFull(r, crcb[:]); err != nil {
		return zero, err
	}
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return zero, errBadBackup
	}
	info.crc = binary.LittleEndian.Uint32(crcb[:])
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return zero, errBadBackup // trailing bytes, or an underlying read error
	}
	return info, nil
}

// buildRestoredStore streams every named segment into a fresh wal.log in the
// staging directory, validating each segment as it reads. The result is a WAL
// whose full replay reproduces the exported terminal state (one terminal
// frame per key, the same shape a compaction writes), followed by the snapshot
// count meta. Nothing is installed until this whole step succeeds.
func buildRestoredStore(stage, backupDir string, info manifestInfo) error {
	wf, err := os.OpenFile(filepath.Join(stage, walName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		wf.Close()
		return err
	}
	l := &walWriter{f: wf, w: bufio.NewWriter(wf)}

	var seen uint64
	var lastKey string
	haveKeys := false

	for _, seg := range info.segments {
		f, err := os.Open(filepath.Join(backupDir, segmentName(seg.index)))
		if err != nil {
			return fail(err)
		}
		r := bufio.NewReader(f)
		hdr := make([]byte, backupSegHeaderSize)
		if err := readFull(r, hdr); err != nil {
			f.Close()
			return fail(err)
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
			watermark != info.watermark || snaps != info.snaps ||
			index != seg.index || count != seg.count || cumulative != seg.cumulative {
			f.Close()
			return fail(errBadBackup)
		}

		ehdr := make([]byte, checkpointEntrySize)
		for i := uint32(0); i < count; i++ {
			if err := readFull(r, ehdr); err != nil {
				f.Close()
				return fail(err)
			}
			h.Write(ehdr)
			flag := ehdr[0]
			keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
			valLen := binary.LittleEndian.Uint32(ehdr[5:9])
			seq := binary.LittleEndian.Uint64(ehdr[9:17])
			if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
				keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
				(flag == ckptFlagDelete && valLen != 0) {
				f.Close()
				return fail(errBadBackup)
			}
			body := make([]byte, int(keyLen)+int(valLen))
			if err := readFull(r, body); err != nil {
				f.Close()
				return fail(err)
			}
			h.Write(body)
			key := string(body[:keyLen])
			// Entries must be strictly ascending and unique across segments.
			if haveKeys && key <= lastKey {
				f.Close()
				return fail(errBadBackup)
			}
			lastKey, haveKeys = key, true
			value := body[keyLen:]
			if flag == ckptFlagPut {
				if err := l.encodeFrameSeq(opPutSeq, key, seq, value); err != nil {
					f.Close()
					return fail(err)
				}
			} else {
				if err := l.encodeFrameSeq(opDeleteSeq, key, seq, nil); err != nil {
					f.Close()
					return fail(err)
				}
			}
		}

		var crcb [crcSize]byte
		if err := readFull(r, crcb[:]); err != nil {
			f.Close()
			return fail(err)
		}
		if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() ||
			binary.LittleEndian.Uint32(crcb[:]) != seg.crc {
			f.Close()
			return fail(errBadBackup)
		}
		if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
			f.Close()
			return fail(errBadBackup)
		}
		f.Close()
		seen += uint64(count)
	}

	if seen != info.total {
		return fail(errBadBackup)
	}
	if info.snaps > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], info.snaps)
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

// installRestoreTarget puts the fully built staging store onto target. The
// target is absent or empty: when it exists as an empty directory it is
// removed first, so the rename lands on no existing entry. A crash in between
// leaves either an empty/absent target or the complete store, never a partial
// one.
func installRestoreTarget(stage, target string) error {
	if info, err := os.Stat(target); err == nil && info.IsDir() {
		if err := os.Remove(target); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(target)); err != nil {
			return err
		}
	}
	if err := os.Rename(stage, target); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(target))
}

// sweepRestoreDebris removes a half-built temporary restore directory left
// next to dir by a kill during Restore. It only ever removes the specific
// sibling names Restore creates.
func sweepRestoreDebris(dir string) {
	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	fixed := base + ".restore.tmp"
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() {
			continue
		}
		if name == fixed || isRestoreTempName(name, base) {
			_ = os.RemoveAll(filepath.Join(parent, name))
		}
	}
	_ = syncDirectory(parent)
}

// isRestoreTempName matches MkdirTemp's "<base>.restore-<random>" directories.
func isRestoreTempName(name, base string) bool {
	prefix := base + ".restore-"
	return len(name) > len(prefix) && name[:len(prefix)] == prefix
}

// openBackupSweep cleans incomplete-backup debris found when opening a
// directory that was used as a backup output: a staging directory or manifest
// temp is always debris, and finished segments are debris only when the
// manifest that completes the artifact is absent. A complete artifact is left
// untouched.
func openBackupSweep(dir string) error {
	if err := sweepBackupDebris(dir); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, backupManifest)); err == nil {
		return nil // complete artifact; leave it in place
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isBackupSegmentName(e.Name()) {
			names = append(names, e.Name())
		}
		if _, ok := parseRingIndex(e.Name()); ok {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := os.Remove(filepath.Join(dir, name)); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if len(names) > 0 {
		return syncDirectory(dir)
	}
	return nil
}
