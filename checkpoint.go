package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// On-disk checkpoints: a chain of self-describing layers that shortens
// reopen. The base layer (generation 0) freezes the complete committed state
// at one instant; every later delta layer covers only the commits after the
// preceding layer. Reopen loads the layers in generation order up to the
// highest intact one and replays just the WAL tail past its cut, so the
// recovered state is byte-for-byte a full log replay while usually touching
// only the small newest layers.
//
// Layer file layout, little-endian:
//
//	magic    uint32   checkpointMagic
//	version  uint32   checkpointVersion2
//	gen      uint64   layer generation, 0 for the base, strictly +1 thereafter
//	flags    uint32   layerFlagBase on the complete-state layer, 0 on a delta
//	wm       uint64   highest commit sequence this layer covers
//	prevWm   uint64   watermark of the preceding layer (0 on the base); a delta
//	                  covers exactly commits (prevWm, wm]
//	snaps    uint64   cumulative snapshot count at capture
//	offset   uint64   WAL byte offset of the cut: reopen replays frames at or
//	                  past this offset after the layer chain is installed
//	count    uint32   number of entries that follow
//	entries  [count]entry
//	crc      uint32   IEEE CRC-32 of every preceding byte
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the terminal version's commit sequence (uint64), the
// key, then the value. Keys are encoded in strictly ascending byte order and
// never repeat. A base entry describes whole terminal state: every live key
// as a put (empty values included) and every tombstone. A delta entry exists
// only for a key whose terminal version is newer than the base watermark, and
// carries that key's current put or tombstone; keys untouched since the base
// are simply absent. Every entry seq is unique against any earlier layer, so
// tail records replay onto the exact predecessor versions.
//
// A layer is an all-or-nothing artifact: built under a temporary name, forced
// whole with one fsync, then atomically renamed into place. On reopen a
// missing, truncated, checksum-bad, unknown-version or otherwise malformed
// layer is rejected as a whole, together with every layer above it (they can
// only apply on top of it); recovery falls back to the highest intact prefix
// or, if the base itself is bad, to a plain full WAL replay. Half a layer is
// never installed.
//
// Layers live in the ckpt directory as layer-%016x.lay. Consolidation (the
// first checkpoint and every checkpointWindowth generation) writes a fresh
// base into a sibling staging directory and swaps the directories
// atomically, so retiring the whole old chain is itself crash-atomic:
//
//	ckpt        live chain directory
//	ckpt.next   a freshly built base awaiting promotion
//	ckpt.old    the previous chain displaced by a swap that has not yet settled
//
// A crash at any point leaves one of those states; reconcileLayerDirs on the
// next open finishes or undoes the swap, so no directory ever needs a second
// checkpoint or a compaction before it opens. The WAL is never modified by a
// checkpoint, and compaction (the only rewrite) retires the entire chain
// durably before swapping the log.
//
// Version 1 checkpoints (one file, snapshot.ckpt, the original single-layer
// layout) stay readable forever: a directory written by an older version
// reopens through its v1 file unchanged and is upgraded to the layered layout
// the first time a checkpoint is taken.
const (
	// v2 layered layout.
	cpDirName   = "ckpt"
	cpNextDir   = "ckpt.next"
	cpOldDir    = "ckpt.old"
	layerTmp    = "layer.tmp"
	layerPrefix = "layer-"
	layerSuffix = ".lay"

	// checkpointWindow bounds how many generations survive between
	// consolidations: a checkpoint that would grow the live chain past this
	// many layers writes a fresh base instead. Compaction also retires the
	// whole chain, so directory space tracks the window, not cumulative
	// commits.
	checkpointWindow = 4

	checkpointName    = "snapshot.ckpt"     // v1 single-file checkpoint
	checkpointTmpName = "snapshot.ckpt.tmp" // v1 staging debris

	checkpointMagic    uint32 = 0x504b4353 // bytes "SCKP"
	checkpointVersion1 uint32 = 1
	checkpointVersion2 uint32 = 2

	layerFlagBase uint32 = 1

	v1HeaderSize        = 4 + 4 + 8 + 8 + 8 + 4
	v2HeaderSize        = 4 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 4
	entryHdrSize        = 1 + 4 + 4 + 8 // flags, keyLen, valLen, seq
	ckptFlagPut    byte = 0
	ckptFlagDelete byte = 1
)

// checkpointEntry is one terminal key state in a base layer.
type checkpointEntry struct {
	key     string
	value   []byte
	seq     uint64 // commit sequence of this terminal version
	present bool
}

// deltaEntry is one key's terminal state in a delta layer: only keys whose
// head version is newer than the base watermark appear.
type deltaEntry struct {
	key     string
	value   []byte
	seq     uint64
	present bool
}

// v2Header is the parsed fixed header of one layer.
type v2Header struct {
	gen    uint64
	base   bool
	wm     uint64
	prevWm uint64
	snaps  uint64
	offset int64
	count  uint32
}

// v2Layer is one fully read and self-validated layer.
type v2Layer struct {
	hdr   v2Header
	full  []checkpointEntry // populated on a base layer
	delta []deltaEntry      // populated on a delta layer
}

// capturedState is the full content of a version-1 checkpoint: the terminal
// state plus the watermarks needed to replay only the WAL that follows it.
type capturedState struct {
	entries []checkpointEntry
	offset  int64
	seq     uint64
	snaps   uint64
}

// errCheckpointRejected marks any validation failure. Such an error is never
// surfaced to the caller of Open: a rejected layer is an expected outcome
// handled by falling back to an earlier layer or a WAL-only replay.
func errCheckpointRejected(err error) error {
	return errors.Join(errors.New("snapshot: rejecting checkpoint layer"), err)
}

// ---- capture ----

// captureBase takes a consistent snapshot of complete committed state for a
// base layer. The caller must hold the store's write lock. Entries are sorted
// by key for a canonical encoding; values are copied so later commits cannot
// mutate the bytes being serialized.
func captureBase(v view, wm, snaps uint64, offset int64) (v2Header, []checkpointEntry) {
	entries := make([]checkpointEntry, 0, len(v))
	for k, n := range v {
		e := checkpointEntry{key: k, present: n.present, seq: n.seq}
		if n.present {
			e.value = cloneBytes(n.value)
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return v2Header{
		gen: 0, base: true, wm: wm, prevWm: 0, snaps: snaps,
		offset: offset, count: uint32(len(entries)),
	}, entries
}

// captureDelta collects the terminal state of every key whose head version is
// newer than the preceding layer's watermark. Layers stack in generation
// order: a delta covers exactly span (prevWm, wm], so reapplying base then
// every delta reproduces current terminal state. Tombstones are included even
// for a key that was both created and deleted inside the span: a later layer
// must be able to shadow an earlier layer's put.
func captureDelta(v view, prevWm, wm, snaps uint64, offset int64, gen uint64) (v2Header, []deltaEntry) {
	entries := make([]deltaEntry, 0)
	for k, n := range v {
		if n.seq <= prevWm {
			continue
		}
		e := deltaEntry{key: k, present: n.present, seq: n.seq}
		if n.present {
			e.value = cloneBytes(n.value)
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return v2Header{
		gen: gen, base: false, wm: wm, prevWm: prevWm, snaps: snaps,
		offset: offset, count: uint32(len(entries)),
	}, entries
}

// ---- encoding ----

func encodeV2Header(hdr v2Header) []byte {
	b := make([]byte, v2HeaderSize)
	binary.LittleEndian.PutUint32(b[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(b[4:8], checkpointVersion2)
	binary.LittleEndian.PutUint64(b[8:16], hdr.gen)
	var flags uint32
	if hdr.base {
		flags = layerFlagBase
	}
	binary.LittleEndian.PutUint32(b[16:20], flags)
	binary.LittleEndian.PutUint64(b[20:28], hdr.wm)
	binary.LittleEndian.PutUint64(b[28:36], hdr.prevWm)
	binary.LittleEndian.PutUint64(b[36:44], hdr.snaps)
	binary.LittleEndian.PutUint64(b[44:52], uint64(hdr.offset))
	binary.LittleEndian.PutUint32(b[52:56], hdr.count)
	return b
}

// writeEntry encodes one entry (shared by base and delta) and folds it into h.
func writeEntry(w *bufio.Writer, h io.Writer, flag byte, key string, value []byte, seq uint64) error {
	if len(key) > maxRecord || len(value) > maxRecord {
		return errors.New("snapshot: checkpoint entry too large")
	}
	e := make([]byte, entryHdrSize)
	e[0] = flag
	binary.LittleEndian.PutUint32(e[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(e[5:9], uint32(len(value)))
	binary.LittleEndian.PutUint64(e[9:17], seq)
	if _, err := w.Write(e); err != nil {
		return err
	}
	h.Write(e)
	if _, err := io.WriteString(w, key); err != nil {
		return err
	}
	h.Write([]byte(key))
	if len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return err
		}
		h.Write(value)
	}
	return nil
}

// finishLayer flushes, appends the CRC, forces the file and closes it.
func finishLayer(f *os.File, w *bufio.Writer, h hash.Hash32) error {
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	if _, err := w.Write(crcb[:]); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// createLayerFile freshly (O_EXCL) writes one complete layer to dir/name,
// forcing it whole. It never touches a live layer; the caller renames name
// into place only after this succeeds. Exactly one of full/delta is used per
// hdr.base.
func createLayerFile(dir, name string, hdr v2Header, full []checkpointEntry, delta []deltaEntry) (err error) {
	path := dir + string(os.PathSeparator) + name
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(path)
		}
	}()

	w := bufio.NewWriter(f)
	h := crc32.NewIEEE()
	head := encodeV2Header(hdr)
	if _, err = w.Write(head); err != nil {
		return err
	}
	h.Write(head)

	if hdr.base {
		for _, e := range full {
			flag := ckptFlagPut
			if !e.present {
				flag = ckptFlagDelete
			}
			if err = writeEntry(w, h, flag, e.key, e.value, e.seq); err != nil {
				return err
			}
		}
	} else {
		for _, e := range delta {
			flag := ckptFlagPut
			if !e.present {
				flag = ckptFlagDelete
			}
			if err = writeEntry(w, h, flag, e.key, e.value, e.seq); err != nil {
				return err
			}
		}
	}
	return finishLayer(f, w, h)
}

// layerName is the on-disk name of a generation's layer file.
func layerName(gen uint64) string {
	return fmt.Sprintf("%s%016x%s", layerPrefix, gen, layerSuffix)
}

// parseLayerName reports whether name is a layer file and returns its
// generation. The zero-padded hex width is fixed.
func parseLayerName(name string) (uint64, bool) {
	if len(name) != len(layerPrefix)+16+len(layerSuffix) ||
		name[:len(layerPrefix)] != layerPrefix ||
		name[len(name)-len(layerSuffix):] != layerSuffix {
		return 0, false
	}
	var gen uint64
	hex := name[len(layerPrefix) : len(layerPrefix)+16]
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		default:
			return 0, false
		}
		gen = gen<<4 + uint64(d)
	}
	return gen, true
}

// ---- installation ----

// installBaseLayer consolidates the chain: it builds a complete generation-0
// layer in ckpt.next, then crash-atomically promotes it over the live chain
// directory (and withdraws any v1 file). Settling or sweeping the displaced
// directory is left to reconcileLayerDirs when a crash interrupts the swap;
// on the success path it is removed here.
func installBaseLayer(dir string, hdr v2Header, full []checkpointEntry) error {
	if !hdr.base || hdr.gen != 0 || uint64(hdr.count) != uint64(len(full)) {
		return errors.New("snapshot: internal error: bad base layer")
	}
	root := dir + string(os.PathSeparator)
	next := root + cpNextDir
	// A staging directory can only be debris from an interrupted checkpoint in
	// an earlier life of this process; open-time reconciliation normally
	// removes it. Rebuild it from scratch.
	if err := os.RemoveAll(next); err != nil {
		return err
	}
	if err := os.Mkdir(next, 0o700); err != nil {
		return err
	}
	if err := createLayerFile(next, layerName(0), hdr, full, nil); err != nil {
		os.RemoveAll(next)
		return err
	}
	if err := syncDirectory(next); err != nil {
		os.RemoveAll(next)
		return err
	}

	// Swap. ckpt.old can only be unsettled debris from an interrupted swap;
	// the rename onto it would otherwise fail on POSIX.
	if err := os.RemoveAll(root + cpOldDir); err != nil {
		os.RemoveAll(next)
		return err
	}
	oldExists := false
	if info, err := os.Stat(root + cpDirName); err == nil {
		oldExists = info.IsDir()
	} else if !errors.Is(err, os.ErrNotExist) {
		os.RemoveAll(next)
		return err
	}
	if oldExists {
		if err := os.Rename(root+cpDirName, root+cpOldDir); err != nil {
			os.RemoveAll(next)
			return err
		}
	}
	if err := os.Rename(root+cpNextDir, root+cpDirName); err != nil {
		// The new chain never became live. Put the old chain back when there
		// was one, so committing (and another checkpoint) can continue with
		// no repair step; otherwise the absent chain means WAL-only recovery.
		if oldExists {
			_ = os.Rename(root+cpOldDir, root+cpDirName)
		}
		os.RemoveAll(next)
		return err
	}
	// The old chain and any v1 single-file checkpoint are superseded now. A
	// crash between the rename and this force still recovers cleanly: reopen
	// sees the new complete chain and sweeps any cp.old/v1 debris. One final
	// directory force makes the promotion and every withdrawal durable.
	if err := os.RemoveAll(root + cpOldDir); err != nil {
		return err
	}
	if err := removeLegacyCheckpointFiles(dir); err != nil {
		return err
	}
	return syncDirectory(dir)
}

// installDeltaLayer appends one generation to the live chain directory.
func installDeltaLayer(dir string, hdr v2Header, delta []deltaEntry) error {
	if hdr.base || uint64(hdr.count) != uint64(len(delta)) {
		return errors.New("snapshot: internal error: bad delta layer")
	}
	live := dir + string(os.PathSeparator) + cpDirName
	// A leftover temp is staging debris from a kill mid-append; never rename
	// over the live layer with it present.
	if err := os.Remove(live + string(os.PathSeparator) + layerTmp); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := createLayerFile(live, layerTmp, hdr, nil, delta); err != nil {
		return err
	}
	if err := os.Rename(live+string(os.PathSeparator)+layerTmp,
		live+string(os.PathSeparator)+layerName(hdr.gen)); err != nil {
		os.Remove(live + string(os.PathSeparator) + layerTmp)
		return err
	}
	return syncDirectory(live)
}

// ---- reconciliation of interrupted swaps and staging debris ----

// stagedBaseIsValid reports whether a ckpt.next staging directory holds
// exactly one complete, self-describing generation-0 base layer. It is used to
// tell a base awaiting promotion (build fully synced, then a kill around the
// swap) from an incomplete build (a kill during the build), which must be
// swept rather than promoted.
func stagedBaseIsValid(nextDir string) bool {
	entries, err := os.ReadDir(nextDir)
	if err != nil {
		return false
	}
	var basePath string
	for _, e := range entries {
		if e.IsDir() {
			return false
		}
		if e.Name() == layerTmp {
			return false
		}
		gen, ok := parseLayerName(e.Name())
		if !ok {
			continue // unknown debris; a valid stage contains none
		}
		if gen != 0 || basePath != "" {
			return false
		}
		basePath = nextDir + string(os.PathSeparator) + e.Name()
	}
	if basePath == "" {
		return false
	}
	l, err := loadV2Layer(basePath)
	if err != nil {
		return false
	}
	return l.hdr.base && l.hdr.gen == 0
}

// reconcileLayerDirs settles the checkpoint directory state on open so the
// directory is immediately usable with no repair step. It resolves the
// ckpt / ckpt.next / ckpt.old swap states, sweeps delta staging temps and v1
// debris, and withdraws the v1 single-file checkpoint once a layered chain is
// present.
func reconcileLayerDirs(dir string) error {
	root := dir + string(os.PathSeparator)

	ckpt := root + cpDirName
	next := root + cpNextDir
	old := root + cpOldDir

	info, err := os.Stat(ckpt)
	ckptIsDir := err == nil && info.IsDir()
	ckptBlocked := false
	switch {
	case err == nil && !info.IsDir():
		// A foreign regular file occupies the chain name; leave it alone and
		// treat the chain as absent rather than deleting user data.
		ckptBlocked = true
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return err
	}

	if ckptIsDir {
		// The live chain is present: anything staged is debris of an
		// interrupted consolidation that settled on the pre-swap state.
		if err := os.RemoveAll(next); err != nil {
			return err
		}
		if err := os.RemoveAll(old); err != nil {
			return err
		}
	} else {
		// Any staged directories are meaningless while a foreign file blocks
		// the chain name; clear them and recover from the WAL.
		if ckptBlocked {
			if err := os.RemoveAll(next); err != nil {
				return err
			}
			if err := os.RemoveAll(old); err != nil {
				return err
			}
		} else {
			nextInfo, nerr := os.Stat(next)
			switch {
			case nerr != nil && !errors.Is(nerr, os.ErrNotExist):
				return nerr
			case nerr == nil && nextInfo.IsDir() && stagedBaseIsValid(next):
				// A promotion was in flight and the staged base is a complete
				// layer, so finish the promotion and discard the displaced
				// chain.
				if err := os.RemoveAll(old); err != nil {
					return err
				}
				if err := os.Rename(next, ckpt); err != nil {
					return err
				}
				ckptIsDir = true
			default:
				// No usable staged base: either none exists, or it holds an
				// incomplete build (a kill during the build, before any
				// rename). Such a directory is debris and must never be
				// promoted; remove it. If the displaced chain remains from a
				// failed rename, restore it instead.
				if nerr == nil {
					if err := os.RemoveAll(next); err != nil {
						return err
					}
				}
				if oldInfo, oerr := os.Stat(old); oerr == nil && oldInfo.IsDir() {
					if err := os.Rename(old, ckpt); err != nil {
						return err
					}
					ckptIsDir = true
				} else if oerr != nil && !errors.Is(oerr, os.ErrNotExist) {
					return oerr
				}
			}
		}
	}

	// Sweep a half-built delta temp. It is never read under its temp name.
	if err := os.Remove(root + cpDirName + string(os.PathSeparator) + layerTmp); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := removeStaleCheckpointTmp(dir); err != nil {
		return err
	}
	// A layered chain supersedes any v1 file left from an upgrade interrupted
	// after the directory swap.
	if ckptIsDir {
		if err := removeLegacyCheckpointFiles(dir); err != nil {
			return err
		}
	}
	return syncDirectory(dir)
}

// removeStaleCheckpointTmp deletes a half-built v1 checkpoint temp. Its
// absence is not an error.
func removeStaleCheckpointTmp(dir string) error {
	err := os.Remove(dir + string(os.PathSeparator) + checkpointTmpName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removeLegacyCheckpointFiles removes v1 checkpoint artifacts.
func removeLegacyCheckpointFiles(dir string) error {
	root := dir + string(os.PathSeparator)
	if err := os.Remove(root + checkpointName); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	return removeStaleCheckpointTmp(dir)
}

// retireCheckpointChain durably withdraws every checkpoint artifact before
// the WAL is replaced. Layer cuts are meaningful only against the exact log
// they were captured against, so the removals (and the directory force) must
// be durable before the replacement log is renamed into place: after a crash
// at any instant no layer can be found paired with a different WAL. Absence
// is normal and not an error.
func retireCheckpointChain(dir string) error {
	root := dir + string(os.PathSeparator)
	for _, name := range []string{cpDirName, cpNextDir, cpOldDir} {
		if err := os.RemoveAll(root + name); err != nil {
			return err
		}
	}
	if err := removeLegacyCheckpointFiles(dir); err != nil {
		return err
	}
	return syncDirectory(dir)
}

// ---- reading ----

// loadV2Layer reads and self-validates one layer file. Any defect (truncation,
// bad checksum, unknown fields, malformed entry, duplicate or unsorted key,
// implausible sequence or trailing bytes) rejects the whole file.
func loadV2Layer(path string) (l v2Layer, err error) {
	f, oerr := os.Open(path)
	if oerr != nil {
		return v2Layer{}, oerr
	}
	defer f.Close()

	reject := func(err error) (v2Layer, error) {
		return v2Layer{}, errCheckpointRejected(err)
	}

	r := bufio.NewReader(f)
	h := crc32.NewIEEE()

	head := make([]byte, v2HeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return reject(err)
	}
	h.Write(head)
	if binary.LittleEndian.Uint32(head[0:4]) != checkpointMagic {
		return reject(errors.New("snapshot: bad layer magic"))
	}
	if ver := binary.LittleEndian.Uint32(head[4:8]); ver != checkpointVersion2 {
		return reject(fmt.Errorf("snapshot: unknown layer version %d", ver))
	}
	l.hdr.gen = binary.LittleEndian.Uint64(head[8:16])
	flags := binary.LittleEndian.Uint32(head[16:20])
	if flags&^layerFlagBase != 0 {
		return reject(errors.New("snapshot: unknown layer flags"))
	}
	l.hdr.base = flags&layerFlagBase != 0
	l.hdr.wm = binary.LittleEndian.Uint64(head[20:28])
	l.hdr.prevWm = binary.LittleEndian.Uint64(head[28:36])
	l.hdr.snaps = binary.LittleEndian.Uint64(head[36:44])
	off := binary.LittleEndian.Uint64(head[44:52])
	l.hdr.offset = int64(off)
	l.hdr.count = binary.LittleEndian.Uint32(head[52:56])
	if uint64(l.hdr.offset) != off || l.hdr.count > maxRecord {
		return reject(errors.New("snapshot: layer header out of range"))
	}
	if l.hdr.prevWm > l.hdr.wm {
		return reject(errors.New("snapshot: layer watermarks out of order"))
	}
	if l.hdr.base {
		if l.hdr.gen != 0 || l.hdr.prevWm != 0 {
			return reject(errors.New("snapshot: malformed base layer header"))
		}
	} else {
		if l.hdr.gen == 0 {
			return reject(errors.New("snapshot: delta layer at generation zero"))
		}
	}

	ehdr := make([]byte, entryHdrSize)
	prevKey := ""
	seenSeq := make(map[uint64]bool, l.hdr.count)
	for i := uint32(0); i < l.hdr.count; i++ {
		if _, err := io.ReadFull(r, ehdr); err != nil {
			return reject(err)
		}
		h.Write(ehdr)
		flag := ehdr[0]
		keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
		valLen := binary.LittleEndian.Uint32(ehdr[5:9])
		seq := binary.LittleEndian.Uint64(ehdr[9:17])
		if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
			keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
			(flag == ckptFlagDelete && valLen != 0) || seq == 0 || seq > l.hdr.wm {
			return reject(errors.New("snapshot: malformed layer entry"))
		}
		if !l.hdr.base && seq <= l.hdr.prevWm {
			return reject(errors.New("snapshot: delta entry outside its watermark span"))
		}
		if seenSeq[seq] {
			return reject(errors.New("snapshot: duplicate sequence in layer"))
		}
		seenSeq[seq] = true
		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			return reject(err)
		}
		h.Write(body)
		key := string(body[:keyLen])
		if key <= prevKey {
			return reject(errors.New("snapshot: layer keys not strictly ordered"))
		}
		prevKey = key
		present := flag == ckptFlagPut
		if l.hdr.base {
			e := checkpointEntry{key: key, seq: seq, present: present}
			if present {
				e.value = cloneBytes(body[keyLen:])
			}
			l.full = append(l.full, e)
		} else {
			e := deltaEntry{key: key, seq: seq, present: present}
			if present {
				e.value = cloneBytes(body[keyLen:])
			}
			l.delta = append(l.delta, e)
		}
	}

	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return reject(err)
	}
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return reject(errors.New("snapshot: layer checksum mismatch"))
	}
	// Nothing may trail the terminal checksum.
	if b, err := r.ReadByte(); err == nil {
		_ = b
		return reject(errors.New("snapshot: layer has trailing bytes"))
	} else if !errors.Is(err, io.EOF) {
		return reject(err)
	}
	return l, nil
}

// loadLayerChain discovers, deep-loads and cross-validates the live layer
// chain. It returns the longest intact generation-0-based prefix in
// generation order; a bad layer and every generation above it are withdrawn.
// A missing chain directory yields (nil, nil). Every accepted layer's cut
// must lie inside the current WAL; a chain whose base does not pair with the
// WAL is rejected wholesale.
func loadLayerChain(dir string, walLen int64) ([]v2Layer, error) {
	live := dir + string(os.PathSeparator) + cpDirName
	entries, err := os.ReadDir(live)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrInvalid) {
			return nil, nil // no chain directory, or a foreign file at its path
		}
		return nil, err
	}

	loaded := make(map[uint64]v2Layer)
	bad := make(map[uint64]bool)
	var maxGen uint64     // highest generation that parsed successfully
	var maxNameGen uint64 // highest generation seen in any layer file name
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		gen, ok := parseLayerName(e.Name())
		if !ok {
			continue // unknown debris is ignored; never treated as state
		}
		if gen > maxNameGen {
			maxNameGen = gen
		}
		l, lerr := loadV2Layer(live + string(os.PathSeparator) + e.Name())
		if lerr != nil {
			bad[gen] = true
			continue
		}
		if l.hdr.gen != gen {
			bad[gen] = true
			continue
		}
		loaded[gen] = l
		if gen > maxGen {
			maxGen = gen
		}
	}

	var chain []v2Layer
	stop := -1 // first generation that cannot be accepted
	prev := v2Layer{}
	for gen := uint64(0); gen <= maxGen; gen++ {
		l, ok := loaded[gen]
		if !ok || bad[gen] {
			stop = int(gen)
			break
		}
		if gen == 0 {
			if !l.hdr.base {
				stop = 0
				break
			}
		} else {
			if l.hdr.base ||
				l.hdr.prevWm != prev.hdr.wm ||
				l.hdr.wm < prev.hdr.wm ||
				l.hdr.snaps < prev.hdr.snaps ||
				l.hdr.offset < prev.hdr.offset {
				stop = int(gen)
				break
			}
		}
		if l.hdr.offset < 0 || l.hdr.offset > walLen {
			stop = int(gen)
			break
		}
		chain = append(chain, l)
		prev = l
	}

	// Withdraw every layer that is not part of the intact run: a bad layer, a
	// gap, anything above the first layer that no longer pairs with this WAL,
	// and any corrupt/orphaned file named with a higher generation than the
	// intact tip. The accepted prefix stays live and pairs with the unchanged
	// WAL on its own.
	lo := maxGen + 1 // when the whole run was accepted, only higher debris drops
	if stop >= 0 {
		lo = uint64(stop)
	}
	if stop >= 0 || maxNameGen > maxGen {
		for gen := lo; gen <= maxNameGen; gen++ {
			if err := os.Remove(live + string(os.PathSeparator) + layerName(gen)); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
		if err := syncDirectory(live); err != nil {
			return nil, err
		}
	}
	return chain, nil
}

// ---- version 1 (single-file) reader, kept for directories written by older
// versions. New code never writes this format. ----

// loadCheckpointV1 reads and validates the legacy snapshot.ckpt. A missing
// file is not an error: ok is false with nil error.
func loadCheckpointV1(dir string) (st capturedState, ok bool, err error) {
	f, rerr := os.Open(dir + string(os.PathSeparator) + checkpointName)
	if rerr != nil {
		if errors.Is(rerr, os.ErrNotExist) {
			return capturedState{}, false, nil
		}
		return capturedState{}, false, rerr
	}
	defer f.Close()

	reject := func(err error) (capturedState, bool, error) {
		return capturedState{}, false, errCheckpointRejected(err)
	}

	r := bufio.NewReader(f)
	h := crc32.NewIEEE()

	head := make([]byte, v1HeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return reject(err)
	}
	h.Write(head)

	magic := binary.LittleEndian.Uint32(head[0:4])
	version := binary.LittleEndian.Uint32(head[4:8])
	if magic != checkpointMagic || version != checkpointVersion1 {
		return reject(errors.New("snapshot: unknown checkpoint format"))
	}
	st.seq = binary.LittleEndian.Uint64(head[8:16])
	st.snaps = binary.LittleEndian.Uint64(head[16:24])
	off := binary.LittleEndian.Uint64(head[24:32])
	count := binary.LittleEndian.Uint32(head[32:36])
	if count > maxRecord {
		return reject(errors.New("snapshot: checkpoint entry count out of range"))
	}
	st.offset = int64(off)
	if uint64(st.offset) != off {
		return reject(errors.New("snapshot: checkpoint offset out of range"))
	}

	st.entries = make([]checkpointEntry, 0, count)
	ehdr := make([]byte, entryHdrSize)
	seen := make(map[string]bool, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, ehdr); err != nil {
			return reject(err)
		}
		h.Write(ehdr)
		flag := ehdr[0]
		keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
		valLen := binary.LittleEndian.Uint32(ehdr[5:9])
		seq := binary.LittleEndian.Uint64(ehdr[9:17])
		if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
			keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
			(flag == ckptFlagDelete && valLen != 0) {
			return reject(errors.New("snapshot: malformed checkpoint entry"))
		}
		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			return reject(err)
		}
		h.Write(body)
		key := string(body[:keyLen])
		if seen[key] {
			return reject(errors.New("snapshot: duplicate key in checkpoint"))
		}
		seen[key] = true
		e := checkpointEntry{key: key, seq: seq, present: flag == ckptFlagPut}
		if e.present {
			e.value = cloneBytes(body[keyLen:])
		}
		st.entries = append(st.entries, e)
	}

	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return reject(err)
	}
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return reject(errors.New("snapshot: checkpoint checksum mismatch"))
	}
	if b, err := r.ReadByte(); err == nil {
		_ = b
		return reject(errors.New("snapshot: checkpoint has trailing bytes"))
	} else if !errors.Is(err, io.EOF) {
		return reject(err)
	}
	return st, true, nil
}

// removeRejectedV1Checkpoint discards a legacy file that failed validation and
// forces the directory. The WAL alone remains a complete source of truth.
func removeRejectedV1Checkpoint(dir string) {
	if err := os.Remove(dir + string(os.PathSeparator) + checkpointName); err != nil {
		return
	}
	_ = syncDirectory(dir)
}
