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

// On-disk checkpoint chain.
//
// A checkpoint freezes one complete committed state so reopening can seed
// memory from it and replay only the write-ahead log written afterwards.
// Rather than rewriting one monolithic file for every checkpoint, this
// version keeps a chain of self-describing layers inside the checkpoint
// directory (checkpointDirName):
//
//   - Layer 0 (the base) holds the complete state at its capture time.
//   - Every later layer is a delta covering only the keys touched by commits
//     strictly after the layer it builds on.
//
// Reopen loads layers in index order, stopping at the last complete, intact
// layer of one unbroken chain, then replays the WAL tail from that layer's
// cut. Any layer that is missing, truncated, checksum-bad, version-unknown or
// whose chain linkage is broken rejects that layer and every later one as a
// whole: recovery falls back to the previous usable layer, or to a from-WAL
// reopen when the base itself is unusable.
//
// Each layer file is independently an all-or-nothing artifact: it is built
// under a temporary name, forced whole with one fsync, then atomically
// renamed into place. A layer's content hashes (CRC-32) link it to the layer
// before it, so deleting or replacing one layer invalidates the suffix; the
// valid prefix is always usable on its own.
//
// Layer layout (all integers little-endian):
//
//	magic     uint32   checkpointMagic
//	version   uint32   checkpointVersion2
//	index     uint32   this layer's position; 0 for the base
//	prevCRC   uint32   content CRC of the previous layer (0 for the base)
//	seq       uint64   highest commit sequence covered at capture time
//	snapshots uint64   cumulative number of snapshots covered
//	offset    uint64   WAL byte offset of the cut: frames at or past this
//	                  offset form the post-layer tail replayed on reopen
//	count     uint32   number of entries that follow
//	entries   [count]entry
//	crc       uint32   IEEE CRC-32 of every preceding byte of this file
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the entry's commit sequence (uint64), the key, then the
// value. The base lists every live key exactly once; a delta lists only
// changed keys, with each key appearing at most once.
//
// The cut offset is measured in the WAL the chain is paired with. A checkpoint
// never modifies the WAL; compaction (the only WAL rewrite) first durably
// withdraws the chain and then re-establishes surviving layers against the
// replacement log before its rename becomes state, so a layer is never read
// against a different WAL than the one its cut describes.
const (
	checkpointDirName = "ckpt"

	// checkpointName/checkpointTmpName are the version-1 single-file layout.
	// They stay readable so a directory written by the old version reopens
	// unchanged; the first checkpoint taken by this version upgrades it into
	// the layered directory and removes the old file.
	checkpointName    = "snapshot.ckpt"
	checkpointTmpName = "snapshot.ckpt.tmp"

	// layerTmpSuffix stages a layer while it is built; it never appears under a
	// final layer name. A file carrying it is crash debris swept on the next
	// open.
	layerTmpSuffix = ".tmp"

	checkpointMagic    uint32 = 0x504b4353 // bytes "SCKP"
	checkpointVersion1 uint32 = 1
	checkpointVersion2 uint32 = 2

	checkpointHeaderSize      = 4 + 4 + 8 + 8 + 8 + 4
	layer2HeaderSize          = 4 + 4 + 4 + 4 + 8 + 8 + 8 + 4
	checkpointEntrySize       = 1 + 4 + 4 + 8 // flags, keyLen, valLen, seq
	ckptFlagPut          byte = 0
	ckptFlagDelete       byte = 1
)

// checkpointEntry is one entry in a layer: a terminal key state.
type checkpointEntry struct {
	key     string
	value   []byte
	seq     uint64
	present bool
}

// layerState is the decoded content of one validated layer plus the content
// hash that links the next layer back to it.
type layerState struct {
	index   uint32
	prevCRC uint32
	entries []checkpointEntry
	offset  int64 // WAL cut of this layer
	seq     uint64
	snaps   uint64
	crc     uint32 // content CRC of this layer (the terminal checksum it carries)
}

// capturedLayer is what Checkpoint serializes for one new layer.
type capturedLayer struct {
	index   uint32
	prevCRC uint32
	entries []checkpointEntry
	offset  uint64
	seq     uint64
	snaps   uint64
}

func layerFileName(index uint32) string {
	return filepath.Join(checkpointDirName, formatLayerIndex(index))
}

func layerTmpName(index uint32) string {
	return layerFileName(index) + layerTmpSuffix
}

// formatLayerIndex renders a layer index as a fixed-width, zero-padded name so
// the on-disk listing is already in chain order.
func formatLayerIndex(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, 10)
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[index%10]
		index /= 10
	}
	return string(buf) + ".ckpt"
}

// parseLayerIndex is the inverse of formatLayerIndex. ok is false for names
// that are not final layer files.
func parseLayerIndex(name string) (uint32, bool) {
	if len(name) != len("0000000000.ckpt") || name[10:] != ".ckpt" {
		return 0, false
	}
	var n uint32
	for i := 0; i < 10; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	return n, true
}

func checkpointDirPath(dir string) string {
	return filepath.Join(dir, checkpointDirName)
}

// errCheckpointRejected marks any validation failure. It is never surfaced to
// the caller of Open: a rejected layer is an expected outcome handled by
// falling back to a shorter chain or a WAL-only reopen.
func errCheckpointRejected(err error) error {
	return errors.Join(errors.New("snapshot: rejecting checkpoint layer"), err)
}

// captureView serializes the terminal state of a view, tombstones included
// (a base layer must reproduce deletions, not just live values). Entries are
// sorted by key for a canonical encoding and values are copied so later
// commits cannot mutate the bytes being written.
func captureView(v *view) []checkpointEntry {
	var entries []checkpointEntry
	v.rangeEach(func(k string, n *node) bool {
		e := checkpointEntry{key: k, present: n.present, seq: n.seq}
		if n.present {
			e.value = cloneBytes(n.value)
		}
		entries = append(entries, e)
		return true
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return entries
}

// captureDelta serializes only the keys in dirtyKeys, reading their terminal
// state from v. Absent keys are skipped: a delta never carries a "never
// existed" entry. Output is sorted by key and de-duplicated.
func captureDelta(v *view, dirtyKeys []string) []checkpointEntry {
	seen := make(map[string]struct{}, len(dirtyKeys))
	keys := make([]string, 0, len(dirtyKeys))
	for _, k := range dirtyKeys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	entries := make([]checkpointEntry, 0, len(keys))
	sort.Strings(keys)
	for _, k := range keys {
		n := v.lookup(k)
		if n == nil {
			continue
		}
		e := checkpointEntry{key: k, present: n.present, seq: n.seq}
		if n.present {
			e.value = cloneBytes(n.value)
		}
		entries = append(entries, e)
	}
	return entries
}

// writeLayerFile serializes layer to relName freshly (O_EXCL), forces the
// whole file with one fsync and closes it. It never touches a live layer; the
// caller renames it into place only once this succeeds. It returns the
// content CRC written into the terminal field, which links the next layer.
func writeLayerFile(dir, relName string, layer capturedLayer) (crc uint32, err error) {
	path := filepath.Join(dir, relName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(path)
		}
	}()

	w := bufio.NewWriter(f)
	head := make([]byte, layer2HeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(head[4:8], checkpointVersion2)
	binary.LittleEndian.PutUint32(head[8:12], layer.index)
	binary.LittleEndian.PutUint32(head[12:16], layer.prevCRC)
	binary.LittleEndian.PutUint64(head[16:24], layer.seq)
	binary.LittleEndian.PutUint64(head[24:32], layer.snaps)
	binary.LittleEndian.PutUint64(head[32:40], layer.offset)
	binary.LittleEndian.PutUint32(head[40:44], uint32(len(layer.entries)))

	h := crc32.NewIEEE()
	if _, err = w.Write(head); err != nil {
		return 0, err
	}
	h.Write(head)

	ehdr := make([]byte, checkpointEntrySize)
	for _, e := range layer.entries {
		if len(e.key) > maxRecord || len(e.value) > maxRecord {
			return 0, errors.New("snapshot: checkpoint entry too large")
		}
		if e.present {
			ehdr[0] = ckptFlagPut
		} else {
			ehdr[0] = ckptFlagDelete
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(e.key)))
		binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(e.value)))
		binary.LittleEndian.PutUint64(ehdr[9:17], e.seq)
		if _, err = w.Write(ehdr); err != nil {
			return 0, err
		}
		h.Write(ehdr)
		if _, err = io.WriteString(w, e.key); err != nil {
			return 0, err
		}
		h.Write([]byte(e.key))
		if len(e.value) > 0 {
			if _, err = w.Write(e.value); err != nil {
				return 0, err
			}
			h.Write(e.value)
		}
	}

	crc = h.Sum32()
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], crc)
	if _, err = w.Write(crcb[:]); err != nil {
		return 0, err
	}
	if err = w.Flush(); err != nil {
		return 0, err
	}
	if err = f.Sync(); err != nil {
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	return crc, nil
}

// loadLayer reads and fully validates one layer file. It returns the decoded
// state including the terminal content hash. Any defect (missing file is the
// caller's concern, truncation, bad magic/version, bad checksum, malformed
// field, duplicated key, trailing bytes) yields a rejection error.
func loadLayer(dir, relName string, wantIndex uint32) (layerState, error) {
	f, err := os.Open(filepath.Join(dir, relName))
	if err != nil {
		return layerState{}, err
	}
	defer f.Close()

	reject := func(err error) (layerState, error) {
		return layerState{}, errCheckpointRejected(err)
	}

	r := bufio.NewReader(f)
	h := crc32.NewIEEE()

	head := make([]byte, layer2HeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return reject(err)
	}
	h.Write(head)

	magic := binary.LittleEndian.Uint32(head[0:4])
	version := binary.LittleEndian.Uint32(head[4:8])
	index := binary.LittleEndian.Uint32(head[8:12])
	if magic != checkpointMagic || version != checkpointVersion2 {
		return reject(errors.New("snapshot: unknown checkpoint format"))
	}
	if index != wantIndex {
		return reject(errors.New("snapshot: checkpoint layer index out of order"))
	}
	var st layerState
	st.index = index
	st.prevCRC = binary.LittleEndian.Uint32(head[12:16])
	st.seq = binary.LittleEndian.Uint64(head[16:24])
	st.snaps = binary.LittleEndian.Uint64(head[24:32])
	off := binary.LittleEndian.Uint64(head[32:40])
	count := binary.LittleEndian.Uint32(head[40:44])
	if count > maxRecord {
		return reject(errors.New("snapshot: checkpoint entry count out of range"))
	}
	st.offset = int64(off)
	if uint64(st.offset) != off || st.offset < 0 {
		return reject(errors.New("snapshot: checkpoint offset out of range"))
	}

	st.entries = make([]checkpointEntry, 0, count)
	ehdr := make([]byte, checkpointEntrySize)
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
			return reject(errors.New("snapshot: duplicate key in checkpoint layer"))
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
	st.crc = binary.LittleEndian.Uint32(crcb[:])
	if st.crc != h.Sum32() {
		return reject(errors.New("snapshot: checkpoint checksum mismatch"))
	}
	if b, err := r.ReadByte(); err == nil {
		_ = b
		return reject(errors.New("snapshot: checkpoint layer has trailing bytes"))
	} else if !errors.Is(err, io.EOF) {
		return reject(err)
	}
	return st, nil
}

// loadCheckpointChain discovers, validates and loads the layered chain. It
// returns the loaded layers (a dense prefix 0..n-1), the index of the next
// layer to write, and the content hash the new layer must link to. An absent
// or unusable chain is not an error: layers is empty and the caller either
// tries the version-1 file or reopens from the WAL.
//
// Corruption in layer k (or a gap before it) makes layer k and every later
// layer unusable. The caller is responsible for removing the rejected suffix;
// this function only reports the boundary.
func loadCheckpointChain(dir string) (layers []layerState, nextIndex uint32, prevCRC uint32, err error) {
	entries, rerr := os.ReadDir(checkpointDirPath(dir))
	if rerr != nil {
		if errors.Is(rerr, os.ErrNotExist) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, rerr
	}
	// Collect final layer files only; temp debris and unrelated files are
	// ignored by chain assembly and swept elsewhere.
	have := make(map[uint32]string)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseLayerIndex(e.Name()); ok {
			have[idx] = filepath.Join(checkpointDirName, e.Name())
		}
	}
	var expectCRC uint32
	for index := uint32(0); ; index++ {
		rel, ok := have[index]
		if !ok {
			return layers, index, expectCRC, nil
		}
		st, lerr := loadLayer(dir, rel, index)
		if lerr != nil {
			return layers, index, expectCRC, nil
		}
		if index == 0 {
			// Base must stand alone.
			if st.prevCRC != 0 {
				return layers, index, expectCRC, nil
			}
		} else if st.prevCRC != expectCRC {
			// Suffix no longer links to the prefix: reject from here.
			return layers, index, expectCRC, nil
		}
		layers = append(layers, st)
		expectCRC = st.crc
	}
}

// loadLegacyCheckpoint reads the version-1 single-file checkpoint. A missing
// file reports ok=false with nil error. It exists solely to reopen directories
// written by the old single-file version; the first new checkpoint upgrades
// them.
func loadLegacyCheckpoint(dir string) (st capturedState, ok bool, err error) {
	f, rerr := os.Open(filepath.Join(dir, checkpointName))
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

	head := make([]byte, checkpointHeaderSize)
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
	ehdr := make([]byte, checkpointEntrySize)
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

// capturedState is a legacy version-1 checkpoint's full content.
type capturedState struct {
	entries []checkpointEntry
	offset  int64
	seq     uint64
	snaps   uint64
}

// installLayer builds one complete layer in its temp name and atomically
// renames it into place, forcing the checkpoint directory afterwards. It
// touches neither the WAL nor the live writer: a crash at any point leaves
// either the previous chain or the chain extended by one complete layer.
func installLayer(dir string, layer capturedLayer) error {
	cdir := checkpointDirPath(dir)
	if err := os.MkdirAll(cdir, 0o700); err != nil {
		return err
	}
	tmp := layerTmpName(layer.index)
	final := layerFileName(layer.index)
	if err := os.Remove(filepath.Join(dir, tmp)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := writeLayerFile(dir, tmp, layer); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(dir, tmp), filepath.Join(dir, final)); err != nil {
		os.Remove(filepath.Join(dir, tmp))
		return err
	}
	// A crash before this force lands either the chain without the new layer
	// or with it fully present; both reopen on their own.
	return syncDirectory(cdir)
}

// removeLayerFrom deletes layer index and any temp for it, best effort. Used
// while retiring a rejected suffix and during compaction's chain rebuild.
func removeLayerFrom(dir string, index uint32) {
	os.Remove(filepath.Join(dir, layerFileName(index)))
	os.Remove(filepath.Join(dir, layerTmpName(index)))
}

// sweepCheckpointDebris removes every half-built layer temp in the checkpoint
// directory (staging debris from a kill between the build and the rename) and
// the legacy version-1 temp. Final layer files and the WAL are untouched.
func sweepCheckpointDebris(dir string) error {
	cdir := checkpointDirPath(dir)
	entries, err := os.ReadDir(cdir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return removeLegacyTemp(dir)
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > len(layerTmpSuffix) && name[len(name)-len(layerTmpSuffix):] == layerTmpSuffix {
			if err := os.Remove(filepath.Join(cdir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return removeLegacyTemp(dir)
}

// removeLegacyTemp deletes a half-built version-1 checkpoint temp.
func removeLegacyTemp(dir string) error {
	err := os.Remove(filepath.Join(dir, checkpointTmpName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removeRejectedChainSuffix deletes every layer at or after firstBad and
// forces the checkpoint directory, so a broken layer and the layers that
// depend on it neither linger nor occupy space. The valid prefix and the WAL
// remain complete sources of truth.
func removeRejectedChainSuffix(dir string, firstBad uint32) {
	cdir := checkpointDirPath(dir)
	for index := firstBad; ; index++ {
		if _, err := os.Stat(filepath.Join(dir, layerFileName(index))); err != nil {
			break
		}
		removeLayerFrom(dir, index)
	}
	_ = syncDirectory(cdir)
}

// removeRejectedLegacy discards a version-1 checkpoint that failed validation.
func removeRejectedLegacy(dir string) {
	if err := os.Remove(filepath.Join(dir, checkpointName)); err != nil {
		return
	}
	_ = syncDirectory(dir)
}

// removeLegacyCheckpoint withdraws the version-1 file during an upgrade to the
// layered layout and forces both directories so the removal is durable before
// the new base is relied upon.
func removeLegacyCheckpoint(dir string) error {
	path := filepath.Join(dir, checkpointName)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	return nil
}

// retireChain durably withdraws the entire layered chain (and any legacy
// checkpoint) while the WAL it pairs with is still in place. Compaction calls
// it before the WAL rename so a layer can never be found against a different
// WAL. The checkpoint directory itself is removed when empty. Its absence is
// normal and not an error.
func retireChain(dir string) error {
	if err := removeLegacyCheckpoint(dir); err != nil {
		return err
	}
	cdir := checkpointDirPath(dir)
	if err := os.RemoveAll(cdir); err != nil {
		return err
	}
	return syncDirectory(dir)
}

// chainInfo is a compact view of the on-disk chain used by the store.
type chainInfo struct {
	layers    []layerState
	nextIndex uint32
	prevCRC   uint32
}

// readChain loads the layered chain. A missing chain returns an empty info.
func readChain(dir string) (chainInfo, error) {
	layers, nextIndex, prevCRC, err := loadCheckpointChain(dir)
	if err != nil {
		return chainInfo{}, err
	}
	return chainInfo{layers: layers, nextIndex: nextIndex, prevCRC: prevCRC}, nil
}

// pruneRejectedChainFiles removes every final layer file whose index is at or
// beyond keep: those layers are missing, corrupt, version-unknown or unlinked
// to the valid prefix and are rejected as a whole. The valid prefix (indices
// below keep) and the WAL are left intact. It is best effort and idempotent.
func pruneRejectedChainFiles(dir string, keep int) {
	entries, err := os.ReadDir(checkpointDirPath(dir))
	if err != nil {
		return
	}
	var bad []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseLayerIndex(e.Name()); ok && int(idx) >= keep {
			bad = append(bad, idx)
		}
	}
	if len(bad) == 0 {
		return
	}
	sort.Slice(bad, func(i, j int) bool { return bad[i] < bad[j] })
	removeRejectedChainSuffix(dir, bad[0])
}

// sweepStagedChain removes a replacement-chain staging directory left by a
// compaction killed before it was promoted. Only checkpointDirName is ever
// read, so a stale staged directory is pure debris.
func sweepStagedChain(dir string) error {
	err := os.RemoveAll(filepath.Join(dir, stagedChainDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
