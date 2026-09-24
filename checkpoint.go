package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// On-disk checkpoint: a self-describing, checksummed freeze of one complete
// committed state that lets reopening skip replaying the whole write-ahead
// log. It is an accelerator only — wal.log stays the complete record of
// history, so a checkpoint that is missing, truncated, corrupted or written
// by an unknown format version is simply ignored and the store recovers by
// replaying the log from the beginning.
//
// Layout, little-endian:
//
//	header:
//	  magic          uint32   checkpointMagic
//	  formatVersion  uint32   checkpointFormatV1
//	  snapshotCount  uint64   cumulative snapshots handed out so far
//	  nextSeq        uint64   sequence watermark: highest commit seq used
//	  walOffset      uint64   byte offset in wal.log where later history starts
//	  entryCount     uint32   number of live-key entries
//	entries, in ascending key order, each:
//	  keyLen         uint32
//	  valLen         uint32
//	  seq            uint64   commit seq of the key's terminal version
//	  key            [keyLen]byte
//	  value          [valLen]byte (may be empty: an empty value is still a hit)
//	trailer:
//	  crc            uint32   IEEE CRC-32 of every preceding byte
//
// Only live (present) keys are stored: a deleted key is part of no terminal
// state, and post-checkpoint frames for it ride the log tail like any other
// change. walOffset is a frame boundary; frames at and after it are replayed
// on top of the frozen entries, which yields the exact state a full replay of
// wal.log would reach.
//
// The checkpoint is written to checkpointTmpName, forced to stable storage,
// and only then atomically renamed over checkpointName, with the directory
// forced afterwards. A crash at any point therefore leaves either no
// checkpoint (or the previous complete one) plus an intact wal.log; a
// half-built temp file is never consulted and is swept on the next open.
const (
	checkpointName    = "checkpoint"
	checkpointTmpName = "checkpoint.tmp"

	checkpointMagic   uint32 = 0x434b5054 // "TCKC"
	checkpointVersion uint32 = 1

	checkpointHeaderSize  = 4 + 4 + 8 + 8 + 8 + 4
	checkpointEntryHeader = 4 + 4 + 8
)

// checkpointEntry is one live key's terminal state.
type checkpointEntry struct {
	key   string
	seq   uint64
	value []byte
}

// checkpointData is everything a checkpoint freeze captures.
type checkpointData struct {
	snapshotCount uint64
	nextSeq       uint64
	walOffset     int64
	entries       []checkpointEntry // sorted by key
}

// buildCheckpointData snapshots the fields a freeze needs. The caller must
// hold s.mu so the view, watermark and log end agree on one committed instant.
func (s *Store) buildCheckpointData() (*checkpointData, error) {
	cur := *s.current.Load()
	keys := make([]string, 0, len(cur))
	for k, n := range cur {
		if n.present {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	entries := make([]checkpointEntry, 0, len(keys))
	for _, k := range keys {
		n := cur[k]
		entries = append(entries, checkpointEntry{key: k, seq: n.seq, value: cloneBytes(n.value)})
	}
	// No frame is buffered between committed groups (every commit flushes and
	// syncs), so the physical end of the log is exactly the next frame
	// boundary and is a safe replay start.
	off, err := s.wal.position()
	if err != nil {
		return nil, err
	}
	return &checkpointData{
		snapshotCount: s.snapshots.Load(),
		nextSeq:       s.nextSeq,
		walOffset:     off,
		entries:       entries,
	}, nil
}

// encodeCheckpoint serializes one complete checkpoint.
func encodeCheckpoint(d *checkpointData) ([]byte, error) {
	if d.walOffset < 0 || len(d.entries) > maxRecord {
		return nil, fmt.Errorf("snapshot: checkpoint out of range")
	}
	size := checkpointHeaderSize + crcSize
	for i := range d.entries {
		e := &d.entries[i]
		if len(e.key) == 0 || len(e.key) > maxRecord || len(e.value) > maxRecord {
			return nil, fmt.Errorf("snapshot: checkpoint entry too large")
		}
		size += checkpointEntryHeader + len(e.key) + len(e.value)
	}

	buf := make([]byte, 0, size)
	put := func(p []byte) { buf = append(buf, p...) }
	var hb [4]byte
	putU32 := func(v uint32) {
		binary.LittleEndian.PutUint32(hb[:], v)
		put(hb[:])
	}
	var b8 [8]byte
	putU64 := func(v uint64) {
		binary.LittleEndian.PutUint64(b8[:], v)
		put(b8[:])
	}

	putU32(checkpointMagic)
	putU32(checkpointVersion)
	putU64(d.snapshotCount)
	putU64(d.nextSeq)
	putU64(uint64(d.walOffset))
	putU32(uint32(len(d.entries)))
	for i := range d.entries {
		e := &d.entries[i]
		putU32(uint32(len(e.key)))
		putU32(uint32(len(e.value)))
		putU64(e.seq)
		put([]byte(e.key))
		put(e.value)
	}
	h := crc32.NewIEEE()
	h.Write(buf)
	putU32(h.Sum32())
	return buf, nil
}

// writeCheckpointTemp writes and durably closes the complete checkpoint under
// the temp name, without touching the live checkpoint or wal.log. Any stale
// temp from an interrupted earlier freeze is removed first.
func writeCheckpointTemp(dir string, d *checkpointData) error {
	if err := removeStaleCheckpointTemp(dir); err != nil {
		return err
	}
	buf, err := encodeCheckpoint(d)
	if err != nil {
		return err
	}
	path := dir + string(os.PathSeparator) + checkpointTmpName
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(path)
		return err
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(buf); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

// activateCheckpoint makes a previously written temp checkpoint state of
// record: the rename is atomic and is then forced through the directory. A
// crash between the two still leaves either the old or the new complete
// checkpoint, never a partial one (renaming over the previous checkpoint also
// reclaims its space in the same step). The checkpoint file is never held open
// by the store, so the rename succeeds everywhere.
func activateCheckpoint(dir string) error {
	if err := renameCheckpoint(dir); err != nil {
		return err
	}
	return syncDirectory(dir)
}

// renameCheckpoint performs only the atomic rename half of activation, so
// Store.Checkpoint can expose the post-rename/pre-directory-sync crash window
// to its interrupt hook. On failure the staged temp is removed and the live
// checkpoint is left untouched.
func renameCheckpoint(dir string) error {
	path := dir + string(os.PathSeparator)
	if err := os.Rename(path+checkpointTmpName, path+checkpointName); err != nil {
		os.Remove(path + checkpointTmpName)
		return err
	}
	return nil
}

// deactivateCheckpoint removes the live checkpoint before an operation that
// rewrites wal.log (compaction), and forces the removal through the
// directory. Its absence is not an error. After it returns durably, no
// checkpoint can pair a stale offset with a rewritten log, even under a crash.
func deactivateCheckpoint(dir string) error {
	path := dir + string(os.PathSeparator) + checkpointName
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(dir)
}

// removeStaleCheckpointTemp deletes a temp file left by a freeze interrupted
// before its atomic rename. Its absence is not an error.
func removeStaleCheckpointTemp(dir string) error {
	err := os.Remove(dir + string(os.PathSeparator) + checkpointTmpName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// loadCheckpoint reads and strictly validates the live checkpoint. Any defect
// — missing file, truncation, trailing bytes, checksum mismatch, unknown magic
// or format version, implausible lengths or offset — rejects the whole file:
// the caller recovers from wal.log alone instead. A rejected checkpoint is
// therefore reported as an ordinary error the caller ignores, never as a half
// checkpoint.
func loadCheckpoint(dir string, walSize int64) (*checkpointData, error) {
	f, err := os.Open(dir + string(os.PathSeparator) + checkpointName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	raw, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if len(raw) < checkpointHeaderSize+crcSize {
		return nil, fmt.Errorf("snapshot: checkpoint truncated")
	}
	body, trailer := raw[:len(raw)-crcSize], raw[len(raw)-crcSize:]
	h := crc32.NewIEEE()
	h.Write(body)
	if binary.LittleEndian.Uint32(trailer) != h.Sum32() {
		return nil, fmt.Errorf("snapshot: checkpoint checksum mismatch")
	}

	p := 0
	u32 := func() uint32 {
		v := binary.LittleEndian.Uint32(body[p : p+4])
		p += 4
		return v
	}
	u64 := func() uint64 {
		v := binary.LittleEndian.Uint64(body[p : p+8])
		p += 8
		return v
	}
	if magic := u32(); magic != checkpointMagic {
		return nil, fmt.Errorf("snapshot: unknown checkpoint magic %#x", magic)
	}
	version := u32()
	if version != checkpointVersion {
		return nil, fmt.Errorf("snapshot: unknown checkpoint format version %d", version)
	}
	d := &checkpointData{}
	d.snapshotCount = u64()
	d.nextSeq = u64()
	d.walOffset = int64(u64())
	count := u32()
	if p != checkpointHeaderSize {
		return nil, fmt.Errorf("snapshot: checkpoint header misaligned")
	}
	if d.walOffset < 0 || d.walOffset > walSize {
		// The resume point must lie inside the log the checkpoint describes;
		// otherwise the two files cannot be one history and the checkpoint is
		// rejected wholesale.
		return nil, fmt.Errorf("snapshot: checkpoint log offset out of range")
	}
	if uint64(count) > uint64(maxRecord) {
		return nil, fmt.Errorf("snapshot: checkpoint entry count out of range")
	}
	// Every entry needs at least its fixed header plus one non-empty key byte,
	// so a declared count that cannot fit the remaining body is rejected
	// before any capacity is allocated.
	bodyLen := uint64(len(body) - checkpointHeaderSize)
	if uint64(count)*uint64(checkpointEntryHeader+1) > bodyLen {
		return nil, fmt.Errorf("snapshot: checkpoint entry count exceeds body")
	}

	d.entries = make([]checkpointEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if p+checkpointEntryHeader > len(body) {
			return nil, fmt.Errorf("snapshot: checkpoint truncated in entry header")
		}
		keyLen := u32()
		valLen := u32()
		seq := u64()
		if keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
			uint64(p)+uint64(keyLen)+uint64(valLen) > uint64(len(body)) {
			return nil, fmt.Errorf("snapshot: checkpoint entry out of range")
		}
		key := string(body[p : p+int(keyLen)])
		p += int(keyLen)
		value := cloneBytes(body[p : p+int(valLen)])
		p += int(valLen)
		if len(d.entries) > 0 && d.entries[len(d.entries)-1].key >= key {
			return nil, fmt.Errorf("snapshot: checkpoint entries not strictly ordered")
		}
		d.entries = append(d.entries, checkpointEntry{key: key, seq: seq, value: value})
	}
	if p != len(body) {
		// Trailing bytes after the declared entries: never silently consume a
		// file that is not exactly one complete checkpoint.
		return nil, fmt.Errorf("snapshot: checkpoint has trailing bytes")
	}
	return d, nil
}
