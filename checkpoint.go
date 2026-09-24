package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// On-disk checkpoint: the complete committed state at one instant frozen into
// a single self-describing file, so reopening can seed memory from it and
// replay only the WAL tail written afterwards instead of the whole log.
//
// Layout, little-endian:
//
//	magic     uint32   checkpointMagic
//	version   uint32   checkpointVersion1
//	seq       uint64   highest commit sequence handed out at capture time
//	snapshots uint64   cumulative number of snapshots handed out
//	offset    uint64   WAL byte offset of the cut: only frames at or past this
//	                   offset belong to the post-checkpoint tail replayed on
//	                   reopen
//	count     uint32   number of terminal key/value entries that follow
//	entries   [count]entry
//	crc       uint32   IEEE CRC-32 of every preceding byte
//
// One entry is a flags byte (0 = put, 1 = tombstone), a uint32 key length, a
// uint32 value length, the terminal version's commit sequence (uint64), the
// key, then the value. A checkpoint describes terminal state only, so every
// key appears exactly once: a put carries its final value (empty values
// included) and a tombstone carries no value. The per-key sequence is stored
// so a tail record replayed afterwards links onto the exact predecessor
// version and later compaction still sees globally unique sequence numbers.
//
// A checkpoint only ever accompanies the WAL it was captured against: the WAL
// is never rewritten by a checkpoint, and compaction (the only rewrite)
// durably removes any live checkpoint before renaming the replacement log
// into place. Thus an offset into the WAL always refers to the log the
// checkpoint describes, and a checkpoint found on reopen is trusted up to
// its own integrity checks.
//
// The file is an all-or-nothing artifact: it is built under a temporary name,
// forced whole with one fsync, and only then renamed into place (the rename
// atomically replaces any previous checkpoint). Missing, truncated,
// checksum-bad or unknown-version files are rejected outright; a half
// checkpoint is never installed as state.
const (
	checkpointName    = "snapshot.ckpt"
	checkpointTmpName = "snapshot.ckpt.tmp"

	checkpointMagic    uint32 = 0x504b4353 // bytes "SCKP"
	checkpointVersion1 uint32 = 1

	checkpointHeaderSize      = 4 + 4 + 8 + 8 + 8 + 4
	checkpointEntrySize       = 1 + 4 + 4 + 8 // flags, keyLen, valLen, seq
	ckptFlagPut          byte = 0
	ckptFlagDelete       byte = 1
)

// checkpointEntry is one terminal key state in a checkpoint.
type checkpointEntry struct {
	key     string
	value   []byte
	seq     uint64 // commit sequence of this terminal version
	present bool
}

// capturedState is the full content of one checkpoint: the terminal state
// plus the watermarks needed to replay only the WAL that follows it.
type capturedState struct {
	entries []checkpointEntry
	offset  int64
	seq     uint64
	snaps   uint64
}

// errCheckpointRejected marks any validation failure. Such an error is never
// surfaced to the caller of Open: a rejected checkpoint is an expected
// outcome handled by falling back to WAL-only replay.
func errCheckpointRejected(err error) error {
	return errors.Join(errors.New("snapshot: rejecting checkpoint"), err)
}

// captureCheckpoint takes a consistent snapshot of committed state. The
// caller must hold the store's write lock. Entries are sorted by key for a
// canonical encoding; values are copied so later commits cannot mutate the
// bytes being serialized. offset is the WAL's current end, the cut point.
func captureCheckpoint(v view, offset, seq, snaps uint64) capturedState {
	entries := make([]checkpointEntry, 0, len(v))
	for k, n := range v {
		e := checkpointEntry{key: k, present: n.present, seq: n.seq}
		if n.present {
			e.value = cloneBytes(n.value)
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return capturedState{
		entries: entries,
		offset:  int64(offset), seq: seq, snaps: snaps,
	}
}

// writeCheckpointFile serializes st to name freshly (O_EXCL), forces the
// whole file with one fsync and closes it. It never touches the live
// checkpoint; the caller renames name over it only once this succeeds.
func writeCheckpointFile(dir, name string, st capturedState) (err error) {
	path := dir + string(os.PathSeparator)
	f, err := os.OpenFile(path+name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(path + name)
		}
	}()

	w := bufio.NewWriter(f)
	head := make([]byte, checkpointHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], checkpointMagic)
	binary.LittleEndian.PutUint32(head[4:8], checkpointVersion1)
	binary.LittleEndian.PutUint64(head[8:16], st.seq)
	binary.LittleEndian.PutUint64(head[16:24], st.snaps)
	binary.LittleEndian.PutUint64(head[24:32], uint64(st.offset))
	binary.LittleEndian.PutUint32(head[32:36], uint32(len(st.entries)))

	h := crc32.NewIEEE()
	if _, err = w.Write(head); err != nil {
		return err
	}
	h.Write(head)

	ehdr := make([]byte, checkpointEntrySize)
	for _, e := range st.entries {
		if len(e.key) > maxRecord || len(e.value) > maxRecord {
			return errors.New("snapshot: checkpoint entry too large")
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
			return err
		}
		h.Write(ehdr)
		if _, err = io.WriteString(w, e.key); err != nil {
			return err
		}
		h.Write([]byte(e.key))
		if len(e.value) > 0 {
			if _, err = w.Write(e.value); err != nil {
				return err
			}
			h.Write(e.value)
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
	return f.Close()
}

// installCheckpoint builds the complete checkpoint in the temp name and
// atomically renames it into place, forcing the directory afterwards. It
// touches neither the WAL nor the live writer: a crash at any point leaves
// either the previous checkpoint or the new complete one, each describing
// the same unchanged WAL. A leftover temp is staging debris and is removed
// on the next open.
func installCheckpoint(dir string, st capturedState) error {
	if err := removeStaleCheckpointTmp(dir); err != nil {
		return err
	}
	if err := writeCheckpointFile(dir, checkpointTmpName, st); err != nil {
		return err
	}
	path := dir + string(os.PathSeparator)
	if err := os.Rename(path+checkpointTmpName, path+checkpointName); err != nil {
		os.Remove(path + checkpointTmpName)
		return err
	}
	// A crash before this force lands either the old or the new checkpoint
	// after restart; both are whole and valid against the unchanged WAL.
	return syncDirectory(dir)
}

// loadCheckpoint reads and validates the checkpoint in dir.
//
// A missing file is not an error: ok is false with nil error. Any defect
// (truncation, bad checksum, unknown version, malformed field or trailing
// bytes) returns ok=false with a non-nil rejection reason, so the caller
// falls back to a WAL-only reopen rather than installing partial state.
func loadCheckpoint(dir string) (st capturedState, ok bool, err error) {
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
	// Nothing may trail the terminal checksum: trailing bytes mean the file
	// is not the single artifact a checkpoint is allowed to be.
	if b, err := r.ReadByte(); err == nil {
		_ = b
		return reject(errors.New("snapshot: checkpoint has trailing bytes"))
	} else if !errors.Is(err, io.EOF) {
		return reject(err)
	}
	return st, true, nil
}

// removeStaleCheckpointTmp deletes a half-built checkpoint temp left by a
// crash mid-checkpoint. Its absence is not an error.
func removeStaleCheckpointTmp(dir string) error {
	err := os.Remove(dir + string(os.PathSeparator) + checkpointTmpName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removeRejectedCheckpoint discards a checkpoint that failed validation and
// forces the directory, so a rejected artifact never lingers or occupies
// space. The WAL alone remains a complete source of truth.
func removeRejectedCheckpoint(dir string) {
	if err := os.Remove(dir + string(os.PathSeparator) + checkpointName); err != nil {
		return
	}
	_ = syncDirectory(dir)
}

// retireCheckpoint durably withdraws a live checkpoint before the WAL is
// replaced. Compaction is the only operation that rewrites wal.log, and a
// checkpoint's cut offset is meaningful only against the exact log it was
// captured against, so the unlink (and its directory force) must be durable
// before the replacement log is renamed into place: after a crash at any
// instant, a checkpoint can never be found paired with a different WAL. Its
// absence is normal and not an error.
func retireCheckpoint(dir string) error {
	path := dir + string(os.PathSeparator) + checkpointName
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(dir)
}
