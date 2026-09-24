package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// On-disk write-ahead log.
//
// Every successful Put/Delete (and every successfully acquired Snapshot) is
// appended as one framed record and forced to stable storage before the call
// reports success, so reopening a directory always replays a prefix of
// complete records (never a half-written value) and the cumulative snapshot
// count is never reset.
//
// Frame layout, little-endian:
//
//	magic    uint32   recordMagic
//	op       byte     opPut, opDelete, opMeta, opPutSeq, opDeleteSeq,
//	                  opBatchBegin or opBatchEnd
//	keyLen   uint32
//	valLen   uint32
//	key      [keyLen]byte
//	value    [valLen]byte (absent for plain deletes)
//	crc      uint32   IEEE CRC-32 of all preceding frame bytes
//
// An opMeta record carries no key and an 8-byte little-endian value holding
// the cumulative number of snapshots handed out so far.
//
// The WAL is never rewritten by a checkpoint (a checkpoint only adds a side
// file); only compaction replaces wal.log. Compaction durably removes any
// live checkpoint before renaming the replacement log into place, so after a
// crash a checkpoint can never be found next to a WAL other than the one it
// was captured against: the checkpoint and the log it pairs with share a
// lifetime established by ordered, synced metadata operations.
//
// opPut/opDelete are the original records and carry no sequence number; they
// remain readable forever so a directory written by an older version reopens
// unchanged. Records written by this version use the Seq variants instead:
// the value area is an 8-byte little-endian commit sequence number (unique
// and increasing per key) followed, for puts, by the actual value
// (valLen = 8 + len(value)). Sequence numbers let a pinning compaction merge
// versions from several eras into one history while keeping their order.
//
// A batch is one opBatchBegin marker (empty key, empty value), the Seq frames
// of its effective changes in list order, and one opBatchEnd marker (empty
// key, 8-byte value holding the number of inner frames). The markers frame
// the atomic commit: replay applies the inner frames only once the complete,
// intact end marker is seen, so a crash that leaves anything short of it
// discards the whole batch down to the begin offset. The whole frame group
// is buffered and forced with a single flush plus a single sync, so return
// from the append means the entire batch is stable.
const (
	walName = "wal.log"
	// compactTmpName holds a freshly built log during compaction; it is
	// atomically renamed over wal.log and never observed directly. A file by
	// this name left behind by a crash mid-compaction is stale and removed on
	// the next open.
	compactTmpName = "wal.log.compact"

	recordMagic  uint32 = 0x534e4150 // "SNAP"
	opPut        byte   = 1
	opDelete     byte   = 2
	opMeta       byte   = 3
	opPutSeq     byte   = 4
	opDeleteSeq  byte   = 5
	opBatchBegin byte   = 6
	opBatchEnd   byte   = 7

	frameHeaderSize = 4 + 1 + 4 + 4
	crcSize         = 4
	metaValueSize   = 8
	seqSize         = 8
	maxRecord       = 1 << 30 // sanity bound on key and value lengths
)

type walWriter struct {
	f *os.File
	w *bufio.Writer

	// syncs counts durable forces (flush + fsync pairs). Groups commit many
	// batches per increment; it is read by tests while the store lock is
	// held and otherwise carries no semantics.
	syncs int
}

func openWALWriter(dir string, replaySize int64) (*walWriter, error) {
	f, err := os.OpenFile(dir+string(os.PathSeparator)+walName, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	// Drop any torn tail that replay could not consume. A negative replaySize
	// means the file is already exactly what should be on disk (e.g. a log
	// just installed by compaction) and must not be truncated.
	if replaySize >= 0 {
		if err := f.Truncate(replaySize); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &walWriter{f: f, w: bufio.NewWriter(f)}, nil
}

func (l *walWriter) writeFrame(op byte, key string, value []byte) error {
	if err := l.encodeFrame(op, key, value); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.syncs++
	return nil
}

// encodeFrame buffers one framed record. Used for ordinary appends (followed
// by a flush and sync) and while building a replacement log from scratch.
func (l *walWriter) encodeFrame(op byte, key string, value []byte) error {
	hdr := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], recordMagic)
	hdr[4] = op
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(value)))

	h := crc32.NewIEEE()
	h.Write(hdr)
	if _, err := l.w.Write(hdr); err != nil {
		return err
	}
	if _, err := io.WriteString(l.w, key); err != nil {
		return err
	}
	h.Write([]byte(key))
	if len(value) > 0 {
		if _, err := l.w.Write(value); err != nil {
			return err
		}
		h.Write(value)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	_, err := l.w.Write(crcb[:])
	return err
}

// encodeFrameSeq buffers a Seq record: seq's 8 bytes followed by value in
// the value area.
func (l *walWriter) encodeFrameSeq(op byte, key string, seq uint64, value []byte) error {
	hdr := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], recordMagic)
	hdr[4] = op
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[9:13], seqSize+uint32(len(value)))

	h := crc32.NewIEEE()
	h.Write(hdr)
	if _, err := l.w.Write(hdr); err != nil {
		return err
	}
	if _, err := io.WriteString(l.w, key); err != nil {
		return err
	}
	h.Write([]byte(key))
	var seqb [seqSize]byte
	binary.LittleEndian.PutUint64(seqb[:], seq)
	if _, err := l.w.Write(seqb[:]); err != nil {
		return err
	}
	h.Write(seqb[:])
	if len(value) > 0 {
		if _, err := l.w.Write(value); err != nil {
			return err
		}
		h.Write(value)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	_, err := l.w.Write(crcb[:])
	return err
}

// appendPut writes and durably commits one value record tagged with seq.
func (l *walWriter) appendPut(key string, value []byte, seq uint64) error {
	if len(key) > maxRecord || seqSize+len(value) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	if err := l.encodeFrameSeq(opPutSeq, key, seq, value); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.syncs++
	return nil
}

// appendDelete writes and durably commits one tombstone tagged with seq.
func (l *walWriter) appendDelete(key string, seq uint64) error {
	if len(key) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	if err := l.encodeFrameSeq(opDeleteSeq, key, seq, nil); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.syncs++
	return nil
}

// appendMeta durably records the cumulative snapshot count.
func (l *walWriter) appendMeta(snapshotCount uint64) error {
	var v [metaValueSize]byte
	binary.LittleEndian.PutUint64(v[:], snapshotCount)
	return l.writeFrame(opMeta, "", v[:])
}

// encodeBatch buffers one batch framed by begin/end markers, without
// flushing or syncing. Every change is a Seq record carrying its own commit
// sequence; end's value holds the inner-frame count. Several batches encoded
// back to back can be forced together with one flushSync, which is how
// concurrent commits share a single disk sync (group commit) while each
// batch stays a self-contained, independently recoverable framed commit.
func (l *walWriter) encodeBatch(changes []batchChange) error {
	var endVal [metaValueSize]byte
	binary.LittleEndian.PutUint64(endVal[:], uint64(len(changes)))
	if err := l.encodeFrame(opBatchBegin, "", nil); err != nil {
		return err
	}
	for i := range changes {
		c := &changes[i]
		var encErr error
		if c.delete {
			encErr = l.encodeFrameSeq(opDeleteSeq, c.key, c.seq, nil)
		} else {
			encErr = l.encodeFrameSeq(opPutSeq, c.key, c.seq, c.value)
		}
		if encErr != nil {
			return encErr
		}
	}
	return l.encodeFrame(opBatchEnd, "", endVal[:])
}

// flushSync forces everything buffered so far to stable storage with one
// flush and one fsync.
func (l *walWriter) flushSync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.syncs++
	return nil
}

// position returns the current write offset (logical end of the log). It is
// only meaningful when no frames are buffered, as at the start of a group.
func (l *walWriter) position() (int64, error) {
	return l.f.Seek(0, io.SeekEnd)
}

// truncateTo rewinds the log to off, dropping anything a failed group
// buffered or auto-flushed past it, and re-arms the buffered writer.
func (l *walWriter) truncateTo(off int64) error {
	l.w.Reset(l.f)
	if err := l.f.Truncate(off); err != nil {
		return err
	}
	_, err := l.f.Seek(off, io.SeekStart)
	return err
}

// appendBatch durably commits one standalone batch: encode and force with a
// single sync. Group leaders use encodeBatch plus flushSync directly so one
// sync can cover multiple batches.
func (l *walWriter) appendBatch(changes []batchChange) error {
	if err := l.encodeBatch(changes); err != nil {
		return err
	}
	return l.flushSync()
}

func (l *walWriter) close() error { return l.f.Close() }

// syncDirectory forces a directory entry change (such as a rename) to stable
// storage.
func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// removeStaleCompact deletes a replacement log left behind by a compaction
// that crashed before its atomic rename. Its absence is not an error.
func removeStaleCompact(dir string) error {
	err := os.Remove(dir + string(os.PathSeparator) + compactTmpName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// batchChange is one effective change in a batch commit.
type batchChange struct {
	key    string
	value  []byte // put payload; ignored when delete is true
	seq    uint64
	delete bool
}

// buildCompactWAL writes the complete replacement log to compactTmpName and
// durably closes it, without touching wal.log or the live writer. It builds
// the complete replacement in compactTmpName and forces it to stable
// storage, so a crash at any point leaves either the old log untouched or a
// fully built temp that open-time cleanup discards.
func buildCompactWAL(dir string, histories []keyHistory, snapshotCount uint64) error {
	if err := removeStaleCompact(dir); err != nil {
		return err
	}

	path := dir + string(os.PathSeparator)
	tmp, err := os.OpenFile(path+compactTmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(path + compactTmpName)
		return err
	}
	l := &walWriter{f: tmp, w: bufio.NewWriter(tmp)}
	for i := range histories {
		// Each key's retained versions are oldest-to-newest; replay prepends
		// them in this order, so the last one becomes the head and every
		// older one remains reachable down its chain. Frames are only
		// buffered here; one flush plus one sync below commits the whole
		// replacement atomically.
		for _, ver := range histories[i].versions {
			if ver.present {
				if err := l.encodeFrameSeq(opPutSeq, histories[i].key, ver.seq, ver.value); err != nil {
					return fail(err)
				}
			} else {
				if err := l.encodeFrameSeq(opDeleteSeq, histories[i].key, ver.seq, nil); err != nil {
					return fail(err)
				}
			}
		}
	}
	// A zero count needs no record: replay of an absent meta yields 0. This
	// keeps a trivially empty log empty rather than growing it.
	if snapshotCount > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], snapshotCount)
		if err := l.encodeFrame(opMeta, "", v[:]); err != nil {
			return fail(err)
		}
	}
	if err := l.w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path + compactTmpName)
		return err
	}
	return nil
}

// installCompactWAL atomically puts a previously built replacement log in
// place. The live writer must already have been closed by the caller:
// renaming over wal.log while a writer still holds it open fails on native
// filesystems that refuse a rename onto an open file, so releasing the old
// file first is what makes the swap succeed everywhere.
//
// renamed reports whether the rename landed. A crash between rename and the
// directory sync is still safe: on reopen wal.log is either the old or the
// complete new log, never a half-written file. When the rename fails the old
// log is intact and is reopened; the returned writer then points at it. When
// the rename landed but a writer cannot be opened on the new log, a nil
// writer is returned and the caller must treat the store as failed.
func installCompactWAL(dir string) (w *walWriter, renamed bool, err error) {
	path := dir + string(os.PathSeparator)
	if rerr := os.Rename(path+compactTmpName, path+walName); rerr != nil {
		// The old log never moved; drop the staged temp so the directory is
		// left with no intermediate state, then reopen the old log so
		// committing can continue without needing a second compaction.
		os.Remove(path + compactTmpName)
		old, openErr := openWALWriter(dir, -1)
		if openErr != nil {
			return nil, false, errors.Join(rerr, openErr)
		}
		return old, false, rerr
	}
	// Make the rename durable; otherwise a crash could expose the old log
	// even though the new one was fully written. The rename itself has
	// already landed either way, so open and return the new writer so the
	// caller adopts the now-linked file despite reporting the error.
	syncErr := syncDirectory(dir)
	next, openErr := openWALWriter(dir, -1)
	if openErr != nil {
		return nil, true, errors.Join(syncErr, openErr)
	}
	return next, true, syncErr
}

// nodeVer is one retained version of a key.
type nodeVer struct {
	seq     uint64
	present bool // false marks a delete
	value   []byte
}

// keyHistory is the retained oldest-to-newest version chain for one key in a
// compacted log.
type keyHistory struct {
	key      string
	versions []nodeVer
}

// readFullFrame reads one WAL frame from r. hdr is the reusable frame-header
// buffer. On a clean end of file it returns cleanEOF=true. Any torn read,
// unknown op, bad length or checksum mismatch returns n=0 with a nil error
// for an ordinary corrupt/torn frame (the caller ends the valid prefix
// before it) or a non-nil error only for an underlying read failure.
func readFullFrame(r *bufio.Reader, hdr []byte) (op byte, body []byte, frameLen int, cleanEOF bool, err error) {
	if _, err := io.ReadFull(r, hdr); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Exact end of log (EOF) or a partial header of a torn append
			// (ErrUnexpectedEOF): either way the valid prefix ends here.
			return 0, nil, 0, true, nil
		}
		return 0, nil, 0, false, err
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	op = hdr[4]
	keyLen := binary.LittleEndian.Uint32(hdr[5:9])
	valLen := binary.LittleEndian.Uint32(hdr[9:13])
	if magic != recordMagic ||
		(op != opPut && op != opDelete && op != opMeta &&
			op != opPutSeq && op != opDeleteSeq &&
			op != opBatchBegin && op != opBatchEnd) ||
		keyLen > maxRecord || valLen > maxRecord ||
		(op == opDelete && valLen != 0) ||
		(op == opPutSeq && valLen < seqSize) ||
		(op == opDeleteSeq && valLen != seqSize) ||
		(op == opMeta && (keyLen != 0 || valLen != metaValueSize)) ||
		(op == opBatchBegin && (keyLen != 0 || valLen != 0)) ||
		(op == opBatchEnd && (keyLen != 0 || valLen != metaValueSize)) {
		return 0, nil, 0, false, nil // corrupt frame
	}

	body = make([]byte, int(keyLen)+int(valLen))
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, 0, false, nil // torn body
	}
	var crcb [crcSize]byte
	if _, err := io.ReadFull(r, crcb[:]); err != nil {
		return 0, nil, 0, false, nil // torn checksum
	}
	h := crc32.NewIEEE()
	h.Write(hdr)
	h.Write(body)
	if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
		return 0, nil, 0, false, nil // checksum mismatch
	}
	return op, body, frameHeaderSize + len(body) + crcSize, false, nil
}

// walSize reports the current size of wal.log, or 0 when it is absent.
func walFileSize(dir string) (int64, error) {
	info, err := os.Stat(dir + string(os.PathSeparator) + walName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// replayWAL applies every complete, intact record in commit order. kv
// records go to fn with their commit sequence (0 for legacy records that
// predate sequence numbers); meta records update snapshotCount. It returns
// the number of bytes occupied by the valid prefix; anything after it is a
// torn or corrupt tail discarded on the next open.
//
// Framed batches are applied atomically: their inner records are buffered
// until the intact end marker and only then handed to fn in list order. A
// torn or corrupt frame anywhere between begin and end (including a missing
// or count-mismatched end marker) makes the valid prefix end before the
// begin marker, so the whole batch disappears on reopen and no half-batch
// ever reaches the in-memory state.
func replayWAL(dir string, fn func(op byte, key string, value []byte, seq uint64)) (int64, uint64, error) {
	f, err := os.Open(dir + string(os.PathSeparator) + walName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer f.Close()
	return replayFrames(f, 0, 0, fn)
}

// replayWALTail is the checkpoint-reopen path: it seeds nothing itself (the
// caller has installed the checkpoint state) and replays only the frames at
// or after start, carrying initialSnap as the starting cumulative snapshot
// count. A missing WAL is reported as an empty tail ending exactly at start,
// which is the normal case for a checkpoint taken on an as-yet unwritten
// store.
func replayWALTail(dir string, start int64, initialSnap uint64, fn func(op byte, key string, value []byte, seq uint64)) (int64, uint64, error) {
	f, err := os.Open(dir + string(os.PathSeparator) + walName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return start, initialSnap, nil
		}
		return 0, 0, err
	}
	defer f.Close()
	return replayFrames(f, start, initialSnap, fn)
}

// replayFrames drives the same recovery as replayWAL but starts at byte
// offset start: a checkpoint reopen seeds memory from the checkpoint and
// only the WAL tail (frames at or after start) is replayed. start is an
// exact frame boundary — it was the writer's end-of-log position when the
// checkpoint was captured, and the capture happens under the same lock that
// forces each whole append — so the checkpoint-covered prefix is merely
// skipped, never parsed. initialSnap is the cumulative snapshot count
// already carried by the checkpoint (0 for a full replay); a meta frame in
// the tail assigns its absolute count as usual. The returned offset is the
// absolute end of the valid prefix, measured from the beginning of the file.
//
// A torn first frame at the cut is not an error but the normal case of a
// commit that crashed mid-append after the checkpoint: the frame never
// committed, so the valid prefix ends at the cut and the checkpoint already
// holds every state through it. Correct pairing of checkpoint and WAL is
// established structurally (compaction retires a checkpoint before it can
// rewrite the log), not by probing here.
func replayFrames(f *os.File, start int64, initialSnap uint64, fn func(op byte, key string, value []byte, seq uint64)) (int64, uint64, error) {
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return 0, 0, err
	}
	r := bufio.NewReader(f)
	var offset int64 = start
	snapshotCount := initialSnap
	hdr := make([]byte, frameHeaderSize)

	// Batch framing state.
	inBatch := false
	batchStart := int64(0) // absolute offset of the batch's begin frame
	var pending []replayedFrame

	// validPrefix reports where the file must be truncated to. A bad frame
	// inside a batch invalidates from the batch's begin marker; outside a
	// batch it invalidates only from that frame.
	discard := func(at int64) (int64, uint64, error) {
		if inBatch {
			return batchStart, snapshotCount, nil
		}
		return at, snapshotCount, nil
	}

	for {
		at := offset
		op, body, n, cleanEOF, rerr := readFullFrame(r, hdr)
		if rerr != nil {
			return 0, 0, rerr
		}
		if cleanEOF {
			return discard(at) // absent/torn tail: valid prefix ends here
		}
		if n == 0 {
			return discard(at) // corrupt or torn frame: prefix ends before it
		}
		offset += int64(n)

		keyLen := binary.LittleEndian.Uint32(hdr[5:9])
		switch op {
		case opBatchBegin:
			if inBatch {
				return batchStart, snapshotCount, nil // nested begin: corrupt batch
			}
			inBatch = true
			batchStart = at
			pending = pending[:0]
		case opBatchEnd:
			count := binary.LittleEndian.Uint64(body)
			if !inBatch || count != uint64(len(pending)) {
				// Stray end, or an end whose count does not match the
				// buffered inner frames: the framed commit is not intact.
				return discard(at)
			}
			for _, fr := range pending {
				fn(fr.op, fr.key, fr.value, fr.seq)
			}
			pending = pending[:0]
			inBatch = false
		case opPut:
			if inBatch {
				return batchStart, snapshotCount, nil // only Seq frames may batch
			}
			fn(opPut, string(body[:keyLen]), body[keyLen:], 0)
		case opDelete:
			if inBatch {
				return batchStart, snapshotCount, nil
			}
			fn(opDelete, string(body[:keyLen]), nil, 0)
		case opPutSeq:
			key := string(body[:keyLen])
			seq := binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			value := body[keyLen+seqSize:]
			if inBatch {
				pending = append(pending, replayedFrame{opPutSeq, key, value, seq})
			} else {
				fn(opPutSeq, key, value, seq)
			}
		case opDeleteSeq:
			key := string(body[:keyLen])
			seq := binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			if inBatch {
				pending = append(pending, replayedFrame{opDeleteSeq, key, nil, seq})
			} else {
				fn(opDeleteSeq, key, nil, seq)
			}
		case opMeta:
			if inBatch {
				return batchStart, snapshotCount, nil // meta may not ride inside a batch
			}
			snapshotCount = binary.LittleEndian.Uint64(body)
		}
	}
}

// replayedFrame is one inner batch frame held during replay until its batch's
// end marker proves the commit is complete.
type replayedFrame struct {
	op    byte
	key   string
	value []byte
	seq   uint64
}
