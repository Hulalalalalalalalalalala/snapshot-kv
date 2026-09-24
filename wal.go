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
// Every successful Put/Delete/Commit (and every successfully acquired
// Snapshot) is appended as framed records and forced to stable storage before
// the call reports success, so reopening a directory always replays a prefix
// of complete frames (never a half-written value) and the cumulative snapshot
// count is never reset.
//
// Frame layout, little-endian:
//
//	magic    uint32   recordMagic
//	op       byte     one of the op* constants below
//	keyLen   uint32
//	valLen   uint32
//	key      [keyLen]byte
//	value    [valLen]byte (absent for plain deletes and opBatchEnd)
//	crc      uint32   IEEE CRC-32 of all preceding frame bytes
//
// An opMeta record carries no key and an 8-byte little-endian value holding
// the cumulative number of snapshots handed out so far.
//
// opPut/opDelete are the original records and carry no sequence number; they
// remain readable forever so a directory written by an older version reopens
// unchanged. They may only appear outside a batch.
//
// Records written by this version for user data are:
//
//   - opBatchBegin: no key, value area holds one uint64 commit sequence
//     number, the total order position shared by every operation of the batch;
//   - zero or more opPutSeq/opDeleteSeq records carrying the batch's
//     operations in batch order. Their value area is the same 8-byte sequence
//     number followed, for puts, by the actual value (valLen = 8 +
//     len(value));
//   - opBatchEnd: no key and no value, closing the batch.
//
// The begin…end group is the atomic durability unit: a crash after begin but
// before end discards the whole group on replay, so a batch is either fully
// visible or fully absent. Several concurrent batches are encoded back to
// back and share one flush plus one fsync (group commit); each batch still has
// its own begin/end pair, so each stays independently atomic on recovery.
//
// Loose opPutSeq/opDeleteSeq records not wrapped in begin/end are also legal:
// compaction rewrites the log using exactly those, one frame per retained
// version.
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

// walWriter appends framed records to one open write-ahead log. Besides the
// per-call append path it supports group commit: concurrent batches encode
// their frames into the shared buffered writer and exactly one of them flushes
// and fsyncs once on behalf of the whole group.
//
// writePos is the logical end of everything encoded so far; committedSize is
// the end of the last region forced to stable storage. Frames between the two
// are in flight (buffered or in the kernel page cache) and can be rolled back
// when a group's fsync fails.
type walWriter struct {
	f *os.File
	w *bufio.Writer

	writePos      int64
	committedSize int64

	// syncFn performs the actual fsync; defaults to f.Sync. Tests replace it
	// to count or block the single synchronization a commit group shares.
	syncFn func() error
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
	pos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &walWriter{
		f:             f,
		w:             bufio.NewWriter(f),
		writePos:      pos,
		committedSize: pos,
		syncFn:        f.Sync,
	}, nil
}

func (l *walWriter) bufWrite(p []byte) error {
	n, err := l.w.Write(p)
	l.writePos += int64(n)
	return err
}

func (l *walWriter) bufWriteString(s string) error {
	n, err := l.w.WriteString(s)
	l.writePos += int64(n)
	return err
}

// writeFrame encodes, flushes and durably commits one generic frame. Used for
// meta records; batch data goes through the group-commit path instead.
func (l *walWriter) writeFrame(op byte, key string, value []byte) error {
	if err := l.encodeFrame(op, key, value); err != nil {
		return err
	}
	return l.flushSync()
}

// encodeFrame buffers one framed record. Used for meta, batch boundary frames
// and while building a replacement log from scratch.
func (l *walWriter) encodeFrame(op byte, key string, value []byte) error {
	hdr := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], recordMagic)
	hdr[4] = op
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(value)))

	h := crc32.NewIEEE()
	h.Write(hdr)
	if err := l.bufWrite(hdr); err != nil {
		return err
	}
	if err := l.bufWriteString(key); err != nil {
		return err
	}
	h.Write([]byte(key))
	if len(value) > 0 {
		if err := l.bufWrite(value); err != nil {
			return err
		}
		h.Write(value)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	return l.bufWrite(crcb[:])
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
	if err := l.bufWrite(hdr); err != nil {
		return err
	}
	if err := l.bufWriteString(key); err != nil {
		return err
	}
	h.Write([]byte(key))
	var seqb [seqSize]byte
	binary.LittleEndian.PutUint64(seqb[:], seq)
	if err := l.bufWrite(seqb[:]); err != nil {
		return err
	}
	h.Write(seqb[:])
	if len(value) > 0 {
		if err := l.bufWrite(value); err != nil {
			return err
		}
		h.Write(value)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	return l.bufWrite(crcb[:])
}

// encodeBatch buffers the begin…end envelope of one batch tagged with seq,
// calling body between the two boundary frames. Everything lands in the
// shared buffer and is made durable by the group leader's single flushSync.
func (l *walWriter) encodeBatch(seq uint64, body func() error) error {
	var seqb [seqSize]byte
	binary.LittleEndian.PutUint64(seqb[:], seq)
	if err := l.encodeFrame(opBatchBegin, "", seqb[:]); err != nil {
		return err
	}
	if err := body(); err != nil {
		return err
	}
	return l.encodeFrame(opBatchEnd, "", nil)
}

// appendPut buffers one value record tagged with seq; used while building a
// compaction replacement log, whose flush and sync is handled by its caller.
func (l *walWriter) appendPut(key string, value []byte, seq uint64) error {
	if len(key) > maxRecord || seqSize+len(value) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	return l.encodeFrameSeq(opPutSeq, key, seq, value)
}

// appendDelete buffers one tombstone tagged with seq; see appendPut.
func (l *walWriter) appendDelete(key string, seq uint64) error {
	if len(key) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	return l.encodeFrameSeq(opDeleteSeq, key, seq, nil)
}

// flushSync forces every buffered byte and the whole preceding log to stable
// storage and advances the committed mark. Group commit calls this exactly
// once per fsync group.
func (l *walWriter) flushSync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.syncFn(); err != nil {
		return err
	}
	l.committedSize = l.writePos
	return nil
}

// rollbackTo discards every encoded byte past size, in the buffer and in the
// file, and repositions the writer at size. A failed group fsync calls it with
// the previous committed mark so failed batches leave no trace and the next
// group appends cleanly.
func (l *walWriter) rollbackTo(size int64) error {
	// Discard buffered bytes first: they must never reach the file past size.
	l.w.Reset(l.f)
	if _, err := l.f.Seek(size, io.SeekStart); err != nil {
		return err
	}
	if err := l.f.Truncate(size); err != nil {
		return err
	}
	l.writePos = size
	return nil
}

func (l *walWriter) committed() int64 { return l.committedSize }

// appendMeta durably records the cumulative snapshot count.
func (l *walWriter) appendMeta(snapshotCount uint64) error {
	var v [metaValueSize]byte
	binary.LittleEndian.PutUint64(v[:], snapshotCount)
	return l.writeFrame(opMeta, "", v[:])
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

// buildCompactWAL writes the complete replacement log to compactTmpName and
// forces it to stable storage; it does not touch wal.log. Pair it with
// installCompactWAL, which the caller runs only after closing the old writer.
// Splitting build from install matters on Windows: replacing wal.log while
// the old writer still holds an open handle is denied there, so the handle
// must be closed before the rename. A crash at any point leaves the old
// wal.log intact and a stale temp file, which the next open removes.
func buildCompactWAL(dir string, histories []keyHistory, snapshotCount uint64) error {
	if err := removeStaleCompact(dir); err != nil {
		return err
	}

	path := dir + string(os.PathSeparator)
	tmp, err := os.OpenFile(path+compactTmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	abort := func(cause error) error {
		tmp.Close()
		os.Remove(path + compactTmpName)
		return cause
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
				if err := l.appendPut(histories[i].key, ver.value, ver.seq); err != nil {
					return abort(err)
				}
			} else {
				if err := l.appendDelete(histories[i].key, ver.seq); err != nil {
					return abort(err)
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
			return abort(err)
		}
	}
	if err := l.w.Flush(); err != nil {
		return abort(err)
	}
	if err := tmp.Sync(); err != nil {
		return abort(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path + compactTmpName)
		return err
	}
	return nil
}

// installCompactWAL atomically renames the temp log built by buildCompactWAL
// over wal.log, forces the directory and returns a fresh writer at the end of
// the new log. The old writer must already be closed, otherwise the replace
// fails on Windows. On a failed rename the temp is removed and wal.log is
// untouched. If the directory sync fails the rename has still happened, so a
// non-nil writer is returned together with the error: the caller must adopt it
// rather than reopen the old, now-unlinked file.
func installCompactWAL(dir string) (*walWriter, error) {
	path := dir + string(os.PathSeparator)
	if err := os.Rename(path+compactTmpName, path+walName); err != nil {
		os.Remove(path + compactTmpName)
		return nil, err
	}
	// Make the rename durable; otherwise a crash could expose the old log
	// even though the new one was fully written. The rename itself has
	// already landed either way, so open and return the new writer so the
	// caller adopts the now-linked file despite reporting the error.
	syncErr := syncDirectory(dir)
	next, openErr := openWALWriter(dir, -1)
	if openErr != nil {
		// Join collapses to whichever error is set (or both, if both).
		return nil, errors.Join(syncErr, openErr)
	}
	return next, syncErr
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

// replayWAL applies every complete, intact record in commit order. kv records
// go to fn with their commit sequence (0 for legacy records that predate
// sequence numbers); meta records update snapshotCount. It returns the number
// of bytes occupied by the valid prefix; anything after it is a torn, corrupt
// or unclosed-batch tail discarded on the next open.
//
// Batch atomicity: records between opBatchBegin and opBatchEnd are buffered
// and only handed to fn once the closing frame is intact. A torn frame, a
// stray marker or a CRC error inside a batch invalidates the whole batch, so
// the valid prefix ends just before its begin frame; a batch can therefore
// only reappear whole, never partially.
func replayWAL(dir string, fn func(op byte, key string, value []byte, seq uint64)) (int64, uint64, error) {
	f, err := os.Open(dir + string(os.PathSeparator) + walName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var offset int64
	// prefix is the end offset of everything already handed to fn: the
	// recovery point if an open batch turns out incomplete.
	var prefix int64
	var snapshotCount uint64
	hdr := make([]byte, frameHeaderSize)

	type rec struct {
		op    byte
		key   string
		value []byte
		seq   uint64
	}
	var inBatch bool
	var batchSeq uint64
	var pending []rec

	// invalidAt returns the valid-prefix end for framing state at hand when
	// the frame starting at frameStart turns out bad.
	invalidAt := func(frameStart int64) (int64, uint64) {
		if inBatch {
			return prefix, snapshotCount // drop the unclosed batch wholesale
		}
		return frameStart, snapshotCount
	}

	for {
		frameStart := offset
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				p, c := invalidAt(frameStart)
				return p, c, nil // absent/torn tail: valid prefix ends here
			}
			return 0, 0, err
		}
		offset += frameHeaderSize

		magic := binary.LittleEndian.Uint32(hdr[0:4])
		op := hdr[4]
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
			(op == opBatchBegin && (keyLen != 0 || valLen != seqSize)) ||
			(op == opBatchEnd && (keyLen != 0 || valLen != 0)) {
			p, c := invalidAt(frameStart)
			return p, c, nil // corrupt frame: valid prefix ends here
		}

		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			p, c := invalidAt(frameStart)
			return p, c, nil
		}
		offset += int64(len(body))

		var crcb [crcSize]byte
		if _, err := io.ReadFull(r, crcb[:]); err != nil {
			p, c := invalidAt(frameStart)
			return p, c, nil
		}
		offset += crcSize

		h := crc32.NewIEEE()
		h.Write(hdr)
		h.Write(body)
		if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
			p, c := invalidAt(frameStart)
			return p, c, nil
		}

		key := string(body[:keyLen])
		switch op {
		case opBatchBegin:
			if inBatch {
				return prefix, snapshotCount, nil // nested begin: corrupt framing
			}
			inBatch = true
			batchSeq = binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			pending = pending[:0]
		case opBatchEnd:
			if !inBatch {
				return frameStart, snapshotCount, nil // stray end
			}
			for _, rc := range pending {
				fn(rc.op, rc.key, rc.value, rc.seq)
			}
			pending = pending[:0]
			inBatch = false
			prefix = offset
		case opMeta:
			if inBatch {
				return prefix, snapshotCount, nil // meta is never batched
			}
			snapshotCount = binary.LittleEndian.Uint64(body)
			prefix = offset
		case opPut, opDelete:
			if inBatch {
				return prefix, snapshotCount, nil // legacy frames are never batched
			}
			if op == opPut {
				fn(opPut, key, body[keyLen:], 0)
			} else {
				fn(opDelete, key, nil, 0)
			}
			prefix = offset
		case opPutSeq, opDeleteSeq:
			seq := binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			if inBatch {
				// The frame must carry the enclosing batch's sequence; a
				// mismatch is framing corruption and kills the whole batch.
				if seq != batchSeq {
					return prefix, snapshotCount, nil
				}
			} else {
				fn(op, key, body[keyLen+seqSize:], seq)
				prefix = offset
				continue
			}
			if op == opPutSeq {
				pending = append(pending, rec{op: opPutSeq, key: key, value: body[keyLen+seqSize:], seq: batchSeq})
			} else {
				pending = append(pending, rec{op: opDeleteSeq, key: key, seq: batchSeq})
			}
		}
	}
}
