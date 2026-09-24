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
//	op       byte     opPut, opDelete, opMeta, opPutSeq or opDeleteSeq
//	keyLen   uint32
//	valLen   uint32
//	key      [keyLen]byte
//	value    [valLen]byte (absent for plain deletes)
//	crc      uint32   IEEE CRC-32 of all preceding frame bytes
//
// An opMeta record carries no key and an 8-byte little-endian value holding
// the cumulative number of snapshots handed out so far.
//
// opPut/opDelete are the original records and carry no sequence number; they
// remain readable forever so a directory written by an older version reopens
// unchanged. Records written by this version use the Seq variants instead:
// the value area is an 8-byte little-endian commit sequence number (unique
// and increasing per key) followed, for puts, by the actual value
// (valLen = 8 + len(value)). Sequence numbers let a pinning compaction merge
// versions from several eras into one history while keeping their order.
const (
	walName = "wal.log"
	// compactTmpName holds a freshly built log during compaction; it is
	// atomically renamed over wal.log and never observed directly. A file by
	// this name left behind by a crash mid-compaction is stale and removed on
	// the next open.
	compactTmpName = "wal.log.compact"

	recordMagic uint32 = 0x534e4150 // "SNAP"
	opPut       byte   = 1
	opDelete    byte   = 2
	opMeta      byte   = 3
	opPutSeq    byte   = 4
	opDeleteSeq byte   = 5

	frameHeaderSize = 4 + 1 + 4 + 4
	crcSize         = 4
	metaValueSize   = 8
	seqSize         = 8
	maxRecord       = 1 << 30 // sanity bound on key and value lengths
)

type walWriter struct {
	f *os.File
	w *bufio.Writer
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
	return l.f.Sync()
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
	return l.f.Sync()
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
	return l.f.Sync()
}

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

// compactWAL atomically replaces the write-ahead log with one containing only
// the given live values, followed by one meta record with snapshotCount. It
// builds the complete replacement in compactTmpName, forces it to stable
// storage, then atomically renames it over wal.log and forces the directory.
// A crash at any point therefore leaves either the old log or the complete
// new log: never a truncated or half-written file. On success it returns a
// fresh writer positioned at the end of the new log. If the directory sync
// fails the rename has still happened, so a non-nil writer is returned
// together with the error: the caller must adopt it rather than keep writing
// to the old, now-unlinked file.
func compactWAL(dir string, histories []keyHistory, snapshotCount uint64) (*walWriter, error) {
	if err := removeStaleCompact(dir); err != nil {
		return nil, err
	}

	path := dir + string(os.PathSeparator)
	tmp, err := os.OpenFile(path+compactTmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
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
					tmp.Close()
					os.Remove(path + compactTmpName)
					return nil, err
				}
			} else {
				if err := l.encodeFrameSeq(opDeleteSeq, histories[i].key, ver.seq, nil); err != nil {
					tmp.Close()
					os.Remove(path + compactTmpName)
					return nil, err
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
			tmp.Close()
			os.Remove(path + compactTmpName)
			return nil, err
		}
	}
	if err := l.w.Flush(); err != nil {
		tmp.Close()
		os.Remove(path + compactTmpName)
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(path + compactTmpName)
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path + compactTmpName)
		return nil, err
	}

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

// replayWAL applies every complete, intact record in commit order. kv
// records go to fn with their commit sequence (0 for legacy records that
// predate sequence numbers); meta records update snapshotCount. It returns
// the number of bytes occupied by the valid prefix; anything after it is a
// torn or corrupt tail discarded on the next open.
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
	var snapshotCount uint64
	hdr := make([]byte, frameHeaderSize)

	for {
		start := offset
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return start, snapshotCount, nil // absent/torn tail: valid prefix ends here
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
				op != opPutSeq && op != opDeleteSeq) ||
			keyLen > maxRecord || valLen > maxRecord ||
			(op == opDelete && valLen != 0) ||
			(op == opPutSeq && valLen < seqSize) ||
			(op == opDeleteSeq && valLen != seqSize) ||
			(op == opMeta && (keyLen != 0 || valLen != metaValueSize)) {
			return start, snapshotCount, nil // corrupt frame: valid prefix ends here
		}

		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			return start, snapshotCount, nil
		}
		offset += int64(len(body))

		var crcb [crcSize]byte
		if _, err := io.ReadFull(r, crcb[:]); err != nil {
			return start, snapshotCount, nil
		}
		offset += crcSize

		h := crc32.NewIEEE()
		h.Write(hdr)
		h.Write(body)
		if binary.LittleEndian.Uint32(crcb[:]) != h.Sum32() {
			return start, snapshotCount, nil
		}

		switch op {
		case opPut:
			key := string(body[:keyLen])
			fn(opPut, key, body[keyLen:], 0)
		case opDelete:
			fn(opDelete, string(body[:keyLen]), nil, 0)
		case opPutSeq:
			key := string(body[:keyLen])
			seq := binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			fn(opPutSeq, key, body[keyLen+seqSize:], seq)
		case opDeleteSeq:
			key := string(body[:keyLen])
			seq := binary.LittleEndian.Uint64(body[keyLen : keyLen+seqSize])
			fn(opDeleteSeq, key, nil, seq)
		case opMeta:
			snapshotCount = binary.LittleEndian.Uint64(body)
		}
	}
}
