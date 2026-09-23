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
//	op       byte     opPut, opDelete or opMeta
//	keyLen   uint32
//	valLen   uint32
//	key      [keyLen]byte
//	value    [valLen]byte (absent for deletes)
//	crc      uint32   IEEE CRC-32 of all preceding frame bytes
//
// An opMeta record carries no key and an 8-byte little-endian value holding
// the cumulative number of snapshots handed out so far.
//
// Compaction rewrites the whole log through the scratch file: it records
// only the live keys plus one opMeta, syncs, and atomically renames the
// scratch over walName, so a crash at any point leaves one complete log.
const (
	walName        = "wal.log"
	walCompactName = "wal.log.tmp" // scratch file a compaction renames over walName

	recordMagic uint32 = 0x534e4150 // "SNAP"
	opPut       byte   = 1
	opDelete    byte   = 2
	opMeta      byte   = 3

	frameHeaderSize = 4 + 1 + 4 + 4
	crcSize         = 4
	metaValueSize   = 8
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
	// Drop any torn tail that replay could not consume.
	if err := f.Truncate(replaySize); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &walWriter{f: f, w: bufio.NewWriter(f)}, nil
}

// writeRecord buffers one framed record without flushing or syncing.
func (l *walWriter) writeRecord(op byte, key string, value []byte) error {
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

func (l *walWriter) writeFrame(op byte, key string, value []byte) error {
	if err := l.writeRecord(op, key, value); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// appendPut writes and durably commits one value record.
func (l *walWriter) appendPut(key string, value []byte) error {
	if len(key) > maxRecord || len(value) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	return l.writeFrame(opPut, key, value)
}

// appendDelete writes and durably commits one tombstone.
func (l *walWriter) appendDelete(key string) error {
	if len(key) > maxRecord {
		return fmt.Errorf("snapshot: record too large")
	}
	return l.writeFrame(opDelete, key, nil)
}

// appendMeta durably records the cumulative snapshot count.
func (l *walWriter) appendMeta(snapshotCount uint64) error {
	var v [metaValueSize]byte
	binary.LittleEndian.PutUint64(v[:], snapshotCount)
	return l.writeFrame(opMeta, "", v[:])
}

func (l *walWriter) close() error { return l.f.Close() }

// writeCompactedWAL writes a fresh log containing exactly the live view and
// the cumulative snapshot count into dir's scratch file, forces it to stable
// storage and returns a writer positioned to keep appending to it. The
// caller renames the scratch file over the live log; until then the on-disk
// state is untouched, and any failure removes the scratch file.
func writeCompactedWAL(dir string, live view, snapshotCount uint64) (*walWriter, string, error) {
	path := dir + string(os.PathSeparator) + walCompactName
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, "", err
	}
	fail := func(err error) (*walWriter, string, error) {
		f.Close()
		os.Remove(path)
		return nil, "", err
	}

	w := &walWriter{f: f, w: bufio.NewWriter(f)}
	for k, n := range live {
		if !n.present {
			continue // deleted keys leave no trace in the compacted log
		}
		if len(k) > maxRecord || len(n.value) > maxRecord {
			return fail(fmt.Errorf("snapshot: record too large"))
		}
		if err := w.writeRecord(opPut, k, n.value); err != nil {
			return fail(err)
		}
	}
	var meta [metaValueSize]byte
	binary.LittleEndian.PutUint64(meta[:], snapshotCount)
	if err := w.writeRecord(opMeta, "", meta[:]); err != nil {
		return fail(err)
	}
	if err := w.w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	return w, path, nil
}

// syncDir forces a directory's own metadata (a rename that happened inside
// it) to stable storage.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// replayWAL applies every complete, intact record in commit order: kv records
// go to fn, meta records update snapshotCount. It returns the number of bytes
// occupied by the valid prefix; anything after it is a torn or corrupt tail
// discarded on the next open.
func replayWAL(dir string, fn func(op byte, key string, value []byte)) (int64, uint64, error) {
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
			(op != opPut && op != opDelete && op != opMeta) ||
			keyLen > maxRecord || valLen > maxRecord ||
			(op == opDelete && valLen != 0) ||
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
			fn(opPut, key, body[keyLen:])
		case opDelete:
			fn(opDelete, string(body[:keyLen]), nil)
		case opMeta:
			snapshotCount = binary.LittleEndian.Uint64(body)
		}
	}
}
