package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
)

// On-disk record layout, all integers big-endian:
//
//	1 byte  record type
//	4 bytes key length
//	4 bytes value length
//	key bytes
//	value bytes
//	4 bytes CRC-32 (IEEE) of everything above
//
// Records are appended and fsynced one at a time, so a committed record
// is fully on disk before its mutating call returns. A crash can leave
// at most one torn record at the tail; replay stops there and the tail
// is truncated on open.
type recType byte

const (
	recPut      recType = 1
	recDelete   recType = 2
	recSnapshot recType = 3
)

const hdrSize = 1 + 4 + 4
const crcSize = 4

func encodeRecord(t recType, key string, val []byte) []byte {
	body := hdrSize + len(key) + len(val)
	buf := make([]byte, body+crcSize)
	buf[0] = byte(t)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(val)))
	copy(buf[hdrSize:], key)
	copy(buf[hdrSize+len(key):], val)
	sum := crc32.ChecksumIEEE(buf[:body])
	binary.BigEndian.PutUint32(buf[body:], sum)
	return buf
}

// append writes one record and fsyncs it before reporting success, so a
// successful Put/Delete/Snapshot is fully committed even if the process
// is killed before Close. Caller holds Store.mu.
func (s *Store) append(t recType, key string, val []byte) error {
	rec := encodeRecord(t, key, val)
	if _, err := io.Copy(s.f, bytes.NewReader(rec)); err != nil {
		return fmt.Errorf("snapshot: write log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("snapshot: sync log: %w", err)
	}
	s.walSize += int64(len(rec))
	return nil
}

// replay loads the on-disk log into s.live and s.snapshots, stopping at
// a torn tail. It sets s.walSize to the offset just past the last valid
// record. Caller must not hold anything; invoked only from Open.
func (s *Store) replay() error {
	live, snaps, size, err := replayFrom(s.f)
	if err != nil {
		return err
	}
	s.live = live
	s.snapshots = snaps
	s.walSize = size
	return nil
}

// replayFrom reads a write-ahead log from its current offset. It returns
// the live key set, cumulative snapshot count, and the offset of the
// first invalid or missing byte. A short final read or a CRC mismatch
// means the tail is torn: replay returns everything decoded before it.
// A record that is structurally invalid beyond truncation (a complete
// header with bad lengths or type) is a hard error.
func replayFrom(r io.ReadSeeker) (map[string][]byte, uint64, int64, error) {
	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("snapshot: replay: %w", err)
	}
	size := start
	live := make(map[string][]byte)
	var snaps uint64

	hdr := make([]byte, hdrSize)
	for {
		off := size
		n, err := io.ReadFull(r, hdr)
		size += int64(n)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Partial or missing header: torn tail, ignore.
			size = off
			return live, snaps, size, nil
		}
		if err != nil {
			return nil, 0, 0, fmt.Errorf("snapshot: replay: %w", err)
		}

		t := recType(hdr[0])
		klen := binary.BigEndian.Uint32(hdr[1:5])
		vlen := binary.BigEndian.Uint32(hdr[5:9])
		if t != recPut && t != recDelete && t != recSnapshot {
			return nil, 0, 0, fmt.Errorf("snapshot: replay: corrupt record at offset %d: unknown type %d: %w", off, t, fs.ErrInvalid)
		}
		if (t == recSnapshot && (klen != 0 || vlen != 0)) ||
			(t == recDelete && vlen != 0) {
			return nil, 0, 0, fmt.Errorf("snapshot: replay: corrupt record at offset %d: %w", off, fs.ErrInvalid)
		}

		payload := make([]byte, int(klen)+int(vlen)+crcSize)
		n, err = io.ReadFull(r, payload)
		size += int64(n)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Payload or CRC torn: ignore the whole record.
			size = off
			return live, snaps, size, nil
		}
		if err != nil {
			return nil, 0, 0, fmt.Errorf("snapshot: replay: %w", err)
		}

		body := make([]byte, 0, hdrSize+int(klen)+int(vlen))
		body = append(body, hdr...)
		body = append(body, payload[:int(klen)+int(vlen)]...)
		want := binary.BigEndian.Uint32(payload[int(klen)+int(vlen):])
		if crc32.ChecksumIEEE(body) != want {
			// A fully present record with a bad CRC is corruption, not
			// a torn tail.
			return nil, 0, 0, fmt.Errorf("snapshot: replay: checksum mismatch at offset %d: %w", off, fs.ErrInvalid)
		}

		key := string(payload[:klen])
		val := payload[klen : int(klen)+int(vlen)]
		switch t {
		case recPut:
			stored := make([]byte, len(val))
			copy(stored, val)
			live[key] = stored
		case recDelete:
			delete(live, key)
		case recSnapshot:
			snaps++
		}
	}
}
