package snapshot

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Incremental backup chains and synthesized restore.
//
// A full export (Store.Backup) is the head of a backup chain; every
// Store.BackupIncremental call appends one incremental ring to it. A ring
// captures only the terminal state of the keys touched between the previous
// link's export watermark and the ring's own anchor: a put carries its value,
// a key deleted in the interval travels as a tombstone so a synthesized
// restore stops seeing it. Commits after the anchor never enter the ring.
//
// Chain layout, inside the chain directory:
//
//	manifest.dat            head manifest (the full export's commit point)
//	segment-XXXXX.dat       head segments
//	incr-NNNNNNNNNN/        one incremental ring, numbered densely from 1
//	  manifest.dat          ring manifest (head layout plus the base watermark)
//	  segment-XXXXX.dat     ring segments
//
// A ring is self-describing in exactly the head's format: same magic, same
// format version, and the watermark and cumulative-snapshot-count fields carry
// the same meaning (the ring's own export watermark and the count at it). The
// ring manifest adds one field, the base watermark of the previous link the
// ring continues from, which is what makes watermark continuity checkable.
//
// A ring is an all-or-nothing artifact: it is assembled and forced to stable
// storage in a temporary sibling directory and renamed into the chain as one
// step, so a kill at any instant leaves either the old chain or the chain
// extended by one complete ring. The staging sibling is swept by the next
// Backup or BackupIncremental into that chain or the next Open of a related
// directory, and the store being backed up is never touched.
//
// Restore of a chain directory validates the whole chain — dense ring
// numbering, per-link manifests and file sets, watermark and snapshot-count
// continuity — and then synthesizes the terminal state at the last link's
// watermark by merging the links' sorted entry streams in chain order, a
// later link overriding an earlier one for the same key and a terminal
// tombstone dropping the key. The merge holds one current entry per link, so
// peak memory tracks the chain length and the live data, never the whole
// keyspace, and no half-synthesized target is ever installed.
const (
	// incrRingPrefix names one ring directory inside the chain directory.
	incrRingPrefix = "incr-"
	// incrStagePrefix prefixes the temporary sibling directory a ring is
	// assembled in before its atomic rename into the chain. A directory
	// carrying it is crash debris swept on the next backup or open.
	incrStagePrefix = ".incr-"

	// backupRingManifestSize is the head manifest layout plus the ring's
	// base watermark.
	backupRingManifestSize = backupManifestSize + 8
)

// chainRing is one validated incremental ring of a backup chain.
type chainRing struct {
	index uint32
	dir   string // absolute path of the ring directory
	info  manifestInfo
	base  uint64 // watermark of the previous link this ring continues from
}

// ringDirName renders a ring index as its chain directory name.
func ringDirName(index uint32) string {
	const digits = "0123456789"
	buf := make([]byte, 10)
	n := index
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[n%10]
		n /= 10
	}
	return incrRingPrefix + string(buf)
}

// parseRingDirName is the inverse of ringDirName. ok is false for names that
// are not well-formed ring directories.
func parseRingDirName(name string) (uint32, bool) {
	mid := len(incrRingPrefix) + 10
	if len(name) != mid || name[:len(incrRingPrefix)] != incrRingPrefix {
		return 0, false
	}
	var n uint32
	for i := len(incrRingPrefix); i < mid; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint32(c-'0')
	}
	return n, true
}

// BackupIncremental appends one incremental ring to the backup chain rooted
// at chainDir. The chain head is a full export made by Backup; each call
// anchors at one fixed commit watermark, exactly as Backup does, and the new
// ring captures the terminal state of every key touched after the previous
// link's watermark — puts with their values, deletes as tombstones. Writes
// and deletes committed after the anchor are not included and proceed
// concurrently without blocking; reads keep hitting memory.
//
// The chain is validated before anything is written: when no usable previous
// watermark exists — the chain head is missing or any link is broken — the
// export is rejected wholesale with an error wrapping fs.ErrInvalid and no
// half-built ring is left behind. The ring itself is staged in a temporary
// sibling directory and renamed into the chain complete, so a kill at any
// point never exposes half a ring; staging debris is swept by the next backup
// into that chain or the next Open of a related directory.
//
// A chain path that does not exist or is not a directory is rejected with an
// error wrapping fs.ErrInvalid. BackupIncremental on a closed store returns
// an error wrapping fs.ErrClosed.
func (s *Store) BackupIncremental(chainDir string) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" {
		return errBadBackup
	}
	info, err := os.Stat(chainDir)
	if err != nil {
		return errBadBackup
	}
	if !info.IsDir() {
		return errBadBackup
	}

	// Clear staging debris from an incremental export killed mid-run, then
	// validate the whole existing chain: there is no usable previous
	// watermark to anchor a new ring to unless the chain is intact.
	sweepIncrementalDebris(chainDir)
	head, rings, err := loadChain(chainDir)
	if err != nil {
		return err
	}
	prev := head.watermark
	if len(rings) > 0 {
		prev = rings[len(rings)-1].info.watermark
	}

	// Anchor at one fixed watermark. This is the only moment the export takes
	// the write lock; everything after it streams from an immutable view.
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return errStoreClosed
	}
	anchor := backupAnchor{
		view:      s.current.Load(),
		watermark: s.nextSeq,
		snaps:     s.snapshots.Load(),
	}
	s.mu.Unlock()
	if anchor.watermark < prev {
		// The chain tip is ahead of this store: the chain belongs to a
		// different or newer state and cannot be extended here.
		return errBadBackup
	}

	return writeRing(chainDir, uint32(len(rings))+1, prev, anchor)
}

// writeRing builds one incremental ring covering (base, anchor.watermark] and
// atomically adds it to the chain. It holds no store lock: the anchored view
// is immutable.
func writeRing(chainDir string, index uint32, base uint64, anchor backupAnchor) error {
	stage, err := os.MkdirTemp(filepath.Dir(chainDir), filepath.Base(chainDir)+incrStagePrefix)
	if err != nil {
		return err
	}
	cleanup := func() {
		os.RemoveAll(stage)
	}

	// Only the terminal state of keys touched after the base watermark enters
	// the ring. Keys are collected first and values read and written a chunk
	// at a time, exactly as the full export does, so the whole keyspace is
	// never resident before hitting disk.
	var keys []string
	anchor.view.rangeEach(func(k string, n *node) bool {
		if n.seq > base {
			keys = append(keys, k)
		}
		return true
	})
	sort.Strings(keys)

	var segments []manifestSegment
	var total uint64
	var segIndex uint32
	for start := 0; start < len(keys); start += backupEntriesPerSegment {
		stop := start + backupEntriesPerSegment
		if stop > len(keys) {
			stop = len(keys)
		}
		entries := make([]backupEntry, 0, stop-start)
		for _, k := range keys[start:stop] {
			n := anchor.view.lookup(k)
			if n == nil || n.seq <= base {
				continue
			}
			e := backupEntry{key: k, seq: n.seq, present: n.present}
			if n.present {
				e.value = cloneBytes(n.value)
			}
			entries = append(entries, e)
		}
		if len(entries) == 0 {
			continue
		}
		cumulative := total + uint64(len(entries))
		crc, err := writeSegment(stage, segIndex, anchor, cumulative, entries)
		if err != nil {
			cleanup()
			return err
		}
		segments = append(segments, manifestSegment{
			index:      segIndex,
			count:      uint32(len(entries)),
			cumulative: cumulative,
			crc:        crc,
		})
		total = cumulative
		segIndex++
	}

	// The manifest completes the ring's content; the rename of the staging
	// directory into the chain is the commit point, so the ring only ever
	// appears complete.
	if err := writeRingManifest(stage, anchor, base, total, segments); err != nil {
		cleanup()
		return err
	}
	if err := syncDirectory(stage); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(stage, filepath.Join(chainDir, ringDirName(index))); err != nil {
		cleanup()
		return err
	}
	return syncDirectory(chainDir)
}

// writeRingManifest serializes the ring's manifest inside the staging
// directory and forces it to stable storage. The layout shares the head
// manifest's magic, format version, watermark, snapshot-count, total and
// per-segment records, and inserts the ring's base watermark after the
// snapshot count.
func writeRingManifest(dir string, anchor backupAnchor, base, total uint64, segs []manifestSegment) error {
	path := filepath.Join(dir, backupManifest)
	tmp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(path)
		return err
	}

	w := bufio.NewWriter(tmp)
	head := make([]byte, backupRingManifestSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint64(head[8:16], anchor.watermark)
	binary.LittleEndian.PutUint64(head[16:24], anchor.snaps)
	binary.LittleEndian.PutUint64(head[24:32], base)
	binary.LittleEndian.PutUint64(head[32:40], total)
	binary.LittleEndian.PutUint32(head[40:44], uint32(len(segs)))
	h := crc32.NewIEEE()
	if _, err := w.Write(head); err != nil {
		return fail(err)
	}
	h.Write(head)

	rec := make([]byte, backupSegRecSize)
	for _, s := range segs {
		binary.LittleEndian.PutUint32(rec[0:4], s.index)
		binary.LittleEndian.PutUint32(rec[4:8], s.count)
		binary.LittleEndian.PutUint64(rec[8:16], s.cumulative)
		binary.LittleEndian.PutUint32(rec[16:20], s.crc)
		if _, err := w.Write(rec); err != nil {
			return fail(err)
		}
		h.Write(rec)
	}
	var crcb [crcSize]byte
	binary.LittleEndian.PutUint32(crcb[:], h.Sum32())
	if _, err := w.Write(crcb[:]); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

// loadChain validates the backup chain rooted at chainDir and returns its
// head manifest and every ring in chain order. The head's file set must be
// exactly its manifest and the segments it names; the only directories
// allowed alongside are densely numbered ring directories (1 and up, no
// gaps). Each ring must be a complete self-describing artifact whose base
// watermark continues the previous link's export watermark, with watermarks
// and cumulative snapshot counts non-decreasing along the chain. Any defect —
// a missing or foreign entry, a gap in the numbering, a broken ring, a
// checksum or version mismatch, or a discontinuous watermark — rejects the
// whole chain with an error wrapping fs.ErrInvalid.
func loadChain(chainDir string) (manifestInfo, []chainRing, error) {
	head, err := loadBackupManifest(chainDir)
	if err != nil {
		return manifestInfo{}, nil, err
	}

	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return manifestInfo{}, nil, err
	}
	want := make(map[uint32]bool, len(head.segments))
	for _, s := range head.segments {
		if s.index != uint32(len(want)) {
			return manifestInfo{}, nil, errBadBackup // dense, ordered 0..n-1
		}
		want[s.index] = true
	}
	got := make(map[uint32]bool, len(want))
	ringDirs := make(map[uint32]string)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			idx, ok := parseRingDirName(name)
			if !ok {
				return manifestInfo{}, nil, errBadBackup // foreign directory
			}
			if _, dup := ringDirs[idx]; dup {
				return manifestInfo{}, nil, errBadBackup
			}
			ringDirs[idx] = name
			continue
		}
		switch {
		case name == backupManifest:
		case name == backupManifestTmp:
			return manifestInfo{}, nil, errBadBackup // an unfinished head export
		default:
			idx, ok := parseSegmentIndex(name)
			if !ok || !want[idx] || got[idx] {
				return manifestInfo{}, nil, errBadBackup
			}
			got[idx] = true
		}
	}
	if len(got) != len(want) {
		return manifestInfo{}, nil, errBadBackup // a named head segment is missing
	}

	rings := make([]chainRing, 0, len(ringDirs))
	prevWM, prevSnaps := head.watermark, head.snaps
	for i := uint32(1); i <= uint32(len(ringDirs)); i++ {
		name, ok := ringDirs[i]
		if !ok {
			return manifestInfo{}, nil, errBadBackup // a gap is a missing ring
		}
		dir := filepath.Join(chainDir, name)
		info, base, err := loadManifestFile(filepath.Join(dir, backupManifest), true)
		if err != nil {
			return manifestInfo{}, nil, err
		}
		if err := verifyBackupFileSet(dir, info.segments); err != nil {
			return manifestInfo{}, nil, err
		}
		if base != prevWM || info.watermark < base || info.snaps < prevSnaps {
			return manifestInfo{}, nil, errBadBackup // watermark discontinuity
		}
		rings = append(rings, chainRing{index: i, dir: dir, info: info, base: base})
		prevWM, prevSnaps = info.watermark, info.snaps
	}
	return head, rings, nil
}

// ringStream streams the entries of one chain link (the head or one ring) in
// ascending key order, validating every segment against the link's manifest
// as it goes: header fields, entry framing, commit sequences inside the
// link's watermark interval, strictly ascending unique keys, per-segment
// checksums and the link's total. Any defect fails the stream with an error
// wrapping fs.ErrInvalid.
type ringStream struct {
	dir  string
	info manifestInfo
	base uint64 // entries must carry commit sequences in (base, info.watermark]

	f       *os.File
	r       *bufio.Reader
	h       hash.Hash32
	seg     int    // index of the open segment in info.segments
	left    uint32 // entries not yet read from the open segment
	open    bool
	seen    uint64
	last    string
	hasLast bool
	cur     backupEntry
}

// advance moves the stream to its next entry and reports whether one is
// available. An exhausted link whose entry total disagrees with its manifest
// is an error.
func (s *ringStream) advance() (bool, error) {
	for {
		if s.open && s.left == 0 {
			if err := s.finishSegment(); err != nil {
				return false, err
			}
			s.open = false
		}
		if !s.open {
			if s.seg >= len(s.info.segments) {
				if s.seen != s.info.total {
					return false, errBadBackup
				}
				return false, nil
			}
			if err := s.openSegment(); err != nil {
				return false, err
			}
		}
		e, err := s.readEntry()
		if err != nil {
			return false, err
		}
		s.left--
		s.seen++
		s.cur = e
		return true, nil
	}
}

// openSegment opens the next segment, reads its header and validates it
// against the manifest record.
func (s *ringStream) openSegment() error {
	seg := s.info.segments[s.seg]
	f, err := os.Open(filepath.Join(s.dir, segmentName(seg.index)))
	if err != nil {
		return err
	}
	s.f = f
	s.r = bufio.NewReader(f)
	s.h = crc32.NewIEEE()

	hdr := make([]byte, backupSegHeaderSize)
	if err := readFull(s.r, hdr); err != nil {
		return err
	}
	s.h.Write(hdr)
	if binary.LittleEndian.Uint32(hdr[0:4]) != backupMagic ||
		binary.LittleEndian.Uint32(hdr[4:8]) != backupVersion ||
		binary.LittleEndian.Uint64(hdr[8:16]) != s.info.watermark ||
		binary.LittleEndian.Uint64(hdr[16:24]) != s.info.snaps ||
		binary.LittleEndian.Uint32(hdr[24:28]) != seg.index ||
		binary.LittleEndian.Uint32(hdr[28:32]) != seg.count ||
		binary.LittleEndian.Uint64(hdr[32:40]) != seg.cumulative {
		return errBadBackup
	}
	s.left = seg.count
	s.open = true
	s.seg++
	return nil
}

// readEntry reads and validates one entry of the open segment.
func (s *ringStream) readEntry() (backupEntry, error) {
	ehdr := make([]byte, checkpointEntrySize)
	if err := readFull(s.r, ehdr); err != nil {
		return backupEntry{}, err
	}
	s.h.Write(ehdr)
	flag := ehdr[0]
	keyLen := binary.LittleEndian.Uint32(ehdr[1:5])
	valLen := binary.LittleEndian.Uint32(ehdr[5:9])
	seq := binary.LittleEndian.Uint64(ehdr[9:17])
	if (flag != ckptFlagPut && flag != ckptFlagDelete) ||
		keyLen == 0 || keyLen > maxRecord || valLen > maxRecord ||
		(flag == ckptFlagDelete && valLen != 0) {
		return backupEntry{}, errBadBackup
	}
	// Every entry must lie inside the link's watermark interval.
	if seq == 0 || seq <= s.base || seq > s.info.watermark {
		return backupEntry{}, errBadBackup
	}
	body := make([]byte, int(keyLen)+int(valLen))
	if err := readFull(s.r, body); err != nil {
		return backupEntry{}, err
	}
	s.h.Write(body)
	key := string(body[:keyLen])
	// Entries must be strictly ascending and unique within the link.
	if s.hasLast && key <= s.last {
		return backupEntry{}, errBadBackup
	}
	s.last, s.hasLast = key, true
	e := backupEntry{key: key, seq: seq, present: flag == ckptFlagPut}
	if e.present {
		e.value = cloneBytes(body[keyLen:])
	}
	return e, nil
}

// finishSegment validates the open segment's terminal checksum and its end.
func (s *ringStream) finishSegment() error {
	seg := s.info.segments[s.seg-1]
	var crcb [crcSize]byte
	if err := readFull(s.r, crcb[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(crcb[:]) != s.h.Sum32() ||
		binary.LittleEndian.Uint32(crcb[:]) != seg.crc {
		return errBadBackup
	}
	if _, err := s.r.ReadByte(); !errors.Is(err, io.EOF) {
		return errBadBackup
	}
	s.f.Close()
	s.f = nil
	return nil
}

// close releases the stream's open segment file, if any.
func (s *ringStream) close() {
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
}

// mergeItem is one link's current entry in the synthesis merge.
type mergeItem struct {
	entry  backupEntry
	stream *ringStream
	ring   int // chain position; higher overrides lower for the same key
}

// mergeHeap orders the links' current entries by key, breaking ties toward
// the newest link so the chain's last word on a key pops first.
type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].entry.key != h[j].entry.key {
		return h[i].entry.key < h[j].entry.key
	}
	return h[i].ring > h[j].ring
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// advanceAndPush moves an item's stream past its current entry and pushes the
// stream's next entry back on the heap when one remains.
func advanceAndPush(h *mergeHeap, it mergeItem) error {
	ok, err := it.stream.advance()
	if err != nil {
		return err
	}
	if ok {
		it.entry = it.stream.cur
		heap.Push(h, it)
	}
	return nil
}

// buildSynthesizedStore streams the whole chain into one fresh wal.log in the
// staging directory: the head and every ring are merged in chain order so a
// later link's entry overrides an earlier link's for the same key, and a key
// whose terminal state is a tombstone is dropped — exactly the terminal state
// a full replay reaches at the chain's final watermark. The merge holds one
// current entry per link, so memory tracks the chain length, never the
// keyspace. Nothing is installed until the whole chain validates.
func buildSynthesizedStore(stage, chainDir string, head manifestInfo, rings []chainRing) error {
	links := make([]*ringStream, 0, len(rings)+1)
	links = append(links, &ringStream{dir: chainDir, info: head})
	for _, rg := range rings {
		links = append(links, &ringStream{dir: rg.dir, info: rg.info, base: rg.base})
	}
	defer func() {
		for _, l := range links {
			l.close()
		}
	}()

	h := &mergeHeap{}
	for ring, l := range links {
		ok, err := l.advance()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, mergeItem{entry: l.cur, stream: l, ring: ring})
		}
	}

	wf, err := os.OpenFile(filepath.Join(stage, walName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		wf.Close()
		return err
	}
	l := &walWriter{f: wf, w: bufio.NewWriter(wf)}

	for h.Len() > 0 {
		it := heap.Pop(h).(mergeItem)
		key := it.entry.key
		// Discard the same key from every older link still at the heap top.
		for h.Len() > 0 && (*h)[0].entry.key == key {
			dup := heap.Pop(h).(mergeItem)
			if err := advanceAndPush(h, dup); err != nil {
				return fail(err)
			}
		}
		if it.entry.present {
			if err := l.encodeFrameSeq(opPutSeq, key, it.entry.seq, it.entry.value); err != nil {
				return fail(err)
			}
		}
		if err := advanceAndPush(h, it); err != nil {
			return fail(err)
		}
	}

	snaps := head.snaps
	if len(rings) > 0 {
		snaps = rings[len(rings)-1].info.snaps
	}
	if snaps > 0 {
		var v [metaValueSize]byte
		binary.LittleEndian.PutUint64(v[:], snaps)
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

// sweepIncrementalDebris removes staging directories left next to dir by an
// incremental export killed before its ring was renamed into the chain. It
// only ever removes the specific sibling names BackupIncremental creates.
func sweepIncrementalDebris(dir string) {
	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	prefix := base + incrStagePrefix
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() {
			continue
		}
		if len(name) > len(prefix) && name[:len(prefix)] == prefix {
			_ = os.RemoveAll(filepath.Join(parent, name))
		}
	}
	_ = syncDirectory(parent)
}
