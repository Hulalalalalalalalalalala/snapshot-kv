package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Backup chain merge and old-ring retirement.
//
// MergeChain folds the chain head and the first n incremental rings into one
// new full head, so a chain that only ever grows can be compacted back down:
// the merged head is exactly the complete committed state at the n-th ring's
// watermark, with keys, values and the cumulative snapshot count preserved,
// and the rings it absorbed are deleted. The rings beyond n stay on the
// chain: their entries, watermarks and snapshot counts are unchanged, but
// they are re-linked and renumbered from 1 behind the new head, so the merged
// chain still validates as one whole — dense ring numbering, continuous
// watermarks, unbroken content-CRC links — and later BackupIncremental rings
// append to it exactly as before.
//
// The merge is fully streamed: the new head is built by a k-way merge over
// the old head segments and the first n rings (all key-sorted streams), and
// each surviving ring is re-encoded entry by entry. One bounded chunk of
// entries is resident at a time; the keyspace is never copied into memory
// wholesale before hitting disk.
//
// The new chain is assembled in a sibling staging directory
// ("<chain>.merge-new-*") next to the chain directory and validated as a
// whole chain before anything is committed. The commit swaps directories:
// the old chain is renamed aside ("<chain>.merge-old-*"), the staged chain is
// renamed onto the chain path, and only then is the old chain deleted. A kill
// at any instant therefore leaves either the old chain or the complete new
// chain at the chain path — at worst a staging or trash sibling remains, and
// a kill between the two renames is repaired by moving the whole old chain
// back. The debris is swept at the next merge, backup or incremental export
// into that directory, or the next open of a related directory.
//
// MergeChain never touches the backed-up store: it reads only chain
// artifacts, so writes, batch commits, snapshot open/close, cursor paging,
// checkpoints, compactions and lock-free reads proceed while it runs. It is
// serialized with incremental exports on the same store (and re-validates the
// chain tip before committing, so a chain advanced concurrently by another
// exporter rejects the merge wholesale rather than being clobbered by it).
const (
	mergeStagePrefix = ".merge-new-"
	mergeTrashPrefix = ".merge-old-"
)

// errMergeAbort stops a stream goroutine when the merge that owns it fails;
// it is internal flow control and never escapes to a caller.
var errMergeAbort = errors.New("snapshot: merge stream aborted")

// MergeChain merges the head of the backup chain in chainDir with its first
// rings incremental rings into one new full head and deletes the absorbed
// rings. The merged chain restores to exactly the state and cumulative
// snapshot count the original chain captured, keeps the remaining rings
// (renumbered from 1 behind the new head), passes the same whole-chain
// validation Restore performs, and accepts further BackupIncremental rings.
//
// The merge streams every artifact in bounded chunks and never holds the
// whole keyspace in memory. It runs entirely on the chain directory: the
// backed-up store is never modified, and live writes, batch commits,
// snapshots, cursors, checkpoints, compactions and reads proceed while it
// runs. The new chain is staged as a sibling directory, validated as a whole,
// and swapped into place so a kill at any instant leaves either the old chain
// or the complete new chain; leftover staging or trash siblings are swept at
// the next merge, backup or export into that directory or the next open of a
// related directory.
//
// The merge takes the chain directory's lease before touching anything, so
// concurrent exports, merges and restores on the same chain directory — from
// this store, another store or another process — are mutually exclusive: at
// most one registered operation advances the chain at a time. A merge that
// cannot take the lease fails wholesale with an error wrapping fs.ErrInvalid
// and leaves the chain exactly as it was; a lease left behind by a killed
// holder is reclaimed, never waited on.
//
// A ring count below one or beyond the chain's ring count, a chain directory
// that is missing, not a directory, missing its head, or itself damaged (a
// missing ring, a broken link, a non-continuous watermark, truncation, a
// checksum failure or an unknown version), or a chain advanced concurrently
// during the merge, rejects the whole operation with an error wrapping
// fs.ErrInvalid and leaves the chain exactly as it was. MergeChain on a
// closed store returns an error wrapping fs.ErrClosed.
func (s *Store) MergeChain(chainDir string, rings int) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize with incremental exports driven by this store so a merge and
	// an export never race to reshape the same chain. This lock is
	// independent of mu: it is held across the merge's disk I/O and never
	// blocks commits or lock-free reads.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" || rings < 1 {
		return errBadBackup
	}
	// Coordinate with every other operation on this chain directory, in this
	// process or another: only the lease holder may repair debris, validate
	// the chain and swap a merged chain into place. A live holder fails the
	// whole merge; a killed holder's lease is reclaimed.
	lease, err := acquireChainLease(chainDir)
	if err != nil {
		return err
	}
	defer lease.release()
	// Repair or clear debris from a merge killed mid-commit before inspecting
	// the chain, exactly as the next backup or open of the directory would.
	sweepMergeDebris(chainDir)
	info, err := os.Stat(chainDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errBadBackup
		}
		return err
	}
	if !info.IsDir() {
		return errBadBackup
	}
	// Clear staging debris from an export (full or incremental) killed
	// mid-run; a manifest temp is pure debris as well.
	for _, stage := range []string{incrStageDir, backupStageDir} {
		if err := os.RemoveAll(filepath.Join(chainDir, stage)); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(chainDir, backupManifestTmp)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}

	art, err := validateBackupChain(chainDir)
	if err != nil {
		return err
	}
	if uint64(rings) > uint64(art.ringCount) {
		return errBadBackup
	}
	n := rings

	// Stage the whole merged chain as a sibling of the chain directory so the
	// final swap is a same-filesystem rename. The chain directory itself is
	// not touched until the commit.
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	stage, err := os.MkdirTemp(parent, base+mergeStagePrefix)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			os.RemoveAll(stage)
		}
	}()

	// The new head covers the chain through ring n: its export watermark and
	// cumulative snapshot count are exactly that ring's.
	tip := art.ringSums[n-1]
	anchor := backupAnchor{watermark: tip.endWM, snaps: tip.snaps}
	segs, total, err := mergeChainHead(chainDir, stage, art, n, anchor)
	if err != nil {
		return err
	}
	if err := writeManifest(stage, stage, anchor, total, segs); err != nil {
		return err
	}
	newHead, err := loadBackupManifest(stage)
	if err != nil {
		return err
	}

	// Re-encode each surviving ring under its new 1-based index, linked to
	// the artifact before it in the merged chain. Entries, watermarks and
	// snapshot counts are carried over unchanged.
	prevLink := newHead.crc
	for j := n + 1; j <= int(art.ringCount); j++ {
		old := art.ringSums[j-1]
		prev := art.ringSums[j-2]
		hdr := ringHeader{
			index:    uint32(j - n),
			prevLink: prevLink,
			startWM:  prev.endWM,
			endWM:    old.endWM,
			snaps:    old.snaps,
		}
		exp := ringExpect{
			index:    uint32(j),
			prevLink: prev.fileCRC,
			startWM:  prev.endWM,
		}
		prevLink, err = renumberRing(chainDir, stage, exp, hdr, old.segCount, old.total)
		if err != nil {
			return err
		}
	}

	// The staged chain must pass the same whole-chain validation Restore
	// performs, and it must reach the same tip the original chain reached.
	newArt, err := validateBackupChain(stage)
	if err != nil {
		return err
	}
	if newArt.ringCount != art.ringCount-uint32(n) ||
		newArt.endWM != art.endWM || newArt.snaps != art.snaps {
		return errBadBackup
	}

	// Refuse to commit over a chain that moved since it was validated (an
	// export or merge driven by another store): the merge is rejected
	// wholesale and the chain keeps its newer shape.
	again, err := validateBackupChain(chainDir)
	if err != nil {
		return err
	}
	if again.head.crc != art.head.crc || again.ringCount != art.ringCount ||
		again.tipCRC != art.tipCRC || again.endWM != art.endWM ||
		again.snaps != art.snaps {
		return errBadBackup
	}

	// Commit: move the old chain aside, move the staged chain onto the chain
	// path, then delete the old chain. A kill before the first rename leaves
	// the old chain; a kill between the renames leaves the whole old chain in
	// the trash sibling (swept back into place next time); a kill after the
	// second rename leaves the complete new chain.
	trash, err := os.MkdirTemp(parent, base+mergeTrashPrefix)
	if err != nil {
		return err
	}
	if err := os.Remove(trash); err != nil {
		return err
	}
	if err := os.Rename(chainDir, trash); err != nil {
		return err
	}
	if err := os.Rename(stage, chainDir); err != nil {
		// Best-effort rollback: the old chain is whole at trash.
		if rb := os.Rename(trash, chainDir); rb == nil {
			_ = syncDirectory(parent)
		}
		return err
	}
	committed = true
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.RemoveAll(trash); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// streamItem carries one entry (or a terminal error) from a chain artifact
// being streamed by a goroutine.
type streamItem struct {
	flag  byte
	key   string
	value []byte
	seq   uint64
	err   error
}

// mergeStream is the pull side of one streamed chain artifact. The goroutine
// validates as it streams, so a consumed stream is a validated artifact.
type mergeStream struct {
	ch   chan streamItem
	stop chan struct{}
	cur  streamItem
	has  bool
	err  error
}

// send hands one item to the consumer; it reports errMergeAbort when the
// merge has been abandoned, which unwinds the streaming goroutine.
func (ms *mergeStream) send(it streamItem) error {
	select {
	case ms.ch <- it:
		return nil
	case <-ms.stop:
		return errMergeAbort
	}
}

// next advances to the next entry; afterwards has reports whether cur holds
// an entry and err holds the stream's terminal error, if any.
func (ms *mergeStream) next() {
	it, ok := <-ms.ch
	if !ok {
		ms.has = false
		return
	}
	if it.err != nil {
		ms.err = it.err
		ms.has = false
		return
	}
	ms.cur = it
	ms.has = true
}

// abort stops the streaming goroutine and lets it exit; it is safe to call
// after the stream has already finished.
func (ms *mergeStream) abort() {
	select {
	case <-ms.stop:
	default:
		close(ms.stop)
	}
}

// startHeadStream streams the head's segments in order, validating as they
// are read.
func startHeadStream(dir string, head manifestInfo) *mergeStream {
	ms := &mergeStream{ch: make(chan streamItem, 1), stop: make(chan struct{})}
	go func() {
		defer close(ms.ch)
		err := streamHeadSegments(dir, head, func(flag byte, key string, value []byte, seq uint64) error {
			return ms.send(streamItem{flag: flag, key: key, value: value, seq: seq})
		})
		if err != nil {
			ms.send(streamItem{err: err})
		}
	}()
	return ms
}

// startRingStream streams one ring's entries in order, validating the whole
// ring and its linkage against exp as it is read.
func startRingStream(dir string, exp ringExpect) *mergeStream {
	ms := &mergeStream{ch: make(chan streamItem, 1), stop: make(chan struct{})}
	go func() {
		defer close(ms.ch)
		f, err := os.Open(filepath.Join(dir, ringName(exp.index)))
		if err != nil {
			ms.send(streamItem{err: err})
			return
		}
		defer f.Close()
		_, err = scanRing(f, exp, func(flag byte, key string, value []byte, seq uint64) error {
			return ms.send(streamItem{flag: flag, key: key, value: value, seq: seq})
		})
		if err != nil {
			ms.send(streamItem{err: err})
		}
	}()
	return ms
}

// mergeChainHead builds the merged head's segments in stage by k-way merging
// the old head with the first n rings, all strictly key-ordered streams. For
// a key present in several streams the latest ring wins (rings in order, then
// the head); a winning tombstone drops the key, so deletes covered by the
// merged rings read back as misses. Only one chunk of entries is resident at
// a time. It returns the manifest segment records and the total entry count.
func mergeChainHead(chainDir, stage string, art artifactInfo, n int, anchor backupAnchor) ([]manifestSegment, uint64, error) {
	streams := make([]*mergeStream, 0, n+1)
	streams = append(streams, startHeadStream(chainDir, art.head))
	prevLink := art.head.crc
	prevWM := art.head.watermark
	for j := 1; j <= n; j++ {
		streams = append(streams, startRingStream(chainDir, ringExpect{
			index:    uint32(j),
			prevLink: prevLink,
			startWM:  prevWM,
		}))
		sum := art.ringSums[j-1]
		prevLink, prevWM = sum.fileCRC, sum.endWM
	}
	defer func() {
		for _, s := range streams {
			s.abort()
		}
	}()
	for _, s := range streams {
		s.next()
	}

	var segs []manifestSegment
	var total uint64
	var segIdx uint32
	chunk := make([]backupEntry, 0, backupEntriesPerSegment)
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		cumulative := total + uint64(len(chunk))
		crc, err := writeSegment(stage, segIdx, anchor, cumulative, chunk)
		if err != nil {
			return err
		}
		segs = append(segs, manifestSegment{
			index:      segIdx,
			count:      uint32(len(chunk)),
			cumulative: cumulative,
			crc:        crc,
		})
		total = cumulative
		segIdx++
		chunk = chunk[:0]
		return nil
	}

	for {
		// The smallest key still pending across all streams is next; among
		// the streams holding it, the latest ring (highest stream index)
		// holds its terminal state.
		winner := -1
		var min string
		for i, s := range streams {
			if s.has && (winner < 0 || s.cur.key < min) {
				min, winner = s.cur.key, i
			}
		}
		if winner < 0 {
			break
		}
		for i, s := range streams {
			if s.has && s.cur.key == min {
				winner = i
			}
		}
		if w := streams[winner]; w.cur.flag == ckptFlagPut {
			chunk = append(chunk, backupEntry{
				key:     w.cur.key,
				value:   w.cur.value,
				seq:     w.cur.seq,
				present: true,
			})
			if len(chunk) == backupEntriesPerSegment {
				if err := flush(); err != nil {
					return nil, 0, err
				}
			}
		}
		for _, s := range streams {
			if s.has && s.cur.key == min {
				s.next()
			}
		}
		for _, s := range streams {
			if s.err != nil {
				return nil, 0, s.err
			}
		}
	}
	for _, s := range streams {
		if s.err != nil {
			return nil, 0, s.err
		}
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	return segs, total, nil
}

// renumberRing streams the old ring identified by exp out of chainDir,
// validates it, and re-encodes it in stage under hdr's new index and link.
// The entries, watermarks, snapshot count, segmenting and entry count are
// carried over unchanged from the validated ring summary. It returns the new
// ring's terminal file CRC, the link the next ring must name.
func renumberRing(chainDir, stage string, exp ringExpect, hdr ringHeader, segCount uint32, total uint64) (uint32, error) {
	src := startRingStream(chainDir, exp)
	defer src.abort()
	src.next()
	crc, err := writeMergedRing(filepath.Join(stage, ringName(hdr.index)), hdr, segCount, total, src)
	if err != nil {
		return 0, err
	}
	if src.err != nil {
		return 0, src.err
	}
	if src.has {
		return 0, errBadBackup // more entries than the validated ring declared
	}
	return crc, nil
}

// writeMergedRing streams one ring file at path, pulling exactly total
// entries (chunked into segCount segments) from src. The layout is the ring
// format writeIncrementalRing emits; only the entry source differs. The file
// is fully synced before its CRC is returned.
func writeMergedRing(path string, hdr ringHeader, segCount uint32, total uint64, src *mergeStream) (uint32, error) {
	tmp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (uint32, error) {
		tmp.Close()
		os.Remove(path)
		return 0, err
	}

	w := bufio.NewWriter(tmp)
	hf := crc32.NewIEEE()
	write := func(b []byte) error {
		if _, err := w.Write(b); err != nil {
			return err
		}
		hf.Write(b)
		return nil
	}

	head := make([]byte, incrRingHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint32(head[8:12], hdr.index)
	binary.LittleEndian.PutUint32(head[12:16], hdr.prevLink)
	binary.LittleEndian.PutUint64(head[16:24], hdr.startWM)
	binary.LittleEndian.PutUint64(head[24:32], hdr.endWM)
	binary.LittleEndian.PutUint64(head[32:40], hdr.snaps)
	binary.LittleEndian.PutUint32(head[40:44], segCount)
	if err := write(head); err != nil {
		return fail(err)
	}

	ehdr := make([]byte, checkpointEntrySize)
	var segChain uint32
	var cumulative uint64
	remaining := total
	for seg := uint32(0); seg < segCount; seg++ {
		size := uint64(backupEntriesPerSegment)
		if remaining < size {
			size = remaining
		}
		remaining -= size

		hs := crc32.NewIEEE()
		sh := make([]byte, incrSegHeaderSize)
		binary.LittleEndian.PutUint32(sh[0:4], backupMagic)
		binary.LittleEndian.PutUint64(sh[4:12], hdr.startWM)
		binary.LittleEndian.PutUint64(sh[12:20], hdr.endWM)
		binary.LittleEndian.PutUint32(sh[20:24], hdr.index)
		binary.LittleEndian.PutUint32(sh[24:28], seg)
		cumulative += size
		binary.LittleEndian.PutUint32(sh[28:32], uint32(size))
		binary.LittleEndian.PutUint64(sh[32:40], cumulative)
		if err := write(sh); err != nil {
			return fail(err)
		}
		hs.Write(sh)

		for i := uint64(0); i < size; i++ {
			if !src.has {
				return fail(errBadBackup) // fewer entries than the ring declared
			}
			it := src.cur
			if len(it.key) > maxRecord || len(it.value) > maxRecord {
				return fail(errBadBackup)
			}
			ehdr[0] = it.flag
			binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(it.key)))
			binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(it.value)))
			binary.LittleEndian.PutUint64(ehdr[9:17], it.seq)
			if err := write(ehdr); err != nil {
				return fail(err)
			}
			hs.Write(ehdr)
			if _, err := io.WriteString(w, it.key); err != nil {
				return fail(err)
			}
			hf.Write([]byte(it.key))
			hs.Write([]byte(it.key))
			if len(it.value) > 0 {
				if err := write(it.value); err != nil {
					return fail(err)
				}
				hs.Write(it.value)
			}
			src.next()
			if src.err != nil {
				return fail(src.err)
			}
		}

		var sc [crcSize]byte
		binary.LittleEndian.PutUint32(sc[:], hs.Sum32())
		if err := write(sc[:]); err != nil {
			return fail(err)
		}
		segChain = crc32.Update(segChain, crc32.IEEETable, sc[:])
	}
	if cumulative != total {
		return fail(errBadBackup)
	}

	foot := make([]byte, incrFooterSize)
	binary.LittleEndian.PutUint64(foot[0:8], cumulative)
	binary.LittleEndian.PutUint32(foot[8:12], segCount)
	binary.LittleEndian.PutUint32(foot[12:16], segChain)
	if err := write(foot); err != nil {
		return fail(err)
	}
	var fc [crcSize]byte
	binary.LittleEndian.PutUint32(fc[:], hf.Sum32())
	// Flush every buffered byte before appending the terminal CRC, exactly as
	// writeIncrementalRing does.
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if _, err := tmp.Write(fc[:]); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(path)
		return 0, err
	}
	return binary.LittleEndian.Uint32(fc[:]), nil
}

// sweepMergeDebris clears the sibling directories a merge can leave next to
// chainDir when it is killed: a staging chain ("<base>.merge-new-*") and a
// retired old chain ("<base>.merge-old-*"). If the chain path itself is
// missing but a retired old chain exists, the merge was killed between the
// two commit renames and the whole old chain is moved back into place first.
// It is best-effort: leftover debris never affects the chain at chainDir.
func sweepMergeDebris(chainDir string) {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	var staging, trash []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if isMergeTempName(name, base, mergeStagePrefix) {
			staging = append(staging, name)
		}
		if isMergeTempName(name, base, mergeTrashPrefix) {
			trash = append(trash, name)
		}
	}
	if len(staging) == 0 && len(trash) == 0 {
		return
	}
	changed := false
	if _, err := os.Stat(chainDir); errors.Is(err, os.ErrNotExist) && len(trash) > 0 {
		// Killed between the two commit renames: the old chain is whole in
		// the trash sibling; put it back before clearing anything.
		if err := os.Rename(filepath.Join(parent, trash[0]), chainDir); err == nil {
			changed = true
			trash = trash[1:]
		}
	}
	for _, name := range staging {
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	for _, name := range trash {
		if err := os.RemoveAll(filepath.Join(parent, name)); err == nil {
			changed = true
		}
	}
	if changed {
		_ = syncDirectory(parent)
	}
}

// isMergeTempName reports whether name is a merge staging or trash directory
// for the chain named base.
func isMergeTempName(name, base, prefix string) bool {
	p := base + prefix
	return len(name) > len(p) && name[:len(p)] == p
}
