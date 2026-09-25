package snapshot

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// On-demand backup-chain merging.
//
// A backup chain only grows: one full head followed by dense incremental
// rings. MergeChain folds the head together with the first rings rings of the
// chain into one fresh full head capturing the complete committed state at
// that last folded ring's watermark; every surviving ring stays on the chain,
// renumbered into a dense 1-based sequence linked to the new head. The merged
// chain is byte-for-byte equivalent at its tip to the old chain through that
// watermark, and Restore keeps validating it as one whole.
//
// Merge debris lives next to the chain directory, never inside it while a
// merge runs:
//
//	<base>.merge.tmp/   staging directory holding the complete new chain
//	<base>.merge.obs    old chain directory parked across the atomic swap
//
// The merge streams a k-way merge over the artifacts (the head plus one
// source per folded ring) and writes the new head in bounded chunks: entries
// travel over small buffered channels and only one output chunk is resident,
// so the whole keyspace is never copied into memory before writing. The swap
// is two directory renames — the live chain is parked aside and the fully
// synced staging directory is renamed into its place — so a kill at any
// instant leaves either the old chain or the complete new chain.
// sweepMergeLeftovers heals a kill between the two renames by parking the old
// chain back, and removes staging debris at the next merge or backup into
// that directory, the next Restore of it, or the next Open of a related
// directory.
//
// The merge reads artifacts only: the store that produced the chain is never
// modified, and MergeChain takes no store lock, so writes, batch commits,
// snapshot open/close, cursor paging, checkpoints, compaction and lock-free
// reads all proceed while it runs. Exports and merges issued by the same
// store serialize on backupMu, so an incremental export and a merge can never
// install over each other or leave half a ring or half a chain.

const (
	mergeStageSuffix = ".merge.tmp"
	mergeObsSuffix   = ".merge.obs"

	// mergeChanEntries bounds how far one artifact source may read ahead of
	// the merge. With one entry per source held by the heap on top of it,
	// residency is bounded by the artifact count and one output chunk, never
	// by the keyspace size.
	mergeChanEntries = 8
)

func mergeStagePath(chainDir string) string {
	return filepath.Join(filepath.Dir(chainDir), filepath.Base(chainDir)+mergeStageSuffix)
}

func mergeObsPath(chainDir string) string {
	return filepath.Join(filepath.Dir(chainDir), filepath.Base(chainDir)+mergeObsSuffix)
}

// errMergeStopped ends a source pump early once the consumer has aborted the
// merge; it never reaches a caller (the consumer already reported its error).
var errMergeStopped = errors.New("snapshot: merge stopped")

// sweepMergeLeftovers heals or removes debris of a merge killed around its
// swap. The artifacts are siblings of chainDir named after chainDir's base, so
// a sweep for one directory never touches another directory's merge. A parked
// old chain with no live chain is renamed back; when the new chain already
// landed the parked copy is discarded; a leftover staging directory is pure
// debris.
func sweepMergeLeftovers(chainDir string) error {
	obs := mergeObsPath(chainDir)
	parent := filepath.Dir(chainDir)

	if info, err := os.Stat(obs); err == nil && info.IsDir() {
		if _, serr := os.Stat(chainDir); serr != nil {
			if !errors.Is(serr, fs.ErrNotExist) {
				return serr
			}
			// Kill between the two renames: the new chain never took its name.
			// Put the parked old chain back.
			if err := os.Rename(obs, chainDir); err != nil {
				return err
			}
			if err := syncDirectory(parent); err != nil {
				return err
			}
		} else {
			// Kill after the new chain landed but before the parked copy was
			// dropped: the complete new chain is state of record.
			if err := os.RemoveAll(obs); err != nil {
				return err
			}
			if err := syncDirectory(parent); err != nil {
				return err
			}
		}
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	if err := os.RemoveAll(mergeStagePath(chainDir)); err != nil &&
		!errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// mergeEntry is one terminal key state streamed out of an artifact.
type mergeEntry struct {
	key   string
	flag  byte
	value []byte
	seq   uint64
}

// mergeMsg is one pump message: ok delivers an entry, otherwise err is the
// terminal result (io.EOF for a fully consumed, intact artifact).
type mergeMsg struct {
	ent mergeEntry
	ok  bool
	err error
}

// startHeadStream pumps the fully validated head entries through a bounded
// channel in ascending key order. Validation is exactly streamHeadSegments';
// a defect terminates the channel with errBadBackup.
func startHeadStream(chainDir string, head manifestInfo, done <-chan struct{}) <-chan mergeMsg {
	ch := make(chan mergeMsg, mergeChanEntries)
	go func() {
		cb := func(flag byte, key string, value []byte, seq uint64) error {
			select {
			case ch <- mergeMsg{ok: true, ent: mergeEntry{key: key, flag: flag, value: value, seq: seq}}:
				return nil
			case <-done:
				return errMergeStopped
			}
		}
		err := streamHeadSegments(chainDir, head, cb)
		finishMergePump(ch, done, err)
	}()
	return ch
}

// startRingStream pumps one fully validated folded ring through a bounded
// channel. Linkage and watermark continuity are exactly scanRing's checks
// against exp; a defect terminates the channel with errBadBackup.
func startRingStream(chainDir string, exp ringExpect, done <-chan struct{}) <-chan mergeMsg {
	ch := make(chan mergeMsg, mergeChanEntries)
	go func() {
		f, err := os.Open(filepath.Join(chainDir, ringName(exp.index)))
		if err != nil {
			finishMergePump(ch, done, err)
			return
		}
		cb := func(flag byte, key string, value []byte, seq uint64) error {
			select {
			case ch <- mergeMsg{ok: true, ent: mergeEntry{key: key, flag: flag, value: value, seq: seq}}:
				return nil
			case <-done:
				return errMergeStopped
			}
		}
		_, serr := scanRing(f, exp, cb)
		f.Close()
		finishMergePump(ch, done, serr)
	}()
	return ch
}

// finishMergePump delivers the terminal result to the consumer. A clean scan
// is reported as io.EOF; errMergeStopped means the consumer already aborted
// and the pump simply exits.
func finishMergePump(ch chan<- mergeMsg, done <-chan struct{}, err error) {
	if errors.Is(err, errMergeStopped) {
		return
	}
	if err == nil {
		err = io.EOF
	}
	select {
	case ch <- mergeMsg{ok: false, err: err}:
	case <-done:
	}
}

// mergeHeap orders items by key, then by source index. Sources are ordered
// head (0), ring 1, ring 2, …, so among equal keys the largest source index
// — the latest artifact — is the terminal state and wins.
type mergeHeapItem struct {
	ent mergeEntry
	src int
}

type mergeHeap []mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].ent.key != h[j].ent.key {
		return h[i].ent.key < h[j].ent.key
	}
	return h[i].src < h[j].src
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeHeapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// streamMergedHead runs the k-way merge and writes the new full head (segments
// then manifest) into stage. The new head carries watermark endWM and the
// cumulative snapshot count snaps from the last folded ring. It streams one
// bounded chunk at a time and never holds the whole keyspace. It returns the
// new manifest's terminal CRC, which the first surviving ring links to.
func streamMergedHead(stage, chainDir string, art artifactInfo, fold uint32, endWM, snaps uint64) (uint32, error) {
	done := make(chan struct{})
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			close(done)
		}
	}
	// Closing done unblocks every pump; defer covers every return path.
	defer stop()
	srcs := make([]<-chan mergeMsg, int(fold)+1)
	srcs[0] = startHeadStream(chainDir, art.head, done)
	prevWM := art.head.watermark
	prevLink := art.head.crc
	for i := uint32(1); i <= fold; i++ {
		srcs[i] = startRingStream(chainDir, ringExpect{
			index:    i,
			prevLink: prevLink,
			startWM:  prevWM,
		}, done)
		prevWM = art.rings[i-1].endWM
		prevLink = art.rings[i-1].fileCRC
	}

	h := &mergeHeap{}
	// refill primes one more item from source src; a clean EOF exhausts it.
	refill := func(src int) error {
		m := <-srcs[src]
		if m.ok {
			heap.Push(h, mergeHeapItem{ent: m.ent, src: src})
			return nil
		}
		if errors.Is(m.err, io.EOF) {
			return nil
		}
		return m.err
	}
	for i := range srcs {
		if err := refill(i); err != nil {
			return 0, err
		}
	}

	anchor := backupAnchor{watermark: endWM, snaps: snaps}
	var segments []manifestSegment
	var total uint64
	var segIndex uint32
	chunk := make([]backupEntry, 0, backupEntriesPerSegment)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		cumulative := total + uint64(len(chunk))
		crc, err := writeSegment(stage, segIndex, anchor, cumulative, chunk)
		if err != nil {
			return err
		}
		segments = append(segments, manifestSegment{
			index:      segIndex,
			count:      uint32(len(chunk)),
			cumulative: cumulative,
			crc:        crc,
		})
		total = cumulative
		segIndex++
		chunk = chunk[:0]
		return nil
	}

	emit := func(e mergeEntry) error {
		// A full head carries present keys only, exactly like Backup: a
		// restored store has no older history for a tombstone to mask, so a
		// delete folded in from a ring simply leaves the key absent.
		if e.flag == ckptFlagDelete {
			return nil
		}
		chunk = append(chunk, backupEntry{
			key:     e.key,
			value:   cloneBytes(e.value),
			seq:     e.seq,
			present: true,
		})
		if len(chunk) == cap(chunk) {
			return flush()
		}
		return nil
	}

	for h.Len() > 0 {
		cur := heap.Pop(h).(mergeHeapItem)
		if err := refill(cur.src); err != nil {
			return 0, err
		}
		// Drain every other artifact's state for this key; the last one (the
		// highest source index) is the terminal state at the fold watermark.
		for h.Len() > 0 && (*h)[0].ent.key == cur.ent.key {
			cur = heap.Pop(h).(mergeHeapItem)
			if err := refill(cur.src); err != nil {
				return 0, err
			}
		}
		if err := emit(cur.ent); err != nil {
			return 0, err
		}
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return writeManifest(stage, stage, anchor, total, segments)
}

// rebasedRing carries the fixed header fields of one renumbered surviving
// ring: its entries and watermarks are unchanged, but its index and backward
// content link are rewritten for its place on the merged chain.
type rebasedRing struct {
	oldIndex uint32
	newIndex uint32
	prevLink uint32
	startWM  uint64
	endWM    uint64
	snaps    uint64
	total    uint64 // entry total, carried over unchanged
	segCount uint32 // segment count, carried over unchanged
}

// streamRebaseRing validates the surviving old ring (against expOld) and
// writes it to the staging directory under its new index with corrected
// linkage, streaming entries straight through the scanner one at a time: only
// the entry currently being copied and one buffered writer are resident.
// Entry bytes, per-entry sequences, watermarks, the snapshot count, segment
// count and cumulative counts are preserved; only the index, prevLink and the
// CRCs that cover them change. It returns the new file CRC the next survivor
// (or a later appended ring) links to.
func streamRebaseRing(stage, chainDir string, rb rebasedRing, expOld ringExpect) (uint32, error) {
	final := ringName(rb.newIndex)
	in, err := os.Open(filepath.Join(chainDir, ringName(rb.oldIndex)))
	if err != nil {
		return 0, err
	}
	defer in.Close()

	tmp, err := os.OpenFile(filepath.Join(stage, final), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (uint32, error) {
		tmp.Close()
		os.Remove(filepath.Join(stage, final))
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

	// The original writer chunked entries at backupEntriesPerSegment, so the
	// segment boundaries and counts are determined entirely by the validated
	// total; reproduce them exactly. The last segment holds the remainder.
	wantSegs := uint32(0)
	if rb.total > 0 {
		wantSegs = uint32((rb.total + backupEntriesPerSegment - 1) / backupEntriesPerSegment)
	}
	if wantSegs != rb.segCount {
		return fail(errBadBackup)
	}

	head := make([]byte, incrRingHeaderSize)
	binary.LittleEndian.PutUint32(head[0:4], backupMagic)
	binary.LittleEndian.PutUint32(head[4:8], backupVersion)
	binary.LittleEndian.PutUint32(head[8:12], rb.newIndex)
	binary.LittleEndian.PutUint32(head[12:16], rb.prevLink)
	binary.LittleEndian.PutUint64(head[16:24], rb.startWM)
	binary.LittleEndian.PutUint64(head[24:32], rb.endWM)
	binary.LittleEndian.PutUint64(head[32:40], rb.snaps)
	binary.LittleEndian.PutUint32(head[40:44], rb.segCount)
	if err := write(head); err != nil {
		return fail(err)
	}

	ehdr := make([]byte, checkpointEntrySize)
	var segChain uint32
	var cumulative uint64
	var inSeg uint32 // entries already written into the open segment
	var seg uint32   // index of the segment being written
	var hs interface {
		Write(p []byte) (int, error)
		Sum32() uint32
	}

	// segCountAt returns the number of entries segment j holds.
	segCountAt := func(j uint32) uint32 {
		if j+1 < rb.segCount {
			return backupEntriesPerSegment
		}
		return uint32(rb.total - uint64(j)*backupEntriesPerSegment)
	}

	startSegment := func() error {
		count := segCountAt(seg)
		cumulative += uint64(count)
		sh := make([]byte, incrSegHeaderSize)
		binary.LittleEndian.PutUint32(sh[0:4], backupMagic)
		binary.LittleEndian.PutUint64(sh[4:12], rb.startWM)
		binary.LittleEndian.PutUint64(sh[12:20], rb.endWM)
		binary.LittleEndian.PutUint32(sh[20:24], rb.newIndex)
		binary.LittleEndian.PutUint32(sh[24:28], seg)
		binary.LittleEndian.PutUint32(sh[28:32], count)
		binary.LittleEndian.PutUint64(sh[32:40], cumulative)
		if err := write(sh); err != nil {
			return err
		}
		h := crc32.NewIEEE()
		h.Write(sh)
		hs = h
		inSeg = 0
		return nil
	}

	writeEntry := func(flag byte, key string, value []byte, seqv uint64) error {
		if inSeg == 0 {
			if err := startSegment(); err != nil {
				return err
			}
		}
		if flag == ckptFlagPut {
			ehdr[0] = ckptFlagPut
		} else {
			ehdr[0] = ckptFlagDelete
		}
		binary.LittleEndian.PutUint32(ehdr[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(ehdr[5:9], uint32(len(value)))
		binary.LittleEndian.PutUint64(ehdr[9:17], seqv)
		if err := write(ehdr); err != nil {
			return err
		}
		hs.Write(ehdr)
		if err := write([]byte(key)); err != nil {
			return err
		}
		hs.Write([]byte(key))
		if len(value) > 0 {
			if err := write(value); err != nil {
				return err
			}
			hs.Write(value)
		}
		inSeg++
		if inSeg == segCountAt(seg) {
			var sc [crcSize]byte
			binary.LittleEndian.PutUint32(sc[:], hs.Sum32())
			if err := write(sc[:]); err != nil {
				return err
			}
			segChain = crc32.Update(segChain, crc32.IEEETable, sc[:])
			seg++
			inSeg = 0
		}
		return nil
	}

	// scanRing fully validates the old ring (header, per-segment headers and
	// CRCs, ordering, footer, file CRC) and hands each entry in order.
	sum, serr := scanRing(in, expOld, func(flag byte, key string, value []byte, seqv uint64) error {
		return writeEntry(flag, key, value, seqv)
	})
	if serr != nil {
		return fail(serr)
	}
	if sum.total != rb.total || sum.segCount != rb.segCount || seg != rb.segCount {
		return fail(errBadBackup)
	}
	if inSeg != 0 {
		return fail(errBadBackup) // a segment was left unfinished
	}

	foot := make([]byte, incrFooterSize)
	binary.LittleEndian.PutUint64(foot[0:8], cumulative)
	binary.LittleEndian.PutUint32(foot[8:12], rb.segCount)
	binary.LittleEndian.PutUint32(foot[12:16], segChain)
	if err := write(foot); err != nil {
		return fail(err)
	}
	fileCRC := hf.Sum32()
	var fc [crcSize]byte
	binary.LittleEndian.PutUint32(fc[:], fileCRC)
	// Flush before the terminal CRC: it is not part of its own input.
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
		os.Remove(filepath.Join(stage, final))
		return 0, err
	}
	return fileCRC, nil
}

// buildMergedChain stages the complete replacement chain: the freshly merged
// full head followed by the surviving rings renumbered densely from 1 and
// relinked to the new head. Nothing in chainDir is touched; on failure the
// staging directory is removed and the live chain stays authoritative.
func buildMergedChain(chainDir, stage string, art artifactInfo, fold uint32, endWM, snaps uint64) error {
	headCRC, err := streamMergedHead(stage, chainDir, art, fold, endWM, snaps)
	if err != nil {
		return err
	}
	// Re-validate each surviving ring against the old chain while rebasing it,
	// tracking linkage in both coordinate systems.
	oldWM := endWM
	oldLink := art.rings[fold-1].fileCRC
	prevLink := headCRC
	for k := fold + 1; k <= art.ringCount; k++ {
		sum := art.rings[k-1]
		newCRC, werr := streamRebaseRing(stage, chainDir, rebasedRing{
			oldIndex: k,
			newIndex: k - fold,
			prevLink: prevLink,
			startWM:  oldWM,
			endWM:    sum.endWM,
			snaps:    sum.snaps,
			total:    sum.total,
			segCount: sum.segCount,
		}, ringExpect{index: k, prevLink: oldLink, startWM: oldWM})
		if werr != nil {
			return werr
		}
		prevLink = newCRC
		oldWM = sum.endWM
		oldLink = sum.fileCRC
	}
	return syncDirectory(stage)
}

// commitMergedChain parks the live chain, promotes the staged chain and drops
// the parked copy, forcing the parent directory at each metadata step. A kill
// at any point leaves either the old chain (sweepMergeLeftovers renames a
// parked copy back) or the complete new chain. A failed second rename rolls
// the parked old chain back before returning.
func commitMergedChain(chainDir, stage, obs string) error {
	parent := filepath.Dir(chainDir)
	if err := os.Rename(chainDir, obs); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.Rename(stage, chainDir); err != nil {
		if rb := os.Rename(obs, chainDir); rb != nil {
			return errors.Join(err, rb)
		}
		_ = syncDirectory(parent)
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	if err := os.RemoveAll(obs); err != nil {
		return err
	}
	return syncDirectory(parent)
}

// MergeChain folds the full chain head together with the first rings
// incremental rings of the backup chain in chainDir into one fresh full head,
// and leaves every later ring on the chain renumbered densely behind it. The
// result is the complete committed state at the watermark of that last folded
// ring: keys and values the chain reaches there and the cumulative snapshot
// count are preserved exactly, batches stay visible as whole batches with
// within-batch last-write-wins intact, and a delete covered by a folded ring
// never exposes its old value again.
//
// The merged chain keeps the existing format version, export-watermark and
// cumulative-snapshot vocabulary with whole-artifact checksums: Restore
// validates it exactly as before, its numbering is dense and watermarks meet
// end to end, and rings appended afterwards link to the new head and stay
// valid. Folded rings are deleted as soon as the new chain is committed.
//
// The merge is segmented and streamed, never copying the whole keyspace into
// memory before landing it; it does not block writes, batch commits,
// snapshots, cursors, checkpoints, compaction or incremental exports, and
// reads keep hitting memory. The backed-up store itself is never modified: a
// snapshot reads and the restored terminal state are byte-identical before
// and after a merge.
//
// A rings count below one or above the number of rings present, a missing
// chain directory, a missing head, a gap, a broken chain, discontinuous
// watermarks, truncation, a checksum failure or an unknown version reject the
// whole operation with an error wrapping fs.ErrInvalid and leave the chain
// exactly as it was, with no half ring or half chain. MergeChain on a closed
// store returns an error wrapping fs.ErrClosed.
func (s *Store) MergeChain(chainDir string, rings int) error {
	if s.closed.Load() {
		return errStoreClosed
	}
	// Serialize against this store's incremental exports so the two can never
	// swap or link over each other. It is independent of the store lock and is
	// held across disk I/O, so commits and lock-free reads never wait on it.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return errStoreClosed
	}
	if chainDir == "" || rings < 1 {
		return errBadBackup
	}

	// Heal or clear debris of a merge killed around its swap before the chain
	// path is inspected, so a parked chain is restored into place first.
	if err := sweepMergeLeftovers(chainDir); err != nil {
		return err
	}

	info, err := os.Stat(chainDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errBadBackup
		}
		return err
	}
	if !info.IsDir() {
		return errBadBackup
	}

	// Clear debris from an export killed mid-run before looking at the chain.
	for _, stage := range []string{incrStageDir, backupStageDir} {
		if err := os.RemoveAll(filepath.Join(chainDir, stage)); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(chainDir, backupManifestTmp)); err != nil &&
		!errors.Is(err, fs.ErrNotExist) {
		return err
	}

	art, err := validateBackupChain(chainDir)
	if err != nil {
		return err
	}
	if rings > int(art.ringCount) {
		return errBadBackup
	}
	fold := uint32(rings)
	folded := art.rings[fold-1]
	endWM := folded.endWM
	snaps := folded.snaps

	stage := mergeStagePath(chainDir)
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	fail := func(err error) error {
		_ = os.RemoveAll(stage)
		return err
	}
	if err := buildMergedChain(chainDir, stage, art, fold, endWM, snaps); err != nil {
		return fail(err)
	}
	if err := commitMergedChain(chainDir, stage, mergeObsPath(chainDir)); err != nil {
		return fail(err)
	}
	return nil
}
