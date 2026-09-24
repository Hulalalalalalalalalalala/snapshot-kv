package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

// checkpointWindow is the retention window: the number of checkpoint
// generations kept live. Appending a layer past the window rebuilds the chain
// around its newest members (Checkpoint rebases against the unchanged WAL);
// compaction retires every layer outside the window with the old WAL and
// re-anchors the surviving layers against the replacement log, after which
// the retired generations occupy no directory space.
const checkpointWindow = 4

// stagedChainDir holds a replacement checkpoint chain while compaction
// rebuilds the WAL. It is renamed over checkpointDirName only once the new
// WAL itself is in place. A directory of this name found on open is crash or
// failure debris and is swept: the finished chain always lives under
// checkpointDirName.
const stagedChainDir = "ckpt.new"

// compactionPlan is the set of retained key histories the replacement WAL
// must carry, independent of whether a checkpoint chain is re-anchored.
type compactionPlan struct {
	keys []string
	hist map[string][]nodeVer // key -> retained versions, ascending seq
}

// histories renders the plan in the shape buildCompactWAL consumes.
func (p *compactionPlan) histories() []keyHistory {
	out := make([]keyHistory, 0, len(p.keys))
	for _, k := range p.keys {
		out = append(out, keyHistory{key: k, versions: p.hist[k]})
	}
	return out
}

// freshView builds the post-compaction in-memory view: one single-node head
// per currently present key; deleted keys drop out, as in the baseline.
func (p *compactionPlan) freshView() *view {
	v := newView(nil)
	for _, k := range p.keys {
		versions := p.hist[k]
		head := versions[len(versions)-1]
		if head.present {
			v.keys[k] = &node{value: head.value, present: true, seq: head.seq}
		}
	}
	return v
}

// buildCompactionPlan assembles retained histories from the pin set the same
// way the baseline did: every live present head, every cursor-pinned version,
// and the tombstone shielding a cursor-pinned older value; per-key versions
// ascending by sequence. pinned/keySeqs are produced by Compact under mu.
func buildCompactionPlan(pinned map[uint64]*node, keySeqs map[string][]uint64) *compactionPlan {
	keys := make([]string, 0, len(keySeqs))
	for k := range keySeqs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hist := make(map[string][]nodeVer, len(keys))
	for _, k := range keys {
		seqs := keySeqs[k]
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		versions := make([]nodeVer, 0, len(seqs))
		for _, seq := range seqs {
			n := pinned[seq]
			versions = append(versions, nodeVer{seq: seq, present: n.present, value: n.value})
		}
		hist[k] = versions
	}
	return &compactionPlan{keys: keys, hist: hist}
}

// segmentFor returns the segment a frame with seq belongs to: 0 for frames at
// or before the first watermark, j for frames strictly after watermark j-1
// and at or before watermark j, and len(watermarks) for the post-window tail.
func segmentFor(seq uint64, watermarks []uint64) int {
	for j, w := range watermarks {
		if seq <= w {
			return j
		}
	}
	return len(watermarks)
}

// segmentedWAL is the replacement WAL split at the surviving layer
// watermarks and the rebuilt layer metadata paired with its segments.
type segmentedWAL struct {
	segments [][]keyHistory // len == len(watermarks)+1
	cuts     []int64        // cut[j]: offset right after segment j; one per layer
	layers   []rebuiltLayer
	tailKeys []string
}

// rebuiltLayer is one re-anchored layer's metadata; its cut is filled in once
// the segmented WAL is written.
type rebuiltLayer struct {
	entries []checkpointEntry
	seq     uint64
	snaps   uint64
}

// planSegmentedWAL partitions the frames the replacement WAL must carry at
// the surviving layers' sequence watermarks and derives, for each surviving
// layer, the entries its rebuilt file must carry — so that "seed layers
// 0..j, replay frames after cut j" reconstructs the identical terminal state
// a full replay of the new WAL does.
//
// windowLayers are the surviving (in-window) layers oldest first; only their
// sequence watermarks and snapshot counts are used. termStates[j] is the full
// terminal key state (puts and tombstones) at windowLayers[j]'s watermark,
// composed from the complete old chain; it is the authority for what each
// rebuilt layer must reproduce. The retained pin/head frames from plan are
// unioned with one terminal frame per key in each term state (deduped by
// sequence), so a version an in-window layer still shows is never shed even
// when the current view no longer needs it: keeping a layer usable must keep
// its state reconstructable. The extra versions are bounded by the window,
// not by cumulative commits.
func planSegmentedWAL(plan *compactionPlan, windowLayers []layerState, termStates [][]checkpointEntry, cur *view) *segmentedWAL {
	n := len(windowLayers)
	watermarks := make([]uint64, n)
	snaps := make([]uint64, n)
	for i, l := range windowLayers {
		watermarks[i] = l.seq
		snaps[i] = l.snaps
	}

	// Collect the full required frame set per key, keyed by sequence so a
	// version requested by several sources (pin, current head, layer state)
	// is written exactly once.
	type frame struct {
		ver nodeVer
	}
	req := make(map[string]map[uint64]frame)
	add := func(key string, ver nodeVer) {
		m := req[key]
		if m == nil {
			m = make(map[uint64]frame)
			req[key] = m
		}
		if _, ok := m[ver.seq]; !ok {
			m[ver.seq] = frame{ver: ver}
		}
	}
	for _, k := range plan.keys {
		for _, ver := range plan.hist[k] {
			add(k, ver)
		}
	}
	for _, entries := range termStates {
		for _, e := range entries {
			ver := nodeVer{seq: e.seq, present: e.present}
			if e.present {
				ver.value = e.value
			}
			add(e.key, ver)
		}
	}
	// Keep every rebuilt layer usable on recovery, not just the older ones.
	// For each key the newest window layer knows (its entries include
	// tombstones), force the key's *current* terminal node into the retained
	// set. That terminal sits in the tail segment; without it, a key that was
	// deleted after the newest layer would have no tail frame to replay and
	// "seed the newest layer, replay the tail" would resurrect it. The
	// pre-compaction view still carries the tombstone, so cur.lookup finds it.
	for _, e := range termStates[n-1] {
		head := cur.lookup(e.key)
		if head == nil {
			continue
		}
		ver := nodeVer{seq: head.seq, present: head.present}
		if head.present {
			ver.value = head.value
		}
		add(e.key, ver)
	}
	segments := make([][]keyHistory, n+1)
	// Latest retained frame of each key at or before each watermark.
	terminalAt := make([]map[string]nodeVer, n)
	for i := range terminalAt {
		terminalAt[i] = make(map[string]nodeVer)
	}
	tailKeySet := make(map[string]struct{})

	keys := make([]string, 0, len(req))
	for k := range req {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		vers := make([]nodeVer, 0, len(req[k]))
		for _, f := range req[k] {
			vers = append(vers, f.ver)
		}
		sort.Slice(vers, func(a, b int) bool { return vers[a].seq < vers[b].seq })
		buckets := make([][]nodeVer, n+1)
		for _, ver := range vers {
			j := segmentFor(ver.seq, watermarks)
			buckets[j] = append(buckets[j], ver)
			if j < n {
				// Versions are ascending, so the last one placed at or before
				// watermark j is its terminal there.
				terminalAt[j][k] = ver
			} else {
				tailKeySet[k] = struct{}{}
			}
		}
		for j := range buckets {
			if len(buckets[j]) > 0 {
				segments[j] = append(segments[j], keyHistory{key: k, versions: buckets[j]})
			}
		}
	}
	// terminalAt[j] must see the newest frame <= watermark j across ALL
	// earlier segments, not only frames placed into segment j: propagate
	// terminals forward across watermarks.
	for j := 1; j < n; j++ {
		for k, v := range terminalAt[j-1] {
			if _, ok := terminalAt[j][k]; !ok {
				terminalAt[j][k] = v
			}
		}
	}
	for j := range segments {
		sort.Slice(segments[j], func(a, b int) bool { return segments[j][a].key < segments[j][b].key })
	}

	layers := make([]rebuiltLayer, n)
	for j := 0; j < n; j++ {
		var entries []checkpointEntry
		// Deterministic key order over this watermark's full terminal state.
		ks := make([]string, 0, len(terminalAt[j]))
		for k := range terminalAt[j] {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		if j == 0 {
			// Full base: complete terminal state at the oldest window layer.
			entries = make([]checkpointEntry, 0, len(ks))
			for _, k := range ks {
				entries = append(entries, entryFromVer(k, terminalAt[j][k]))
			}
		} else {
			// Delta: only keys whose terminal version changed since the
			// previous window layer (including appearance and deletion).
			entries = make([]checkpointEntry, 0)
			for _, k := range ks {
				prev, had := terminalAt[j-1][k]
				cur := terminalAt[j][k]
				if had && prev.seq == cur.seq {
					continue
				}
				entries = append(entries, entryFromVer(k, cur))
			}
		}
		layers[j] = rebuiltLayer{entries: entries, seq: watermarks[j], snaps: snaps[j]}
	}

	tailKeys := make([]string, 0, len(tailKeySet))
	for k := range tailKeySet {
		tailKeys = append(tailKeys, k)
	}
	sort.Strings(tailKeys)
	return &segmentedWAL{segments: segments, layers: layers, tailKeys: tailKeys}
}

func entryFromVer(key string, ver nodeVer) checkpointEntry {
	e := checkpointEntry{key: key, seq: ver.seq, present: ver.present}
	if ver.present {
		e.value = cloneBytes(ver.value)
	}
	return e
}

// encodeHistoryFrames buffers one key history's frames oldest-to-newest.
func encodeHistoryFrames(l *walWriter, h keyHistory) error {
	for _, ver := range h.versions {
		if ver.present {
			if err := l.encodeFrameSeq(opPutSeq, h.key, ver.seq, ver.value); err != nil {
				return err
			}
		} else {
			if err := l.encodeFrameSeq(opDeleteSeq, h.key, ver.seq, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// buildSegmentedCompactArtifacts writes the complete replacement WAL (one
// contiguous segment per surviving layer followed by the post-window tail and
// the snapshot-count meta) and stages the re-anchored chain under
// stagedChainDir. Nothing the store currently uses is touched: on any error
// all staged files are removed and the old WAL and chain stay authoritative.
func buildSegmentedCompactArtifacts(dir string, seg *segmentedWAL, snapshotCount uint64) error {
	if err := removeStaleCompact(dir); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(dir, stagedChainDir)); err != nil {
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
		os.RemoveAll(path + stagedChainDir)
		return err
	}
	l := &walWriter{f: tmp, w: bufio.NewWriter(tmp)}

	// Write segments in order; the cut of layer j is the file position right
	// after its segment, which is an exact frame boundary (the next segment's
	// first frame starts there).
	cuts := make([]int64, len(seg.layers))
	for j, hists := range seg.segments {
		for i := range hists {
			if err := encodeHistoryFrames(l, hists[i]); err != nil {
				return fail(err)
			}
		}
		if j < len(seg.layers) {
			if err := l.w.Flush(); err != nil {
				return fail(err)
			}
			pos, perr := tmp.Seek(0, 2)
			if perr != nil {
				return fail(perr)
			}
			cuts[j] = pos
		}
	}
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
		return fail(err)
	}

	// Stage the replacement chain: layer j references cut j of the new WAL.
	// Layers link by content hash, so each prevCRC is the CRC of the file
	// written immediately before it and must be filled in write order.
	stagePath := filepath.Join(dir, stagedChainDir)
	if err := os.MkdirAll(stagePath, 0o700); err != nil {
		return fail(errors.New("snapshot: cannot stage checkpoint chain"))
	}
	var prevCRC uint32
	for j, meta := range seg.layers {
		layer := capturedLayer{
			index:   uint32(j),
			prevCRC: prevCRC,
			entries: meta.entries,
			offset:  uint64(cuts[j]),
			seq:     meta.seq,
			snaps:   meta.snaps,
		}
		name := layerFileName(uint32(j))
		crc, werr := writeLayerFile(dir, filepath.Join(stagedChainDir, filepath.Base(name)), layer)
		if werr != nil {
			return fail(werr)
		}
		prevCRC = crc
	}
	if err := syncDirectory(stagePath); err != nil {
		return fail(err)
	}
	seg.cuts = cuts
	return nil
}

// activateStagedChain promotes the staged replacement chain into place after
// the replacement WAL has been renamed over wal.log, and forces the parent
// directory. Its absence means compaction simply leaves no live checkpoint.
func activateStagedChain(dir string) error {
	stage := filepath.Join(dir, stagedChainDir)
	if _, err := os.Stat(stage); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.RemoveAll(filepath.Join(dir, checkpointDirName)); err != nil {
		return err
	}
	if err := os.Rename(stage, filepath.Join(dir, checkpointDirName)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

// removeStagedChain best-effort deletes a staged replacement chain.
func removeStagedChain(dir string) error {
	err := os.RemoveAll(filepath.Join(dir, stagedChainDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// replaceCheckpointChain atomically swaps the live chain for layers. It
// stages the new chain, durably removes the old one and promotes the staging
// directory; the WAL (and every layer cut it references) is left untouched,
// so this is used both when Checkpoint prunes the retention window and never
// by compaction (which stages alongside its new WAL).
func replaceCheckpointChain(dir string, layers []capturedLayer) error {
	if err := os.RemoveAll(filepath.Join(dir, stagedChainDir)); err != nil {
		return err
	}
	stagePath := filepath.Join(dir, stagedChainDir)
	if err := os.MkdirAll(stagePath, 0o700); err != nil {
		return err
	}
	fail := func(err error) error {
		os.RemoveAll(stagePath)
		return err
	}
	// Layers link by content hash; fill prevCRC in write order regardless of
	// what the caller populated, so the rebuilt chain is always self-consistent.
	var prevCRC uint32
	for _, layer := range layers {
		layer.prevCRC = prevCRC
		name := filepath.Base(layerFileName(layer.index))
		crc, werr := writeLayerFile(dir, filepath.Join(stagedChainDir, name), layer)
		if werr != nil {
			return fail(werr)
		}
		prevCRC = crc
	}
	if err := syncDirectory(stagePath); err != nil {
		return fail(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, checkpointDirName)); err != nil {
		return fail(err)
	}
	if err := os.Rename(stagePath, filepath.Join(dir, checkpointDirName)); err != nil {
		return fail(err)
	}
	return syncDirectory(dir)
}

// sortEntries orders layer entries by key.
func sortEntries(entries []checkpointEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
}
