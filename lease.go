package snapshot

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cross-storage coordination of one shared backup chain.
//
// The chain directory has no owner on its own: several Store objects, even in
// different processes, can be asked to run a full export, an incremental ring
// append, a chain merge or a restore against the same chain directory at the
// same time. The lease is the one self-describing holder record that makes
// chain advancement exclusive.
//
// Two files cooperate:
//
//	<chain>.lease          the authoritative lease, held in the chain
//	                       directory's parent. It is a single, stable path
//	                       even across a merge, which swaps the chain
//	                       directory itself, so exclusion is one atomic claim
//	                       for every kind of operation.
//	<chain>/lease.dat      a self-describing mirror of the record carried
//	                       inside the chain directory, for inspection. It is
//	                       descriptive only: exclusion is decided solely by the
//	                       authoritative sibling file, so this mirror can never
//	                       wedge a chain, and a stale one is always swept.
//
// A lease names its holder (operation kind, process id, host, issue time and a
// random nonce) and carries an absolute expiry time. The holder that created
// it is the only request allowed to advance the chain; every other request
// finds the live lease, fails the whole operation without touching the chain,
// and reports an error wrapping fs.ErrInvalid. Only the two standard error
// classes ever escape: fs.ErrInvalid for contention and every malformed chain
// case, fs.ErrClosed for an operation on a closed store.
//
// Leases are crash-safe: the file is built under a unique temporary name,
// forced and hard-linked into place (link(2) never overwrites), so a lease is
// either wholly present or absent. A holder killed mid-operation stops
// renewing its lease; once the expiry passes the next backup, incremental
// export, merge, restore or open of the directory treats the lease as dead,
// removes it, and takes over — the chain underneath is either the old chain
// or one complete new chain, so recovery never needs a manual step and cannot
// deadlock. A well-formed lease that has not reached its expiry is never
// taken over: the contender fails with fs.ErrInvalid and changes nothing. An
// unparseable lease cannot be a live holder (a live holder only ever installs
// a complete file), so it is treated as dead debris and reclaimed rather than
// wedging the chain.
//
// A live holder renews its lease periodically, atomically rewriting the same
// record with a moved-out expiry, so a long streamed export or merge keeps the
// lease while its process is alive regardless of chain length. A merge copies
// a freshly renewed record into the staged new chain before its swap, so the
// in-directory mirror is present in the complete new chain as well.
//
// Lease file layout (all integers little-endian):
//
//	magic    uint32   leaseMagic
//	version  uint32   leaseVersion
//	kind     uint8    leaseKind{Full,Incremental,Merge,Restore}
//	hostLen  uint16   bytes of the host name
//	pid      uint64   holder process id
//	issued   uint64   holder issue time, Unix nanoseconds
//	expires  uint64   lease expiry time, Unix nanoseconds
//	nonce    [8]byte  random holder identity
//	host     [hostLen]byte
//	crc      uint32   IEEE CRC-32 of every preceding byte
const (
	leaseFileName = "lease.dat"
	leaseTmpName  = "lease.dat.tmp"
	// leaseClaimPrefix names the unique temp a contender builds its claim in
	// inside the chain directory before linking the authoritative sibling:
	// "<chain>/.lease-claim-<nonce>". It lives where no other contender's
	// debris sweep globs, so concurrent claims can never unlink one another's
	// in-flight temp.
	leaseClaimPrefix = ".lease-claim-"
	// leaseSiblingSuffix names the authoritative lease as a sibling of the
	// chain directory: filepath.Join(parent, base+leaseSiblingSuffix).
	leaseSiblingSuffix = ".chain-lease"

	leaseMagic   uint32 = 0x5341454c // bytes "LEAS"
	leaseVersion uint32 = 1

	leaseKindFull        byte = 1
	leaseKindIncremental byte = 2
	leaseKindMerge       byte = 3
	leaseKindRestore     byte = 4

	leaseFixedSize = 4 + 4 + 1 + 2 + 8 + 8 + 8 + 8 // magic, version, kind, hostLen, pid, issued, expires, nonce
)

// leaseTTL is how long one lease record stays valid without renewal. It is
// short enough that a killed holder is reclaimed promptly, while a live
// holder rewrites the record well inside it. Tests override it.
var leaseTTL = 10 * time.Second

// leaseNow is the clock the lease protocol reads; tests override it.
var leaseNow = time.Now

// errLeaseBusy is reported when a live, unexpired lease held by another
// holder blocks chain advancement. It wraps fs.ErrInvalid like every other
// whole-operation rejection; no third error class is introduced.
var errLeaseBusy = errors.Join(
	errors.New("snapshot: backup chain is leased by another holder"),
	fs.ErrInvalid,
)

// leaseRecord is the parsed identity and validity window of one lease file.
type leaseRecord struct {
	kind    byte
	pid     uint64
	issued  int64
	expires int64
	nonce   [8]byte
	host    string
}

// sameHolder reports whether two records identify the same lease holder. The
// renewal-rewritten expiry is deliberately not part of the identity.
func (r leaseRecord) sameHolder(o leaseRecord) bool {
	return r.kind == o.kind && r.pid == o.pid && r.issued == o.issued &&
		r.nonce == o.nonce && r.host == o.host
}

// leaseToken is one acquired lease. Its holder must call release exactly
// once; the background renewer stops first. Holder identity (nonce, issue
// time, pid, host and kind) is immutable and is the fencing token: a successor
// lease always carries a freshly generated identity.
type leaseToken struct {
	authPath string // authoritative sibling lease this token owns
	authTmp  string // unique temp name renewals rewrite through
	dir      string // chain directory the in-directory mirror lives in

	mu  sync.Mutex
	rec leaseRecord // guarded by mu; only expires changes

	stop chan struct{}
	done chan struct{}
}

// chainLeasePath is the authoritative lease path for the chain dir.
func chainLeasePath(dir string) string {
	return filepath.Join(filepath.Dir(dir), filepath.Base(dir)+leaseSiblingSuffix)
}

// encodeLease serializes rec into a self-describing record ending in its CRC.
func encodeLease(rec leaseRecord) []byte {
	host := []byte(rec.host)
	buf := make([]byte, leaseFixedSize+len(host)+crcSize)
	binary.LittleEndian.PutUint32(buf[0:4], leaseMagic)
	binary.LittleEndian.PutUint32(buf[4:8], leaseVersion)
	buf[8] = rec.kind
	binary.LittleEndian.PutUint16(buf[9:11], uint16(len(host)))
	binary.LittleEndian.PutUint64(buf[11:19], rec.pid)
	binary.LittleEndian.PutUint64(buf[19:27], uint64(rec.issued))
	binary.LittleEndian.PutUint64(buf[27:35], uint64(rec.expires))
	copy(buf[35:43], rec.nonce[:])
	copy(buf[leaseFixedSize:], host)
	crc := crc32.ChecksumIEEE(buf[:leaseFixedSize+len(host)])
	binary.LittleEndian.PutUint32(buf[leaseFixedSize+len(host):], crc)
	return buf
}

// parseLease decodes and fully verifies one lease file's bytes. A truncated,
// corrupt or foreign record returns ok=false: such bytes can never be a live
// holder's record (holders only install a complete linked file), so the
// caller treats the file as dead debris.
func parseLease(data []byte) (leaseRecord, bool) {
	var zero leaseRecord
	if len(data) < leaseFixedSize+crcSize {
		return zero, false
	}
	hostLen := int(binary.LittleEndian.Uint16(data[9:11]))
	if len(data) != leaseFixedSize+hostLen+crcSize {
		return zero, false
	}
	if binary.LittleEndian.Uint32(data[0:4]) != leaseMagic ||
		binary.LittleEndian.Uint32(data[4:8]) != leaseVersion {
		return zero, false
	}
	if binary.LittleEndian.Uint32(data[len(data)-crcSize:]) !=
		crc32.ChecksumIEEE(data[:leaseFixedSize+hostLen]) {
		return zero, false
	}
	kind := data[8]
	if kind != leaseKindFull && kind != leaseKindIncremental &&
		kind != leaseKindMerge && kind != leaseKindRestore {
		return zero, false
	}
	rec := leaseRecord{
		kind:    kind,
		pid:     binary.LittleEndian.Uint64(data[11:19]),
		issued:  int64(binary.LittleEndian.Uint64(data[19:27])),
		expires: int64(binary.LittleEndian.Uint64(data[27:35])),
		host:    string(data[leaseFixedSize : leaseFixedSize+hostLen]),
	}
	copy(rec.nonce[:], data[35:43])
	return rec, true
}

// leaseStateAtPath reads and classifies one lease path.
//
//	live=true:  a well-formed, unexpired lease — a contender must fail
//	dead=true:  a well-formed but expired lease, or an unparseable/non-file
//	            entry — the caller may reclaim the path
//	both false: no lease is present
//
// A genuine read error other than "not exists" passes through.
func leaseStateAtPath(path string) (rec leaseRecord, live, dead bool, err error) {
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		if errors.Is(rerr, fs.ErrNotExist) {
			return rec, false, false, nil
		}
		// A directory or other non-regular entry occupying a lease name is
		// foreign debris, never a live holder's record.
		if info, serr := os.Stat(path); serr == nil && !info.Mode().IsRegular() {
			return rec, false, true, nil
		}
		return rec, false, false, rerr
	}
	parsed, ok := parseLease(data)
	if !ok {
		return rec, false, true, nil // corrupt debris, never a live holder
	}
	if leaseNow().UnixNano() >= parsed.expires {
		return parsed, false, true, nil
	}
	return parsed, true, false, nil
}

// writeLeaseFile durably writes data to a fresh temp name and forces it; the
// caller links or renames it into its final name.
func writeLeaseFile(tmpPath string, data []byte, exclusive bool) error {
	flag := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if exclusive {
		flag |= os.O_EXCL
	}
	tmp, err := os.OpenFile(tmpPath, flag, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

// newHolderRecord builds a fresh identity for kind, valid from now until the
// current lease TTL elapses.
func newHolderRecord(kind byte) (leaseRecord, error) {
	rec := leaseRecord{
		kind:    kind,
		pid:     uint64(os.Getpid()),
		issued:  leaseNow().UnixNano(),
		expires: leaseNow().Add(leaseTTL).UnixNano(),
		host:    hostname(),
	}
	if _, err := rand.Read(rec.nonce[:]); err != nil {
		return rec, err
	}
	return rec, nil
}

// hostname returns a short best-effort holder host label; descriptive only.
func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "unknown-host"
	}
	return name
}

// writeInsideMirror places the self-describing record at dir/lease.dat through
// a temp name and forces the directory. It is descriptive, never authoritative,
// so a failure is returned for the caller to ignore at its discretion.
func writeInsideMirror(dir string, rec leaseRecord) error {
	final := filepath.Join(dir, leaseFileName)
	tmp := filepath.Join(dir, leaseTmpName+"-"+hex.EncodeToString(rec.nonce[:]))
	if err := writeLeaseFile(tmp, encodeLease(rec), false); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDirectory(dir)
}

// acquireLease takes exclusive advancement rights over the backup chain in
// dir. Every kind of holder claims the same single, stable sibling path with a
// never-overwriting hard link, so exactly one holder can own the chain at a
// time even across a merge's directory swap. The call fails with
// errLeaseBusy when a live lease guards the chain, reclaims an expired or
// corrupt lease first, and otherwise installs a fresh lease and its
// in-directory mirror.
func acquireLease(dir string, kind byte) (*leaseToken, error) {
	authPath := chainLeasePath(dir)
	parent := filepath.Dir(authPath)

	// A bounded number of reclaim/claim rounds absorbs races with other
	// processes also discovering a dead lease; contention on a live lease
	// fails on the first inspection.
	for attempt := 0; attempt < 100; attempt++ {
		_, live, dead, err := leaseStateAtPath(authPath)
		if err != nil {
			return nil, err
		}
		if live {
			return nil, errLeaseBusy
		}
		if dead {
			if err := removeLeaseDebris(parent, filepath.Base(authPath)); err != nil {
				return nil, err
			}
		}

		rec, err := newHolderRecord(kind)
		if err != nil {
			return nil, err
		}
		data := encodeLease(rec)
		// Build the claim temp inside the chain directory: it is a location no
		// other contender's debris sweep globs, so concurrent claims can never
		// unlink each other's in-flight temp. The hard link then publishes it
		// at the authoritative sibling path.
		tmpPath := filepath.Join(dir, leaseClaimPrefix+hex.EncodeToString(rec.nonce[:]))
		if err := writeLeaseFile(tmpPath, data, true); err != nil {
			if errors.Is(err, fs.ErrExist) {
				continue
			}
			return nil, err
		}
		// link(2) fails rather than overwrite: exactly one contender claims
		// the one authoritative position.
		if lerr := os.Link(tmpPath, authPath); lerr != nil {
			os.Remove(tmpPath)
			if errors.Is(lerr, fs.ErrExist) {
				continue // another contender claimed first; re-inspect
			}
			return nil, lerr
		}
		os.Remove(tmpPath)
		if err := syncDirectory(dir); err != nil {
			return nil, err
		}
		if err := syncDirectory(parent); err != nil {
			return nil, err
		}
		tok := &leaseToken{
			authPath: authPath,
			authTmp:  authPath + ".tmp-renew-" + hex.EncodeToString(rec.nonce[:]),
			dir:      dir,
			rec:      rec,
			stop:     make(chan struct{}),
			done:     make(chan struct{}),
		}
		// The in-directory mirror is descriptive; a failure never invalidates
		// the authoritative claim.
		_ = writeInsideMirror(dir, rec)
		sweepClaimDebris(dir, hex.EncodeToString(rec.nonce[:]))
		go tok.renew()
		return tok, nil
	}
	return nil, errLeaseBusy
}

// sweepClaimDebris removes in-chain claim temps a contender hard-killed during
// its claim left behind, keeping ownNonce's name (already unlinked by the
// winner). Best effort; such names are whitelisted by chain validation
// regardless, so they are never mistaken for artifact files.
func sweepClaimDebris(dir, ownNonce string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > len(leaseClaimPrefix) && name[:len(leaseClaimPrefix)] == leaseClaimPrefix {
			if name[len(leaseClaimPrefix):] == ownNonce {
				continue
			}
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}

// stillOurs reports that the authoritative lease still holds a live record
// identifying this holder. It is the guard every renewal, commit fence and
// release use: a holder that stalled past its TTL and was reclaimed must never
// touch the lease a contender has since installed.
func (t *leaseToken) stillOurs() bool {
	cur, live, _, err := leaseStateAtPath(t.authPath)
	if err != nil || !live {
		return false
	}
	t.mu.Lock()
	identity := t.rec
	t.mu.Unlock()
	return cur.sameHolder(identity)
}

// freshen extends the lease's expiry immediately and returns a record
// snapshot, for a renewal or a commit that must install a fresh mirror (the
// merge swap copies it into the staged new chain).
func (t *leaseToken) freshen() leaseRecord {
	t.mu.Lock()
	t.rec.expires = leaseNow().Add(leaseTTL).UnixNano()
	snap := t.rec
	t.mu.Unlock()
	return snap
}

// renew keeps the lease ahead of its expiry while the holder operation runs.
// Every rewrite is the same all-or-nothing install, so contenders only ever
// read a complete record. Both the authoritative sibling and the in-directory
// mirror are refreshed. Around a merge's swap t.dir names the old chain
// before the rename and the new chain after it; the merge separately installs
// a fresh mirror into the staged chain to cover the rename window. A failed
// renewal simply lets the lease age; the operation itself is unaffected.
func (t *leaseToken) renew() {
	defer close(t.done)
	gap := leaseTTL / 3
	if gap <= 0 {
		gap = leaseTTL
	}
	ticker := time.NewTicker(gap)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			if !t.stillOurs() {
				return
			}
			rec := t.freshen()
			data := encodeLease(rec)
			if err := writeLeaseFile(t.authTmp, data, false); err == nil {
				if err := os.Rename(t.authTmp, t.authPath); err == nil {
					_ = syncDirectory(filepath.Dir(t.authPath))
				} else {
					os.Remove(t.authTmp)
				}
			}
			_ = writeInsideMirror(t.dir, rec)
		}
	}
}

// release surrenders the lease. It removes only an authoritative record that
// still identifies this exact holder and clears the in-directory mirror; if
// the lease aged out and was taken over, the successor's record is left in
// place. Removing either file never touches the chain itself.
func (t *leaseToken) release() {
	close(t.stop)
	<-t.done
	if t.stillOurs() {
		if err := os.Remove(t.authPath); err == nil {
			_ = syncDirectory(filepath.Dir(t.authPath))
		}
	}
	os.Remove(t.authTmp)
	clearInsideMirror(t.dir)
}

// clearInsideMirror removes the descriptive lease.dat mirror and its temps
// from dir. Best effort.
func clearInsideMirror(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isLeaseCoordinationName(e.Name()) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	_ = syncDirectory(dir)
}

// removeLeaseDebris deletes one dead authoritative lease and its temp files,
// forcing the directory.
func removeLeaseDebris(parent, leaseBase string) error {
	if err := os.Remove(filepath.Join(parent, leaseBase)); err != nil &&
		!errors.Is(err, fs.ErrNotExist) {
		return err
	}
	sweepLeaseTmps(parent, leaseBase)
	return syncDirectory(parent)
}

// sweepLeaseTmps removes regular files that are install/renewal temps of the
// lease at parent/<leaseBase>: they share the "<leaseBase>.tmp" prefix. Best
// effort.
func sweepLeaseTmps(parent, leaseBase string) {
	prefix := leaseBase + ".tmp"
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > len(prefix) && name[:len(prefix)] == prefix {
			_ = os.Remove(filepath.Join(parent, name))
		}
	}
}

// isLeaseCoordinationName reports whether name is coordination metadata: the
// in-directory lease mirror, one of its temps, or a contender's claim temp.
// Chain validation and directory preparation treat these as coordination
// metadata, never as foreign artifact files.
func isLeaseCoordinationName(name string) bool {
	return name == leaseFileName ||
		(len(name) > len(leaseTmpName) && name[:len(leaseTmpName)] == leaseTmpName) ||
		(len(name) > len(leaseClaimPrefix) && name[:len(leaseClaimPrefix)] == leaseClaimPrefix)
}

// recoverChainCoordinationOwned runs after the caller has itself acquired the
// authoritative lease. Any merge staging/trash siblings are leftovers of a
// prior killed holder and are repaired unconditionally; the caller's own
// authoritative lease and the mirror it holds are preserved.
func recoverChainCoordinationOwned(dir string) {
	sweepMergeDebrisCore(dir)
}

// recoverChainCoordination reclaims coordination debris a killed holder can
// leave in and next to dir, before any chain operation inspects the chain:
//
//   - while a live authoritative lease exists the directory is owned by a
//     holder (a merge may be mid-swap), so nothing is moved or deleted;
//   - once the authoritative lease is dead, a killed merge's swap is repaired
//     (old chain moved back when the chain path is missing; staging/trash
//     siblings removed), the dead lease and its temps are reaped, and a stale
//     in-directory mirror is cleared.
func recoverChainCoordination(dir string) {
	authPath := chainLeasePath(dir)
	if _, live, _, err := leaseStateAtPath(authPath); err == nil && live {
		return
	}
	sweepMergeDebrisCore(dir)
	// Reap a dead authoritative lease and any install/renewal temps.
	if err := os.Remove(authPath); err == nil {
		_ = syncDirectory(filepath.Dir(authPath))
	}
	sweepLeaseTmps(filepath.Dir(authPath), filepath.Base(authPath))
	// No live authoritative holder: every in-directory mirror is stale.
	clearInsideMirror(dir)
}

// sweepMergeDebrisCore repairs the sibling directories a merge can leave next
// to chainDir when it is killed: a staging chain ("<base>.merge-new-*") and a
// retired old chain ("<base>.merge-old-*"). If the chain path itself is
// missing but a retired old chain exists, the merge was killed between the two
// commit renames and the whole old chain is moved back into place first, so a
// sweep can never delete the old chain while the chain name is gone. It does
// not touch the authoritative lease; the caller decides that. Best effort.
func sweepMergeDebrisCore(chainDir string) {
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
	if _, serr := os.Stat(chainDir); errors.Is(serr, fs.ErrNotExist) && len(trash) > 0 {
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
