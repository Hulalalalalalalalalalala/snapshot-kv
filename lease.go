package snapshot

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Chain directory coordination.
//
// A backup chain directory is shared state: several store objects — in this
// process or in others — may export into it, merge it or restore from it at
// the same time. To keep the chain consistent, every such operation first
// takes the chain's lease: a small sibling file ("<chain>.lease") that names
// its holder (a process id plus a random token) and carries an expiry time.
// At most one registered operation holds the lease at a time; an operation
// that cannot take it fails wholesale with an error wrapping fs.ErrInvalid
// and touches nothing on or around the chain.
//
// The lease is reclaimed, never waited on. A holder that is force-killed
// leaves the file behind, and the next backup, merge, restore or open of a
// related directory recognizes the lease as stale — its holder process is
// gone or its expiry has passed — removes it and proceeds. The chain is
// therefore never wedged behind a dead holder and needs no manual cleanup.
// A live holder renews its expiry in the background, so a long export or
// merge is never reclaimed from under itself.
//
// The lease file lives next to the chain directory, not inside it, so it is
// never part of the artifact the whole-chain validation sees, and a leftover
// lease can never make a complete chain unreadable.
const (
	chainLeaseSuffix = ".lease"

	leaseMagic   uint32 = 0x4b4c4b53 // bytes "SKLK"
	leaseVersion uint32 = 1

	// leaseSize is magic, version, holder pid, holder token and expiry; the
	// terminal CRC follows it.
	leaseSize = 4 + 4 + 8 + 8 + 8

	// leaseTTL is how long a lease stays fresh without a renewal. It only
	// ever retires a lease whose holder stopped renewing — a killed holder
	// (or one whose pid can no longer be confirmed) — never a live one,
	// which renews well inside the TTL.
	leaseTTL = 30 * time.Second
	// leaseHeartbeat is how often a live holder re-stamps its lease.
	leaseHeartbeat = 5 * time.Second
)

// chainLease is a held chain lease. The zero value holds nothing.
type chainLease struct {
	path  string
	info  os.FileInfo // identity of the lease file this holder created
	pid   uint64
	token uint64
	stop  chan struct{}
	done  chan struct{}
}

// acquireChainLease takes the lease coordinating operations on the chain
// directory chainDir. A lease left behind by a killed holder is reclaimed on
// the spot; a lease still held by a live operation fails the whole call with
// an error wrapping fs.ErrInvalid. The lease is never waited on.
func acquireChainLease(chainDir string) (*chainLease, error) {
	parent := filepath.Dir(chainDir)
	if info, err := os.Stat(parent); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errBadBackup
		}
		return nil, err
	} else if !info.IsDir() {
		return nil, errBadBackup
	}
	path := filepath.Join(parent, filepath.Base(chainDir)+chainLeaseSuffix)
	for attempt := 0; attempt < 8; attempt++ {
		l, err := tryAcquireLease(path)
		if err == nil {
			return l, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		stale, err := leaseIsStale(path)
		if err != nil {
			return nil, err
		}
		if !stale {
			// A live operation holds the chain.
			return nil, errBadBackup
		}
		// Reclaim the dead holder's lease. A contender may win the removal
		// or the next create; either way the loop re-reads the state and
		// exactly one contender ends up holding the lease.
		_ = os.Remove(path)
	}
	return nil, errBadBackup
}

// tryAcquireLease creates the lease file exclusively and stamps it with this
// process's holder identity. It fails with os.ErrExist when a lease file
// already exists.
func tryAcquireLease(path string) (*chainLease, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	l := &chainLease{
		path:  path,
		pid:   uint64(os.Getpid()),
		token: binary.LittleEndian.Uint64(token[:]),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	fail := func(err error) (*chainLease, error) {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	if _, err := f.Write(l.stamp()); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	// Confirm the file on the path is still the one just created: a
	// contender that reclaimed and recreated it in the meantime holds the
	// lease, not us.
	cur, err := os.Stat(path)
	if err != nil {
		os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil, errBadBackup
		}
		return nil, err
	}
	if !os.SameFile(cur, info) {
		return nil, errBadBackup
	}
	l.info = info
	go l.heartbeat()
	return l, nil
}

// stamp renders the lease content with a fresh expiry.
func (l *chainLease) stamp() []byte {
	buf := make([]byte, leaseSize+crcSize)
	binary.LittleEndian.PutUint32(buf[0:4], leaseMagic)
	binary.LittleEndian.PutUint32(buf[4:8], leaseVersion)
	binary.LittleEndian.PutUint64(buf[8:16], l.pid)
	binary.LittleEndian.PutUint64(buf[16:24], l.token)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(time.Now().Add(leaseTTL).UnixNano()))
	binary.LittleEndian.PutUint32(buf[32:36], crc32.ChecksumIEEE(buf[:leaseSize]))
	return buf
}

// heartbeat keeps the lease fresh until release; a live holder's lease
// therefore never expires no matter how long its operation runs.
func (l *chainLease) heartbeat() {
	defer close(l.done)
	ticker := time.NewTicker(leaseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.renew()
		}
	}
}

// renew re-stamps the lease with a fresh expiry, best-effort: if the file is
// no longer the one this holder created there is nothing to renew.
func (l *chainLease) renew() {
	cur, err := os.Stat(l.path)
	if err != nil || !os.SameFile(cur, l.info) {
		return
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return
	}
	if _, err := f.Write(l.stamp()); err == nil {
		_ = f.Sync()
	}
	_ = f.Close()
}

// release gives the lease back. Only the file this holder created is
// removed: a lease reclaimed and re-created by someone else is theirs.
func (l *chainLease) release() {
	close(l.stop)
	<-l.done
	cur, err := os.Stat(l.path)
	if err == nil && os.SameFile(cur, l.info) {
		_ = os.Remove(l.path)
	}
}

// parseLease decodes lease file content; ok is false for anything that is
// not a well-formed, checksum-good lease of a known version.
func parseLease(data []byte) (pid, token uint64, expiry int64, ok bool) {
	if len(data) != leaseSize+crcSize {
		return 0, 0, 0, false
	}
	if binary.LittleEndian.Uint32(data[0:4]) != leaseMagic ||
		binary.LittleEndian.Uint32(data[4:8]) != leaseVersion {
		return 0, 0, 0, false
	}
	if binary.LittleEndian.Uint32(data[32:36]) != crc32.ChecksumIEEE(data[:leaseSize]) {
		return 0, 0, 0, false
	}
	return binary.LittleEndian.Uint64(data[8:16]),
		binary.LittleEndian.Uint64(data[16:24]),
		int64(binary.LittleEndian.Uint64(data[24:32])), true
}

// leaseIsStale reports whether the lease at path was abandoned: its holder
// process is certainly gone, or its expiry has passed (a live holder renews
// long before that). A missing file counts as stale so the caller's create
// retry proceeds.
func leaseIsStale(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	pid, _, expiry, ok := parseLease(data)
	if !ok {
		// Not a readable lease: either foreign content or a lease caught
		// mid-write. Reclaim it only once it is clearly old, so a live
		// writer finishing its create is never clobbered.
		info, serr := os.Stat(path)
		if serr != nil {
			if errors.Is(serr, os.ErrNotExist) {
				return true, nil
			}
			return false, serr
		}
		return time.Since(info.ModTime()) > leaseTTL, nil
	}
	if alive, certain := processAlive(pid); certain && !alive {
		return true, nil
	}
	return time.Now().UnixNano() > expiry, nil
}

// processAlive reports whether pid names a live process. certain is false
// when the platform cannot answer (the caller then falls back to the lease
// expiry); a process that exists but belongs to another user counts as
// alive.
func processAlive(pid uint64) (alive, certain bool) {
	if pid == 0 || int64(pid) < 0 || pid != uint64(int(pid)) {
		return false, true
	}
	p, err := os.FindProcess(int(pid))
	if err != nil {
		return false, false
	}
	err = p.Signal(syscall.Signal(0))
	switch {
	case err == nil:
		return true, true
	case errors.Is(err, syscall.EPERM):
		return true, true
	case errors.Is(err, syscall.ESRCH), errors.Is(err, os.ErrProcessDone):
		return false, true
	default:
		// Signal 0 is not implemented everywhere; when it is not, the
		// lease expiry alone decides.
		return false, false
	}
}

// reclaimStaleLease removes the lease coordinating operations on chainDir
// when its holder is gone. It is the open-time counterpart of the
// acquisition-time reclamation: a directory related to a killed operation
// opens normally, with no lease wedged behind a dead holder. Best-effort,
// like the other open-time sweeps.
func reclaimStaleLease(chainDir string) {
	parent := filepath.Dir(chainDir)
	path := filepath.Join(parent, filepath.Base(chainDir)+chainLeaseSuffix)
	if _, err := os.Stat(path); err != nil {
		return
	}
	stale, err := leaseIsStale(path)
	if err != nil || !stale {
		return
	}
	_ = os.Remove(path)
}
