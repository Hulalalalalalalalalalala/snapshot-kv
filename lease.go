package snapshot

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// Cross-object coordination of one backup chain directory.
//
// Several storage objects (separate Stores, even separate processes) can
// share one chain directory. The operations that reshape a chain — a full
// export writing into it, an incremental ring append, a chain merge and a
// restore reading it for synthesis — are mutually exclusive. At any instant
// the chain is either exactly the chain before the registered operation or
// the complete chain after it; an intermediate state is never observable.
//
// Coordination is a small "chain.lock" file inside the chain directory:
//
//   - Liveness is the file's write lock, taken as a non-blocking
//     open-file-description lock (F_OFD_SETLK) over the whole file. The
//     holder keeps it open and locked for the whole operation. A process
//     killed mid-operation releases the lock automatically as its
//     descriptors close, so a chain can never be mutually excluded forever.
//   - The fixed-size body names the holder (process id and a per-process
//     unique token), the operation kind and a deadline, protected by a
//     CRC-32. The deadline is a secondary fence for filesystems that do not
//     release byte-range locks on process death: a lock whose deadline has
//     passed names a dead holder and is stealable.
//
// Acquisition never waits. A request that finds a live holder fails
// wholesale, before it inspects or touches the chain, with an error wrapping
// fs.ErrInvalid. The holder renews the deadline in the background for the
// duration of a long streaming export or merge. The lock file is removed when
// the operation finishes.
//
// A leftover lock file from a killed holder is reclaimed at the next backup,
// incremental export, merge or restore into that directory, and at the next
// Open of the directory or a related sibling; afterwards the chain is usable
// again with no manual cleanup.

const (
	chainLockName = "chain.lock"

	// chainLeaseTTL is how long a held lease stays valid without a renewal.
	// The holder renews well inside it; only a killed or frozen holder lets
	// it lapse, and the deadline only decides liveness on filesystems that
	// fail to drop a dead process's byte-range lock.
	chainLeaseTTL = 30 * time.Second
	// chainLeaseRenew is the background renewal interval.
	chainLeaseRenew = chainLeaseTTL / 3

	// chainLockBodySize is pidToken(8) + op(1) + deadlineSec(8) +
	// deadlineNsec(8) + crc(4).
	chainLockBodySize = 8 + 1 + 8 + 8 + 4

	// Linux open-file-description fcntl command. Not exported by the syscall
	// package but part of the stable Linux ABI (the same value
	// golang.org/x/sys/unix defines) and identical across Linux
	// architectures.
	fOFDSetLK = 37
)

// Lease operation kinds, recorded as one byte.
const (
	chainOpBackup      byte = 'B'
	chainOpIncremental byte = 'I'
	chainOpMerge       byte = 'M'
	chainOpRestore     byte = 'R'
)

// errChainBusy reports that another registered operation currently holds the
// chain. The rejected request changes nothing and the error satisfies
// errors.Is(err, fs.ErrInvalid), like every other whole-request rejection.
var errChainBusy = errors.Join(
	errors.New("snapshot: backup chain is in use by another operation"),
	fs.ErrInvalid,
)

// chainLease is one acquired coordination lease. file is open and
// write-locked until release; stageFile, when set, holds the same lock on a
// file staged inside a merge's sibling directory, so exclusion survives the
// merge's directory swap.
type chainLease struct {
	dir  string
	path string
	file *os.File

	stage     string
	stagePath string
	stageFile *os.File

	op    byte
	token uint64
	stop  chan struct{}
	done  chan struct{}
}

// leaseTokenSource hands out process-unique holder tokens.
var leaseTokenSource atomic.Uint64

// nextLeaseToken returns a holder token unique within this process.
func nextLeaseToken() uint64 {
	return leaseTokenSource.Add(1)
}

// chainTime is the clock leases use; production reads the wall clock.
var chainTime = time.Now

// acquireChainLease coordinates one operation over the existing chain
// directory dir. It never waits: a live holder makes the whole request fail
// with errChainBusy before the caller inspects the chain. A lock file a killed
// holder left (its process lock is gone, or its deadline has passed) is
// reclaimed, so the next operation proceeds with no manual cleanup.
func acquireChainLease(dir string, op byte) (*chainLease, error) {
	path := filepath.Join(dir, chainLockName)

	var f *os.File
	for attempt := 0; attempt < 2; attempt++ {
		opened, oerr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if oerr != nil {
			return nil, oerr
		}
		held, lerr := tryOFDWriteLock(opened)
		if lerr != nil {
			opened.Close()
			return nil, lerr
		}
		if held && fdStillNamed(opened, path) {
			f = opened
			break
		}
		if held {
			// The lock succeeded on a file another reclaimer unlinked between
			// our open and lock. Drop it and retry once.
			opened.Close()
			continue
		}
		// The lock is currently held. It names a reclaimable dead holder
		// only when its record says its deadline has passed; otherwise the
		// request fails busy without touching the holder's file.
		stale, serr := chainLeaseRecordStale(path)
		opened.Close()
		if serr != nil {
			return nil, serr
		}
		if !stale || attempt > 0 {
			return nil, errChainBusy
		}
		// Fence the expired holder: on a filesystem that drops a dead
		// process's lock the initial lock attempt would already have
		// succeeded, so this path is the lock-retaining case. Drop the dead
		// name and create a fresh inode to lock; fdStillNamed below makes a
		// reclaimer whose file is stolen in turn fail busy.
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		_ = syncDirectory(dir)
	}
	if f == nil {
		return nil, errChainBusy
	}

	l := &chainLease{
		dir:   dir,
		path:  path,
		file:  f,
		op:    op,
		token: nextLeaseToken(),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	if err := l.rewriteRecord(chainTime().Add(chainLeaseTTL)); err != nil {
		l.drop()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		l.drop()
		return nil, err
	}
	go l.renewLoop()
	return l, nil
}

// prepareSwap takes the same write lease on a chain.lock staged inside a
// merge's sibling directory. Once the merge renames that sibling onto the
// chain path, the staged inode is the live chain lock while the original
// descriptor rides into the trash sibling, so exclusion spans the whole
// commit rather than ending at the first rename.
func (l *chainLease) prepareSwap(stage string) error {
	path := filepath.Join(stage, chainLockName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	held, err := tryOFDWriteLock(f)
	if err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if !held {
		f.Close()
		os.Remove(path)
		return errChainBusy
	}
	l.stage = stage
	l.stagePath = path
	l.stageFile = f
	return l.rewriteRecord(chainTime().Add(chainLeaseTTL))
}

// committedSwap is called after the staged sibling has been renamed onto the
// chain path: the staged descriptor now locks the chain path itself. It closes
// the original descriptor, whose inode moved into the trash sibling.
func (l *chainLease) committedSwap(chainDir string) {
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
	l.path = filepath.Join(chainDir, chainLockName)
	l.dir = chainDir
	// The staged descriptor now backs the chain entry under its new name.
	l.stagePath = l.path
	l.stage = ""
}

// abortSwap releases a staged lock taken with prepareSwap when the swap never
// landed.
func (l *chainLease) abortSwap() {
	if l.stageFile != nil {
		_ = os.Remove(l.stagePath)
		_ = syncDirectory(l.stage)
		_ = l.stageFile.Close()
		l.stageFile = nil
	}
	l.stage, l.stagePath = "", ""
}

// release ends coordination: it stops renewal, removes the live lock file
// while its lock is still held, forces the directory and closes the
// descriptor. Removal-before-close means a concurrent acquirer never observes
// the chain unlocked while a replacement record is half-written. It is
// idempotent.
func (l *chainLease) release() {
	if l == nil {
		return
	}
	select {
	case <-l.stop:
		return
	default:
		close(l.stop)
	}
	<-l.done
	if l.stageFile != nil {
		// After a committed swap stagePath is the live chain lock; before one
		// it is the staged sibling's copy.
		_ = os.Remove(l.stagePath)
		_ = syncDirectory(l.dir)
		if l.stage != "" {
			_ = syncDirectory(l.stage)
		}
		_ = l.stageFile.Close()
		l.stageFile = nil
	}
	if l.file != nil {
		_ = os.Remove(l.path)
		_ = syncDirectory(l.dir)
		_ = l.file.Close()
		l.file = nil
	}
}

// drop releases a lease whose own record could not be made durable; the
// caller is about to return an error, so no renewer is running.
func (l *chainLease) drop() {
	if l.stageFile != nil {
		_ = l.stageFile.Close()
		_ = os.Remove(l.stagePath)
	}
	_ = l.file.Close()
	_ = os.Remove(l.path)
	_ = syncDirectory(l.dir)
}

// rewriteRecord writes this holder's one fixed-size lease record at offset 0.
// It also mirrors the record onto the staged lock while a merge is
// committing.
func (l *chainLease) rewriteRecord(deadline time.Time) error {
	body := encodeLeaseRecord(l.token, l.op, deadline)
	if err := writeLeaseBody(l.file, body); err != nil {
		return err
	}
	if l.stageFile != nil {
		if err := writeLeaseBody(l.stageFile, body); err != nil {
			return err
		}
	}
	return nil
}

// renewLoop keeps the deadline ahead of wall time for the whole operation. A
// renewal failure is ignored: it means the filesystem is failing or the
// holder has been killed, either of which ends the operation anyway.
func (l *chainLease) renewLoop() {
	defer close(l.done)
	t := time.NewTicker(chainLeaseRenew)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			_ = l.rewriteRecord(chainTime().Add(chainLeaseTTL))
			_ = l.file.Sync()
		}
	}
}

// writeLeaseBody truncates the lock file to exactly one record and writes it
// at offset 0. The fixed size means a reader never sees a torn mix of old and
// new records: a concurrent reader at worst reads the previous fixed-size
// record, which is also this holder's.
func writeLeaseBody(f *os.File, body []byte) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.WriteAt(body, 0); err != nil {
		return err
	}
	return nil
}

// encodeLeaseRecord builds holder token, operation kind, deadline and CRC.
func encodeLeaseRecord(token uint64, op byte, deadline time.Time) []byte {
	body := make([]byte, chainLockBodySize)
	// The high 32 bits carry the pid for forensics; the low 32 are unique in
	// this process, so the combined token never repeats within one host.
	binary.LittleEndian.PutUint64(body[0:8],
		uint64(os.Getpid())<<32|uint64(uint32(token)))
	body[8] = op
	sec, nsec := deadline.Unix(), deadline.Nanosecond()
	binary.LittleEndian.PutUint64(body[9:17], uint64(sec))
	binary.LittleEndian.PutUint64(body[17:25], uint64(nsec))
	binary.LittleEndian.PutUint32(body[25:29], crc32.ChecksumIEEE(body[:25]))
	return body
}

// chainLeaseRecordStale reports whether an existing lock file carries one
// intact record whose deadline has passed. An empty, truncated or damaged
// record is treated as stale only when the file itself has been left untouched
// past a TTL: that cannot be a live holder mid-renewal (renewals rewrite a
// fixed-size body), and it bounds how long a crash at the wrong byte blocks
// the chain.
func chainLeaseRecordStale(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	body := make([]byte, chainLockBodySize)
	_, err = io.ReadFull(f, body)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// A record shorter than one fixed body is debris from a holder
			// killed mid-write. A live holder rewrites immediately, so once
			// the file has sat untouched past a TTL it is reclaimable.
			return chainTime().Sub(info.ModTime()) > chainLeaseTTL, nil
		}
		return false, err
	}
	var one [1]byte
	if extra, rerr := f.Read(one[:]); !(errors.Is(rerr, io.EOF) && extra == 0) {
		return false, nil
	}
	if crc32.ChecksumIEEE(body[:25]) != binary.LittleEndian.Uint32(body[25:29]) {
		return chainTime().Sub(info.ModTime()) > chainLeaseTTL, nil
	}
	deadline := time.Unix(
		int64(binary.LittleEndian.Uint64(body[9:17])),
		int64(binary.LittleEndian.Uint64(body[17:25])),
	)
	return chainTime().After(deadline), nil
}

// reclaimChainLease clears a lock file no live operation can hold: one whose
// write lock is free (the holder process is gone) or whose record has expired
// (a dead holder on a filesystem that keeps locks). A fresh, locked lease is
// left untouched. It is the open-time counterpart of acquisition-time
// reclaim.
func reclaimChainLease(dir string) {
	path := filepath.Join(dir, chainLockName)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return // absent: nothing to reclaim; other errors are not ours to force
	}
	remove := false
	held, lerr := tryOFDWriteLock(f)
	switch {
	case lerr != nil:
	case held:
		remove = true // no live holder keeps this inode locked
	default:
		stale, serr := chainLeaseRecordStale(path)
		remove = serr == nil && stale
	}
	f.Close()
	if remove {
		if err := os.Remove(path); err == nil {
			_ = syncDirectory(dir)
		}
	}
}

// coordinateChain is the single entry point every chain-reshaping operation
// uses. It first reconciles a merge killed (or still committing) with the
// chain path missing: a live lease sitting in a trash sibling means a swap is
// in flight and the request fails busy; a dead holder's trash is moved back
// whole. It then acquires the chain lease and, only while holding it, sweeps
// staging and trash siblings, so a busy request can never delete a running
// operation's staged chain. When create is false a directory that is still
// absent after reconciliation is an invalid chain; when true it is created (a
// brand-new full backup target).
func coordinateChain(chainDir string, op byte, create bool) (*chainLease, error) {
	info, err := os.Stat(chainDir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if chainTrashHeldLive(chainDir) {
			return nil, errChainBusy
		}
		sweepMergeDebris(chainDir)
		info, err = os.Stat(chainDir)
		if errors.Is(err, fs.ErrNotExist) {
			if !create {
				return nil, errBadBackup
			}
			if err := os.MkdirAll(chainDir, 0o700); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		} else if !info.IsDir() {
			return nil, errBadBackup
		}
	} else if !info.IsDir() {
		return nil, errBadBackup
	}
	lease, err := acquireChainLease(chainDir, op)
	if err != nil {
		// Another holder may have renamed the chain into its trash sibling in
		// the window between the stat and the lock open. That is a busy chain,
		// not a filesystem error; anything else missing is an invalid chain.
		if errors.Is(err, fs.ErrNotExist) {
			if chainTrashHeldLive(chainDir) {
				return nil, errChainBusy
			}
			return nil, errBadBackup
		}
		return nil, err
	}
	// The chain lease is held: no other operation reshapes this chain, so any
	// debris sibling belongs to a dead holder and is safe to repair or clear.
	// sweepMergeDebris additionally refuses siblings that carry a live lease.
	sweepMergeDebris(chainDir)
	return lease, nil
}

// chainTrashHeldLive reports whether a merge's trash sibling for chainDir
// carries a live lease, i.e. a merge is paused between its two commit
// renames.
func chainTrashHeldLive(chainDir string) bool {
	parent := filepath.Dir(chainDir)
	base := filepath.Base(chainDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() && isMergeTempName(e.Name(), base, mergeTrashPrefix) &&
			dirHasLiveLease(filepath.Join(parent, e.Name())) {
			return true
		}
	}
	return false
}

// dirHasLiveLease reports whether dir carries a chain.lock currently held by
// some operation (the lock cannot be taken and its record has not expired).
// A fresh open gets a new open file description, so this conflicts even with a
// lease held by another goroutine in the same process.
func dirHasLiveLease(dir string) bool {
	f, err := os.OpenFile(filepath.Join(dir, chainLockName), os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	held, err := tryOFDWriteLock(f)
	if err != nil || held {
		return false
	}
	stale, err := chainLeaseRecordStale(filepath.Join(dir, chainLockName))
	return err == nil && !stale
}

// tryOFDWriteLock attempts, without blocking, an open-file-description write
// lock spanning the whole file (a zero length means through EOF). OFD locks
// conflict even between open file descriptions in the same process, so they
// coordinate goroutines here as well as separate processes. EAGAIN/EACCES mean
// another holder has the lock; anything else is a real error.
func tryOFDWriteLock(f *os.File) (bool, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return false, err
	}
	var held bool
	var lerr error
	rerr := rc.Control(func(fd uintptr) {
		lk := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: io.SeekStart,
			Start:  0,
			Len:    0,
		}
		err := ignoringEINTR(func() error {
			return syscall.FcntlFlock(fd, fOFDSetLK, &lk)
		})
		switch {
		case err == nil:
			held = true
		case errors.Is(err, syscall.EAGAIN), errors.Is(err, syscall.EACCES):
			held = false
		default:
			lerr = err
		}
	})
	if rerr != nil {
		return false, rerr
	}
	return held, lerr
}

// ignoringEINTR retries fn when it is interrupted by a signal.
func ignoringEINTR(fn func() error) error {
	for {
		err := fn()
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// fdStillNamed reports whether open file f is the directory entry path
// currently names. It compares device and inode, so a file unlinked and
// replaced between the open and the lock is detected.
func fdStillNamed(f *os.File, path string) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	pi, err := os.Stat(path)
	if err != nil {
		return false
	}
	fs1, ok1 := fi.Sys().(*syscall.Stat_t)
	fs2, ok2 := pi.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		// Unknown filesystem metadata: trust the name rather than reject a
		// healthy lock.
		return true
	}
	return fs1.Dev == fs2.Dev && fs1.Ino == fs2.Ino
}
