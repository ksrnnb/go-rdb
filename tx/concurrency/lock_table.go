package concurrency

import (
	"errors"
	"sync"
	"time"

	"github.com/ksrnnb/go-rdb/file"
)

const maxWaitingTime = 10 * time.Second

const (
	xLocked  = -1
	unlocked = 0
)

var ErrLockAbort = errors.New("concurrency: lock abort error")

type Lock struct {
	blk *file.BlockID
	val int // -1 is xLocked, over 1 is sLocked number, 0 is unlocked
}

type LockTable struct {
	locks []*Lock
	mux   sync.Mutex
}

func NewLockTable() *LockTable {
	return &LockTable{}
}

func (lt *LockTable) SLock(blk *file.BlockID) error {
	start := time.Now()

	for lt.hasXLock(blk) && !isWaitingTooLong(start) {
		// TODO: 修正
		time.Sleep(10 * time.Millisecond)
	}

	if lt.hasXLock(blk) {
		return ErrLockAbort
	}

	lt.mux.Lock()
	defer lt.mux.Unlock()
	val := lt.getLockVal(blk)
	lt.setLockVal(blk, val+1)

	return nil
}

func (lt *LockTable) hasXLock(blk *file.BlockID) bool {
	return lt.getLockVal(blk) == xLocked
}

func (lt *LockTable) XLock(blk *file.BlockID) error {
	start := time.Now()
	for lt.hasAnyLocks(blk) && !isWaitingTooLong(start) {
		// TODO: 修正
		time.Sleep(10 * time.Millisecond)
	}

	if lt.hasAnyLocks(blk) {
		return ErrLockAbort
	}

	lt.mux.Lock()
	defer lt.mux.Unlock()
	lt.setLockVal(blk, xLocked)

	return nil
}

// > 1 means XLock() assumes that the transaction already has an slock
func (lt *LockTable) hasAnyLocks(blk *file.BlockID) bool {
	return lt.getLockVal(blk) != unlocked
}

func (lt *LockTable) Unlock(blk *file.BlockID) {
	lt.mux.Lock()
	defer lt.mux.Unlock()
	val := lt.getLockVal(blk)

	if isSLocked(val) {
		lt.setLockVal(blk, val-1)
	} else {
		lt.deleteLock(blk)
		// TODO: notifyAll()
	}
}

func (lt *LockTable) getLockVal(blk *file.BlockID) int {
	for _, lock := range lt.locks {
		if blk.Equals(lock.blk) {
			return lock.val
		}
	}

	return unlocked
}

func (lt *LockTable) setLockVal(blk *file.BlockID, val int) {
	for i, lock := range lt.locks {
		if blk.Equals(lock.blk) {
			lock.val = val
			lt.locks[i] = lock
			return
		}
	}
	lt.locks = append(lt.locks, &Lock{blk: blk, val: val})
}

// deleteLock() delete specified block
func (lt *LockTable) deleteLock(blk *file.BlockID) {
	var locks []*Lock
	for _, lock := range lt.locks {
		if blk.Equals(lock.blk) {
			continue
		}
		locks = append(locks, lock)
	}
	lt.locks = locks
}

func isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxWaitingTime)

	return time.Now().After(limit)
}

// 1以上の場合は、sLock している数
func isSLocked(val int) bool {
	return val >= 1
}
