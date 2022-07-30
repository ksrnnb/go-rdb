package concurrency

import (
	"errors"
	"sync"
	"time"

	"github.com/ksrnnb/go-rdb/file"
)

const maxWaitingTime = 15 * time.Second
const xLocked = -1

var ErrLockAbort = errors.New("concurrency: lock abort error")

type Lock struct {
	blk *file.BlockID
	val int // -1 is xLocked, over 1 is sLocked, 0 is unlocked
}

type LockTable struct {
	locks []*Lock
	mux   sync.Mutex
}

func NewLockTable() *LockTable {
	return &LockTable{}
}

func (lt *LockTable) SLock(blk *file.BlockID) error {
	lt.mux.Lock()
	defer lt.mux.Unlock()
	start := time.Now()
	for lt.hasXLock(blk) && !isWaitingTooLong(start) {
		// TODO: 修正
		time.Sleep(1 * time.Second)
	}

	if lt.hasXLock(blk) {
		return ErrLockAbort
	}

	val := lt.getLockVal(blk)
	lt.setLockVal(blk, val+1)

	return nil
}

func (lt *LockTable) hasXLock(blk *file.BlockID) bool {
	return lt.getLockVal(blk) == xLocked
}

func (lt *LockTable) XLock(blk *file.BlockID) error {
	lt.mux.Lock()
	defer lt.mux.Unlock()
	start := time.Now()
	for lt.hasOtherSLocks(blk) && !isWaitingTooLong(start) {
		// TODO: 修正
		time.Sleep(1 * time.Second)
	}

	if lt.hasOtherSLocks(blk) {
		return ErrLockAbort
	}

	lt.setLockVal(blk, xLocked)
	return nil
}

// > 1 means XLock() assumes that the transaction already has an slock
func (lt *LockTable) hasOtherSLocks(blk *file.BlockID) bool {
	return lt.getLockVal(blk) > 1
}

func (lt *LockTable) Unlock(blk *file.BlockID) {
	lt.mux.Lock()
	defer lt.mux.Unlock()
	val := lt.getLockVal(blk)

	if val > 1 {
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

	return 0
}

func (lt *LockTable) setLockVal(blk *file.BlockID, val int) {
	for _, lock := range lt.locks {
		if blk.Equals(lock.blk) {
			lock.val = val
		}
	}
}

// deleteLock() delete specified block
func (lt *LockTable) deleteLock(blk *file.BlockID) {
	for key, lock := range lt.locks {
		if blk.Equals(lock.blk) {
			// https://github.com/golang/go/wiki/SliceTricks#delete
			copy(lt.locks[key:], lt.locks[key+1:])
			lt.locks[len(lt.locks)-1] = nil
			lt.locks = lt.locks[:len(lt.locks)-1]
		}
	}
}

func isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxWaitingTime)

	return time.Now().After(limit)
}
