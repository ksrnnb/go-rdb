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
	mux   *sync.Mutex
	cond  *sync.Cond
	locks []*Lock
}

type lockResult struct {
	err error
}

func NewLockTable() *LockTable {
	mux := &sync.Mutex{}
	return &LockTable{
		mux:  mux,
		cond: sync.NewCond(mux),
	}
}

func (lt *LockTable) SLock(blk *file.BlockID) error {
	start := time.Now()

	lr := make(chan lockResult)
	go lt.sLock(lr, blk, start)

	select {
	case result := <-lr:
		return result.err
	case <-time.After(maxWaitingTime):
		lt.cond.Broadcast()
	}
	return ErrLockAbort
}

func (lt *LockTable) sLock(lr chan<- lockResult, blk *file.BlockID, start time.Time) {
	lt.mux.Lock()

	defer func() {
		lt.mux.Unlock()
	}()

	for lt.hasXLock(blk) && !isWaitingTooLong(start) {
		lt.cond.Wait()
	}

	if lt.hasXLock(blk) {
		lr <- lockResult{err: ErrLockAbort}
		return
	}

	val := lt.getLockVal(blk)
	lt.setLockVal(blk, val+1)
	lr <- lockResult{}
}

func (lt *LockTable) XLock(blk *file.BlockID) error {
	start := time.Now()
	lr := make(chan lockResult)
	defer close(lr)

	go lt.xLock(lr, blk, start)

	select {
	case result := <-lr:
		return result.err
	case <-time.After(maxWaitingTime):
		lt.cond.Broadcast()
	}
	return nil
}

func (lt *LockTable) xLock(lr chan<- lockResult, blk *file.BlockID, start time.Time) {
	lt.mux.Lock()

	defer func() {
		lt.mux.Unlock()
	}()

	for lt.hasOtherSLocks(blk) && !isWaitingTooLong(start) {
		lt.cond.Wait()
	}

	if lt.hasOtherSLocks(blk) {
		lr <- lockResult{err: ErrLockAbort}
		return
	}

	lt.setLockVal(blk, xLocked)
	lr <- lockResult{}
}

func (lt *LockTable) Unlock(blk *file.BlockID) {
	lt.mux.Lock()
	defer lt.mux.Unlock()
	val := lt.getLockVal(blk)

	if lt.hasOtherSLocks(blk) {
		lt.setLockVal(blk, val-1)
	} else {
		// slock が1個だけ or xlock の場合
		lt.deleteLock(blk)
		lt.cond.Broadcast()
	}
}

func (lt *LockTable) hasXLock(blk *file.BlockID) bool {
	return lt.getLockVal(blk) == xLocked
}

// hasOtherSLocks は、他の concurrency manager も slock しているかどうかを返す
func (lt *LockTable) hasOtherSLocks(blk *file.BlockID) bool {
	return lt.getLockVal(blk) > 1
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
