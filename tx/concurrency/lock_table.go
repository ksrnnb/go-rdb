package concurrency

import (
	"errors"
	"sync"
	"time"

	"github.com/ksrnnb/go-rdb/file"
)

const maxWaitingTime = 60 * time.Second

var ErrLockAbort = errors.New("concurrency: lock abort error")

type Lock struct {
	blk file.BlockID
	val int // -1 is xLocked, over 1 is sLocked number, 0 is unlocked
}

type LockStatus int

const (
	XLocked  = -1
	Unlocked = 0
)

func (ls LockStatus) UnlockSLock() LockStatus {
	return ls - 1
}

func (ls LockStatus) SLock() LockStatus {
	return ls + 1
}

func (ls LockStatus) IsSLocked() bool {
	return ls > 1
}

type LockTable struct {
	cond  *sync.Cond
	locks map[file.BlockID]LockStatus
}

type lockResult struct {
	err error
}

func NewLockTable() *LockTable {
	return &LockTable{
		cond:  sync.NewCond(&sync.Mutex{}),
		locks: make(map[file.BlockID]LockStatus),
	}
}

func (lt *LockTable) SLock(blk file.BlockID) error {
	start := time.Now()

	lr := make(chan lockResult)
	go lt.sLock(lr, blk, start)

	select {
	case result := <-lr:
		return result.err
	case <-time.After(maxWaitingTime):
		lt.cond.Broadcast()
		result := <-lr
		return result.err
	}
}

func (lt *LockTable) sLock(lr chan<- lockResult, blk file.BlockID, start time.Time) {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()

	for lt.hasXLock(blk) && !isWaitingTooLong(start) {
		lt.cond.Wait()
	}

	if lt.hasXLock(blk) {
		lr <- lockResult{err: ErrLockAbort}
		return
	}

	l := lt.getLockStatus(blk)
	lt.setLockStatus(blk, l.SLock())
	lr <- lockResult{}
}

func (lt *LockTable) XLock(blk file.BlockID) error {
	start := time.Now()
	lr := make(chan lockResult)
	defer close(lr)

	go lt.xLock(lr, blk, start)

	select {
	case result := <-lr:
		return result.err
	case <-time.After(maxWaitingTime):
		lt.cond.Broadcast()
		result := <-lr
		return result.err
	}
}

func (lt *LockTable) xLock(lr chan<- lockResult, blk file.BlockID, start time.Time) {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()

	for lt.hasOtherSLocks(blk) && !isWaitingTooLong(start) {
		lt.cond.Wait()
	}

	if lt.hasOtherSLocks(blk) {
		lr <- lockResult{err: ErrLockAbort}
		return
	}

	lt.setLockStatus(blk, XLocked)
	lr <- lockResult{}
}

func (lt *LockTable) Unlock(blk file.BlockID) {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()
	ls := lt.getLockStatus(blk)

	if lt.hasOtherSLocks(blk) {
		lt.setLockStatus(blk, ls.UnlockSLock())
	} else {
		// slock が1個だけ or xlock の場合
		lt.deleteLock(blk)
		lt.cond.Broadcast()
	}
}

func (lt *LockTable) hasXLock(blk file.BlockID) bool {
	return lt.getLockStatus(blk) == XLocked
}

// hasOtherSLocks は、他の concurrency manager も slock しているかどうかを返す
func (lt *LockTable) hasOtherSLocks(blk file.BlockID) bool {
	return lt.getLockStatus(blk).IsSLocked()
}

func (lt *LockTable) getLockStatus(blk file.BlockID) LockStatus {
	st, ok := lt.locks[blk]
	if ok {
		return st
	}
	return Unlocked
}

func (lt *LockTable) setLockStatus(blk file.BlockID, ls LockStatus) {
	lt.locks[blk] = ls
}

// deleteLock() delete specified block
func (lt *LockTable) deleteLock(blk file.BlockID) {
	delete(lt.locks, blk)
}

func isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxWaitingTime)

	return time.Now().After(limit)
}
