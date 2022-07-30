package concurrency

import (
	"github.com/ksrnnb/go-rdb/file"
)

var lockTable *LockTable = NewLockTable()

type LockType int

const (
	SLockType LockType = iota + 1
	XLockType
)

type ConcurrencyManagerLock struct {
	ty  LockType
	blk *file.BlockID
}

type ConcurrencyManager struct {
	locks []*ConcurrencyManagerLock
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{}
}

func (cm *ConcurrencyManager) SLock(blk *file.BlockID) error {
	// ConcurrencyManager が sLock または xLock を所持している場合は、同じトランザクションなのでそのまま読み込みできる
	if cm.getConcurrencyManagerLock(blk) != nil {
		return nil
	}

	err := lockTable.SLock(blk)
	if err != nil {
		return err
	}

	cm.setConcurrencyManagerLock(blk, SLockType)
	return nil
}

func (cm *ConcurrencyManager) XLock(blk *file.BlockID) error {
	// ConcurrencyManager が sLock または xLock を所持している場合は、同じトランザクションなのでそのまま書き込みできる
	if cm.getConcurrencyManagerLock(blk) != nil {
		return nil
	}

	err := lockTable.XLock(blk)
	if err != nil {
		return err
	}

	cm.setConcurrencyManagerLock(blk, XLockType)
	return nil
}

func (cm *ConcurrencyManager) Release() {
	for _, lock := range cm.locks {
		lockTable.Unlock(lock.blk)
	}

	cm.locks = nil
}

func (cm *ConcurrencyManager) hasXLock(blk *file.BlockID) bool {
	lock := cm.getConcurrencyManagerLock(blk)

	if lock == nil {
		return false
	}

	return lock.ty == XLockType
}

func (cm *ConcurrencyManager) getConcurrencyManagerLock(blk *file.BlockID) *ConcurrencyManagerLock {
	for _, lock := range cm.locks {
		if blk.Equals(lock.blk) {
			return lock
		}
	}

	return nil
}

func (cm *ConcurrencyManager) setConcurrencyManagerLock(blk *file.BlockID, ty LockType) {
	for i, lock := range cm.locks {
		if blk.Equals(lock.blk) {
			lock.ty = ty
			cm.locks[i] = lock
			return
		}
	}
	cm.locks = append(cm.locks, &ConcurrencyManagerLock{blk: blk, ty: ty})
}
