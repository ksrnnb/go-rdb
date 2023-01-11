package tx

import (
	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/logs"
)

// Undo-Only Recovery を採用
type RecoveryManager struct {
	lm    *logs.LogManager
	bm    *buffer.BufferManager
	tx    *Transaction
	txnum int
}

func NewRecoveryManager(tx *Transaction, txnum int, lm *logs.LogManager, bm *buffer.BufferManager) (*RecoveryManager, error) {
	rm := &RecoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}

	_, err := writeStartToLog(lm, txnum)

	if err != nil {
		return nil, err
	}

	return rm, nil
}

// ログレコード書き込み前にトランザクションのバッファをフラッシュする
func (rm *RecoveryManager) Commit() error {
	err := rm.bm.FlushAll(rm.txnum)

	if err != nil {
		return err
	}

	lsn, err := writeCommitToLog(rm.lm, rm.txnum)

	if err != nil {
		return err
	}

	return rm.lm.Flush(lsn)
}

// ログレコード書き込み前にトランザクションのバッファをフラッシュする
func (rm *RecoveryManager) Rollback() error {
	err := rm.doRollBack()
	if err != nil {
		return err
	}

	err = rm.bm.FlushAll(rm.txnum)

	if err != nil {
		return err
	}

	lsn, err := writeRollBackToLog(rm.lm, rm.txnum)

	if err != nil {
		return err
	}

	return rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) Recover() error {
	err := rm.doRecover()
	if err != nil {
		return err
	}

	err = rm.bm.FlushAll(rm.txnum)

	if err != nil {
		return err
	}

	lsn, err := writeCheckPointToLog(rm.lm)

	if err != nil {
		return err
	}

	return rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) SetInt(buf *buffer.Buffer, offset, newVal int) (latestLSN int, err error) {
	p := buf.Contents()

	oldVal, err := p.GetInt(offset)

	if err != nil {
		return 0, err
	}

	blk := buf.Block()
	return writeSetIntToLog(rm.lm, rm.txnum, blk, offset, oldVal, newVal)
}

func (rm *RecoveryManager) SetString(buf *buffer.Buffer, offset int, newVal string) (latestLSN int, err error) {
	p := buf.Contents()

	oldVal, err := p.GetString(offset)

	if err != nil {
		return 0, err
	}

	blk := buf.Block()
	return writeSetStringToLog(rm.lm, rm.txnum, blk, offset, oldVal, newVal)
}

func (rm *RecoveryManager) doRollBack() error {
	iter, err := rm.lm.Iterator()

	if err != nil {
		return err
	}

	for iter.HasNext() {
		b, err := iter.Next()
		if err != nil {
			return err
		}

		rec, err := CreateLogRecord(b)

		if err != nil {
			return err
		}

		if rec.TxNumber() == rm.txnum {
			if rec.Op() == Start {
				return nil
			}
			rec.Undo(rm.tx)
		}
	}

	return nil
}

func (rm *RecoveryManager) doRecover() error {
	var finishedTxs []int
	iter, err := rm.lm.Iterator()

	if err != nil {
		return err
	}

	for iter.HasNext() {
		b, err := iter.Next()
		if err != nil {
			return err
		}

		rec, err := CreateLogRecord(b)

		if err != nil {
			return err
		}

		if rec.Op() == CheckPoint {
			return nil
		}

		if rec.Op() == Commit || rec.Op() == Rollback {
			finishedTxs = append(finishedTxs, rec.TxNumber())
		} else if contains(finishedTxs, rec.TxNumber()) {
			rec.Undo(rm.tx)
		}
	}
	return nil
}

func contains(heystack []int, needle int) bool {
	for _, e := range heystack {
		if e == needle {
			return true
		}
	}

	return false
}
