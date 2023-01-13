package tx

import (
	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	"github.com/ksrnnb/go-rdb/tx/concurrency"
)

const endOfFile = -1

type Transaction struct {
	fm    *file.FileManager
	bm    *buffer.BufferManager
	rm    *RecoveryManager
	cm    *concurrency.ConcurrencyManager
	bl    *BufferList
	txNum int
}

func NewTransaction(fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager, lt *concurrency.LockTable, tng *TransactionNumberGenerator) (*Transaction, error) {
	tx := &Transaction{
		fm:    fm,
		bm:    bm,
		cm:    concurrency.NewConcurrencyManager(lt),
		bl:    NewBufferList(bm),
		txNum: tng.nextTxNumber(),
	}

	var err error
	tx.rm, err = NewRecoveryManager(tx, tx.txNum, lm, bm)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (tx *Transaction) Commit() error {
	err := tx.rm.Commit()
	if err != nil {
		return err
	}

	tx.cm.Release()
	return tx.bl.unpinAll()
}

func (tx *Transaction) Rollback() error {
	err := tx.rm.Rollback()
	if err != nil {
		return err
	}

	tx.cm.Release()
	return tx.bl.unpinAll()
}

func (tx *Transaction) Recover() error {
	err := tx.bm.FlushAll(tx.txNum)

	if err != nil {
		return err
	}

	return tx.rm.Recover()
}

func (tx *Transaction) Pin(blk *file.BlockID) error {
	return tx.bl.pin(blk)
}

func (tx *Transaction) Unpin(blk *file.BlockID) error {
	return tx.bl.unpin(blk)
}

func (tx *Transaction) GetInt(blk *file.BlockID, offset int) (int, error) {
	err := tx.cm.SLock(blk)
	if err != nil {
		return 0, err
	}

	txBuf, err := tx.bl.getTxBuffer(blk)
	if err != nil {
		return 0, err
	}

	p := txBuf.buf.Contents()
	val, err := p.GetInt(offset)

	if err != nil {
		return 0, err
	}

	return val, nil
}

func (tx *Transaction) SetInt(blk *file.BlockID, offset int, val int, okToLog bool) error {
	err := tx.cm.XLock(blk)
	if err != nil {
		return err
	}

	txBuf, err := tx.bl.getTxBuffer(blk)
	if err != nil {
		return err
	}

	lsn := -1
	if okToLog {
		lsn, err = tx.rm.SetInt(txBuf.buf, offset)
		if err != nil {
			return err
		}
	}
	p := txBuf.buf.Contents()
	err = p.SetInt(offset, val)
	if err != nil {
		return err
	}

	txBuf.buf.SetModified(tx.txNum, lsn)
	return nil
}

func (tx *Transaction) GetString(blk *file.BlockID, offset int) (string, error) {
	err := tx.cm.SLock(blk)
	if err != nil {
		return "", err
	}

	txBuf, err := tx.bl.getTxBuffer(blk)
	if err != nil {
		return "", err
	}

	p := txBuf.buf.Contents()
	val, err := p.GetString(offset)
	if err != nil {
		return "", err
	}

	return val, nil
}

func (tx *Transaction) SetString(blk *file.BlockID, offset int, val string, okToLog bool) error {
	err := tx.cm.XLock(blk)
	if err != nil {
		return err
	}

	txBuf, err := tx.bl.getTxBuffer(blk)
	if err != nil {
		return err
	}

	lsn := -1
	if okToLog {
		lsn, err = tx.rm.SetString(txBuf.buf, offset)
		if err != nil {
			return err
		}
	}
	p := txBuf.buf.Contents()
	err = p.SetString(offset, val)
	if err != nil {
		return err
	}

	txBuf.buf.SetModified(tx.txNum, lsn)
	return nil
}

func (tx *Transaction) Size(filename string) (int, error) {
	blk := file.NewBlockID(filename, endOfFile)
	err := tx.cm.SLock(blk)
	if err != nil {
		return 0, err
	}

	return tx.fm.Length(filename)
}

func (tx *Transaction) Append(filename string) error {
	blk := file.NewBlockID(filename, endOfFile)
	err := tx.cm.XLock(blk)
	if err != nil {
		return err
	}

	_, err = tx.fm.Append(filename)
	return err
}

func (tx *Transaction) BlockSize() int {
	return tx.fm.BlockSize()
}

func (tx *Transaction) AvailableBuffers() int {
	return tx.bm.Available()
}
