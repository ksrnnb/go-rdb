package tx

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

type RollbackRecord struct {
	txnum int
}

func NewRollbackRecord(p *file.Page) (*RollbackRecord, error) {
	tpos := intByteSize
	txnum := p.GetInt(tpos)

	if err := p.Err(); err != nil {
		return nil, err
	}
	return &RollbackRecord{txnum: txnum}, nil
}

// Op() returns the log record's type
func (rr *RollbackRecord) Op() int {
	return Rollback
}

// TxNumber() returns the transaction id stored with the log record
func (rr *RollbackRecord) TxNumber() int {
	return rr.txnum
}

// Undo() undoes the operation encoded by this log record
// do nothing in Rollback
func (rr *RollbackRecord) Undo(tx *Transaction) {}

func (rr *RollbackRecord) String() string {
	return "<ROLLBACK " + strconv.Itoa(rr.txnum) + ">"
}

func writeRollBackToLog(lm *logs.LogManager, txnum int) (latestLSN int, err error) {
	rec := make([]byte, 2*intByteSize)
	p := file.NewPageWithBuf(rec)
	p.SetInt(0, Rollback)
	p.SetInt(intByteSize, txnum)

	if err := p.Err(); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
