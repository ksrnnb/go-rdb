package tx

import (
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

type CheckPointRecord struct{}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

// Op() returns the log record's type
func (cpr *CheckPointRecord) Op() int {
	return CheckPoint
}

// TxNumber() returns the transaction id stored with the log record\
// checkpoint has no associated transaction -> returns -1
func (cpr *CheckPointRecord) TxNumber() int {
	return -1
}

// Undo() undoes the operation encoded by this log record
// do nothing in checkpoint
func (cpr *CheckPointRecord) Undo(tx *Transaction) {}

func (cpr *CheckPointRecord) String() string {
	return "<CHECKPOINT>"
}

func writeCheckPointToLog(lm *logs.LogManager) (latestLSN int, err error) {
	rec := make([]byte, intByteSize)
	p := file.NewPageWithBuf(rec)
	p.SetInt(0, CheckPoint)

	if err := p.Err(); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
