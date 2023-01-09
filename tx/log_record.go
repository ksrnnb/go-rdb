package tx

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
)

const (
	CheckPoint = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type LogRecord interface {
	Op() int              // returns the log record's type
	TxNumber() int        // returns the transaction id stored with the log record
	Undo(tx *Transaction) // undoes the operation encoded by this log record
}

func CreateLogRecord(b []byte) (LogRecord, error) {
	p := file.NewPageWithBuf(b)
	recordType, err := p.GetInt(0)
	if err != nil {
		return nil, fmt.Errorf("tx: CreateLogRecord() failed, %w", err)
	}

	switch recordType {
	case CheckPoint:
		return NewCheckPointRecord(), nil
	case Commit:
		return NewCommitRecord(p)
	case Start:
		return NewStartRecord(p)
	case Rollback:
		return NewRollbackRecord(p)
	case SetInt:
		return NewSetIntRecord(p)
	case SetString:
		return NewSetStringRecord(p)
	default:
		return nil, fmt.Errorf("tx: CreateLogRecord() failed, recordType value of page is invalid")
	}
}
