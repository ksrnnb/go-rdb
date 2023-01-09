package tx

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

type CommitRecord struct {
	txnum int
}

func NewCommitRecord(p *file.Page) (*CommitRecord, error) {
	tpos := intByteSize
	txnum, err := p.GetInt(tpos)

	if err != nil {
		return nil, err
	}
	return &CommitRecord{txnum: txnum}, nil
}

// Op() returns the log record's type
func (cr *CommitRecord) Op() int {
	return Commit
}

// TxNumber() returns the transaction id stored with the log record
func (cr *CommitRecord) TxNumber() int {
	return cr.txnum
}

// Undo() undoes the operation encoded by this log record
// do nothing in Commit
func (cr *CommitRecord) Undo(tx *Transaction) {}

func (cr *CommitRecord) String() string {
	return "<COMMIT " + strconv.Itoa(cr.txnum) + ">"
}

func writeCommitToLog(lm *logs.LogManager, txnum int) (latestLSN int, err error) {
	rec := make([]byte, 2*intByteSize)
	p := file.NewPageWithBuf(rec)
	err = p.SetInt(0, Commit)
	if err != nil {
		return 0, err
	}

	err = p.SetInt(intByteSize, txnum)
	if err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
