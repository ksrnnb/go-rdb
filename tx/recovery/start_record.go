package recovery

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	"github.com/ksrnnb/go-rdb/tx"
)

type StartRecord struct {
	txnum int
}

func NewStartRecord(p *file.Page) (*StartRecord, error) {
	tpos := intByteSize
	txnum := p.GetInt(tpos)

	if err := p.Err(); err != nil {
		return nil, err
	}
	return &StartRecord{txnum: txnum}, nil
}

// Op() returns the log record's type
func (sr *StartRecord) Op() int {
	return Start
}

// TxNumber() returns the transaction id stored with the log record
func (sr *StartRecord) TxNumber() int {
	return sr.txnum
}

// Undo() undoes the operation encoded by this log record
// do nothing in Start
func (sr *StartRecord) Undo(tx *tx.Transaction) {}

func (sr *StartRecord) String() string {
	return "<START " + strconv.Itoa(sr.txnum) + ">"
}

func writeStartToLog(lm *logs.LogManager, txnum int) (latestLSN int, err error) {
	rec := make([]byte, 2*intByteSize)
	p := file.NewPageWithBuf(rec)
	p.SetInt(0, Start)
	p.SetInt(intByteSize, txnum)

	if err := p.Err(); err != nil {
		return 0, err
	}
	return lm.Append(rec)
}
