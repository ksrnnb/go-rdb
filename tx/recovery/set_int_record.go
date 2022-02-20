package recovery

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	"github.com/ksrnnb/go-rdb/tx"
)

type SetIntRecord struct {
	txnum  int
	offset int
	val    int
	blk    *file.BlockID
}

func NewSetIntRecord(p *file.Page) (*SetIntRecord, error) {
	sir := &SetIntRecord{}

	tpos := intByteSize
	sir.txnum = p.GetInt(tpos)

	fpos := tpos + intByteSize
	filename := p.GetString(fpos)

	bpos := fpos + file.MaxLength(filename)
	blknum := p.GetInt(bpos)

	sir.blk = file.NewBlockID(filename, blknum)
	opos := bpos + intByteSize
	sir.offset = p.GetInt(opos)

	vpos := opos + intByteSize
	sir.val = p.GetInt(vpos)

	if err := p.Err(); err != nil {
		return nil, err
	}

	return sir, nil
}

// Op() returns the log record's type
func (sir *SetIntRecord) Op() int {
	return SetInt
}

// TxNumber() returns the transaction id stored with the log record
func (sir *SetIntRecord) TxNumber() int {
	return sir.txnum
}

// Undo() undoes the operation encoded by this log record
func (sir *SetIntRecord) Undo(tx *tx.Transaction) {
	tx.Pin(sir.blk)
	tx.SetInt(sir.blk, sir.offset, sir.val, false)
	tx.Unpin(sir.blk)
}

func (sir *SetIntRecord) String() string {
	return "<SETINT " + strconv.Itoa(sir.txnum) + " " + sir.blk.String() +
		" " + strconv.Itoa(sir.offset) + " " + strconv.Itoa(sir.val) + ">"
}

func writeSetIntToLog(lm *logs.LogManager, txnum int, blk *file.BlockID, offset int, val int) (latestLSN int, err error) {
	tpos := intByteSize
	fpos := tpos + intByteSize
	bpos := fpos + file.MaxLength(blk.FileName())
	opos := bpos + intByteSize
	vpos := opos + intByteSize

	rec := make([]byte, intByteSize)
	p := file.NewPageWithBuf(rec)
	p.SetInt(0, SetInt)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	if err := p.Err(); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
