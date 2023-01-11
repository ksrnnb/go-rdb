package tx

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

type SetIntRecord struct {
	txnum  int
	offset int
	oldVal int
	newVal int
	blk    *file.BlockID
}

func NewSetIntRecord(p *file.Page) (*SetIntRecord, error) {
	sir := &SetIntRecord{}

	tpos := intByteSize

	var err error
	sir.txnum, err = p.GetInt(tpos)
	if err != nil {
		return nil, err
	}

	fpos := tpos + intByteSize
	filename, err := p.GetString(fpos)
	if err != nil {
		return nil, err
	}

	bpos := fpos + file.MaxLength(filename)
	blknum, err := p.GetInt(bpos)
	if err != nil {
		return nil, err
	}

	sir.blk = file.NewBlockID(filename, blknum)
	opos := bpos + intByteSize
	sir.offset, err = p.GetInt(opos)
	if err != nil {
		return nil, err
	}

	oldValPos := opos + intByteSize
	sir.oldVal, err = p.GetInt(oldValPos)
	if err != nil {
		return nil, err
	}
	newValPos := opos + intByteSize
	sir.newVal, err = p.GetInt(newValPos)
	if err != nil {
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
func (sir *SetIntRecord) Undo(tx *Transaction) {
	tx.Pin(sir.blk)
	tx.SetInt(sir.blk, sir.offset, sir.oldVal, false)
	tx.Unpin(sir.blk)
}

func (sir *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d %d>", sir.txnum, sir.blk.String(), sir.offset, sir.oldVal, sir.newVal)
}

func writeSetIntToLog(lm *logs.LogManager, txnum int, blk *file.BlockID, offset int, oldVal int, newVal int) (latestLSN int, err error) {
	tpos := intByteSize
	fpos := tpos + intByteSize
	bpos := fpos + file.MaxLength(blk.FileName())
	opos := bpos + intByteSize
	oldValPos := opos + intByteSize
	newValPos := oldValPos + oldValPos
	resSize := newValPos + intByteSize

	rec := make([]byte, resSize)
	p := file.NewPageWithBuf(rec)

	if err := p.SetInt(0, SetInt); err != nil {
		return 0, err
	}

	if err := p.SetInt(tpos, txnum); err != nil {
		return 0, err
	}

	if err := p.SetString(fpos, blk.FileName()); err != nil {
		return 0, err
	}

	if err := p.SetInt(bpos, blk.Number()); err != nil {
		return 0, err
	}

	if err := p.SetInt(opos, offset); err != nil {
		return 0, err
	}

	if err := p.SetInt(oldValPos, oldVal); err != nil {
		return 0, err
	}

	if err := p.SetInt(newValPos, newVal); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
