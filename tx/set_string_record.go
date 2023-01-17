package tx

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/bytebuffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

var intByteSize = getIntByteSize()

func getIntByteSize() int {
	return bytebuffer.IntByteSize
}

type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    *file.BlockID
}

func NewSetStringRecord(p *file.Page) (*SetStringRecord, error) {
	ssr := &SetStringRecord{}

	tpos := intByteSize
	var err error

	ssr.txnum, err = p.GetInt(tpos)
	if err != nil {
		return nil, err
	}

	fpos := tpos + intByteSize
	filename, err := p.GetString(fpos)
	if err != nil {
		return nil, err
	}

	bpos := fpos + file.MaxLengthInString(filename)
	blknum, err := p.GetInt(bpos)
	if err != nil {
		return nil, err
	}

	ssr.blk = file.NewBlockID(filename, blknum)
	opos := bpos + intByteSize
	ssr.offset, err = p.GetInt(opos)
	if err != nil {
		return nil, err
	}

	vpos := opos + intByteSize
	ssr.val, err = p.GetString(vpos)

	if err != nil {
		return nil, err
	}

	return ssr, nil
}

// Op() returns the log record's type
func (ssr *SetStringRecord) Op() int {
	return SetString
}

// TxNumber() returns the transaction id stored with the log record
func (ssr *SetStringRecord) TxNumber() int {
	return ssr.txnum
}

// Undo() undoes the operation encoded by this log record
func (ssr *SetStringRecord) Undo(tx *Transaction) {
	tx.Pin(ssr.blk)
	tx.SetString(ssr.blk, ssr.offset, ssr.val, false)
	tx.Unpin(ssr.blk)
}

func (ssr *SetStringRecord) String() string {
	return "<SETSTRING " + strconv.Itoa(ssr.txnum) + " " + ssr.blk.String() +
		" " + strconv.Itoa(ssr.offset) + " " + ssr.val + ">"
}

func writeSetStringToLog(lm *logs.LogManager, txnum int, blk *file.BlockID, offset int, val string) (latestLSN int, err error) {
	tpos := intByteSize
	fpos := tpos + intByteSize
	bpos := fpos + file.MaxLengthInString(blk.FileName())
	opos := bpos + intByteSize
	vpos := opos + intByteSize
	recLen := vpos + file.MaxLengthInString(val)

	rec := make([]byte, recLen)
	p := file.NewPageWithBuf(rec)

	if err := p.SetInt(0, SetString); err != nil {
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

	if err := p.SetString(vpos, val); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
