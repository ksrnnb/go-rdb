package recovery

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/bytebuffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	"github.com/ksrnnb/go-rdb/tx"
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
	ssr.txnum = p.GetInt(tpos)

	fpos := tpos + intByteSize
	filename := p.GetString(fpos)

	bpos := fpos + file.MaxLength(filename)
	blknum := p.GetInt(bpos)

	ssr.blk = file.NewBlockID(filename, blknum)
	opos := bpos + intByteSize
	ssr.offset = p.GetInt(opos)

	vpos := opos + intByteSize
	ssr.val = p.GetString(vpos)

	if err := p.Err(); err != nil {
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
func (ssr *SetStringRecord) Undo(tx *tx.Transaction) {
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
	bpos := fpos + file.MaxLength(blk.FileName())
	opos := bpos + intByteSize
	vpos := opos + intByteSize
	recLen := vpos + file.MaxLength(val)

	rec := make([]byte, recLen)
	p := file.NewPageWithBuf(rec)
	p.SetInt(0, SetString)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	if err := p.Err(); err != nil {
		return 0, err
	}

	return lm.Append(rec)
}
