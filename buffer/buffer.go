package buffer

import (
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

type Buffer struct {
	fm       *file.FileManager
	lm       *logs.LogManager
	contents *file.Page
	blk      *file.BlockID
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *file.FileManager, lm *logs.LogManager) *Buffer {
	return &Buffer{
		fm:    fm,
		lm:    lm,
		txnum: -1,
		lsn:   -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockID {
	return b.blk
}

func (b *Buffer) SetModified(txnum, lsn int) {
	b.txnum = txnum

	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

func (b *Buffer) AssignToBlock(blk *file.BlockID) {
	b.flush()
	b.blk = blk
	b.fm.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.blk, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
