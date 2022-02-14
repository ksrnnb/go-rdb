package buffer

import (
	"fmt"

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
		fm:       fm,
		lm:       lm,
		txnum:    -1,
		lsn:      -1,
		contents: file.NewPage(fm.BlockSize()),
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

// IsPinned()はpin状態かどうかを返す
// b.pinsの初期値は0
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

func (b *Buffer) assignToBlock(blk *file.BlockID) error {
	err := b.flush()

	if err != nil {
		return fmt.Errorf("buffer: assignToBlock() failed, %w", err)
	}

	err = b.fm.Read(blk, b.contents)
	if err != nil {
		return fmt.Errorf("buffer: assignToBlock() failed, %w", err)
	}

	b.blk = blk
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txnum >= 0 {
		err := b.lm.Flush(b.lsn)

		if err != nil {
			return fmt.Errorf("buffer: flush() failed, %w", err)
		}

		err = b.fm.Write(b.blk, b.contents)
		if err != nil {
			return fmt.Errorf("buffer: flush() failed, %w", err)
		}

		b.txnum = -1
	}

	return nil
}

// pinしている数をインクリメントする
func (b *Buffer) pin() {
	b.pins++
}

// pinしている数をデクリメントする
func (b *Buffer) unpin() {
	b.pins--
}
