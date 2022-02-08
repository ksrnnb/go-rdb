package log

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
)

type LogIterator struct {
	fm         *file.FileManager
	blk        *file.BlockID
	p          *file.Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *file.FileManager, blk *file.BlockID) (*LogIterator, error) {
	b := make([]byte, fm.BlockSize())
	p := file.NewPageWithBuf(b)

	li := &LogIterator{
		fm:  fm,
		blk: blk,
		p:   p,
	}

	err := li.moveToBlock(blk)

	if err != nil {
		return nil, fmt.Errorf("log: NewLogIterator() failed to move to block, %w", err)
	}

	return li, nil
}

func (li *LogIterator) moveToBlock(blk *file.BlockID) error {
	err := li.fm.Read(blk, li.p)

	if err != nil {
		return fmt.Errorf("log: moveToBlock() failed to read file to page, %w", err)
	}

	li.boundary, err = li.p.GetInt(0)

	if err != nil {
		return fmt.Errorf("log: moveToBlock() failed to get integer from page, %w", err)
	}

	li.currentPos = li.boundary

	return nil
}

func (li *LogIterator) HasNext() bool {
	return li.currentPos < li.fm.BlockSize() || li.blk.Number() > 0
}

// ブロックNo.の大きいものから小さいものに向かってiterateする
func (li *LogIterator) Next() ([]byte, error) {
	if li.currentPos == li.fm.BlockSize() {
		blk := file.NewBlockID(li.blk.FileName(), li.blk.Number()-1)
		li.moveToBlock(blk)
	}

	rec, err := li.p.GetBytes(li.currentPos)

	if err != nil {
		return []byte{}, err
	}

	// TODO: ここは怪しい。int64Sizeではなく、実際に格納されている大きさでは？
	li.currentPos += int64Size + len(rec)
	return rec, nil
}
