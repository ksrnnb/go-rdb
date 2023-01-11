package logs

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
)

// LogIteratorはログレコードの新しいものから古いものに向かってiterateする
// 探すログレコードは末尾の方にある可能性が高いため、末尾から読む方が効率的
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

// 指定したブロック領域をイテレータのページに読み込む
// ブロック領域の先頭に入っている値をboundary（最後にログが書き込まれた場所）
// currentPosの値をboundaryにすることで、最後にログが書き込まれた場所を指定する
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

// Next()はページの位置が、イテレータのcurrentPosの場合の文字列を取得する
// currentPosがブロックサイズの場合は、次のブロックに移動してから文字列を取得する
func (li *LogIterator) Next() ([]byte, error) {
	if li.currentPos == li.fm.BlockSize() {
		// 同じファイルの1つ前のブロックを作って、読み込む
		li.blk = file.NewBlockID(li.blk.FileName(), li.blk.Number()-1)
		err := li.moveToBlock(li.blk)
		if err != nil {
			return []byte{}, err
		}
	}

	rec, err := li.p.GetBytes(li.currentPos)
	if err != nil {
		return []byte{}, err
	}

	li.currentPos += intByteSize + len(rec)
	return rec, nil
}
