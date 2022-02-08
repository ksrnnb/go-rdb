package log

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/bytebuffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/iterator"
)

const int64Size = bytebuffer.Int64Size

type LogManager struct {
	fm           *file.FileManager
	logFileName  string
	logPage      *file.Page
	currentBlk   *file.BlockID
	latestLSN    int // LSN: Log Sequence Number
	lastSavedLSN int
}

// NewLogManger()は1ブロックサイズ分のバイトバッファをもつページを1つ確保する
// 指定したログファイルの大きさが0の場合は、新しくブロックを割り当て
// 0でない（既に書き込まれている）場合は、ログサイズ分のブロックを生成して、
// 読み取った内容をページおに書き込む
func NewLogManager(fm file.FileManager, logFileName string) (*LogManager, error) {
	var lm *LogManager

	b := make([]byte, fm.BlockSize())
	logPage := file.NewPageWithBuf(b)

	logSize, err := fm.Length(logFileName)

	if err != nil {
		return nil, fmt.Errorf("log: NewLogManager() cannot get log size, %w", err)
	}

	if logSize == 0 {
		lm.currentBlk, err = lm.appendNewBlock()

		if err != nil {
			return nil, fmt.Errorf("log: NewLogManager() cannot append new block, %w", err)
		}
	} else {
		lm.currentBlk = file.NewBlockID(logFileName, logSize-1)
		err = lm.fm.Read(lm.currentBlk, logPage)

		if err != nil {
			return nil, fmt.Errorf("log: NewLogManager() cannot read file to logPage, %w", err)
		}
	}

	return lm, nil
}

// FlushWithLSN()は、指定したLSNと最後にディスクに書き込んだLSNを比較する
// 指定したLSNのほうが小さい場合は、既にディスクに書き込まれている必要がある。
// それ以外の場合は、ページをディスクに書き込む
func (lm *LogManager) FlushWithLSN(lsn int) error {
	if lsn >= lm.lastSavedLSN {
		return lm.flush()
	}

	return nil
}

// flush()はログページをディスクに書き込み、
// 最後にディスクに書き込んだLSN（lastSavedLSN）を更新する
func (lm *LogManager) flush() error {
	err := lm.fm.Write(lm.currentBlk, lm.logPage)

	if err != nil {
		return fmt.Errorf("log: flush() cannot write page to block, %w", err)
	}

	lm.lastSavedLSN = lm.latestLSN

	return err
}

// Append()は、
// ログレコードのサイズを計算して、現在のページに収まるかどうかを判断
// 収まらない場合は、現在のページをディスクに書き込み、appendNewBlock()を呼ぶ
// 処理後、LSNを1インクリメントする（latestLSN）
func (lm *LogManager) Append(logrec []byte) (latestLSN int, err error) {
	boundary, err := lm.logPage.GetInt(0)

	if err != nil {
		return 0, fmt.Errorf("log: Append() failed to get integer, %w", err)
	}

	recSize := len(logrec)
	// TODO: int64Sizeでいいのか？？
	bytesNeeded := recSize + int64Size

	if boundary-bytesNeeded < int64Size {
		err = lm.flush()
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to flush, %w", err)
		}

		lm.currentBlk, err = lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to append block, %w", err)
		}

		// appendNewBlock()を実行した後は、ページは新しい空のページとなる。
		// 先頭にブロックサイズが格納されている。
		boundary, err = lm.logPage.GetInt(0)
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to get integer, %w", err)
		}
	}

	recPos := boundary - bytesNeeded
	err = lm.logPage.SetBytes(recPos, logrec)
	if err != nil {
		return 0, fmt.Errorf("log: Append() failed to set bytes, %w", err)
	}

	lm.logPage.SetInt(0, recPos)
	if err != nil {
		return 0, fmt.Errorf("log: Append() failed to set int, %w", err)
	}

	lm.latestLSN += 1
	return lm.latestLSN, nil
}

// appendNewBlock()はログファイルに新しくブロックを割り当てて、
// ログページの先頭に新しく割り当てたブロックのサイズを格納する。
// 生成したブロックにページの内容を書き込んだあと、そのブロックを返す。
func (lm *LogManager) appendNewBlock() (*file.BlockID, error) {
	blk, err := lm.fm.Append(lm.logFileName)

	if err != nil {
		return nil, fmt.Errorf("log: appendNewBlock() cannot append new block, %w", err)
	}

	err = lm.logPage.SetInt(0, lm.fm.BlockSize())

	if err != nil {
		return nil, fmt.Errorf("log: appendNewBlock() cannot set integer to lm.logPage, %w", err)
	}

	err = lm.fm.Write(blk, lm.logPage)

	if err != nil {
		return nil, fmt.Errorf("log: appendNewBlock() cannot write to lm.logPage, %w", err)
	}

	return blk, nil
}

// ログのイテレータを返す
func (lm *LogManager) Iterator() (iterator.ByteIterator, error) {
	err := lm.flush()

	if err != nil {
		return nil, fmt.Errorf("log: iterator() cannot flush, %w", err)
	}
	return NewLogIterator(lm.fm, lm.currentBlk)
}
