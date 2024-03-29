package logs

import (
	"fmt"
	"sync"

	"github.com/ksrnnb/go-rdb/bytebuffer"
	"github.com/ksrnnb/go-rdb/file"
)

const intByteSize = bytebuffer.IntByteSize

// boundaryPosition は常に page の先頭 = 0
const boundaryPosition = 0

// Log Page structure
// page の末尾からログレコードを書き込むことで、LogIterator が最新のログレコードから読み取ることができるようになる
// ByteBuffer の仕様上、こうしたほうが便利
//
// boundary                                page
//
// 0                               position                                size
// ↓                                 ↓                                      ↓
// --------------------------------------------------------------------------
// | boundary position (int32) | ... | record n | ... | record 2 | record 1 |
// --------------------------------------------------------------------------.
type LogManager struct {
	fm           *file.FileManager
	logFileName  string
	logPage      *file.Page
	currentBlk   file.BlockID
	latestLSN    int // LSN: Log Sequence Number
	lastSavedLSN int
	mux          sync.Mutex
}

// NewLogManager()は1ブロックサイズ分のバイトバッファをもつページを1つ確保する
// 指定したログファイルの大きさが0の場合は、新しくブロックを割り当て
// 0でない（既に書き込まれている）場合は、ログサイズ分のブロックを生成して、
// 読み取った内容をページに書き込む
func NewLogManager(fm *file.FileManager, logFileName string) (*LogManager, error) {
	lm := &LogManager{
		fm:          fm,
		logFileName: logFileName,
	}

	b := make([]byte, fm.BlockSize())
	lm.logPage = file.NewPageWithBuf(b)

	logSize, err := fm.Length(lm.logFileName)

	if err != nil {
		return nil, fmt.Errorf("log: NewLogManager() cannot get log size, %w", err)
	}

	if logSize == 0 {
		lm.currentBlk, err = lm.appendNewBlock()

		if err != nil {
			return nil, fmt.Errorf("log: NewLogManager() cannot append new block, %w", err)
		}
	} else {
		lm.currentBlk = file.NewBlockID(lm.logFileName, logSize-1)
		err = lm.fm.Read(lm.currentBlk, lm.logPage)

		if err != nil {
			return nil, fmt.Errorf("log: NewLogManager() cannot read file to logPage, %w", err)
		}
	}

	return lm, nil
}

// Flush()は、指定したLSNと最後にディスクに書き込んだLSNを比較する
// 指定したLSNのほうが小さい場合は、既にディスクに書き込まれている。
// それ以外の場合は、ページをディスクに書き込む
func (lm *LogManager) Flush(lsn int) error {
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
	return nil
}

// Append は、ログレコードをブロックに追加する
// ログレコードのサイズを計算して、現在のページに収まるかどうかを判断
// 収まらない場合は、現在のページをディスクに書き込み、appendNewBlock()を呼ぶ
// 処理後、LSNを1インクリメントする（latestLSN）
func (lm *LogManager) Append(logrec []byte) (latestLSN int, err error) {
	lm.mux.Lock()
	defer lm.mux.Unlock()
	// boundaryは前回書き込んだ最後の位置
	boundary, err := lm.getBoundary()
	if err != nil {
		return 0, fmt.Errorf("log: Append() failed to get integer, %w", err)
	}

	recSize := len(logrec)
	// 文字列と文字列の長さ（int）の分の容量
	bytesNeeded := recSize + intByteSize

	// fmt.Printf("lastSavedLSN: %d, latestLSN: %d, boundary: %d, logrec: %s, recSize: %d, bytesNeeded: %d\n\n",
	// 	lm.lastSavedLSN, lm.latestLSN, boundary, logrec, recSize, bytesNeeded)

	// boundary より左側に bytesNeeded 分だけ書き込む余地があるかどうか
	// intByteSize は boundary 分の容量
	if boundary-bytesNeeded < intByteSize {
		err = lm.flush()
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to flush, %w", err)
		}

		lm.currentBlk, err = lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to append block, %w", err)
		}

		// appendNewBlock()を実行した後は、ページは新しい空のページとなるので、boundary を改めて取得する
		boundary, err = lm.getBoundary()
		if err != nil {
			return 0, fmt.Errorf("log: Append() failed to get int, %w", err)
		}
	}

	// 最後に書き込んだ位置から文字列格納に必要なバイト数だけ前に移動
	recPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recPos, logrec)

	// 最後に書き込んだレコードの位置を更新する
	err = lm.setBoundary(recPos)
	if err != nil {
		return 0, fmt.Errorf("log: Append() failed to set int, %w", err)
	}

	// 書き込みができたらlatestLSNを更新する
	lm.latestLSN += 1
	return lm.latestLSN, nil
}

// appendNewBlock()はログファイルに新しくブロックを割り当てて、
// ログページの先頭に新しく割り当てたブロックのサイズを格納する。
// 生成したブロックにページの内容を書き込んだあと、そのブロックを返す。
func (lm *LogManager) appendNewBlock() (file.BlockID, error) {
	blk, err := lm.fm.Append(lm.logFileName)

	if err != nil {
		return file.BlockID{}, fmt.Errorf("log: appendNewBlock() cannot append new block, %w", err)
	}

	// boundary を末尾に設定
	err = lm.setBoundary(lm.fm.BlockSize())
	if err != nil {
		return file.BlockID{}, fmt.Errorf("log: appendNewBlock() cannot set integer to lm.logPage, %w", err)
	}

	err = lm.fm.Write(blk, lm.logPage)

	if err != nil {
		return file.BlockID{}, fmt.Errorf("log: appendNewBlock() cannot write to lm.logPage, %w", err)
	}

	return blk, nil
}

// ログのイテレータを返す
func (lm *LogManager) Iterator() (*LogIterator, error) {
	err := lm.flush()

	if err != nil {
		return nil, fmt.Errorf("log: iterator() cannot flush, %w", err)
	}
	return NewLogIterator(lm.fm, lm.currentBlk)
}

// boundary の値を取得する
func (lm *LogManager) getBoundary() (int, error) {
	return lm.logPage.GetInt(boundaryPosition)
}

// boundary の値を設定する
func (lm *LogManager) setBoundary(val int) error {
	return lm.logPage.SetInt(boundaryPosition, val)
}
