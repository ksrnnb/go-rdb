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
	pins     int // ページがピン留めされた回数
	txnum    int // ページの変更を行なったトランザクションを識別する。-1 は未変更
	lsn      int // 最新のログレコードの Log Sequence Number
}

// 1個のバッファは1ページ分もつ
func NewBuffer(fm *file.FileManager, lm *logs.LogManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		txnum:    -1,
		lsn:      -1,
		contents: file.NewPage(fm.BlockSize()),
	}
}

// Contents() returns associated page.
// If the client modifies the page, it is also responsible for
// generating an appropriatge log record and
// calling the buffer's setModified() method.
func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// バッファがもつブロックを返す
func (b *Buffer) Block() *file.BlockID {
	return b.blk
}

// トランザクションNo.とLSNを設定
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

// トランザクションNo.を返す
func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

// 指定したブロックの内容をBufferのpageに割り当てる
// 割り当て前に、バッファの内容はflush()してディスクに書き込まれる
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

// トランザクションの値が0以上（SetModifiy()を実行）であれば
// ログマネージャのFlushを実行し、ログファイルに書き込む
// ファイルのブロックにページの内容を書き込む
// トランザクションNo.は-1に戻す
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
