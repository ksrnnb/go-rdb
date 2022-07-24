package buffer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

// 10 * 1000 msec => 10 sec
const maxTimeMilliSecond = 10000

var errNotExistUnpin = errors.New("unpin buffer doesn't exist")

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int
	mux          sync.Mutex
}

// NewBufferManager()はBufferManagerを返す
// bufferPoolはnumbuffs個のBufferが初期値となるため、全てunpin状態
func NewBufferManager(fm *file.FileManager, lm *logs.LogManager, numbuffs int) *BufferManager {
	bp := make([]*Buffer, numbuffs)
	bm := &BufferManager{
		bufferPool:   bp,
		numAvailable: numbuffs,
	}

	for i := 0; i < numbuffs; i++ {
		bm.bufferPool[i] = NewBuffer(fm, lm)
	}

	return bm
}

// unpin状態のバッファ数を返す
func (bm *BufferManager) Available() int {
	bm.mux.Lock()
	defer bm.mux.Unlock()
	return bm.numAvailable
}

// トランザクションNo.が一致するバッファを全てディスクに書き込む
func (bm *BufferManager) FlushAll(txnum int) error {
	bm.mux.Lock()
	defer bm.mux.Unlock()
	for _, b := range bm.bufferPool {
		if b.ModifyingTx() == txnum {
			err := b.flush()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Unpin()は引数のBufferをunpinする
func (bm *BufferManager) Unpin(b *Buffer) {
	bm.mux.Lock()
	defer bm.mux.Unlock()
	b.unpin()

	if !b.IsPinned() {
		bm.numAvailable++

		// TODO: unpinされたことを通知する
	}
}

// Pin()は引数のブロックをpinする
// 空きがない場合は1秒ごとに再確認（最大10秒）
// pinできたらBufferを返す
// ディスクに書き込む可能性のあるメソッドはPin()またはFlushAll()のみ
func (bm *BufferManager) Pin(blk *file.BlockID) (*Buffer, error) {
	bm.mux.Lock()
	defer bm.mux.Unlock()
	t := time.Now()
	b, err := bm.tryToPin(blk)

	if err != nil && errors.Is(err, errNotExistUnpin) {
		return nil, fmt.Errorf("buffer(): Pin() failed, %w", err)
	}

	for b == nil && !bm.isWaitingTooLong(t) {
		// TODO: 他のスレッドでUnpinされるのを待つ
		// 暫定で1秒ごとにtryする
		time.Sleep(1 * time.Second)
		b, err = bm.tryToPin(blk)
		if err != nil && errors.Is(err, errNotExistUnpin) {
			return nil, fmt.Errorf("buffer(): Pin() failed, %w", err)
		}

	}

	if b == nil {
		// maxTImeMilliSecondを超えてもpinできない場合はエラーを返す
		return nil, fmt.Errorf("buffer: Pin() failed, time over")
	}

	return b, nil
}

// isWaitingTooLong()はmaxTimeを超えてwaitしているかどうかを返す
func (bm *BufferManager) isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxTimeMilliSecond * time.Millisecond)

	return time.Now().After(limit)
}

// tryToPin()はbufferPoolからブロックを探す
// ブロックがunpin状態だったらpin状態にしてBufferを返す
// ブロックがない場合は、unpin状態のBufferを探す
// unpinのBufferがあれば、引数のブロックをBufferに割り当てる（ディスク書き込み）
// Bufferがない場合はnilを返す（=> pinできなかった）
func (bm *BufferManager) tryToPin(blk *file.BlockID) (*Buffer, error) {
	b := bm.findExistingBuffer(blk)

	if b == nil {
		b = bm.chooseUnpinnedBuffer()

		if b == nil {
			return nil, errNotExistUnpin
		}

		err := b.assignToBlock(blk)
		if err != nil {
			return nil, fmt.Errorf("buffer(): tryToPin() failed, %w", err)
		}
	}

	if !b.IsPinned() {
		bm.numAvailable--
	}

	b.pin()
	return b, nil
}

// findExistingBuffer()は引数と同じブロックをbufferPoolの中から探す
func (bm *BufferManager) findExistingBuffer(blk *file.BlockID) *Buffer {
	for _, b := range bm.bufferPool {
		block := b.Block()
		if block != nil && block.Equals(blk) {
			return b
		}
	}

	return nil
}

// chooseUnpinnedBuffer()はbufferPoolのうち、
// unpin状態のものを探して一番初めに見つかったものを返す
func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, b := range bm.bufferPool {
		if !b.IsPinned() {
			return b
		}
	}

	return nil
}
