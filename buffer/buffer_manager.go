package buffer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

// デッドロックを回避するために、最大の待ち時間を設定する
const maxWaitingTime = 10 * time.Second

var errNotExistUnpin = errors.New("unpin buffer doesn't exist")

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int
	cond         *sync.Cond
}

type pinResult struct {
	buffer *Buffer
	err    error
}

// NewBufferManager()はBufferManagerを返す
// bufferPoolはnumbuffs個のBufferが初期値となるため、全てunpin状態
func NewBufferManager(fm *file.FileManager, lm *logs.LogManager, numbuffs int) *BufferManager {
	bp := make([]*Buffer, numbuffs)
	bm := &BufferManager{
		bufferPool:   bp,
		numAvailable: numbuffs,
		cond:         sync.NewCond(&sync.Mutex{}),
	}

	for i := 0; i < numbuffs; i++ {
		bm.bufferPool[i] = NewBuffer(fm, lm)
	}

	return bm
}

// unpin状態のバッファ数を返す
func (bm *BufferManager) Available() int {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
	return bm.numAvailable
}

// トランザクションNo.が一致するバッファを全てディスクに書き込む
func (bm *BufferManager) FlushAll(txnum int) error {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
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
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
	b.unpin()

	if !b.IsPinned() {
		bm.numAvailable++
		bm.cond.Broadcast()
	}
}

// Pin()は引数のブロックをpinする
// pinできたらBufferを返す
// ディスクに書き込む可能性のあるメソッドはPin()またはFlushAll()のみ
func (bm *BufferManager) Pin(blk *file.BlockID) (*Buffer, error) {
	start := time.Now()
	result := make(chan pinResult)
	defer close(result)

	go bm.pin(result, blk, start)

	select {
	case pr := <-result:
		return pr.buffer, pr.err
	case <-time.After(maxWaitingTime):
		bm.cond.Broadcast()
		pr := <-result
		return pr.buffer, pr.err
	}
}

func (bm *BufferManager) pin(result chan<- pinResult, blk *file.BlockID, start time.Time) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()

	b, err := bm.tryToPin(blk)

	if err != nil {
		if !errors.Is(err, errNotExistUnpin) {
			result <- pinResult{nil, fmt.Errorf("buffer(): Pin() failed, %w", err)}
		}
	}

	for b == nil && !isWaitingTooLong(start) {
		bm.cond.Wait()
		b, err = bm.tryToPin(blk)
		if err != nil {
			result <- pinResult{nil, fmt.Errorf("buffer(): Pin() failed, %w", err)}
		}
	}

	result <- pinResult{b, nil}
}

// tryToPin()はbufferPoolからブロックを探す
// ブロックがunpin状態だったらpin状態にしてBufferを返す
// ブロックがない場合は、unpin状態のBufferを探す
// unpinのBufferがあれば、引数のブロックをBufferに割り当てる（ディスク書き込み）
// Bufferがない場合はnilを返す（=> pinできなかった）
func (bm *BufferManager) tryToPin(blk *file.BlockID) (b *Buffer, err error) {
	b = bm.findExistingBuffer(blk)

	if b == nil {
		b, err = bm.chooseUnpinnedBuffer()
		if err != nil {
			return nil, err
		}

		err = b.assignToBlock(blk)
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
// => Naive strategy を採用
func (bm *BufferManager) chooseUnpinnedBuffer() (*Buffer, error) {
	for _, b := range bm.bufferPool {
		if !b.IsPinned() {
			return b, nil
		}
	}

	return nil, errNotExistUnpin
}

func isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxWaitingTime)

	return time.Now().After(limit)
}
