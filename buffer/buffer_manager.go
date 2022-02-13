package buffer

import (
	"fmt"
	"time"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
)

// 10 * 1000 msec => 10 sec
const maxTimeMilliSecond = 10000

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int
}

func NewBufferManager(fm *file.FileManager, lm *logs.LogManager, numbuffs int) *BufferManager {
	bm := &BufferManager{numAvailable: numbuffs}

	for i := 0; i < numbuffs; i++ {
		bm.bufferPool[i] = NewBuffer(fm, lm)
	}

	return bm
}

// TODO: sync
func (bm *BufferManager) Available() int {
	return bm.numAvailable
}

func (bm *BufferManager) FlushAll(txnum int) {
	for _, b := range bm.bufferPool {
		if b.ModifyingTx() == txnum {
			b.flush()
		}
	}
}

func (bm *BufferManager) Unpin(b *Buffer) {
	b.unpin()

	if !b.IsPinned() {
		bm.numAvailable++

		// TODO: javaのnotifyAll()を参照して実装
		// notifyAll()
	}
}

func (bm *BufferManager) Pin(blk *file.BlockID) (*Buffer, error) {
	t := time.Now()
	b := bm.tryToPin(blk)

	for b == nil && !bm.isWaitingTooLong(t) {
		// TODO: javaのwait()
		// wait(maxTimeMilliSecond)
		b = bm.tryToPin(blk)
	}

	if b == nil {
		// TODO: fix error message
		return nil, fmt.Errorf("buffer error")
	}

	return b, nil
}

func (bm *BufferManager) isWaitingTooLong(start time.Time) bool {
	limit := start.Add(maxTimeMilliSecond * time.Millisecond)

	return time.Now().After(limit)
}

func (bm *BufferManager) tryToPin(blk *file.BlockID) *Buffer {
	b := bm.findExistingBuffer(blk)

	if b == nil {
		b = bm.chooseUnpinnedBuffer()

		if b == nil {
			return nil
		}

		b.AssignToBlock(blk)
	}

	if !b.IsPinned() {
		bm.numAvailable--
	}

	b.pin()
	return b
}

func (bm *BufferManager) findExistingBuffer(blk *file.BlockID) *Buffer {
	for _, b := range bm.bufferPool {
		block := b.Block()
		if block != nil && block.Equals(blk) {
			return b
		}
	}

	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, b := range bm.bufferPool {
		if !b.IsPinned() {
			return b
		}
	}

	return nil
}
