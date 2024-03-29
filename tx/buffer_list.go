package tx

import (
	"errors"

	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
)

var ErrBufferNotFound = errors.New("tx: buffer is not found")

type txBuffer struct {
	blk file.BlockID
	buf *buffer.Buffer
}

type BufferList struct {
	txBuffers []*txBuffer
	pins      []file.BlockID
	bm        *buffer.BufferManager
}

func newTxBuffer(blk file.BlockID, buf *buffer.Buffer) *txBuffer {
	return &txBuffer{blk: blk, buf: buf}
}

func NewBufferList(bm *buffer.BufferManager) *BufferList {
	return &BufferList{bm: bm}
}

func (bl *BufferList) getTxBuffer(blk file.BlockID) (*txBuffer, error) {
	for _, txBuf := range bl.txBuffers {
		if txBuf.blk.Equals(blk) {
			return txBuf, nil
		}
	}

	return nil, ErrBufferNotFound
}

func (bl *BufferList) pin(blk file.BlockID) error {
	buf, err := bl.bm.Pin(blk)

	if err != nil {
		return err
	}

	bl.txBuffers = append(bl.txBuffers, newTxBuffer(blk, buf))
	bl.pins = append(bl.pins, blk)
	return nil
}

func (bl *BufferList) unpin(blk file.BlockID) error {
	txBuf, err := bl.getTxBuffer(blk)
	if err != nil {
		if errors.Is(err, ErrBufferNotFound) {
			return nil
		}
		return err
	}

	bl.bm.Unpin(txBuf.buf)
	bl.removePin(blk)
	if !bl.containsPin(blk) {
		bl.removeTxBuffer(blk)
	}

	return nil
}

func (bl *BufferList) unpinAll() error {
	for _, blk := range bl.pins {
		txBuf, err := bl.getTxBuffer(blk)

		if errors.Is(err, ErrBufferNotFound) {
			continue
		} else if err != nil {
			return err
		}

		bl.bm.Unpin(txBuf.buf)
	}

	bl.txBuffers = make([]*txBuffer, 0)
	bl.pins = make([]file.BlockID, 0)
	return nil
}

func (bl *BufferList) removePin(blk file.BlockID) {
	for i, pin := range bl.pins {
		if pin.Equals(blk) {
			bl.dispatchRemovePin(blk, i)
			return
		}
	}
}

func (bl *BufferList) dispatchRemovePin(blk file.BlockID, index int) {
	bl.pins = append(bl.pins[:index], bl.pins[index+1:]...)
}

func (bl *BufferList) containsPin(blk file.BlockID) bool {
	for _, pin := range bl.pins {
		if pin.Equals(blk) {
			return true
		}
	}

	return false
}

func (bl *BufferList) removeTxBuffer(blk file.BlockID) {
	for i, txBuf := range bl.txBuffers {
		if txBuf.blk.Equals(blk) {
			bl.txBuffers = append(bl.txBuffers[:i], bl.txBuffers[i+1:]...)
			return
		}
	}
}
