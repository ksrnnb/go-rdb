package planner

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type ChunkScan struct {
	buffers       []*record.RecordPage
	tx            *tx.Transaction
	fileName      string
	layout        *record.Layout
	startBlkNum   int
	endBlkNum     int
	currentBlkNum int
	rp            *record.RecordPage
	currentSlot   int
}

func NewChunkScan(tx *tx.Transaction, fileName string, layout *record.Layout, startBlkNum int, endBlkNum int) (*ChunkScan, error) {
	cs := &ChunkScan{
		tx:          tx,
		fileName:    fileName,
		layout:      layout,
		startBlkNum: startBlkNum,
		endBlkNum:   endBlkNum,
	}
	buffers := make([]*record.RecordPage, 0)

	for i := startBlkNum; i <= endBlkNum; i++ {
		blk := file.NewBlockID(fileName, i)
		rp, err := record.NewRecordPage(tx, blk, layout)
		if err != nil {
			return nil, err
		}
		buffers = append(buffers, rp)
	}
	cs.buffers = buffers
	cs.moveToBlock(startBlkNum)
	return cs, nil
}

func (cs *ChunkScan) Close() error {
	for i := 0; i < len(cs.buffers); i++ {
		blk := file.NewBlockID(cs.fileName, cs.startBlkNum+i)
		if err := cs.tx.Unpin(blk); err != nil {
			return err
		}
	}
	return nil
}

func (cs *ChunkScan) BeforeFirst() error {
	cs.moveToBlock(cs.startBlkNum)
	return nil
}

func (cs *ChunkScan) Next() (bool, error) {
	s, err := cs.rp.NextAfter(cs.currentSlot)
	if err != nil {
		return false, err
	}
	cs.currentSlot = s
	for cs.currentSlot < 0 {
		if cs.currentBlkNum == cs.endBlkNum {
			return false, nil
		}
		cs.moveToBlock(cs.rp.Block().Number() + 1)
		s, err := cs.rp.NextAfter(cs.currentSlot)
		if err != nil {
			return false, err
		}
		cs.currentSlot = s
	}
	return true, nil
}

func (cs *ChunkScan) GetInt(fieldName string) (int, error) {
	return cs.rp.GetInt(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetString(fieldName string) (string, error) {
	return cs.rp.GetString(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetVal(fieldName string) (query.Constant, error) {
	ft, err := cs.layout.Schema().FieldType(fieldName)
	if err != nil {
		return query.Constant{}, err
	}
	switch ft {
	case record.Integer:
		v, err := cs.GetInt(fieldName)
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(v), nil
	case record.String:
		v, err := cs.GetString(fieldName)
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(v), nil
	}
	return query.Constant{}, fmt.Errorf("invalid field type %v", ft)
}

func (cs *ChunkScan) HasField(fieldName string) bool {
	return cs.layout.Schema().HasField(fieldName)
}

func (cs *ChunkScan) moveToBlock(blkNum int) {
	cs.currentBlkNum = blkNum
	cs.rp = cs.buffers[cs.currentBlkNum-cs.startBlkNum]
	cs.currentSlot = -1
}
