package btree

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

const (
	flagPos      = 0
	numRecordPos = record.IntByteSize
)

// ------------------------------------------------------------------------------
// | page flag (int32) | numRecords (int32) | record 1 | ... | record n | ... |
// ------------------------------------------------------------------------------
type BTreePage struct {
	tx         *tx.Transaction
	currentBlk *file.BlockID
	layout     *record.Layout
}

func NewBTreePage(tx *tx.Transaction, currentBlk *file.BlockID, layout *record.Layout) (*BTreePage, error) {
	err := tx.Pin(currentBlk)
	if err != nil {
		return nil, err
	}
	return &BTreePage{tx, currentBlk, layout}, nil
}

func (btp *BTreePage) FindSlotBefore(searchKey query.Constant) (int, error) {
	slot := 0
	numRecords, err := btp.getNumRecords()
	if err != nil {
		return 0, err
	}
	v, err := btp.GetDataValue(slot)
	if err != nil {
		return 0, err
	}

	for slot < numRecords && v.CompareTo(searchKey) < 0 {
		slot++

		newNumRecords, err := btp.getNumRecords()
		if err != nil {
			return 0, err
		}
		newV, err := btp.GetDataValue(slot)
		if err != nil {
			return 0, err
		}
		numRecords = newNumRecords
		v = newV
	}
	return slot - 1, nil
}

func (btp *BTreePage) Close() error {
	if btp.currentBlk == nil {
		return nil
	}
	err := btp.tx.Unpin(btp.currentBlk)
	if err != nil {
		return err
	}
	btp.currentBlk = nil
	return nil
}

func (btp *BTreePage) IsFull() (bool, error) {
	numRecords, err := btp.getNumRecords()
	if err != nil {
		return false, err
	}
	return btp.slotPos(numRecords+1) >= btp.tx.BlockSize(), nil
}

func (btp *BTreePage) Split(splitPos int, flag int) (*file.BlockID, error) {
	newBlk, err := btp.AppendNew(flag)
	if err != nil {
		return nil, err
	}
	newPage, err := NewBTreePage(btp.tx, newBlk, btp.layout)
	if err != nil {
		return nil, err
	}
	err = btp.transferRecoreds(splitPos, newPage)
	if err != nil {
		return nil, err
	}
	err = newPage.SetFlag(flag)
	if err != nil {
		return nil, err
	}
	err = newPage.Close()
	if err != nil {
		return nil, err
	}
	return newBlk, err
}

func (btp *BTreePage) GetFlag() (int, error) {
	return btp.tx.GetInt(btp.currentBlk, 0)
}

func (btp *BTreePage) SetFlag(flag int) error {
	return btp.tx.SetInt(btp.currentBlk, flagPos, flag, true)
}

func (btp *BTreePage) GetDataValue(slot int) (query.Constant, error) {
	return btp.getVal(slot, index.IndexDataValueField)
}

func (btp *BTreePage) AppendNew(flag int) (*file.BlockID, error) {
	blk, err := btp.tx.Append(btp.currentBlk.FileName())
	if err != nil {
		return nil, err
	}
	err = btp.tx.Pin(blk)
	if err != nil {
		return nil, err
	}
	btp.Format(blk, flag)
	return blk, nil
}

func (btp *BTreePage) Format(blk *file.BlockID, flag int) error {
	err := btp.tx.SetInt(blk, flagPos, flag, false)
	if err != nil {
		return err
	}
	// numRecords = 0
	err = btp.tx.SetInt(blk, record.IntByteSize, 0, false)
	if err != nil {
		return err
	}
	recSize := btp.layout.SlotSize()
	for pos := 2 * record.IntByteSize; pos+recSize <= btp.tx.BlockSize(); pos += recSize {
		if err := btp.makeDefaultDecord(blk, pos); err != nil {
			return err
		}
	}
	return nil
}

// makeDefaultDecord は指定した位置にゼロ値を設定する
func (btp *BTreePage) makeDefaultDecord(blk *file.BlockID, pos int) error {
	for _, fn := range btp.layout.Schema().Fields() {
		offset, err := btp.layout.Offset(fn)
		if err != nil {
			return err
		}
		ft, err := btp.layout.Schema().FieldType(fn)
		if err != nil {
			return err
		}

		switch ft {
		case record.Integer:
			err := btp.tx.SetInt(blk, pos+offset, 0, false)
			if err != nil {
				return err
			}
		case record.String:
			err := btp.tx.SetString(blk, pos+offset, "", false)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid field type %v", ft)
		}
	}
	return nil
}

func (btp *BTreePage) getChildNum(slot int) (int, error) {
	return btp.getInt(slot, index.IndexBlockField)
}

func (btp *BTreePage) insertDirectory(slot int, val query.Constant, blkNum int) error {
	err := btp.insert(slot)
	if err != nil {
		return err
	}
	err = btp.setVal(slot, index.IndexDataValueField, val)
	if err != nil {
		return err
	}
	return btp.setInt(slot, index.IndexBlockField, blkNum)
}

func (btp *BTreePage) getDataRid(slot int) (*record.RecordID, error) {
	blkNum, err := btp.getInt(slot, index.IndexBlockField)
	if err != nil {
		return nil, err
	}
	id, err := btp.getInt(slot, index.IndexIdField)
	if err != nil {
		return nil, err
	}
	return record.NewRecordID(blkNum, id), nil
}

func (btp *BTreePage) insertLeaf(slot int, val query.Constant, rid *record.RecordID) error {
	err := btp.insert(slot)
	if err != nil {
		return err
	}
	err = btp.setVal(slot, index.IndexDataValueField, val)
	if err != nil {
		return err
	}
	err = btp.setInt(slot, index.IndexBlockField, rid.BlockNumber())
	if err != nil {
		return err
	}
	return btp.setInt(slot, index.IndexIdField, rid.Slot())
}

func (btp *BTreePage) delete(slot int) error {
	numRecords, err := btp.getNumRecords()
	if err != nil {
		return err
	}

	for i := slot + 1; i < numRecords; i++ {
		err = btp.copyRecord(i, i-1)
		if err != nil {
			return err
		}
	}
	return btp.setNumRecords(numRecords - 1)
}

func (btp *BTreePage) getNumRecords() (int, error) {
	return btp.tx.GetInt(btp.currentBlk, record.IntByteSize)
}

func (btp *BTreePage) getInt(slot int, fieldName string) (int, error) {
	pos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return 0, err
	}
	return btp.tx.GetInt(btp.currentBlk, pos)
}

func (btp *BTreePage) getString(slot int, fieldName string) (string, error) {
	pos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return "", err
	}
	return btp.tx.GetString(btp.currentBlk, pos)
}

func (btp *BTreePage) getVal(slot int, fieldName string) (query.Constant, error) {
	ft, err := btp.layout.Schema().FieldType(fieldName)
	if err != nil {
		return query.Constant{}, err
	}

	switch ft {
	case record.Integer:
		v, err := btp.getInt(slot, fieldName)
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(v), nil
	case record.String:
		v, err := btp.getString(slot, fieldName)
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(v), nil
	}
	return query.Constant{}, fmt.Errorf("invalid field type %v", ft)
}

func (btp *BTreePage) setInt(slot int, fieldName string, val int) error {
	pos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return err
	}
	return btp.tx.SetInt(btp.currentBlk, pos, val, true)
}

func (btp *BTreePage) setString(slot int, fieldName string, val string) error {
	pos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return err
	}
	return btp.tx.SetString(btp.currentBlk, pos, val, true)
}

func (btp *BTreePage) setVal(slot int, fieldName string, val query.Constant) error {
	ft, err := btp.layout.Schema().FieldType(fieldName)
	if err != nil {
		return err
	}

	switch ft {
	case record.Integer:
		return btp.setInt(slot, fieldName, val.AsInt())
	case record.String:
		return btp.setString(slot, fieldName, val.AsString())
	}
	return fmt.Errorf("invalid field type %v", ft)
}

func (btp *BTreePage) setNumRecords(n int) error {
	return btp.tx.SetInt(btp.currentBlk, numRecordPos, n, true)
}

func (btp *BTreePage) insert(slot int) error {
	numRecords, err := btp.getNumRecords()
	if err != nil {
		return err
	}
	for i := numRecords; i > slot; i-- {
		err = btp.copyRecord(i-1, i)
		if err != nil {
			return err
		}
	}
	return btp.setNumRecords(numRecords + 1)
}

func (btp *BTreePage) copyRecord(from int, to int) error {
	schema := btp.layout.Schema()
	for _, fn := range schema.Fields() {
		v, err := btp.getVal(from, fn)
		if err != nil {
			return err
		}

		err = btp.setVal(to, fn, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (btp *BTreePage) transferRecoreds(slot int, dest *BTreePage) error {
	destSlot := 0
	numRecords, err := btp.getNumRecords()
	if err != nil {
		return err
	}
	for slot < numRecords {
		err := dest.insert(destSlot)
		if err != nil {
			return err
		}

		err = btp.copyRecord(slot, destSlot)
		if err != nil {
			return err
		}
		err = btp.delete(slot)
		if err != nil {
			return err
		}
		newNumRecords, err := btp.getNumRecords()
		if err != nil {
			return err
		}

		destSlot++
		numRecords = newNumRecords
	}
	return err
}

func (btp *BTreePage) fieldPos(slot int, fieldName string) (int, error) {
	offset, err := btp.layout.Offset(fieldName)
	if err != nil {
		return 0, err
	}
	return btp.slotPos(offset), nil
}

func (btp *BTreePage) slotPos(slot int) int {
	slotSize := btp.layout.SlotSize()
	return record.IntByteSize + record.IntByteSize + (slot * slotSize)
}
