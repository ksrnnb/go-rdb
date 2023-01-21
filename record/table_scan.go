package record

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/tx"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	fileName    string
	currentSlot int
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: fmt.Sprintf("%s.tbl", tableName),
	}

	size, err := tx.Size(ts.fileName)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		err = ts.moveToNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		err = ts.BeforeFirst()
		if err != nil {
			return nil, err
		}
	}
	return ts, nil
}

func (ts *TableScan) Close() error {
	if ts.rp == nil {
		return nil
	}
	return ts.tx.Unpin(ts.rp.Block())
}

// 現在のレコードをファイルの最初のレコードの前に位置付ける
func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

// 現在のレコードをファイルの次のレコードに位置付ける
// 現在のブロックにそれ以上レコードがない場合は、別のレコードが見つかるまで
// ファイル内の後続のブロックを読み込む
func (ts *TableScan) Next() (bool, error) {
	nextSlot, err := ts.rp.NextAfter(ts.currentSlot)
	if err != nil {
		return false, err
	}
	ts.currentSlot = nextSlot

	for ts.currentSlot < 0 {
		atLast, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLast {
			return false, nil
		}

		err = ts.moveToBlock(ts.rp.Block().Number() + 1)
		if err != nil {
			return false, err
		}

		nextSlot, err := ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
			return false, err
		}
		ts.currentSlot = nextSlot
	}
	return true, nil
}

func (ts *TableScan) GetInt(fieldName string) (int, error) {
	return ts.rp.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fieldName)
}

// TODO: implement GetVal
func (ts *TableScan) GetVal(fieldName string) {

}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) SetInt(fieldName string, val int) error {
	return ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fieldName, val)
}

// TODO: implement SetVal
func (ts *TableScan) SetVal(fieldName string, val interface{}) error {
	return nil
}

// Insert は現在のレコードのブロックから開始して、空きを探す
// 空きがあったら使用済みに変換して、現在のレコードに位置付ける
// SetInt などを実行する前に必ず Insert を実行して currentSlot を更新する必要がある
func (ts *TableScan) Insert() error {
	nextSlot, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return err
	}
	ts.currentSlot = nextSlot

	for ts.currentSlot < 0 {
		atLast, err := ts.atLastBlock()
		if err != nil {
			return err
		}
		if atLast {
			err = ts.moveToNewBlock()
			if err != nil {
				return err
			}
		} else {
			err = ts.moveToBlock(ts.rp.Block().Number() + 1)
			if err != nil {
				return err
			}
		}

		nextSlot, err := ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return err
		}
		ts.currentSlot = nextSlot
	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRid(rid *RecordID) error {
	err := ts.Close()
	if err != nil {
		return err
	}
	blk := file.NewBlockID(ts.fileName, rid.BlockNumber())
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentSlot = rid.Slot()
	return nil
}

func (ts *TableScan) GetRid() *RecordID {
	return NewRecordID(ts.rp.Block().Number(), ts.currentSlot)
}

func (ts *TableScan) moveToBlock(blknum int) error {
	err := ts.Close()
	if err != nil {
		return err
	}

	blk := file.NewBlockID(ts.fileName, blknum)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	if err != nil {
		return err
	}

	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) moveToNewBlock() error {
	err := ts.Close()
	if err != nil {
		return err
	}

	blk, err := ts.tx.Append(ts.fileName)
	if err != nil {
		return err
	}

	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	err = ts.rp.Format()
	if err != nil {
		return err
	}

	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	size, err := ts.tx.Size(ts.fileName)
	if err != nil {
		return false, err
	}
	return ts.rp.Block().Number() == size-1, nil
}
