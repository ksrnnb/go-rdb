package record

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/tx"
)

type RecordFlag uint8

const (
	Empty RecordFlag = iota
	Used
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *file.BlockID
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, blk *file.BlockID, layout *Layout) *RecordPage {
	tx.Pin(blk)
	return &RecordPage{tx, blk, layout}
}

// GetInt は指定されたレコードの指定されたフィールドの値を取得する
func (rp *RecordPage) GetInt(slot int, fieldName string) (int, error) {
	ofs, err := rp.layout.Offset(fieldName)
	if err != nil {
		return 0, err
	}

	fieldPos := rp.offset(slot) + ofs
	return rp.tx.GetInt(rp.blk, fieldPos)
}

func (rp *RecordPage) GetString(slot int, fieldName string) (string, error) {
	ofs, err := rp.layout.Offset(fieldName)
	if err != nil {
		return "", err
	}

	fieldPos := rp.offset(slot) + ofs
	return rp.tx.GetString(rp.blk, fieldPos)
}

func (rp *RecordPage) SetInt(slot int, fieldName string, val int) error {
	ofs, err := rp.layout.Offset(fieldName)
	if err != nil {
		return err
	}

	fieldPos := rp.offset(slot) + ofs
	return rp.tx.SetInt(rp.blk, fieldPos, val, true)
}

func (rp *RecordPage) SetString(slot int, fieldName string, val string) error {
	ofs, err := rp.layout.Offset(fieldName)
	if err != nil {
		return err
	}

	fieldPos := rp.offset(slot) + ofs
	return rp.tx.SetString(rp.blk, fieldPos, val, true)
}

// Delete はレコードのフラグを Empty にする
func (rp *RecordPage) Delete(slot int) error {
	return rp.setFlag(slot, Empty)
}

// Format はページ内の全てのレコードスロットをデフォルト値にする
// 全てのフラグを Empty にして、Integer は 0, String は "" にする
func (rp *RecordPage) Format() error {
	slot := 0
	for rp.isValidSlot(slot) {
		rp.tx.SetInt(rp.blk, rp.offset(slot), int(Empty), false)
		schema := rp.layout.Schema()
		for _, fn := range schema.Fields() {
			ofs, err := rp.layout.Offset(fn)
			if err != nil {
				return tx.ErrBufferNotFound
			}

			fieldPos := rp.offset(slot) + ofs
			fieldType, err := schema.fieldType(fn)
			if err != nil {
				return err
			}

			switch fieldType {
			case Integer:
				err := rp.tx.SetInt(rp.blk, fieldPos, 0, false)
				if err != nil {
					return err
				}
			case String:
				err := rp.tx.SetString(rp.blk, fieldPos, "", false)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid field type [%d]", fieldType)
			}
		}
		slot++
	}
	return nil
}

// NextAfter は指定した slot に続く、 Used の slot を返す
func (rp *RecordPage) NextAfter(slot int) (int, error) {
	return rp.searchAfter(slot, Used)
}

// InsertAfter は指定した slot に続く最初の Empty の slot を探す
// 見つかったらフラグを Used に変更し、その slot 番号を返す
func (rp *RecordPage) InsertAfter(slot int) (int, error) {
	newSlot, err := rp.searchAfter(slot, Empty)
	if err != nil {
		return 0, err
	}

	if newSlot >= 0 {
		err := rp.setFlag(newSlot, Used)
		if err != nil {
			return 0, err
		}
	}
	return newSlot, nil
}

func (rp *RecordPage) Block() *file.BlockID {
	return rp.blk
}

func (rp *RecordPage) setFlag(slot int, flag RecordFlag) error {
	return rp.tx.SetInt(rp.blk, rp.offset(slot), int(flag), true)
}

func (rp *RecordPage) searchAfter(slot int, flag RecordFlag) (int, error) {
	slot++
	for rp.isValidSlot(slot) {
		fl, err := rp.tx.GetInt(rp.blk, rp.offset(slot))
		if err != nil {
			return 0, err
		}
		if RecordFlag(fl) == flag {
			return slot, nil
		}
		slot++
	}

	return -1, nil
}

func (rp *RecordPage) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.SlotSize()
}
