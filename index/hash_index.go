package index

import (
	"errors"
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type HashIndex struct {
	tx        *tx.Transaction
	indexName string
	layout    *record.Layout
	searchKey query.Constant
	ts        *query.TableScan
}

const NumBuckets = 100

// インデックスレコードがもつフィールドの定義
const (
	IndexIdField        = "id"
	IndexBlockField     = "block"
	IndexDataRidField   = "data_record_id"
	IndexDataValueField = "data_value"
)

func NewHashIndex(tx *tx.Transaction, indexName string, layout *record.Layout) *HashIndex {
	return &HashIndex{tx: tx, indexName: indexName, layout: layout}
}

func (hi *HashIndex) BeforeFirst(searchKey query.Constant) error {
	err := hi.Close()
	if err != nil {
		return err
	}
	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % NumBuckets
	tableName := fmt.Sprintf("%s%d", hi.indexName, bucket)
	hi.ts, err = query.NewTableScan(hi.tx, tableName, hi.layout)
	return err
}

func (hi *HashIndex) Next() (bool, error) {
	if hi.ts == nil {
		return false, errors.New("HashIndex doesn't have TableScan")
	}

	hasNext, err := hi.ts.Next()
	if err != nil {
		return false, err
	}
	for hasNext {
		v, err := hi.ts.GetVal(IndexDataValueField)
		if err != nil {
			return false, err
		}
		if v.Equals(hi.searchKey) {
			return true, nil
		}
		newHasNext, err := hi.ts.Next()
		if err != nil {
			return false, err
		}
		hasNext = newHasNext
	}
	return false, nil
}

func (hi *HashIndex) GetDataRid() (*record.RecordID, error) {
	if hi.ts == nil {
		return nil, errors.New("HashIndex doesn't have TableScan")
	}

	blknum, err := hi.ts.GetInt(IndexBlockField)
	if err != nil {
		return nil, err
	}
	id, err := hi.ts.GetInt(IndexIdField)
	if err != nil {
		return nil, err
	}
	return record.NewRecordID(blknum, id), nil
}

func (hi *HashIndex) Insert(val query.Constant, rid *record.RecordID) error {
	err := hi.BeforeFirst(val)
	if err != nil {
		return err
	}
	err = hi.ts.Insert()
	if err != nil {
		return err
	}

	err = hi.ts.SetInt(IndexBlockField, rid.BlockNumber())
	if err != nil {
		return err
	}

	err = hi.ts.SetInt(IndexIdField, rid.Slot())
	if err != nil {
		return err
	}

	err = hi.ts.SetVal(IndexDataValueField, val)
	if err != nil {
		return err
	}
	return nil
}

func (hi *HashIndex) Delete(val query.Constant, rid *record.RecordID) error {
	err := hi.BeforeFirst(val)
	if err != nil {
		return err
	}

	hasNext, err := hi.ts.Next()
	if err != nil {
		return err
	}
	for hasNext {
		v, err := hi.ts.GetVal(IndexDataValueField)
		if err != nil {
			return err
		}
		if v.Equals(hi.searchKey) {
			return hi.ts.Delete()
		}
		newHasNext, err := hi.ts.Next()
		if err != nil {
			return err
		}
		hasNext = newHasNext
	}
	return nil
}

func (hi *HashIndex) Close() error {
	if hi.ts == nil {
		return nil
	}
	return hi.ts.Close()
}

func SearchCostHashIndex(numBlocks int, rpb int) int {
	return numBlocks / NumBuckets
}
