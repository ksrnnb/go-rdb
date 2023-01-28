package planner

import (
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/query"
)

type IndexJoinScan struct {
	lhs       query.Scanner
	rhs       *query.TableScan
	idx       index.Index
	joinField string
}

func NewIndexJoinScan(lhs query.Scanner, idx index.Index, joinField string, rhs *query.TableScan) (*IndexJoinScan, error) {
	ijs := &IndexJoinScan{lhs, rhs, idx, joinField}
	if err := ijs.BeforeFirst(); err != nil {
		return nil, err
	}
	return ijs, nil
}

func (ijs *IndexJoinScan) BeforeFirst() error {
	if err := ijs.lhs.BeforeFirst(); err != nil {
		return err
	}
	if _, err := ijs.lhs.Next(); err != nil {
		return err
	}
	return ijs.resetIndex()
}

func (ijs *IndexJoinScan) Next() (bool, error) {
	for {
		hasNext, err := ijs.idx.Next()
		if err != nil {
			return false, err
		}
		if hasNext {
			rid, err := ijs.idx.GetDataRid()
			if err != nil {
				return false, err
			}
			if err := ijs.rhs.MoveToRid(rid); err != nil {
				return false, err
			}
			return true, nil
		}

		hasNext, err = ijs.lhs.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			return false, nil
		}
		err = ijs.resetIndex()
		if err != nil {
			return false, err
		}
	}
}

func (ijs *IndexJoinScan) GetInt(fieldName string) (int, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetInt(fieldName)
	}
	return ijs.lhs.GetInt(fieldName)
}

func (ijs *IndexJoinScan) GetString(fieldName string) (string, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetString(fieldName)
	}
	return ijs.lhs.GetString(fieldName)
}

func (ijs *IndexJoinScan) GetVal(fieldName string) (query.Constant, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetVal(fieldName)
	}
	return ijs.lhs.GetVal(fieldName)
}

func (ijs *IndexJoinScan) HasField(fieldName string) bool {
	return ijs.rhs.HasField(fieldName) || ijs.lhs.HasField(fieldName)
}

func (ijs *IndexJoinScan) Close() error {
	if err := ijs.lhs.Close(); err != nil {
		return err
	}
	if err := ijs.idx.Close(); err != nil {
		return err
	}
	return ijs.rhs.Close()
}

func (ijs *IndexJoinScan) resetIndex() error {
	searchKey, err := ijs.lhs.GetVal(ijs.joinField)
	if err != nil {
		return err
	}
	return ijs.idx.BeforeFirst(searchKey)
}
