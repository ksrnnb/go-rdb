package planner

import (
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/query"
)

type IndexSelectScan struct {
	ts  *query.TableScan
	idx index.Index
	val query.Constant
}

func NewIndexSelectScan(ts *query.TableScan, idx index.Index, val query.Constant) (*IndexSelectScan, error) {
	iss := &IndexSelectScan{ts, idx, val}
	if err := iss.BeforeFirst(); err != nil {
		return nil, err
	}
	return iss, nil
}

func (iss *IndexSelectScan) BeforeFirst() error {
	return iss.idx.BeforeFirst(iss.val)
}

func (iss *IndexSelectScan) Next() (bool, error) {
	hasNext, err := iss.idx.Next()
	if err != nil {
		return false, err
	}
	if hasNext {
		rid, err := iss.idx.GetDataRid()
		if err != nil {
			return false, err
		}
		if err := iss.ts.MoveToRid(rid); err != nil {
			return false, err
		}
	}
	return hasNext, nil
}

func (iss *IndexSelectScan) GetInt(fieldName string) (int, error) {
	return iss.ts.GetInt(fieldName)
}

func (iss *IndexSelectScan) GetString(fieldName string) (string, error) {
	return iss.ts.GetString(fieldName)
}

func (iss *IndexSelectScan) GetVal(fieldName string) (query.Constant, error) {
	return iss.ts.GetVal(fieldName)
}

func (iss *IndexSelectScan) HasField(fieldName string) bool {
	return iss.ts.HasField(fieldName)
}

func (iss *IndexSelectScan) Close() error {
	if err := iss.idx.Close(); err != nil {
		return err
	}
	return iss.ts.Close()
}
