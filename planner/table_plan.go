package planner

import (
	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type TablePlan struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
	si        metadata.StatInfo
}

func NewTablePlan(tx *tx.Transaction, tableName string, md *metadata.MetadataManager) (*TablePlan, error) {
	layout, err := md.Layout(tableName, tx)
	if err != nil {
		return nil, err
	}
	si, err := md.GetStatInfo(tableName, layout, tx)
	if err != nil {
		return nil, err
	}
	return &TablePlan{tx, tableName, layout, si}, nil
}

func (tp *TablePlan) Open() (query.Scanner, error) {
	return query.NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.si.DistinctValues(fieldName)
}

func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}
