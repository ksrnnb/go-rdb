package materialization

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

// TemporaryTable は TableManager の CreateTable メソッドでは生成されない
type TemporaryTable struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
}

func NewTemporaryTable(tx *tx.Transaction, schema *record.Schema, generator *NextTableNameGenerator) (*TemporaryTable, error) {
	layout, err := record.NewLayout(schema)
	if err != nil {
		return nil, err
	}
	return &TemporaryTable{
		tx:        tx,
		tableName: generator.NextTableName(),
		layout:    layout,
	}, nil
}

func (tt *TemporaryTable) Open() (query.UpdateScanner, error) {
	return query.NewTableScan(tt.tx, tt.tableName, tt.layout)
}

func (tt *TemporaryTable) TableName() string {
	return tt.tableName
}

func (tt *TemporaryTable) Layout() *record.Layout {
	return tt.layout
}
