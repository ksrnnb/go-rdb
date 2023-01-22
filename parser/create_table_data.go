package parser

import "github.com/ksrnnb/go-rdb/record"

type CreateTableData struct {
	tableName string
	schema    *record.Schema
}

func NewCreateTableData(tableName string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{tableName, schema}
}

func (c *CreateTableData) TableName() string {
	return c.tableName
}

func (c *CreateTableData) Schema() *record.Schema {
	return c.schema
}
