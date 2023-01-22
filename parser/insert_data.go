package parser

import "github.com/ksrnnb/go-rdb/query"

type InsertData struct {
	tableName string
	fields    []string
	values    []query.Constant
}

func NewInsertData(tableName string, fileds []string, values []query.Constant) *InsertData {
	return &InsertData{tableName, fileds, values}
}

func (id *InsertData) TableName() string {
	return id.tableName
}

func (id *InsertData) Fields() []string {
	return id.fields
}

func (id *InsertData) Values() []query.Constant {
	return id.values
}
