package parser

import "github.com/ksrnnb/go-rdb/query"

type DeleteData struct {
	tableName string
	pred      *query.Predicate
}

func NewDeleteData(tableName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{tableName, pred}
}

func (dd *DeleteData) TableName() string {
	return dd.tableName
}

func (dd *DeleteData) Predicate() *query.Predicate {
	return dd.pred
}
