package parser

import "github.com/ksrnnb/go-rdb/query"

type ModifyData struct {
	tableName string
	fieldName string
	newVal    query.Expression
	pred      *query.Predicate
}

func NewModifyData(tableName string, fieldName string, newVal query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{tableName, fieldName, newVal, pred}
}

func (md *ModifyData) TableName() string {
	return md.tableName
}

func (md *ModifyData) TargetField() string {
	return md.fieldName
}

func (md *ModifyData) NewValue() query.Expression {
	return md.newVal
}

func (md *ModifyData) Predicate() *query.Predicate {
	return md.pred
}
