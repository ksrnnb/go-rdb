package query

import "github.com/ksrnnb/go-rdb/record"

type ExpressionType uint8

const (
	FieldNameExpression = iota + 1
	ConstantExpression
)

type Expression struct {
	val       Constant
	fieldName string
	etype     ExpressionType
}

func NewExpressionFromConstant(val Constant) Expression {
	return Expression{val: val, etype: ConstantExpression}
}

func NewExpressionFromFieldName(fieldName string) Expression {
	return Expression{fieldName: fieldName, etype: FieldNameExpression}
}

func (e Expression) IsConstant() bool {
	return e.etype == ConstantExpression
}

func (e Expression) IsFieldName() bool {
	return e.etype == FieldNameExpression
}

func (e Expression) AsConstant() Constant {
	return e.val
}

func (e Expression) AsFieldName() string {
	return e.fieldName
}

func (e Expression) Evaluate(s Scan) (Constant, error) {
	if e.IsConstant() {
		return e.val, nil
	}
	return s.GetVal(e.fieldName)
}

func (e Expression) AppliesTo(schema *record.Schema) bool {
	if e.IsConstant() {
		return true
	}
	return schema.HasField(e.fieldName)
}

func (e Expression) String() string {
	if e.IsConstant() {
		return e.val.String()
	}
	return e.fieldName
}
