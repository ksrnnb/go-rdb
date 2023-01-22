package query

import (
	"fmt"
	"math"

	"github.com/ksrnnb/go-rdb/record"
)

type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs, rhs Expression) Term {
	return Term{lhs, rhs}
}

func (t Term) IsSatisfied(s Scanner) (bool, error) {
	lhsVal, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	rhsVal, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	return lhsVal.Equals(rhsVal), nil
}

// AppliesTo は Term の lhs, rhs ともに Expression の条件を満たす場合に true を返す
// Expression が Identifier の場合は、スキーマに定義されているかどうかを確認する
func (t Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t Term) ReductionFactor(p Planner) int {
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhs := p.DistinctValues(t.lhs.AsFieldName())
		rhs := p.DistinctValues(t.rhs.AsFieldName())
		if lhs > rhs {
			return lhs
		}
		return rhs
	}
	if t.lhs.IsFieldName() {
		return p.DistinctValues(t.lhs.AsFieldName())
	}

	if t.rhs.IsFieldName() {
		return p.DistinctValues(t.rhs.AsFieldName())
	}

	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}
	return math.MaxInt
}

func (t Term) EquatesWithConstant(fieldName string) Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	}
	return Constant{}
}

func (t Term) EquatesWithFieldName(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}

func (t Term) String() string {
	return fmt.Sprintf("%s=%s", t.lhs.String(), t.rhs.String())
}
