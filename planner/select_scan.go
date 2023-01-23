package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type SelectPlan struct {
	p    Planner
	pred *query.Predicate
}

func NewSelectPlan(p Planner, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{p, pred}
}

func (sp *SelectPlan) Open() (query.Scanner, error) {
	s, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(s, sp.pred), nil
}

func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int {
	// predicate によってどれだけ減るかを計算する
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

func (sp *SelectPlan) DistinctValues(fieldName string) int {
	if !sp.pred.EquatesWithConstant(fieldName).IsUnknown() {
		return 1
	}

	fieldName2 := sp.pred.EquatesWithField(fieldName)
	if fieldName2 == "" {
		return sp.p.DistinctValues(fieldName)
	}

	v1 := sp.p.DistinctValues(fieldName)
	v2 := sp.p.DistinctValues(fieldName2)
	if v1 > v2 {
		return v2
	}
	return v1
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}
