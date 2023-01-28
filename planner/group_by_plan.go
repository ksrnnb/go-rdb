package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type GroupPlan struct {
	p           Planner
	groupFields []string
	aggFns      []AggregationFunction
	schema      *record.Schema
}

func NewGroupPlan(tx *tx.Transaction, p Planner, groupFields []string, aggFns []AggregationFunction, generator *NextTableNameGenerator) (*GroupPlan, error) {
	schema := record.NewSchema()
	for _, fn := range groupFields {
		if err := schema.Add(fn, p.Schema()); err != nil {
			return nil, err
		}
	}
	for _, aggFn := range aggFns {
		schema.AddIntField(aggFn.FieldName())
	}
	return &GroupPlan{p, groupFields, aggFns, schema}, nil
}

func (gp *GroupPlan) Open() (query.Scanner, error) {
	s, err := gp.p.Open()
	if err != nil {
		return nil, err
	}
	return NewGroupByScan(s, gp.groupFields, gp.aggFns)
}

func (gp *GroupPlan) BlocksAccessed() int {
	return gp.p.BlocksAccessed()
}

func (gp *GroupPlan) RecordsOutput() int {
	numGroups := 1
	for _, fn := range gp.groupFields {
		numGroups *= gp.p.DistinctValues(fn)
	}
	return numGroups
}

func (gp *GroupPlan) DistinctValues(fieldName string) int {
	if gp.p.Schema().HasField(fieldName) {
		return gp.p.DistinctValues(fieldName)
	}
	return gp.RecordsOutput()
}

func (gp *GroupPlan) Schema() *record.Schema {
	return gp.schema
}
