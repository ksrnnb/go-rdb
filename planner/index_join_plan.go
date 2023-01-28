package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type IndexJoinPlan struct {
	p1        Planner
	p2        Planner
	ii        *metadata.IndexInfo
	joinField string
	schema    *record.Schema
}

func NewIndexJoinPlan(p1 Planner, p2 Planner, ii *metadata.IndexInfo, joinField string) (*IndexJoinPlan, error) {
	schema := record.NewSchema()
	if err := schema.AddAll(p1.Schema()); err != nil {
		return nil, err
	}
	if err := schema.AddAll(p1.Schema()); err != nil {
		return nil, err
	}
	return &IndexJoinPlan{p1, p2, ii, joinField, schema}, nil
}

func (ijp *IndexJoinPlan) Open() (query.Scanner, error) {
	s1, err := ijp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := ijp.p2.Open()
	if err != nil {
		return nil, err
	}
	ts, ok := s2.(*query.TableScan)
	if !ok {
		return nil, errors.New("scanner must be table scan")
	}
	idx, err := ijp.ii.Open()
	if err != nil {
		return nil, err
	}
	return NewIndexJoinScan(s1, idx, ijp.joinField, ts)
}

func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.p1.BlocksAccessed() + (ijp.p1.RecordsOutput() * ijp.ii.BlocksAccessed()) + ijp.RecordsOutput()
}

func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.p1.RecordsOutput() * ijp.ii.RecordsOutput()
}

func (ijp *IndexJoinPlan) DistinctValues(fieldName string) int {
	if ijp.p1.Schema().HasField(fieldName) {
		return ijp.p1.DistinctValues(fieldName)
	}
	return ijp.p2.DistinctValues(fieldName)
}

func (ijp *IndexJoinPlan) Schema() *record.Schema {
	return ijp.schema
}
