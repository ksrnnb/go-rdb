package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type IndexSelectPlan struct {
	p   Planner
	ii  *metadata.IndexInfo
	val query.Constant
}

func NewIndexSelectPlanner(p Planner, ii *metadata.IndexInfo, val query.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{p, ii, val}
}

func (isp *IndexSelectPlan) Open() (query.Scanner, error) {
	s, err := isp.p.Open()
	if err != nil {
		return nil, err
	}
	ts, ok := s.(*query.TableScan)
	if !ok {
		return nil, errors.New("scanner must be TableScan")
	}
	idx, err := isp.ii.Open()
	if err != nil {
		return nil, err
	}
	return NewIndexSelectScan(ts, idx, isp.val)
}

func (isp *IndexSelectPlan) BlocksAccessed() int {
	return isp.ii.BlocksAccessed() + isp.RecordsOutput()
}

func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.ii.RecordsOutput()
}

func (isp *IndexSelectPlan) DistinctValues(fieldName string) int {
	return isp.ii.DistinctValues(fieldName)
}

func (isp *IndexSelectPlan) Schema() *record.Schema {
	return isp.p.Schema()
}
