package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type ProjectPlan struct {
	p      Planner
	schema *record.Schema
}

func NewProjectPlan(p Planner, fieldNames []string) (*ProjectPlan, error) {
	ps := &ProjectPlan{p: p, schema: record.NewSchema()}

	for _, fn := range fieldNames {
		err := ps.schema.Add(fn, ps.p.Schema())
		if err != nil {
			return nil, err
		}
	}
	return ps, nil
}

func (ps *ProjectPlan) Open() (query.Scanner, error) {
	s, err := ps.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, ps.schema.Fields()), nil
}

func (ps *ProjectPlan) BlocksAccessed() int {
	return ps.p.BlocksAccessed()
}

func (ps *ProjectPlan) RecordsOutput() int {
	return ps.p.RecordsOutput()
}

func (ps *ProjectPlan) DistinctValues(fieldName string) int {
	return ps.p.DistinctValues(fieldName)
}

func (ps *ProjectPlan) Schema() *record.Schema {
	return ps.schema
}
