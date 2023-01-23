package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type ProjectScan struct {
	p      Planner
	schema *record.Schema
}

func NewProjectScan(p Planner, fieldNames []string) (*ProjectScan, error) {
	ps := &ProjectScan{p: p, schema: record.NewSchema()}

	for _, fn := range fieldNames {
		err := ps.schema.Add(fn, ps.p.Schema())
		if err != nil {
			return nil, err
		}
	}
	return ps, nil
}

func (ps *ProjectScan) Open() (query.Scanner, error) {
	s, err := ps.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, ps.schema.Fields()), nil
}

func (ps *ProjectScan) BlocksAccessed() int {
	return ps.p.BlocksAccessed()
}

func (ps *ProjectScan) RecordsOutput() int {
	return ps.p.RecordsOutput()
}

func (ps *ProjectScan) DistinctValues(fieldName string) int {
	return ps.p.DistinctValues(fieldName)
}

func (ps *ProjectScan) Schema() *record.Schema {
	return ps.schema
}
