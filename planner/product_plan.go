package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type ProductScan struct {
	p1     Planner
	p2     Planner
	schema *record.Schema
}

func NewProductScan(p1 Planner, p2 Planner) (*ProductScan, error) {
	ps := &ProductScan{p1: p1, p2: p2, schema: record.NewSchema()}

	err := ps.schema.AddAll(p1.Schema())
	if err != nil {
		return nil, err
	}
	ps.schema.AddAll(p2.Schema())
	if err != nil {
		return nil, err
	}

	return ps, nil
}

func (ps *ProductScan) Open() (query.Scanner, error) {
	s1, err := ps.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := ps.p1.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2)
}

func (ps *ProductScan) BlocksAccessed() int {
	return ps.p1.BlocksAccessed() + (ps.p1.RecordsOutput() * ps.p2.BlocksAccessed())
}

func (ps *ProductScan) RecordsOutput() int {
	return ps.p1.RecordsOutput() * ps.p2.RecordsOutput()
}

func (ps *ProductScan) DistinctValues(fieldName string) int {
	if ps.p1.Schema().HasField(fieldName) {
		return ps.p1.DistinctValues(fieldName)
	}
	return ps.p2.DistinctValues(fieldName)
}

func (ps *ProductScan) Schema() *record.Schema {
	return ps.schema
}
