package planner

import (
	"errors"
	"math"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type MergeJoinPlan struct {
	p1         Planner
	p2         Planner
	fieldName1 string
	fieldName2 string
	schema     *record.Schema
}

// NewMergeJoinPlan はスキーマを合成して SortPlan を生成する
func NewMergeJoinPlan(tx *tx.Transaction, p1 Planner, p2 Planner, fieldName1 string, fieldName2 string, generator *NextTableNameGenerator) (*MergeJoinPlan, error) {
	schema := record.NewSchema()
	if err := schema.AddAll(p1.Schema()); err != nil {
		return nil, err
	}
	if err := schema.AddAll(p2.Schema()); err != nil {
		return nil, err
	}

	return &MergeJoinPlan{
		p1:         NewSortPlan(tx, []string{fieldName1}, p1, generator),
		p2:         NewSortPlan(tx, []string{fieldName2}, p2, generator),
		fieldName1: fieldName1,
		fieldName2: fieldName2,
		schema:     schema,
	}, nil
}

func (mp *MergeJoinPlan) Open() (query.Scanner, error) {
	s1, err := mp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := mp.p2.Open()
	if err != nil {
		return nil, err
	}
	ss2, ok := s2.(*SortScan)
	if !ok {
		return nil, errors.New("invalid scanner type")
	}
	return NewMergeJoinScan(s1, ss2, mp.fieldName1, mp.fieldName2)
}

func (mp *MergeJoinPlan) BlocksAccessed() int {
	return mp.p1.BlocksAccessed() + mp.p2.BlocksAccessed()
}

func (mp *MergeJoinPlan) RecordsOutput() int {
	maxVal := math.Max(float64(mp.p1.DistinctValues(mp.fieldName1)), float64(mp.p2.DistinctValues(mp.fieldName2)))
	return int(float64(mp.p1.RecordsOutput()*mp.p2.RecordsOutput()) / maxVal)
}

func (mp *MergeJoinPlan) DistinctValues(fieldName string) int {
	if mp.p1.Schema().HasField(fieldName) {
		return mp.p1.DistinctValues(fieldName)
	}
	return mp.p2.DistinctValues(fieldName)
}

func (mp *MergeJoinPlan) Schema() *record.Schema {
	return mp.schema
}
