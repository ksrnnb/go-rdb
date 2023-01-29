package planner

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type MultiBufferProductPlan struct {
	tx        *tx.Transaction
	lhs, rhs  Planner
	schema    *record.Schema
	generator *NextTableNameGenerator
}

func NewMultiBufferProductPlan(tx *tx.Transaction, lhs Planner, rhs Planner, generator *NextTableNameGenerator) (*MultiBufferProductPlan, error) {
	schema := record.NewSchema()
	if err := schema.AddAll(lhs.Schema()); err != nil {
		return nil, err
	}
	if err := schema.AddAll(rhs.Schema()); err != nil {
		return nil, err
	}
	return &MultiBufferProductPlan{tx, lhs, rhs, schema, generator}, nil
}

// Open は left side を MaterializeScan として、 right side を TemporaryTable として MultiBufferProductScan を生成する
func (mp *MultiBufferProductPlan) Open() (query.Scanner, error) {
	leftScan, err := mp.lhs.Open()
	if err != nil {
		return nil, err
	}
	tt, err := mp.copyRecordsFrom(mp.rhs)
	if err != nil {
		return nil, err
	}
	return NewMultiBufferProductScan(mp.tx, leftScan, fmt.Sprintf("%s.tbl", tt.TableName()), tt.Layout())
}

func (mp *MultiBufferProductPlan) BlocksAccessed() int {
	available := mp.tx.AvailableBuffers()
	size := NewMaterializePlan(mp.tx, mp.rhs, mp.generator).BlocksAccessed()
	numChunks := size / available
	return mp.rhs.BlocksAccessed() + mp.lhs.BlocksAccessed()*numChunks
}

func (mp *MultiBufferProductPlan) RecordsOutput() int {
	return mp.lhs.RecordsOutput() * mp.rhs.RecordsOutput()
}

func (mp *MultiBufferProductPlan) DistinctValues(fieldName string) int {
	if mp.lhs.Schema().HasField(fieldName) {
		return mp.lhs.DistinctValues(fieldName)
	}
	return mp.rhs.DistinctValues(fieldName)
}

func (mp *MultiBufferProductPlan) Schema() *record.Schema {
	return mp.schema
}

func (mp *MultiBufferProductPlan) copyRecordsFrom(p Planner) (*TemporaryTable, error) {
	src, err := p.Open()
	if err != nil {
		return nil, err
	}
	schema := p.Schema()
	tt := NewTemporaryTable(mp.tx, schema, mp.generator)
	dest, err := tt.Open()
	if err != nil {
		return nil, err
	}
	hasNext, err := src.Next()
	if err != nil {
		return nil, err
	}
	for hasNext {
		if err := dest.Insert(); err != nil {
			return nil, err
		}
		for _, fn := range schema.Fields() {
			v, err := src.GetVal(fn)
			if err != nil {
				return nil, err
			}
			if err := dest.SetVal(fn, v); err != nil {
				return nil, err
			}
		}
		newHasNext, err := src.Next()
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}
	if err := src.Close(); err != nil {
		return nil, err
	}
	if err := dest.Close(); err != nil {
		return nil, err
	}
	return tt, nil
}
