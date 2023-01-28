package planner

import (
	"math"

	"github.com/ksrnnb/go-rdb/materialization"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type MaterializePlan struct {
	tx        *tx.Transaction
	srcPlan   Planner
	generator *materialization.NextTableNameGenerator
}

func NewMaterializePlan(tx *tx.Transaction, srcPlan Planner, generator *materialization.NextTableNameGenerator) *MaterializePlan {
	return &MaterializePlan{tx, srcPlan, generator}
}

// Open は TemporaryTable を生成して、 srcPlan の結果を TemporaryTable に保存する
// TemporaryTable の scanner を返す
func (mp *MaterializePlan) Open() (query.Scanner, error) {
	schema := mp.srcPlan.Schema()
	tempTable := materialization.NewTemporaryTable(mp.tx, schema, mp.generator)
	srcScanner, err := mp.srcPlan.Open()
	if err != nil {
		return nil, err
	}
	destScanner, err := tempTable.Open()
	if err != nil {
		return nil, err
	}
	hasNext, err := srcScanner.Next()
	if err != nil {
		return nil, err
	}
	for hasNext {
		if err := destScanner.Insert(); err != nil {
			return nil, err
		}
		for _, fn := range schema.Fields() {
			v, err := srcScanner.GetVal(fn)
			if err != nil {
				return nil, err
			}
			if err := destScanner.SetVal(fn, v); err != nil {
				return nil, err
			}
			newHasNext, err := srcScanner.Next()
			if err != nil {
				return nil, err
			}
			hasNext = newHasNext
		}
	}
	if err := srcScanner.Close(); err != nil {
		return nil, err
	}
	if err := destScanner.BeforeFirst(); err != nil {
		return nil, err
	}
	return destScanner, nil
}

func (mp *MaterializePlan) BlocksAccessed() int {
	layout := record.NewLayout(mp.srcPlan.Schema())
	rpb := float64(mp.tx.BlockSize()) / float64(layout.SlotSize())
	return int(math.Ceil(float64(mp.srcPlan.RecordsOutput()) / rpb))
}

func (mp *MaterializePlan) RecordsOutput() int {
	return mp.srcPlan.RecordsOutput()
}

func (mp *MaterializePlan) DistinctValues(fieldName string) int {
	return mp.srcPlan.DistinctValues(fieldName)
}

func (mp *MaterializePlan) Schema() *record.Schema {
	return mp.srcPlan.Schema()
}
