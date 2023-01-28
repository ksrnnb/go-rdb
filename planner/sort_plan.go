package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type SortPlan struct {
	tx         *tx.Transaction
	p          Planner
	schema     *record.Schema
	comparator RecordComparator
	generator  *NextTableNameGenerator
}

func NewSortPlan(tx *tx.Transaction, sortFields []string, p Planner, generator *NextTableNameGenerator) *SortPlan {
	return &SortPlan{
		tx:         tx,
		p:          p,
		schema:     p.Schema(),
		comparator: NewRecordComparator(sortFields),
	}
}

// Open はマージソートを実行する
func (sp *SortPlan) Open() (query.Scanner, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}
	if err := src.Close(); err != nil {
		return nil, err
	}
	for len(runs) > 2 {
		newRuns, err := sp.doAMergeIteration(runs)
		if err != nil {
			return nil, err
		}
		runs = newRuns
	}
	return NewSortScan(runs, sp.comparator)
}

func (sp *SortPlan) BlocksAccessed() int {
	mp := NewMaterializePlan(sp.tx, sp.p, sp.generator)
	return mp.BlocksAccessed()
}

func (sp *SortPlan) RecordsOutput() int {
	return sp.p.RecordsOutput()
}

func (sp *SortPlan) DistinctValues(fieldName string) int {
	return sp.p.DistinctValues(fieldName)
}

func (sp *SortPlan) Schema() *record.Schema {
	return sp.schema
}

// splitIntoRuns はマージソートの split phase
func (sp *SortPlan) splitIntoRuns(src query.Scanner) ([]*TemporaryTable, error) {
	temps := make([]*TemporaryTable, 0)
	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}
	hasNext, err := src.Next()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return temps, nil
	}
	currentTemp := NewTemporaryTable(sp.tx, sp.schema, sp.generator)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}
	hasNext, err = sp.copy(src, currentScan)
	if err != nil {
		return nil, err
	}
	for hasNext {
		cmp, err := sp.comparator.Compare(src, currentScan)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			// start a new run
			if err := currentScan.Close(); err != nil {
				return nil, err
			}
			currentTemp = NewTemporaryTable(sp.tx, sp.schema, sp.generator)
			temps = append(temps, currentTemp)
			us, err := currentTemp.Open()
			if err != nil {
				return nil, err
			}
			currentScan = us
		}
		newHasNext, err := sp.copy(src, currentScan)
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}
	if err := currentScan.Close(); err != nil {
		return nil, err
	}
	return temps, nil
}

// doAMergeIteration はマージソートの merge phase
func (sp *SortPlan) doAMergeIteration(runs []*TemporaryTable) ([]*TemporaryTable, error) {
	result := make([]*TemporaryTable, 0)
	var p1, p2 *TemporaryTable
	for len(runs) > 1 {
		p1, runs = runs[0], runs[1:]
		p2, runs = runs[0], runs[1:]
		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil, err
		}
		result = append(result, merged)
	}
	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result, nil
}

// mergeTwoRuns は2つの temporary table を値の小さい順に 1つの temporary table にマージする
func (sp *SortPlan) mergeTwoRuns(p1 *TemporaryTable, p2 *TemporaryTable) (*TemporaryTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}
	result := NewTemporaryTable(sp.tx, sp.schema, sp.generator)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}
	hasMore1, err := src1.Next()
	if err != nil {
		return nil, err
	}
	hasMore2, err := src2.Next()
	if err != nil {
		return nil, err
	}
	for hasMore1 && hasMore2 {
		cmp, err := sp.comparator.Compare(src1, src2)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			newHasMore1, err := sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
			hasMore1 = newHasMore1
		} else {
			newHasMore2, err := sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
			hasMore2 = newHasMore2
		}
	}
	if hasMore1 {
		for hasMore1 {
			newHasMore1, err := sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
			hasMore1 = newHasMore1
		}
	} else {
		for hasMore2 {
			newHasMore2, err := sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
			hasMore2 = newHasMore2
		}
	}
	if err := src1.Close(); err != nil {
		return nil, err
	}
	if err := src2.Close(); err != nil {
		return nil, err
	}
	if err := dest.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func (sp *SortPlan) copy(src query.Scanner, dest query.UpdateScanner) (bool, error) {
	if err := dest.Insert(); err != nil {
		return false, err
	}
	for _, fn := range sp.schema.Fields() {
		v, err := src.GetVal(fn)
		if err != nil {
			return false, err
		}
		if err := dest.SetVal(fn, v); err != nil {
			return false, err
		}
	}
	return src.Next()
}
