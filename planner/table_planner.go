package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type TablePlanner struct {
	plan      *TablePlan
	pred      *query.Predicate
	schema    *record.Schema
	indexes   map[string]*metadata.IndexInfo
	tx        *tx.Transaction
	generator *NextTableNameGenerator
}

func NewTablePlanner(tableName string, pred *query.Predicate, tx *tx.Transaction, mdm *metadata.MetadataManager, generator *NextTableNameGenerator) (*TablePlanner, error) {
	plan, err := NewTablePlan(tx, tableName, mdm)
	if err != nil {
		return nil, err
	}
	schema := plan.Schema()
	indexes, err := mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return nil, err
	}
	return &TablePlanner{plan, pred, schema, indexes, tx, generator}, nil
}

func (tp *TablePlanner) MakeSelectPlan() (Planner, error) {
	p := tp.makeIndexSelect()
	if p == nil {
		p = tp.plan
	}
	return tp.addSelectPredicate(p)
}

func (tp *TablePlanner) MakeJoinPlan(currentPlan Planner) (Planner, error) {
	currentSchema := currentPlan.Schema()
	joinPred, err := tp.pred.JoinSubPredicate(tp.schema, currentSchema)
	if err != nil {
		if !errors.Is(err, query.ErrNoSubPredicate) {
			return nil, err
		}
	}
	if joinPred == nil {
		return nil, nil
	}
	p, err := tp.makeIndexJoin(currentPlan, currentSchema)
	if err != nil {
		return nil, err
	}
	if p == nil {
		p, err = tp.makeProductJoin(currentPlan, currentSchema)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (tp *TablePlanner) MakeProductPlan(currentPlan Planner) (Planner, error) {
	p, err := tp.addSelectPredicate(tp.plan)
	if err != nil {
		return nil, err
	}
	return NewMultiBufferProductPlan(tp.tx, currentPlan, p, tp.generator)
}

func (tp *TablePlanner) makeIndexSelect() Planner {
	for fn, ii := range tp.indexes {
		val := tp.pred.EquatesWithConstant(fn)
		if !val.IsUnknown() {
			return NewIndexSelectPlan(tp.plan, ii, val)
		}
	}
	return nil
}

func (tp *TablePlanner) makeIndexJoin(currentPlan Planner, currentSchema *record.Schema) (Planner, error) {
	for fn, ii := range tp.indexes {
		outerField := tp.pred.EquatesWithField(fn)
		if outerField != "" && currentSchema.HasField(outerField) {
			ip, err := NewIndexJoinPlan(currentPlan, tp.plan, ii, outerField)
			if err != nil {
				return nil, err
			}
			p, err := tp.addSelectPredicate(ip)
			if err != nil {
				return nil, err
			}
			return tp.addJoinPredicate(p, currentSchema)
		}
	}
	return nil, nil
}

func (tp *TablePlanner) makeProductJoin(currentPlan Planner, currentSchema *record.Schema) (Planner, error) {
	p, err := tp.MakeProductPlan(currentPlan)
	if err != nil {
		return nil, err
	}
	return tp.addJoinPredicate(p, currentSchema)
}

func (tp *TablePlanner) addSelectPredicate(p Planner) (Planner, error) {
	selectPred, err := tp.pred.SelectSubPredicate(tp.schema)
	if err != nil {
		if !errors.Is(err, query.ErrNoSubPredicate) {
			return nil, err
		}
	}
	if selectPred != nil {
		return NewSelectPlan(p, selectPred), nil
	}
	return p, nil
}

func (tp *TablePlanner) addJoinPredicate(p Planner, currentSchema *record.Schema) (Planner, error) {
	joinPred, err := tp.pred.JoinSubPredicate(currentSchema, tp.schema)
	if err != nil {
		if !errors.Is(err, query.ErrNoSubPredicate) {
			return nil, err
		}
	}
	if joinPred != nil {
		return NewSelectPlan(p, joinPred), nil
	}
	return p, nil
}
