package planner

import (
	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/tx"
)

type HeuristicQueryPlanner struct {
	tps       []*TablePlanner
	mdm       *metadata.MetadataManager
	generator *NextTableNameGenerator
}

func NewHeuristicQueryPlanner(mdm *metadata.MetadataManager, generator *NextTableNameGenerator) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{tps: make([]*TablePlanner, 0), mdm: mdm, generator: generator}
}

func (hp *HeuristicQueryPlanner) CreatePlan(data *parser.QueryData, tx *tx.Transaction) (Planner, error) {
	// step1: Create a TablePlanner for each mentioned table
	for _, tn := range data.Tables() {
		tp, err := NewTablePlanner(tn, data.Predicate(), tx, hp.mdm, hp.generator)
		if err != nil {
			return nil, err
		}
		hp.tps = append(hp.tps, tp)
	}

	// step2: Choose the lowest-size plan to begin the join order
	currentPlan, err := hp.getLowestSelectPlan()
	if err != nil {
		return nil, err
	}
	for len(hp.tps) != 0 {
		p, err := hp.getLowestJoinPlan(currentPlan)
		if err != nil {
			return nil, err
		}
		if p != nil {
			currentPlan = p
		} else {
			newP, err := hp.getLowestProductPlan(currentPlan)
			if err != nil {
				return nil, err
			}
			currentPlan = newP
		}
	}
	return NewProjectPlan(currentPlan, data.Fields())
}

func (hp *HeuristicQueryPlanner) getLowestSelectPlan() (Planner, error) {
	var bestTPIndex int
	var bestPlan Planner
	for i, tp := range hp.tps {
		plan, err := tp.MakeSelectPlan()
		if err != nil {
			return nil, err
		}
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTPIndex = i
			bestPlan = plan
		}
	}
	// delete best table planner
	hp.tps = append(hp.tps[:bestTPIndex], hp.tps[bestTPIndex+1:]...)
	return bestPlan, nil
}

func (hp *HeuristicQueryPlanner) getLowestJoinPlan(p Planner) (Planner, error) {
	var bestTPIndex int
	var bestPlan Planner
	for i, tp := range hp.tps {
		plan, err := tp.MakeJoinPlan(p)
		if err != nil {
			return nil, err
		}
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTPIndex = i
			bestPlan = plan
		}
	}
	// delete best table planner
	if bestPlan != nil {
		hp.tps = append(hp.tps[:bestTPIndex], hp.tps[bestTPIndex+1:]...)
	}
	return bestPlan, nil
}

func (hp *HeuristicQueryPlanner) getLowestProductPlan(p Planner) (Planner, error) {
	var bestTPIndex int
	var bestPlan Planner
	for i, tp := range hp.tps {
		plan, err := tp.MakeProductPlan(p)
		if err != nil {
			return nil, err
		}
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTPIndex = i
			bestPlan = plan
		}
	}
	// delete best table planner
	hp.tps = append(hp.tps[:bestTPIndex], hp.tps[bestTPIndex+1:]...)
	return bestPlan, nil
}
