package planner

import (
	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/tx"
)

type BasicQueryPlanner struct {
	mdm *metadata.MetadataManager
}

func NewBasicQueryPlanner(mdm *metadata.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{mdm}
}

func (bqp *BasicQueryPlanner) CreatePlan(qd *parser.QueryData, tx *tx.Transaction) (Planner, error) {
	// Step1: Create a plan for each mentioned table or view
	plans := make([]Planner, 0)
	for _, tableName := range qd.Tables() {
		viewDefinition, err := bqp.mdm.GetViewDefinition(tableName, tx)
		if err != nil {
			return nil, err
		}
		if viewDefinition == "" {
			tp, err := NewTablePlan(tx, tableName, bqp.mdm)
			if err != nil {
				return nil, err
			}
			plans = append(plans, tp)
		} else {
			parser, err := parser.NewParser(viewDefinition)
			if err != nil {
				return nil, err
			}
			viewData, err := parser.Query()
			if err != nil {
				return nil, err
			}
			plan, err := bqp.CreatePlan(viewData, tx)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		}
	}

	// Step2: Create the product of all table plans
	p := plans[0]
	var err error
	for _, nextPlan := range plans[1:] {
		p, err = NewProductPlan(p, nextPlan)
		if err != nil {
			return nil, err
		}
	}

	// Step3: Add a selection plan for the predicate
	p = NewSelectPlan(p, qd.Predicate())

	// Step4: Project on the field names
	return NewProjectPlan(p, qd.Fields())
}
