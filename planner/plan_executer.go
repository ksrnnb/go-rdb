package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/tx"
)

type PlanExecuter struct {
	qp QueryPlanner
	up UpdatePlanner
}

func NewPlanExecuter(qp QueryPlanner, up UpdatePlanner) *PlanExecuter {
	return &PlanExecuter{qp, up}
}

func (pe *PlanExecuter) CreateQueryPlan(query string, tx *tx.Transaction) (Planner, error) {
	parser, err := parser.NewParser(query)
	if err != nil {
		return nil, err
	}
	qd, err := parser.Query()
	if err != nil {
		return nil, err
	}
	return pe.qp.CreatePlan(qd, tx)
}

func (pe *PlanExecuter) ExecuteUpdate(query string, tx *tx.Transaction) (int, error) {
	p, err := parser.NewParser(query)
	if err != nil {
		return 0, err
	}
	cmd, err := p.UpdateCommand()
	if err != nil {
		return 0, err
	}
	switch v := cmd.(type) {
	case *parser.InsertData:
		return pe.up.ExecuteInsert(v, tx)
	case *parser.DeleteData:
		return pe.up.ExecuteDelete(v, tx)
	case *parser.ModifyData:
		return pe.up.ExecuteModify(v, tx)
	case *parser.CreateTableData:
		return pe.up.ExecuteCreateTable(v, tx)
	case *parser.CreateViewData:
		return pe.up.ExecuteCreateView(v, tx)
	case *parser.CreateIndexData:
		return pe.up.ExecuteCreateIndex(v, tx)
	}
	return 0, errors.New("invalid update command")
}
