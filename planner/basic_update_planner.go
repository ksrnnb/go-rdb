package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/tx"
)

type BasicUpdatePlanner struct {
	mdm *metadata.MetadataManager
}

func NewBasicUpdatePlanner(mdm *metadata.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm}
}

func (bup *BasicUpdatePlanner) ExecuteDelete(dd *parser.DeleteData, tx *tx.Transaction) (int, error) {
	p, err := NewTablePlan(tx, dd.TableName(), bup.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(p, dd.Predicate())
	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScanner)
	if !ok {
		return 0, errors.New("scanner should be update scanner")
	}
	hasNext, err := us.Next()
	if err != nil {
		return 0, err
	}
	count := 0
	for hasNext {
		err := us.Delete()
		if err != nil {
			return 0, err
		}
		count++
		newHasNext, err := us.Next()
		if err != nil {
			return 0, err
		}
		hasNext = newHasNext
	}
	err = us.Close()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (bup *BasicUpdatePlanner) ExecuteModify(md *parser.ModifyData, tx *tx.Transaction) (int, error) {
	p, err := NewTablePlan(tx, md.TableName(), bup.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(p, md.Predicate())
	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScanner)
	if !ok {
		return 0, errors.New("scanner should be update scanner")
	}
	hasNext, err := us.Next()
	if err != nil {
		return 0, err
	}
	count := 0
	for hasNext {
		val, err := md.NewValue().Evaluate(us)
		if err != nil {
			return 0, err
		}
		err = us.SetVal(md.TargetField(), val)
		if err != nil {
			return 0, err
		}
		count++
		newHasNext, err := us.Next()
		if err != nil {
			return 0, err
		}
		hasNext = newHasNext
	}
	err = us.Close()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (bup *BasicUpdatePlanner) ExecuteInsert(id *parser.InsertData, tx *tx.Transaction) (int, error) {
	tp, err := NewTablePlan(tx, id.TableName(), bup.mdm)
	if err != nil {
		return 0, err
	}
	s, err := tp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScanner)
	if !ok {
		return 0, errors.New("scanner should be update scanner")
	}
	vals := id.Values()
	for i, fn := range id.Fields() {
		val := vals[i]
		err = us.SetVal(fn, val)
		if err != nil {
			return 0, err
		}
	}
	err = us.Close()
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (bup *BasicUpdatePlanner) ExecuteCreateTable(ctd *parser.CreateTableData, tx *tx.Transaction) (int, error) {
	return 0, bup.mdm.CreateTable(ctd.TableName(), ctd.Schema(), tx)
}

func (bup *BasicUpdatePlanner) ExecuteCreateView(cvd *parser.CreateViewData, tx *tx.Transaction) (int, error) {
	return 0, bup.mdm.CreateView(cvd.ViewName(), cvd.ViewDefinition(), tx)
}

func (bup *BasicUpdatePlanner) ExecuteCreateIndex(cid *parser.CreateIndexData, tx *tx.Transaction) (int, error) {
	return 0, bup.mdm.CreateIndex(cid.IndexName(), cid.TableName(), cid.FieldName(), tx)
}
