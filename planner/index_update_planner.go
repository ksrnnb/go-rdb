package planner

import (
	"errors"

	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/tx"
)

type IndexUpdatePlanner struct {
	mdm *metadata.MetadataManager
}

func NewIndexUpdatePlanner(mdm *metadata.MetadataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{mdm}
}

func (iup *IndexUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) (int, error) {
	tn := data.TableName()
	p, err := NewTablePlan(tx, tn, iup.mdm)
	if err != nil {
		return 0, err
	}

	s, err := p.Open()
	if err != nil {
		return 0, err
	}
	us := s.(query.UpdateScanner)
	if err := us.Insert(); err != nil {
		return 0, err
	}
	rid, err := us.GetRid()
	if err != nil {
		return 0, err
	}

	indexes, err := iup.mdm.GetIndexInfo(tn, tx)
	if err != nil {
		return 0, err
	}
	values := data.Values()
	for i, fn := range data.Fields() {
		val := values[i]

		if err := us.SetVal(fn, val); err != nil {
			return 0, err
		}
		ii := indexes[fn]
		if ii == nil {
			continue
		}

		idx, err := ii.Open()
		if err != nil {
			return 0, err
		}
		if err := idx.Insert(val, rid); err != nil {
			return 0, err
		}
		if err := idx.Close(); err != nil {
			return 0, err
		}
	}
	if err := us.Close(); err != nil {
		return 0, err
	}
	return 1, nil
}

func (iup *IndexUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) (int, error) {
	tn := data.TableName()
	p, err := NewTablePlan(tx, tn, iup.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(p, data.Predicate())
	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScanner)
	if !ok {
		return 0, errors.New("invalid Scanner")
	}
	indexes, err := iup.mdm.GetIndexInfo(tn, tx)
	if err != nil {
		return 0, err
	}
	hasNext, err := us.Next()
	if err != nil {
		return 0, err
	}
	count := 0
	for hasNext {
		rid, err := us.GetRid()
		if err != nil {
			return 0, err
		}
		for fn, ii := range indexes {
			val, err := us.GetVal(fn)
			if err != nil {
				return 0, err
			}
			idx, err := ii.Open()
			if err != nil {
				return 0, err
			}
			if err = idx.Delete(val, rid); err != nil {
				return 0, err
			}
			if err = idx.Close(); err != nil {
				return 0, err
			}
		}
		if err := us.Delete(); err != nil {
			return 0, err
		}
		count++
		newHasNext, err := us.Next()
		if err != nil {
			return 0, err
		}
		hasNext = newHasNext
	}
	if err := us.Close(); err != nil {
		return 0, err
	}
	return count, nil
}

func (iup *IndexUpdatePlanner) ExecuteModify(data *parser.ModifyData, tx *tx.Transaction) (int, error) {
	tn := data.TableName()
	fn := data.TargetField()
	p, err := NewTablePlan(tx, tn, iup.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(p, data.Predicate())
	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScanner)
	if !ok {
		return 0, errors.New("invalid Scanner")
	}

	indexes, err := iup.mdm.GetIndexInfo(tn, tx)
	if err != nil {
		return 0, err
	}
	ii := indexes[fn]
	var idx index.Index
	if ii != nil {
		idx, err = ii.Open()
		if err != nil {
			return 0, err
		}
	}
	hasNext, err := us.Next()
	if err != nil {
		return 0, err
	}
	count := 0
	for hasNext {
		newVal, err := data.NewValue().Evaluate(us)
		if err != nil {
			return 0, err
		}
		if err := us.SetVal(fn, newVal); err != nil {
			return 0, err
		}

		// then update the appropriate index, if it exists
		if idx != nil {
			oldVal, err := us.GetVal(fn)
			if err != nil {
				return 0, err
			}
			rid, err := us.GetRid()
			if err != nil {
				return 0, err
			}
			if err := idx.Delete(oldVal, rid); err != nil {
				return 0, err
			}
			if err := idx.Insert(newVal, rid); err != nil {
				return 0, err
			}
		}
		count++
		newHasNext, err := us.Next()
		if err != nil {
			return 0, err
		}
		hasNext = newHasNext
	}
	if idx != nil {
		if err := idx.Close(); err != nil {
			return 0, err
		}
	}
	if err := us.Close(); err != nil {
		return 0, err
	}
	return count, nil
}

func (iup *IndexUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) (int, error) {
	return 0, iup.mdm.CreateTable(data.TableName(), data.Schema(), tx)
}

func (iup *IndexUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, tx *tx.Transaction) (int, error) {
	return 0, iup.mdm.CreateView(data.ViewName(), data.ViewDefinition(), tx)
}

func (iup *IndexUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) (int, error) {
	return 0, iup.mdm.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), tx)
}
