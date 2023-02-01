package metadata

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

// -------------------------------
// |        view_catalogs        |
// -------------------------------
// | view_name       varchar(16) |
// | view_definition varchar(16) |
// | field_name      varchar(16) |
// -------------------------------

const MaxViewDefinitionLength = 100

const viewCatalogTableName = "view_catalogs"

const (
	viewNameField       = "view_name"
	viewDefinitionField = "view_definition"
)

type ViewManager struct {
	tm *TableManager
}

func NewViewManager(isNew bool, tm *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	vm := &ViewManager{tm}
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(viewNameField, MaxFieldNameLength)
		schema.AddStringField(viewDefinitionField, MaxFieldNameLength)
		err := vm.tm.CreateTable(viewCatalogTableName, schema, tx)
		if err != nil {
			return nil, err
		}
	}
	return vm, nil
}

func (vm *ViewManager) CreateView(viewName string, definition string, tx *tx.Transaction) error {
	layout, err := vm.tm.Layout(viewCatalogTableName, tx)
	if err != nil {
		return err
	}
	ts, err := query.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return err
	}
	err = ts.Insert()
	if err != nil {
		return err
	}
	err = ts.SetString(viewNameField, viewName)
	if err != nil {
		return err
	}
	err = ts.SetString(viewDefinitionField, definition)
	if err != nil {
		return err
	}
	return ts.Close()
}

func (vm *ViewManager) Definition(viewName string, tx *tx.Transaction) (string, error) {
	definition := ""
	layout, err := vm.tm.Layout(viewCatalogTableName, tx)
	if err != nil {
		return "", err
	}

	ts, err := query.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return "", err
	}

	hasNext, err := ts.Next()
	if err != nil {
		return "", err
	}

	for hasNext {
		vn, err := ts.GetString(viewNameField)
		if err != nil {
			return "", err
		}
		if vn == viewName {
			definition, err = ts.GetString(viewDefinitionField)
			if err != nil {
				return "", err
			}
			break
		}
		newHasNext, err := ts.Next()
		if err != nil {
			return "", err
		}
		hasNext = newHasNext
	}

	err = ts.Close()
	if err != nil {
		return "", err
	}
	return definition, nil
}
