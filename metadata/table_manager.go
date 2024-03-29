package metadata

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

// --------------------------
// |     table_catalogs     |
// --------------------------
// | table_name varchar(16) |
// | slot_size  int         |
// --------------------------

// --------------------------
// |     field_catalogs     |
// --------------------------
// | table_name varchar(16) |
// | field_name varchar(16) |
// | field_type int         |
// | length     int         |
// | offset     int         |
// --------------------------

const (
	MaxFieldNameLength = 16
	MaxTableNameLength = 16
)

const (
	tableCatalogTableName = "table_catalogs"
	fieldCatalogTableName = "field_catalogs"
)

const (
	tableNameField = "table_name"
	slotSizeField  = "slot_size"
	fieldNameField = "field_name"
	fieldTypeField = "field_type"
	lengthField    = "length"
	offsetField    = "offset"
)

type TableManager struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) (*TableManager, error) {
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField(tableNameField, MaxTableNameLength)
	tcatSchema.AddIntField(slotSizeField)
	tcatLayout := record.NewLayout(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField(tableNameField, MaxTableNameLength)
	fcatSchema.AddStringField(fieldNameField, MaxFieldNameLength)
	fcatSchema.AddIntField(fieldTypeField)
	fcatSchema.AddIntField(lengthField)
	fcatSchema.AddIntField(offsetField)
	fcatLayout := record.NewLayout(fcatSchema)

	tm := &TableManager{tcatLayout, fcatLayout}
	if isNew {
		err := tm.CreateTable(tableCatalogTableName, tcatSchema, tx)
		if err != nil {
			return nil, err
		}

		err = tm.CreateTable(fieldCatalogTableName, fcatSchema, tx)
		if err != nil {
			return nil, err
		}
	}
	return tm, nil
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayout(schema)
	err := tm.createTableCatalogTable(tableName, tx, layout)
	if err != nil {
		return err
	}
	return tm.createFieldCatalogTable(tableName, schema, tx, layout)
}

func (tm *TableManager) createTableCatalogTable(tableName string, tx *tx.Transaction, layout *record.Layout) error {
	tcatTs, err := query.NewTableScan(tx, tableCatalogTableName, tm.tcatLayout)
	if err != nil {
		return err
	}

	err = tcatTs.Insert()
	if err != nil {
		return err
	}

	err = tcatTs.SetString(tableNameField, tableName)
	if err != nil {
		return err
	}

	err = tcatTs.SetInt(slotSizeField, layout.SlotSize())
	if err != nil {
		return err
	}

	return tcatTs.Close()
}

func (tm *TableManager) createFieldCatalogTable(tableName string, schema *record.Schema, tx *tx.Transaction, layout *record.Layout) error {
	fcatTs, err := query.NewTableScan(tx, fieldCatalogTableName, tm.fcatLayout)
	if err != nil {
		return err
	}

	for _, fn := range schema.Fields() {
		err := fcatTs.Insert()
		if err != nil {
			return err
		}

		err = fcatTs.SetString(tableNameField, tableName)
		if err != nil {
			return err
		}

		err = fcatTs.SetString(fieldNameField, fn)
		if err != nil {
			return err
		}

		ft, err := schema.FieldType(fn)
		if err != nil {
			return err
		}

		err = fcatTs.SetInt(fieldTypeField, ft.AsInt())
		if err != nil {
			return err
		}

		length, err := schema.Length(fn)
		if err != nil {
			return err
		}

		err = fcatTs.SetInt(lengthField, length)
		if err != nil {
			return err
		}

		offset, err := layout.Offset(fn)
		if err != nil {
			return err
		}

		err = fcatTs.SetInt(offsetField, offset)
		if err != nil {
			return err
		}
	}
	return fcatTs.Close()
}

func (tm *TableManager) Layout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	size := -1
	tcatTs, err := query.NewTableScan(tx, tableCatalogTableName, tm.tcatLayout)
	if err != nil {
		return nil, err
	}

	hasNext, err := tcatTs.Next()
	if err != nil {
		return nil, err
	}

	for hasNext {
		str, err := tcatTs.GetString(tableNameField)
		if err != nil {
			return nil, err
		}

		if str == tableName {
			newSize, err := tcatTs.GetInt(slotSizeField)
			if err != nil {
				return nil, err
			}
			size = newSize
			break
		}
		newHasNext, err := tcatTs.Next()
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}

	err = tcatTs.Close()
	if err != nil {
		return nil, err
	}

	if size == -1 {
		return nil, fmt.Errorf("table is not found: %s", tableName)
	}

	schema := record.NewSchema()
	offsets := make(map[string]int)

	fcatTs, err := query.NewTableScan(tx, fieldCatalogTableName, tm.fcatLayout)
	if err != nil {
		return nil, err
	}

	hasNext, err = fcatTs.Next()
	if err != nil {
		return nil, err
	}

	for hasNext {
		str, err := fcatTs.GetString(tableNameField)
		if err != nil {
			return nil, err
		}

		if str == tableName {
			fn, err := fcatTs.GetString(fieldNameField)
			if err != nil {
				return nil, err
			}
			ft, err := fcatTs.GetInt(fieldTypeField)
			if err != nil {
				return nil, err
			}
			fl, err := fcatTs.GetInt(lengthField)
			if err != nil {
				return nil, err
			}
			fo, err := fcatTs.GetInt(offsetField)
			if err != nil {
				return nil, err
			}
			offsets[fn] = fo
			schema.AddField(fn, record.FieldType(ft), fl)
		}
		newHasNext, err := fcatTs.Next()
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}
	err = fcatTs.Close()
	if err != nil {
		return nil, err
	}

	return record.NewLayoutWithOffsets(schema, offsets, size), nil
}
