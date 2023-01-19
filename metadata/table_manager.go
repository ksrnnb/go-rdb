package metadata

import (
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

const (
	MaxFieldNameLength = 16
	MaxTableNameLength = 16
)

const (
	tableCategoryTableName = "table_categories"
	fieldCategoryTableName = "field_categories"
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
	tcatLayout, err := record.NewLayout(tcatSchema)
	if err != nil {
		return nil, err
	}

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField(tableNameField, MaxTableNameLength)
	fcatSchema.AddStringField(fieldNameField, MaxFieldNameLength)
	fcatSchema.AddIntField(fieldTypeField)
	fcatSchema.AddIntField(lengthField)
	fcatSchema.AddIntField(offsetField)
	fcatLayout, err := record.NewLayout(fcatSchema)
	if err != nil {
		return nil, err
	}

	tm := &TableManager{tcatLayout, fcatLayout}
	if isNew {
		err := tm.CreateTable(tableCategoryTableName, tcatSchema, tx)
		if err != nil {
			return nil, err
		}

		err = tm.CreateTable(fieldCategoryTableName, fcatSchema, tx)
		if err != nil {
			return nil, err
		}
	}
	return tm, nil
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout, err := record.NewLayout(schema)
	if err != nil {
		return err
	}
	err = tm.createTableCategoryTable(tableName, tx, layout)
	if err != nil {
		return err
	}
	return tm.createFieldCategoryTable(tableName, schema, tx, layout)
}

func (tm *TableManager) createTableCategoryTable(tableName string, tx *tx.Transaction, layout *record.Layout) error {
	tcatTs, err := record.NewTableScan(tx, tableCategoryTableName, tm.tcatLayout)
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

func (tm *TableManager) createFieldCategoryTable(tableName string, schema *record.Schema, tx *tx.Transaction, layout *record.Layout) error {
	fcatTs, err := record.NewTableScan(tx, fieldCategoryTableName, tm.fcatLayout)
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
	tcatTs, err := record.NewTableScan(tx, tableCategoryTableName, tm.tcatLayout)
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

	schema := record.NewSchema()
	offsets := make(map[string]int)

	fcatTs, err := record.NewTableScan(tx, fieldCategoryTableName, tm.fcatLayout)
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
