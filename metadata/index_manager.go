package metadata

import (
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

const (
	indexCategoryTableName = "index_categories"
)
const (
	indexNameField = "index_name"
)

type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *tx.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	si          StatInfo
}

func NewIndexInfo(indexName string, fieldName string, tableSchema *record.Schema, tx *tx.Transaction, si StatInfo) (*IndexInfo, error) {
	indexLayout, err := createIndexLayout(tableSchema, fieldName)
	if err != nil {
		return nil, err
	}
	return &IndexInfo{indexName, fieldName, tx, tableSchema, indexLayout, si}, nil
}

func (ii *IndexInfo) Open() index.Index {
	return index.NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
}

func (ii *IndexInfo) BlocksAccessed() int {
	rpb := ii.calculateRecordsPerBlock()
	numBlocks := ii.si.RecordsOutput() / rpb
	return index.SearchCost(index.HashIndexType, numBlocks, rpb)
}

func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}
	return ii.si.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) calculateRecordsPerBlock() int {
	return ii.tx.BlockSize() / ii.indexLayout.SlotSize()
}

func createIndexLayout(tableSchema *record.Schema, fieldName string) (*record.Layout, error) {
	schema := record.NewSchema()
	schema.AddIntField(index.IndexIdField)
	schema.AddIntField(index.IndexBlockNumberField)

	ft, err := tableSchema.FieldType(fieldName)
	if err != nil {
		return nil, err
	}

	switch ft {
	case record.Integer:
		schema.AddIntField(index.IndexDataValueField)
	case record.String:
		l, err := tableSchema.Length(fieldName)
		if err != nil {
			return nil, err
		}
		schema.AddStringField(index.IndexDataValueField, l)
	}

	return record.NewLayout(schema)
}

type IndexManager struct {
	layout *record.Layout
	tm     *TableManager
	sm     *StatisticManager
}

func NewIndexManager(isNew bool, tm *TableManager, sm *StatisticManager, tx *tx.Transaction) (*IndexManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(indexNameField, MaxFieldNameLength)
		schema.AddStringField(tableNameField, MaxFieldNameLength)
		schema.AddStringField(fieldNameField, MaxFieldNameLength)
		err := tm.CreateTable(indexCategoryTableName, schema, tx)
		if err != nil {
			return nil, err
		}
	}
	layout, err := tm.Layout(indexCategoryTableName, tx)
	if err != nil {
		return nil, err
	}
	return &IndexManager{layout, tm, sm}, nil
}

// CreateIndex は indexCategoryTableName テーブルにインデックスのレコードを追加する
func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	ts, err := query.NewTableScan(tx, indexCategoryTableName, im.layout)
	if err != nil {
		return err
	}

	err = ts.Insert()
	if err != nil {
		return err
	}

	err = ts.SetString(indexNameField, indexName)
	if err != nil {
		return err
	}
	err = ts.SetString(tableNameField, tableName)
	if err != nil {
		return err
	}
	err = ts.SetString(fieldNameField, fieldName)
	if err != nil {
		return err
	}
	return ts.Close()
}

// IndexInfo は indexCategoryTableName をスキャンして、指定したテーブルのインデックス情報を取得する
func (im *IndexManager) IndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	iis := make(map[string]*IndexInfo)
	ts, err := query.NewTableScan(tx, indexCategoryTableName, im.layout)
	if err != nil {
		return nil, err
	}
	hasNext, err := ts.Next()
	if err != nil {
		return nil, err
	}
	for hasNext {
		tn, err := ts.GetString(tableNameField)
		if err != nil {
			return nil, err
		}
		if tn != tableName {
			continue
		}
		indexName, err := ts.GetString(indexNameField)
		if err != nil {
			return nil, err
		}
		fieldName, err := ts.GetString(fieldNameField)
		if err != nil {
			return nil, err
		}
		layout, err := im.tm.Layout(tableName, tx)
		if err != nil {
			return nil, err
		}
		si, err := im.sm.StatInfo(tableName, layout, tx)
		if err != nil {
			return nil, err
		}
		ii, err := NewIndexInfo(indexName, fieldName, layout.Schema(), tx, si)
		if err != nil {
			return nil, err
		}
		iis[fieldName] = ii

		newHasNext, err := ts.Next()
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}
	err = ts.Close()
	if err != nil {
		return nil, err
	}
	return iis, nil
}
