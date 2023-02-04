package metadata

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

// --------------------------
// |     index_catalogs     |
// --------------------------
// | index_name varchar(16) |
// | table_name varchar(16) |
// | field_name varchar(16) |
// --------------------------

const (
	indexCatalogTableName = "index_catalogs"
)
const (
	indexNameField = "index_name"
)

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
		err := tm.CreateTable(indexCatalogTableName, schema, tx)
		if err != nil {
			return nil, err
		}
	}
	layout, err := tm.Layout(indexCatalogTableName, tx)
	if err != nil {
		return nil, err
	}
	return &IndexManager{layout, tm, sm}, nil
}

// CreateIndex は indexCatalogTableName テーブルにインデックスのレコードを追加する
func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	ts, err := query.NewTableScan(tx, indexCatalogTableName, im.layout)
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

// IndexInfo は indexCatalogTableName をスキャンして、指定したテーブルのインデックス情報を取得する
func (im *IndexManager) IndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	iis := make(map[string]*IndexInfo)
	ts, err := query.NewTableScan(tx, indexCatalogTableName, im.layout)
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
			newHasNext, err := ts.Next()
			if err != nil {
				return nil, err
			}
			hasNext = newHasNext
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
