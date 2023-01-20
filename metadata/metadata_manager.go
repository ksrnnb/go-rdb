package metadata

import (
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type MetadataManager struct {
	tm *TableManager
	vm *ViewManager
	sm *StatisticManager
	im *IndexManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) (*MetadataManager, error) {
	tm, err := NewTableManager(isNew, tx)
	if err != nil {
		return nil, err
	}

	vm, err := NewViewManager(isNew, tm, tx)
	if err != nil {
		return nil, err
	}

	sm, err := NewStatisticManager(tm, tx)
	if err != nil {
		return nil, err
	}

	im, err := NewIndexManager(isNew, tm, sm, tx)
	if err != nil {
		return nil, err
	}
	return &MetadataManager{tm, vm, sm, im}, nil
}

func (mm *MetadataManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	return mm.tm.CreateTable(tableName, schema, tx)
}

func (mm *MetadataManager) Layout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	return mm.tm.Layout(tableName, tx)
}

func (mm *MetadataManager) CreateView(viewName string, definition string, tx *tx.Transaction) error {
	return mm.vm.CreateView(viewName, definition, tx)
}

func (mm *MetadataManager) GetViewDefinition(viewName string, tx *tx.Transaction) (string, error) {
	return mm.vm.Definition(viewName, tx)
}

func (mm *MetadataManager) CreateIndex(indexName, tableName, fieldName string, tx *tx.Transaction) error {
	return mm.im.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mm *MetadataManager) GetIndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	return mm.im.IndexInfo(tableName, tx)
}

func (mm *MetadataManager) GetStatInfo(tablename string, layout *record.Layout, tx *tx.Transaction) (StatInfo, error) {
	return mm.sm.StatInfo(tablename, layout, tx)
}
