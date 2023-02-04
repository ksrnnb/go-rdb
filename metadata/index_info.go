package metadata

import (
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/index/btree"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
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

func (ii *IndexInfo) Open() (index.Index, error) {
	// return index.NewHashIndex(ii.tx, ii.indexName, ii.indexLayout), nil
	return btree.NewBTreeIndex(ii.tx, ii.indexName, ii.indexLayout)
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

	return record.NewLayout(schema), nil
}
