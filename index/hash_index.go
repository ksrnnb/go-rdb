package index

import (
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type HashIndex struct{}

func NewHashIndex(tx *tx.Transaction, indexName string, layout *record.Layout) *HashIndex {
	return &HashIndex{}
}

func (hi *HashIndex) Next() bool {
	return true
}
