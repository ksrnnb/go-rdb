package index

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

// インデックスレコードがもつフィールドの定義
const (
	IndexIdField          = "id"
	IndexBlockNumberField = "block_number"
	IndexDataRidField     = "data_record_id"
	IndexDataValueField   = "data_value"
)

type Index interface {
	Next() (bool, error)
	BeforeFirst(searchKey query.Constant) error
	GetDataRid() (*record.RecordID, error)
	Insert(dataVal query.Constant, rid *record.RecordID) error
	Delete(dataVal query.Constant, rid *record.RecordID) error
	Close() error
}

type IndexType uint8

const (
	HashIndexType IndexType = iota + 1
)

func SearchCost(it IndexType, numBlocks int, rpb int) int {
	return 0
}
