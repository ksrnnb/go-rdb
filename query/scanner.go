package query

import "github.com/ksrnnb/go-rdb/record"

type Scanner interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt(fieldName string) (int, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (Constant, error)
	HasField(fieldName string) bool
	Close() error
}

// UpdateScanner は TableScan と SelectScan のみ実装
// Select した結果は update できる
type UpdateScanner interface {
	Scanner
	SetInt(fieldName string, val int) error
	SetString(fieldName string, val string) error
	SetVal(fieldName string, val Constant) error
	Insert() error
	Delete() error

	GetRid() (*record.RecordID, error)
	MoveToRid(rid *record.RecordID) error
}
