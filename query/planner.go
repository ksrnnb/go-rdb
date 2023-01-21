package query

import (
	"github.com/ksrnnb/go-rdb/record"
)

type Planner interface {
	Open() Scanner
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	Schema() *record.Schema
}
