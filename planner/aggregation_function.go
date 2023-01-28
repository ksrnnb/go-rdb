package planner

import "github.com/ksrnnb/go-rdb/query"

type AggregationFunction interface {
	ProcessFirst(s query.Scanner) error
	ProcessNext(s query.Scanner) error
	FieldName() string
	Value() query.Constant
}
