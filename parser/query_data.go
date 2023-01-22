package parser

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
)

type QueryData struct {
	fields []string
	tables []string
	pred   *query.Predicate
}

func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{fields, tables, pred}
}

func (qd *QueryData) Fields() []string {
	return qd.fields
}

func (qd *QueryData) Tables() []string {
	return qd.tables
}

func (qd *QueryData) Predicate() *query.Predicate {
	return qd.pred
}

func (qd *QueryData) String() string {
	var s string
	for i, fn := range qd.fields {
		if i == 0 {
			s = fmt.Sprintf("select %s", fn)
		} else {
			s = fmt.Sprintf("%s, %s", s, fn)
		}
	}

	for i, tn := range qd.tables {
		if i == 0 {
			s = fmt.Sprintf("%s from %s", s, tn)
		} else {
			s = fmt.Sprintf("%s, %s", s, tn)
		}
	}

	ps := qd.pred.String()
	if ps == "" {
		return s
	}
	return fmt.Sprintf("%s where %s", s, ps)
}
