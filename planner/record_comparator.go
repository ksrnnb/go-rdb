package planner

import "github.com/ksrnnb/go-rdb/query"

type RecordComparator struct {
	fields []string
}

func NewRecordComparator(fields []string) RecordComparator {
	return RecordComparator{fields}
}

func (rc RecordComparator) Compare(s1 query.Scanner, s2 query.Scanner) (int, error) {
	for _, fn := range rc.fields {
		val1, err := s1.GetVal(fn)
		if err != nil {
			return 0, err
		}
		val2, err := s2.GetVal(fn)
		if err != nil {
			return 0, err
		}
		result := val1.CompareTo(val2)
		if result != 0 {
			return result, nil
		}
	}
	return 0, nil
}
