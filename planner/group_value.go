package planner

import "github.com/ksrnnb/go-rdb/query"

type GroupValue struct {
	values map[string]query.Constant
}

func NewGroupValue(s query.Scanner, fields []string) (GroupValue, error) {
	values := make(map[string]query.Constant)
	for _, fn := range fields {
		v, err := s.GetVal(fn)
		if err != nil {
			return GroupValue{}, nil
		}
		values[fn] = v
	}
	return GroupValue{values}, nil
}

func (gv GroupValue) GetVal(fieldName string) query.Constant {
	return gv.values[fieldName]
}

func (gv1 GroupValue) Equals(gv2 GroupValue) bool {
	for fn, v1 := range gv1.values {
		v2 := gv2.GetVal(fn)
		if !v1.Equals(v2) {
			return false
		}
	}
	return true
}

func (gv GroupValue) HashCode() uint32 {
	var hashVal uint32
	for _, c := range gv.values {
		hashVal += c.HashCode()
	}
	return hashVal
}
