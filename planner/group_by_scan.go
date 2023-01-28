package planner

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
)

type GroupByScan struct {
	scan        query.Scanner
	groupFields []string
	aggFns      []AggregationFunction
	groupVal    GroupValue
	moreGroups  bool
}

func NewGroupByScan(scan query.Scanner, groupFields []string, aggFns []AggregationFunction) (*GroupByScan, error) {
	gs := &GroupByScan{scan: scan, groupFields: groupFields, aggFns: aggFns}
	if err := gs.BeforeFirst(); err != nil {
		return nil, err
	}
	return gs, nil
}

func (gs *GroupByScan) BeforeFirst() error {
	if err := gs.scan.BeforeFirst(); err != nil {
		return err
	}
	moreGroups, err := gs.scan.Next()
	if err != nil {
		return err
	}
	gs.moreGroups = moreGroups
	return nil
}

func (gs *GroupByScan) Next() (bool, error) {
	if !gs.moreGroups {
		return false, nil
	}
	for _, aggFn := range gs.aggFns {
		if err := aggFn.ProcessFirst(gs.scan); err != nil {
			return false, err
		}
	}
	groupVal, err := NewGroupValue(gs.scan, gs.groupFields)
	if err != nil {
		return false, err
	}
	hasMoreGroup, err := gs.scan.Next()
	if err != nil {
		return false, err
	}
	gs.moreGroups = hasMoreGroup
	for gs.moreGroups {
		gv, err := NewGroupValue(gs.scan, gs.groupFields)
		if err != nil {
			return false, err
		}
		if !groupVal.Equals(gv) {
			break
		}
		for _, aggFn := range gs.aggFns {
			if err := aggFn.ProcessNext(gs.scan); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (gs *GroupByScan) Close() error {
	return gs.scan.Close()
}

func (gs *GroupByScan) GetInt(fieldName string) (int, error) {
	v, err := gs.GetVal(fieldName)
	if err != nil {
		return 0, err
	}
	return v.AsInt(), nil
}

func (gs *GroupByScan) GetString(fieldName string) (string, error) {
	v, err := gs.GetVal(fieldName)
	if err != nil {
		return "", err
	}
	return v.AsString(), nil
}

func (gs *GroupByScan) GetVal(fieldName string) (query.Constant, error) {
	if contains(gs.groupFields, fieldName) {
		return gs.groupVal.GetVal(fieldName), nil
	}
	for _, aggFn := range gs.aggFns {
		if aggFn.FieldName() == fieldName {
			return aggFn.Value(), nil
		}
	}
	return query.Constant{}, fmt.Errorf("no field %s", fieldName)
}

func (gs *GroupByScan) HasField(fieldName string) bool {
	if contains(gs.groupFields, fieldName) {
		return true
	}
	for _, aggFn := range gs.aggFns {
		if aggFn.FieldName() == fieldName {
			return true
		}
	}
	return false
}

// TODO: use generics...
func contains(heystack []string, needle string) bool {
	for _, e := range heystack {
		if e == needle {
			return true
		}
	}

	return false
}
