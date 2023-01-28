package planner

import "github.com/ksrnnb/go-rdb/query"

type MergeJoinScan struct {
	s1         query.Scanner
	s2         *SortScan
	fieldName1 string
	fieldName2 string
	joinVal    query.Constant
}

func NewMergeJoinScan(s1 query.Scanner, s2 *SortScan, fieldName1 string, fieldName2 string) (*MergeJoinScan, error) {
	ms := &MergeJoinScan{
		s1:         s1,
		s2:         s2,
		fieldName1: fieldName1,
		fieldName2: fieldName2,
	}
	if err := ms.BeforeFirst(); err != nil {
		return nil, err
	}
	return ms, nil
}

func (ms *MergeJoinScan) Close() error {
	if err := ms.s1.Close(); err != nil {
		return err
	}
	return ms.s2.Close()
}

func (ms *MergeJoinScan) BeforeFirst() error {
	if err := ms.s1.BeforeFirst(); err != nil {
		return err
	}
	return ms.s2.BeforeFirst()
}

func (ms *MergeJoinScan) Next() (bool, error) {
	hasMore2, err := ms.s2.Next()
	if err != nil {
		return false, err
	}
	v2, err := ms.s2.GetVal(ms.fieldName2)
	if err != nil {
		return false, err
	}
	if hasMore2 && v2.Equals(ms.joinVal) {
		return true, nil
	}

	hasMore1, err := ms.s1.Next()
	if err != nil {
		return false, err
	}
	v1, err := ms.s1.GetVal(ms.fieldName1)
	if err != nil {
		return false, err
	}
	if hasMore1 && v1.Equals(ms.joinVal) {
		return true, nil
	}

	for hasMore1 && hasMore2 {
		v1, err := ms.s1.GetVal(ms.fieldName1)
		if err != nil {
			return false, err
		}
		v2, err := ms.s2.GetVal(ms.fieldName2)
		if err != nil {
			return false, err
		}
		if v1.IsLessThan(v2) {
			newHasMore1, err := ms.s1.Next()
			if err != nil {
				return false, err
			}
			hasMore1 = newHasMore1
		} else if v1.IsGreaterThan(v2) {
			newHasMore2, err := ms.s2.Next()
			if err != nil {
				return false, err
			}
			hasMore2 = newHasMore2
		} else {
			if err := ms.s2.SavePosition(); err != nil {
				return false, err
			}
			newJoinVal, err := ms.s2.GetVal(ms.fieldName2)
			if err != nil {
				return false, err
			}
			ms.joinVal = newJoinVal
			return true, nil
		}
	}
	return false, nil
}

func (ms *MergeJoinScan) GetInt(fieldName string) (int, error) {
	if ms.s1.HasField(fieldName) {
		return ms.s1.GetInt(fieldName)
	}
	return ms.s2.GetInt(fieldName)
}

func (ms *MergeJoinScan) GetString(fieldName string) (string, error) {
	if ms.s1.HasField(fieldName) {
		return ms.s1.GetString(fieldName)
	}
	return ms.s2.GetString(fieldName)
}

func (ms *MergeJoinScan) GetVal(fieldName string) (query.Constant, error) {
	if ms.s1.HasField(fieldName) {
		return ms.s1.GetVal(fieldName)
	}
	return ms.s2.GetVal(fieldName)
}

func (ms *MergeJoinScan) HasField(fieldName string) bool {
	return ms.s1.HasField(fieldName) || ms.s2.HasField(fieldName)
}
