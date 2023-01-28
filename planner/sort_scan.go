package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type SortScan struct {
	s1            query.UpdateScanner
	s2            query.UpdateScanner
	currentScan   query.UpdateScanner
	comparator    RecordComparator
	hasMore1      bool
	hasMore2      bool
	savedPosition []*record.RecordID
}

func NewSortScan(runs []*TemporaryTable, comparator RecordComparator) (*SortScan, error) {
	var err error
	ss := &SortScan{comparator: comparator}
	ss.s1, err = runs[0].Open()
	if err != nil {
		return nil, err
	}
	ss.hasMore1, err = ss.s1.Next()
	if err != nil {
		return nil, err
	}
	if len(runs) == 1 {
		return ss, nil
	}

	ss.s2, err = runs[1].Open()
	if err != nil {
		return nil, err
	}
	ss.hasMore2, err = ss.s2.Next()
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (ss *SortScan) BeforeFirst() error {
	if err := ss.s1.BeforeFirst(); err != nil {
		return err
	}
	hasMore1, err := ss.s1.Next()
	if err != nil {
		return err
	}
	ss.hasMore1 = hasMore1

	if ss.s2 == nil {
		return nil
	}

	if err := ss.s2.BeforeFirst(); err != nil {
		return err
	}
	hasMore2, err := ss.s2.Next()
	if err != nil {
		return err
	}
	ss.hasMore2 = hasMore2
	return nil
}

func (ss *SortScan) Next() (bool, error) {
	if ss.currentScan == ss.s1 {
		hasMore1, err := ss.s1.Next()
		if err != nil {
			return false, err
		}
		ss.hasMore1 = hasMore1
	} else if ss.currentScan == ss.s2 {
		hasMore2, err := ss.s2.Next()
		if err != nil {
			return false, err
		}
		ss.hasMore2 = hasMore2
	}

	if !(ss.hasMore1 || ss.hasMore2) {
		return false, nil
	}
	if ss.hasMore1 && ss.hasMore2 {
		cmp, err := ss.comparator.Compare(ss.s1, ss.s2)
		if err != nil {
			return false, err
		}
		if cmp < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}
	return true, nil
}

func (ss *SortScan) Close() error {
	if err := ss.s1.Close(); err != nil {
		return err
	}
	if ss.s2 == nil {
		return nil
	}
	return ss.s2.Close()
}
func (ss *SortScan) GetInt(fieldName string) (int, error) {
	return ss.currentScan.GetInt(fieldName)
}

func (ss *SortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

func (ss *SortScan) GetVal(fieldName string) (query.Constant, error) {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *SortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *SortScan) SavePosition() error {
	rid1, err := ss.s1.GetRid()
	if err != nil {
		return err
	}
	rid2, err := ss.s2.GetRid()
	if err != nil {
		return err
	}
	ss.savedPosition = []*record.RecordID{rid1, rid2}
	return nil
}

func (ss *SortScan) RestorePosition() error {
	rid1 := ss.savedPosition[0]
	rid2 := ss.savedPosition[1]

	if err := ss.s1.MoveToRid(rid1); err != nil {
		return err
	}
	return ss.s2.MoveToRid(rid2)
}
