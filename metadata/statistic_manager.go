package metadata

import (
	"sync"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

const refreshThreshold = 100

type StatInfo struct {
	numBlocks  int
	numRecords int
}

func NewStatInfo(numBlocks, numRecords int) StatInfo {
	return StatInfo{numBlocks, numRecords}
}

func (si StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

func (si StatInfo) RecordsOutput() int {
	return si.numRecords
}

func (si StatInfo) DistinctValues(fieldName string) int {
	// this is wildly inaccurate
	return 1 + (si.numRecords / 3)
}

type StatisticManager struct {
	tm         *TableManager
	tableStats map[string]StatInfo
	numCalls   int
	mux        *sync.Mutex
}

func NewStatisticManager(tm *TableManager, tx *tx.Transaction) (*StatisticManager, error) {
	sm := &StatisticManager{tm: tm, mux: &sync.Mutex{}}
	err := sm.refreshStatistics(tx)
	if err != nil {
		return nil, err
	}
	return sm, nil
}

// StatInfo を呼び出した回数が閾値を超えると再計算する
func (sm *StatisticManager) StatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (StatInfo, error) {
	sm.mux.Lock()
	defer sm.mux.Unlock()

	sm.numCalls++
	if sm.numCalls > refreshThreshold {
		sm.refreshStatistics(tx)
	}

	for tn, si := range sm.tableStats {
		if tn == tableName {
			return si, nil
		}
	}

	si, err := sm.calculateTableStatistics(tableName, layout, tx)
	if err != nil {
		return StatInfo{}, err
	}

	sm.tableStats[tableName] = si
	return si, nil
}

func (sm *StatisticManager) refreshStatistics(tx *tx.Transaction) error {
	sm.mux.Lock()
	defer sm.mux.Unlock()

	sm.tableStats = make(map[string]StatInfo)
	sm.numCalls = 0

	tcatLayout, err := sm.tm.Layout(tableCategoryTableName, tx)
	if err != nil {
		return err
	}

	ts, err := query.NewTableScan(tx, tableCategoryTableName, tcatLayout)
	if err != nil {
		return err
	}
	hasNext, err := ts.Next()
	if err != nil {
		return err
	}
	for hasNext {
		tn, err := ts.GetString(tableNameField)
		if err != nil {
			return err
		}
		layout, err := sm.tm.Layout(tn, tx)
		if err != nil {
			return err
		}
		si, err := sm.calculateTableStatistics(tn, layout, tx)
		if err != nil {
			return err
		}
		sm.tableStats[tn] = si
		newHasNext, err := ts.Next()
		if err != nil {
			return err
		}
		hasNext = newHasNext
	}

	return ts.Close()
}

func (sm *StatisticManager) calculateTableStatistics(tableName string, layout *record.Layout, tx *tx.Transaction) (StatInfo, error) {
	numRecords := 0
	numBlocks := 0

	ts, err := query.NewTableScan(tx, tableName, layout)
	if err != nil {
		return StatInfo{}, err
	}

	hasNext, err := ts.Next()
	if err != nil {
		return StatInfo{}, err
	}

	for hasNext {
		numRecords++
		rid, err := ts.GetRid()
		if err != nil {
			return StatInfo{}, err
		}
		numBlocks = rid.BlockNumber() + 1

		newHasNext, err := ts.Next()
		if err != nil {
			return StatInfo{}, err
		}
		hasNext = newHasNext
	}

	err = ts.Close()
	if err != nil {
		return StatInfo{}, err
	}

	return NewStatInfo(numBlocks, numRecords), nil
}
