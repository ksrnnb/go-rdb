package query

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/record"
)

type SelectScan struct {
	scan Scanner
	pred *Predicate
}

func NewSelectScan(scan Scanner, pred *Predicate) *SelectScan {
	return &SelectScan{scan, pred}
}

func (ss *SelectScan) BeforeFirst() error {
	return ss.scan.BeforeFirst()
}

// Next は述語を満たすレコードを探す
func (ss *SelectScan) Next() (bool, error) {
	hasNext, err := ss.scan.Next()
	if err != nil {
		return false, err
	}

	for hasNext {
		isSatisfied, err := ss.pred.IsSatisfied(ss.scan)
		if err != nil {
			return false, err
		}
		if isSatisfied {
			return true, nil
		}
		newHasNext, err := ss.scan.Next()
		if err != nil {
			return false, err
		}
		hasNext = newHasNext
	}
	return false, nil
}

// Scanner methods

func (ss *SelectScan) GetInt(fieldName string) (int, error) {
	return ss.scan.GetInt(fieldName)
}

func (ss *SelectScan) GetString(fieldName string) (string, error) {
	return ss.scan.GetString(fieldName)
}

func (ss *SelectScan) GetVal(fieldName string) (Constant, error) {
	return ss.scan.GetVal(fieldName)
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.scan.HasField(fieldName)
}

func (ss *SelectScan) Close() error {
	return ss.scan.Close()
}

// UpdateScanner methods

func (ss *SelectScan) SetInt(fieldName string, val int) error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.SetInt(fieldName, val)
}

func (ss *SelectScan) SetString(fieldName string, val string) error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.SetString(fieldName, val)
}

func (ss *SelectScan) SetVal(fieldName string, val Constant) error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.SetVal(fieldName, val)
}

func (ss *SelectScan) Delete() error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.Delete()
}

func (ss *SelectScan) Insert() error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.Insert()
}

func (ss *SelectScan) GetRid() (*record.RecordID, error) {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return nil, fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.GetRid()
}

func (ss *SelectScan) MoveToRid(rid *record.RecordID) error {
	us, ok := ss.scan.(UpdateScanner)
	if !ok {
		return fmt.Errorf("underlying scan must be UpdateScanner, but got %T", ss.scan)
	}
	return us.MoveToRid(rid)
}
