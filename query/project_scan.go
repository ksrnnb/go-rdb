package query

import "errors"

type ProjectScan struct {
	scan      Scan
	fieldList []string
}

func NewProjectScan(scan Scan, filedList []string) *ProjectScan {
	return &ProjectScan{scan, filedList}
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.scan.BeforeFirst()
}

func (ps *ProjectScan) Next() (bool, error) {
	return ps.scan.Next()
}

func (ps *ProjectScan) GetInt(fieldName string) (int, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetInt(fieldName)
	}
	return 0, errors.New("field not found")
}

func (ps *ProjectScan) GetString(fieldName string) (string, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetString(fieldName)
	}
	return "", errors.New("field not found")
}

func (ps *ProjectScan) GetVal(fieldName string) (Constant, error) {
	if ps.HasField(fieldName) {
		return ps.scan.GetVal(fieldName)
	}
	return Constant{}, errors.New("field not found")
}

func (ps *ProjectScan) HasField(fieldName string) bool {
	for _, fn := range ps.fieldList {
		if fn == fieldName {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() error {
	return ps.scan.Close()
}
