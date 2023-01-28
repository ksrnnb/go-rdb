package planner

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/query"
)

type MaxFunction struct {
	fieldName string
	val       query.Constant
}

func NewMaxFunction(fieldName string) *MaxFunction {
	return &MaxFunction{fieldName: fieldName}
}

func (f *MaxFunction) ProcessFirst(s query.Scanner) error {
	v, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	f.val = v
	return nil
}

func (f *MaxFunction) ProcessNext(s query.Scanner) error {
	newVal, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	if newVal.IsGreaterThan(f.val) {
		f.val = newVal
	}
	return nil
}

func (f *MaxFunction) FieldName() string {
	return fmt.Sprintf("max_of_%s", f.fieldName)
}

func (f *MaxFunction) Value() query.Constant {
	return f.val
}
