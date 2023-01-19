package record

import (
	"fmt"

	"github.com/ksrnnb/go-rdb/file"
)

const IntByteSize = 4

type FieldType uint8

const (
	Unknown FieldType = iota
	Integer
	String
)

func (ft FieldType) AsInt() int {
	return int(ft)
}

type FieldInfo struct {
	fieldType FieldType
	length    int
}

func NewFieldInfo(fieldType FieldType, length int) *FieldInfo {
	return &FieldInfo{fieldType,
		length}
}

type Schema struct {
	fields    []string
	fieldInfo map[string]*FieldInfo
}

func NewSchema() *Schema {
	return &Schema{fieldInfo: make(map[string]*FieldInfo)}
}

func (s *Schema) AddField(fieldName string, fieldType FieldType, length int) {
	s.fields = append(s.fields, fieldName)
	s.fieldInfo[fieldName] = NewFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, Integer, 0)
}

func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, String, length)
}

func (s *Schema) Add(fieldName string, ss *Schema) error {
	ft, err := ss.FieldType(fieldName)
	if err != nil {
		return err
	}

	l, err := ss.Length(fieldName)
	if err != nil {
		return err
	}

	s.AddField(fieldName, ft, l)
	return nil
}

func (s *Schema) HasField(fieldName string) bool {
	for _, fn := range s.fields {
		if fn == fieldName {
			return true
		}
	}
	return false
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) FieldType(fieldName string) (FieldType, error) {
	fi, ok := s.fieldInfo[fieldName]
	if !ok {
		return Unknown, fmt.Errorf("cannot get field info: field name [%s]", fieldName)
	}
	return fi.fieldType, nil
}

func (s *Schema) Length(fieldName string) (int, error) {
	fi, ok := s.fieldInfo[fieldName]
	if !ok {
		return 0, fmt.Errorf("cannot get field info: field name [%s]", fieldName)
	}
	return fi.length, nil
}

func (s *Schema) lengthInBytes(fieldName string) (int, error) {
	fi, ok := s.fieldInfo[fieldName]
	if !ok {
		return 0, fmt.Errorf("cannot get field info: field name [%s]", fieldName)
	}

	switch fi.fieldType {
	case Integer:
		return IntByteSize, nil
	case String:
		strlen, err := s.Length(fieldName)
		if err != nil {
			return 0, err
		}
		return file.MaxLength(strlen), nil
	default:
		return 0, fmt.Errorf("invalid field type [%d]", fi.fieldType)
	}
}
