package record

import "fmt"

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

func NewLayout(s *Schema) *Layout {
	offsets := make(map[string]int)
	pos := IntByteSize
	for _, fn := range s.Fields() {
		offsets[fn] = pos
		// schema の field から取得しているのでエラーは起こり得ない
		ofs, _ := s.lengthInBytes(fn)
		pos += ofs
	}
	return &Layout{s, offsets, pos}
}

func NewLayoutWithOffsets(s *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{s, offsets, slotSize}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) (int, error) {
	for fn, ofs := range l.offsets {
		if fn == fieldName {
			return ofs, nil
		}
	}
	return 0, fmt.Errorf("invalid field name [%s]", fieldName)
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}
