package query

import (
	"strconv"

	"github.com/ksrnnb/go-rdb/hashes"
)

type ConstantType uint8

const (
	UnknownConstant = iota
	IntConstant
	StringConstant
)

type Constant struct {
	intVal    int
	stringVal string
	ctype     ConstantType
}

func NewConstant(val interface{}) Constant {
	switch v := val.(type) {
	case int:
		return Constant{intVal: v, ctype: IntConstant}
	case string:
		return Constant{stringVal: v, ctype: StringConstant}
	default:
		return Constant{ctype: UnknownConstant}
	}
}

func (c Constant) IsUnknown() bool {
	return c.ctype == UnknownConstant
}

func (c Constant) AsInt() int {
	return c.intVal
}

func (c Constant) AsString() string {
	return c.stringVal
}

func (c Constant) Equals(cc Constant) bool {
	if c.ctype != cc.ctype {
		return false
	}
	switch c.ctype {
	case IntConstant:
		return c.intVal == cc.intVal
	case StringConstant:
		return c.stringVal == cc.stringVal
	default:
		return false
	}
}

func (c Constant) CompareTo(cc Constant) int {
	if c.ctype != IntConstant {
		return c.compareToInt(cc)
	}
	return c.compareToString(cc)
}

func (c Constant) String() string {
	if c.ctype == IntConstant {
		return strconv.Itoa(c.intVal)
	}
	return c.stringVal
}

func (c Constant) HashCode() uint32 {
	return hashes.HashCode(c)
}

func (c Constant) compareToInt(cc Constant) int {
	if c.intVal < cc.intVal {
		return -1
	} else if c.intVal == cc.intVal {
		return 0
	}
	return 1
}

func (c Constant) compareToString(cc Constant) int {
	if c.stringVal < cc.stringVal {
		return -1
	} else if c.stringVal == cc.stringVal {
		return 0
	}
	return 1
}
