package bytebuffer

import (
	"testing"
)

func TestSetPosition(t *testing.T) {
	bb := NewWithBlockSize(5)

	bb.Position(0)

	if bb.pos != 0 {
		t.Errorf("bb.pos = %d want 0", bb.pos)
	}

	bb.Position(4)

	if bb.pos != 4 {
		t.Errorf("bb.pos = %d want 5", bb.pos)
	}
}

func TestCannotSetPosition(t *testing.T) {
	bb := NewWithBlockSize(5)
	bb.Position(0)
	bb.Position(5)

	if bb.pos != 0 {
		t.Errorf("bb.pos = %d want 0", bb.pos)
	}
}

func TestPut(t *testing.T) {
	bb := NewWithBlockSize(1024)
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb.Put(hello)

	if bb.Error() != nil {
		t.Errorf(`bb.Error() = %v want nil`, bb.Error())
	}
}

func TestGet(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb := NewWithBlockSize(1024)
	bb.Put(hello)

	dst := make([]byte, len(hello))
	bb.Position(0)
	bb.Get(dst)

	if string(dst) != "hello" {
		t.Errorf(`bb.Get(dst) = '%s', want "hello"`, dst)
	}
}
