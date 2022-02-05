package bytebuffer

import (
	"testing"
)

func TestPosition(t *testing.T) {
	bb := NewWithBlockSize(0)

	bb.Position(0)

	if bb.pos != 0 {
		t.Errorf("bb.pos = %d want 0", bb.pos)
	}

	bb.Position(5)

	if bb.pos != 5 {
		t.Errorf("bb.pos = %d want 0", bb.pos)
	}
}

func TestPut(t *testing.T) {
	bb := NewWithBlockSize(0)
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb.Put(hello)

	if string(bb.buf) != "hello" {
		t.Errorf(`bb.buf = %s want "hello"`, bb.buf)
	}

	if bb.pos != len(hello) {
		t.Errorf(`bb.pos = %d want "%d"`, bb.pos, len(hello))
	}
}

func TestGet(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb := NewWithBlockSize(len(hello))
	bb.Put(hello)

	dst := make([]byte, len(hello))
	bb.Position(0)
	bb.Get(dst)

	if string(dst) != "hello" {
		t.Errorf(`bb.Get(dst) = '%s', want "hello"`, dst)
	}
}
