package bytebuffer

import (
	"testing"
)

func TestSetPosition(t *testing.T) {
	bb := New(5)

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
	bb := New(5)
	bb.Position(0)
	bb.Position(5)

	if bb.pos != 0 {
		t.Errorf("bb.pos = %d want 0", bb.pos)
	}
}

func TestPut(t *testing.T) {
	bb := New(1024)
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb.Put(hello)

	if bb.Error() != nil {
		t.Errorf(`bb.Error() = %v want nil`, bb.Error())
	}
}

func TestGet(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb := New(1024)
	bb.Put(hello)

	dst := make([]byte, len(hello))
	bb.Position(0)
	bb.Get(dst)

	if string(dst) != "hello" {
		t.Errorf(`bb.Get(dst) = '%s', want "hello"`, dst)
	}
}

func TestPutInt(t *testing.T) {
	bb := New(1024)

	bb.PutInt(100)

	if bb.Error() != nil {
		t.Errorf(`bb.Error() = %v want nil`, bb.Error())
	}

	newBB := New(0)
	newBB.PutInt(100)

	if newBB.Error() == nil {
		t.Errorf(`newBB.Error() should has error but is nil`)
	}
}

func TestGetInt(t *testing.T) {
	bb := New(1024)
	bb.PutInt(100)
	bb.Position(0)

	val, err := bb.GetInt()

	if err != nil {
		t.Errorf(`err = %v want nil`, err)
	}

	if val != 100 {
		t.Errorf(`val = %d want 100`, val)
	}
}

func TestJapanese(t *testing.T) {
	bb := New(1024)
	hello := []byte("こんにちわ")
	bb.Put(hello)
	bb.Position(0)

	dst := make([]byte, len(hello))
	err := bb.Get(dst)

	if err != nil {
		t.Errorf(`err = %v want nil`, err)
	}

	if string(dst) != "こんにちわ" {
		t.Errorf(`bb.Get(dst) = '%s', want "こんにちわ"`, dst)
	}
}

func TestMaxByte(t *testing.T) {
	cases := map[string]struct {
		str string
		len int
	}{
		"alphabet":                 {"abcd", 8},
		"number":                   {"123456", 10},
		"japanese":                 {"あいうえお", 19},
		"alphabet number japanese": {"abc123あいう", 19},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			if MaxLength(tt.str) != tt.len {
				t.Errorf("'MaxLength(%s)' should be %d, but given %d", tt.str, tt.len, MaxLength(tt.str))
			}
		})
	}
}
