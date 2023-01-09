package bytebuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetPosition(t *testing.T) {
	bb := New(5)

	tests := []struct {
		name string
		pos  int
		want int
	}{
		{
			name: "position 4",
			pos:  0,
			want: 0,
		},
		{
			name: "position 4",
			pos:  4,
			want: 4,
		},
		{
			name: "position 5 over capacity",
			pos:  5,
			want: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bb.Position(test.pos)
			assert.True(t, bb.pos == test.want)
		})
	}
}

func TestPut(t *testing.T) {
	bb := New(1024)
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb.Put(hello)

	assert.NoError(t, bb.Err())
}

func TestGet(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb := New(1024)
	bb.Put(hello)

	dst := make([]byte, len(hello))
	bb.Position(0)
	bb.Get(dst)

	assert.Equal(t, "hello", string(dst))
}

func TestPutInt(t *testing.T) {
	bb := New(1024)

	bb.PutInt(100)

	assert.NoError(t, bb.Err())

	newBB := New(0)
	newBB.PutInt(100)

	assert.Error(t, newBB.Err())
}

func TestGetInt(t *testing.T) {
	bb := New(1024)
	bb.PutInt(100)
	bb.Position(0)

	val := bb.GetInt()

	assert.Equal(t, 100, val)
}

func TestJapanese(t *testing.T) {
	bb := New(1024)
	hello := []byte("こんにちわ")
	bb.Put(hello)
	bb.Position(0)

	dst := make([]byte, len(hello))
	bb.Get(dst)

	assert.Equal(t, "こんにちわ", string(dst))
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
			assert.Equal(t, tt.len, MaxLength(tt.str))
		})
	}
}
