package bytebuffer

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetPosition(t *testing.T) {
	bb := New(5)

	tests := []struct {
		name      string
		pos       int
		want      int
		wantError bool
	}{
		{
			name:      "position 4",
			pos:       0,
			want:      0,
			wantError: false,
		},
		{
			name:      "position 4",
			pos:       4,
			want:      4,
			wantError: false,
		},
		{
			name:      "position 5 over capacity",
			pos:       5,
			want:      4,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := bb.SetPosition(test.pos)
			if test.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.True(t, bb.pos == test.want)
		})
	}
}

func TestPut(t *testing.T) {
	bb := New(1024)
	hello := []byte{'h', 'e', 'l', 'l', 'o'}

	assert.NoError(t, bb.Put(hello))
}

func TestGet(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	bb := New(1024)
	require.NoError(t, bb.Put(hello))

	dst := make([]byte, len(hello))
	require.NoError(t, bb.SetPosition(0))

	assert.NoError(t, bb.Get(dst))
	assert.Equal(t, "hello", string(dst))
}

func TestPutInt(t *testing.T) {
	bb := New(1024)
	assert.NoError(t, bb.PutInt(100))

	newBB := New(0)
	assert.Error(t, newBB.PutInt(100))
}

func TestGetInt(t *testing.T) {
	bb := New(1024)
	require.NoError(t, bb.PutInt(100))
	require.NoError(t, bb.SetPosition(0))

	val := bb.GetInt()
	assert.Equal(t, 100, val)
}

func TestJapanese(t *testing.T) {
	bb := New(1024)
	hello := []byte("こんにちわ")

	require.NoError(t, bb.Put(hello))
	require.NoError(t, bb.SetPosition(0))

	dst := make([]byte, len(hello))
	err := bb.Get(dst)

	assert.NoError(t, err)
	assert.Equal(t, "こんにちわ", string(dst))
}

func TestMaxByte(t *testing.T) {
	cases := map[string]struct {
		str string
		len int
	}{
		"alphabet":                 {"abcd", 20},
		"number":                   {"123456", 28},
		"japanese":                 {"あいうえお", 24},
		"alphabet number japanese": {"abc123あいう", 40},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.len, MaxLength(utf8.RuneCountInString(tt.str)))
		})
	}
}
