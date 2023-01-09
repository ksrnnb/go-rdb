package file_test

import (
	"testing"

	. "github.com/ksrnnb/go-rdb/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBytes(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	page := NewPage(1024)

	pos := 50
	require.NoError(t, page.SetBytes(pos, hello))
	v, err := page.GetInt(pos)
	assert.NoError(t, err)
	assert.Equal(t, 5, v)

	bv, err := page.GetBytes(pos)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(bv))
}

func TestGetInt(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	require.NoError(t, page.SetInt(pos, 100))
	val, err := page.GetInt(pos)

	assert.NoError(t, err)
	assert.Equal(t, 100, val)
}

func TestGetString(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	require.NoError(t, page.SetString(pos, "hello"))
	str, err := page.GetString(pos)

	assert.NoError(t, err)
	assert.Equal(t, "hello", str)
}

func TestMaxLength(t *testing.T) {
	cases := map[string]struct {
		str      string
		expected int
	}{
		"alphabet": {"abcd", 8},
		"number":   {"123", 7},
		"japanese": {"あいうえお", 19},
		"mix":      {"abc123あいうえお", 25},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, MaxLength(tt.str))
		})
	}
}
