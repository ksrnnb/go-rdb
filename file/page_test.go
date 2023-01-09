package file_test

import (
	"testing"

	. "github.com/ksrnnb/go-rdb/file"
	"github.com/stretchr/testify/assert"
)

func TestGetBytes(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	page := NewPage(1024)

	pos := 50
	page.SetBytes(pos, hello)
	v := page.GetInt(pos)
	assert.Equal(t, 5, v)

	bv := page.GetBytes(pos)
	assert.Equal(t, "hello", string(bv))
}

func TestGetInt(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	page.SetInt(pos, 100)
	val := page.GetInt(pos)

	assert.Equal(t, 100, val)
}

func TestGetString(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	page.SetString(pos, "hello")
	str := page.GetString(pos)

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
