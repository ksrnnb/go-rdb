package file_test

import (
	"testing"

	. "github.com/ksrnnb/go-rdb/file"
)

func TestGetBytes(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	page := NewPage(1024)

	pos := 50
	err := page.SetBytes(pos, hello)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	v, err := page.GetInt(pos)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if v != 5 {
		t.Errorf("page.GetInt(%d) = '%d' want 5", pos, v)
	}

	bv, err := page.GetBytes(pos)

	if err != nil {
		t.Fatalf("page.GetBytes(%d) error, %v", pos, err)
	}

	if string(bv) != "hello" {
		t.Errorf("page.GetBytes(%d) = '%s' want 'hello'", pos, bv)
	}
}

func TestGetInt(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	err := page.SetInt(pos, 100)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	val, err := page.GetInt(pos)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if val != 100 {
		t.Errorf(`val = '%d', want 100`, val)
	}
}

func TestGetString(t *testing.T) {
	page := NewPage(1024)

	pos := 50
	err := page.SetString(pos, "hello")

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	str, err := page.GetString(pos)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if str != "hello" {
		t.Errorf(`str = '%s', want "hello"`, str)
	}
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
			if MaxLength(tt.str) != tt.expected {
				t.Errorf("MaxLength(%s) should be %d, but given %d", tt.str, tt.expected, MaxLength(tt.str))
			}
		})
	}
}
