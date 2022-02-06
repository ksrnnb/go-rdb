package file_test

import (
	"testing"

	. "github.com/ksrnnb/go-rdb/file"
)

func TestGetBytes(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	page := NewPage(1024)

	err := page.SetBytes(0, hello)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	v, err := page.GetInt(0)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if v != 5 {
		t.Errorf("page.GetInt(0) = '%d' want 5", v)
	}

	bv, err := page.GetBytes(0)

	if err != nil {
		t.Fatalf("page.GetBytes(0) error, %v", err)
	}

	if string(bv) != "hello" {
		t.Errorf("page.GetBytes(0) = '%s' want 'hello'", bv)
	}
}

func TestGetInt(t *testing.T) {
	page := NewPage(1024)

	err := page.SetInt(0, 100)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	val, err := page.GetInt(0)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if val != 100 {
		t.Errorf(`val = '%d', want 100`, val)
	}
}

func TestGetString(t *testing.T) {
	page := NewPage(1024)

	err := page.SetString(0, "hello")

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	str, err := page.GetString(0)

	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if str != "hello" {
		t.Errorf(`str = '%s', want "hello"`, str)
	}
}

func TestMaxLength(t *testing.T) {
	len := MaxLength(5)

	// UTF-8 => 4 bytes
	maxBytesPerChar := 4
	int64Size := 64
	expected := 5*maxBytesPerChar + int64Size

	if len != expected {
		t.Errorf(`len = '%d', want %d`, len, expected)
	}
}
