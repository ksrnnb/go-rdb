package file_test

import (
	"testing"

	. "github.com/ksrnnb/go-rdb/file"
)

func TestGetBytes(t *testing.T) {
	hello := []byte{'h', 'e', 'l', 'l', 'o'}
	page := NewWithBlockSize(len(hello))

	page.SetBytes(0, hello)

	v, err := page.GetInt(0)

	if err != nil {
		t.Fatalf("page.GetInt(0) error, %v", err)
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
