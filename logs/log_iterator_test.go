package logs

import (
	"testing"

	"github.com/ksrnnb/go-rdb/file"
)

const dbDirectory = "./../data"

func newLogIterator(t *testing.T) *LogIterator {
	t.Helper()
	fm := newFileManaer(t)

	blk := file.NewBlockID("tempLogBlock", 0)
	li, err := NewLogIterator(fm, blk)

	if err != nil {
		t.Fatalf("failed to create new log iterator, %v", err)
	}

	return li
}

func newFileManaer(t *testing.T) *file.FileManager {
	t.Helper()
	fm, err := file.NewFileManager(dbDirectory, 400)

	if err != nil {
		t.Fatalf("failed to create file manager, %v", err)
	}
	return fm
}

func TestIteratesLogIterator(t *testing.T) {
	li := newLogIterator(t)

	if !li.HasNext() {
		t.Fatalf("HasNext() should be true but get false")
	}

	_, err := li.Next()

	if err != nil {
		t.Errorf("Next() failed, %v", err)
	}
}
