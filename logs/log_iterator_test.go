package logs

import (
	"testing"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLogIterator(t *testing.T) *LogIterator {
	t.Helper()
	fm := newFileManager(t)

	blk := file.NewBlockID("tempLogBlock", 0)
	li, err := NewLogIterator(fm, blk)

	require.NoError(t, err)

	return li
}

func newFileManager(t *testing.T) *file.FileManager {
	t.Helper()
	fm, err := file.NewFileManager("data", 400)

	require.NoError(t, err)
	return fm
}

func TestIteratesLogIterator(t *testing.T) {
	li := newLogIterator(t)

	if !li.HasNext() {
		t.Fatalf("HasNext() should be true but get false")
	}

	_, err := li.Next()

	assert.NoError(t, err)
}
