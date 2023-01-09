package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqualsBlockId(t *testing.T) {
	b1 := NewBlockID("test1", 1)
	b2 := NewBlockID("test2", 2)

	assert.False(t, b1.Equals(b2))

	b3 := NewBlockID("test1", 1)

	assert.True(t, b1.Equals(b3))
}

func TestStringBlockId(t *testing.T) {
	b := NewBlockID("test", 1)
	assert.Equal(t, "[file test, block 1]", b.String())
}
