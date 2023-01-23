package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileManager(t *testing.T) {
	_, err := NewFileManager("data", 1024)
	assert.NoError(t, err)
}

func TestFile(t *testing.T) {
	bs := 400
	fm, err := NewFileManager("data", bs)

	require.NoError(t, err)

	blk := NewBlockID("tempTestFile", 2)
	p1 := NewPage(fm.BlockSize())

	pos1 := 88
	str := "abcdefghijklm"
	require.NoError(t, p1.SetString(pos1, str))

	size := MaxLengthInString(str)
	pos2 := pos1 + size
	intVal := 345
	require.NoError(t, p1.SetInt(pos2, intVal))

	err = fm.Write(blk, p1)
	assert.NoError(t, err)

	// p1からファイルに書き込んだ内容を、p2で読み取る
	p2 := NewPage(fm.BlockSize())
	err = fm.Read(blk, p2)
	assert.NoError(t, err)

	pos1Val, err := p2.GetString(pos1)
	assert.NoError(t, err)
	assert.Equal(t, str, pos1Val)

	pos2Val, err := p2.GetInt(pos2)
	assert.NoError(t, err)
	assert.Equal(t, intVal, pos2Val)
}
