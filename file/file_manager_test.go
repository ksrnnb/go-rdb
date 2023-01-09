package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbDirectory = "./../data"

func TestNewFileManager(t *testing.T) {
	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		err = os.Mkdir(dbDirectory, 0744)

		if err != nil {
			t.Fatalf("TestNewFileManager: db directory cannot be created, %v", err)
		}
	}

	tmpFileName := filepath.Join(dbDirectory, "tempTest")
	_, err := os.Create(tmpFileName)

	if err != nil {
		t.Fatalf("TestNewFileManager: file cannot be created, %v", err)
	}

	_, err = NewFileManager(dbDirectory, 1024)

	if err != nil {
		t.Errorf("TestNewFileManager: file manager cannot be created, %v", err)
	}

	if _, err := os.Stat(tmpFileName); !os.IsNotExist(err) {
		t.Errorf("TestNewFileManager: temp file should be removed, but it exists, %v", err)
	}
}

func TestFile(t *testing.T) {
	bs := 400
	fm, err := NewFileManager(dbDirectory, bs)

	require.NoError(t, err)

	blk := NewBlockID("tempTestFile", 2)
	p1 := NewPage(fm.BlockSize())

	pos1 := 88
	str := "abcdefghijklm"
	require.NoError(t, p1.SetString(pos1, str))

	size := MaxLength(str)
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
