package file

import (
	"os"
	"path/filepath"
	"testing"
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

	if err != nil {
		t.Errorf("TestNewFileManager: file manager cannot be created, %v", err)
	}

	blk := NewBlockID("testfile", 2)
	p1 := NewPage(fm.BlockSize())

	pos1 := 88
	str := "abcdefghijklm"
	err = p1.SetString(pos1, str)

	if err != nil {
		t.Errorf("TestNewFileManager: p1.SetString(pos1, str) failed, %v", err)
	}

	size := MaxLength(len(str))
	pos2 := pos1 + size
	intVal := 345
	p1.SetInt(pos2, intVal)

	if err != nil {
		t.Errorf("TestNewFileManager: p1.SetInt(pos2, intVal) failed, %v", err)
	}

	err = fm.Write(blk, p1)

	if err != nil {
		t.Errorf("TestNewFileManager: fm.Write(blk, p1) failed, %v", err)
	}

	// p1からファイルに書き込んだ内容を、p2で読み取る
	p2 := NewPage(fm.BlockSize())
	err = fm.Read(blk, p2)

	if err != nil {
		t.Errorf("TestNewFileManager: fm.Read(blk, p2) failed, %v", err)
	}

	pos1Val, err := p2.GetString(pos1)
	if err != nil {
		t.Errorf("TestNewFileManager: p1.GetString(pos1) failed, %v", err)
	}

	if pos1Val != str {
		t.Errorf("TestNewFileManager: pos1Val = %s, want %s", pos1Val, str)
	}

	pos2Val, err := p2.GetInt(pos2)
	if err != nil {
		t.Errorf("TestNewFileManager: p2.GetInt(pos2) failed, %v", err)
	}

	if pos2Val != intVal {
		t.Errorf("TestNewFileManager: pos2Val = %d, want %d", pos2Val, intVal)
	}
}
