package file

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// TODO: synchronized
type FileManager struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
}

func NewFileManager(dbDirectory string, bs int) (*FileManager, error) {
	var isNew bool
	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		isNew = true
	}

	fm := &FileManager{
		dbDirectory: dbDirectory,
		blockSize:   bs,
		isNew:       isNew,
		openFiles:   map[string]*os.File{},
	}

	err := filepath.Walk(dbDirectory, func(path string, fi fs.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("file: NewFileManager failed, %v", err)
		}

		if fi.IsDir() {
			return nil
		}

		if strings.HasPrefix(fi.Name(), "temp") {
			err = os.Remove(filepath.Join(fm.dbDirectory, path))
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return fm, nil
}

func (fm *FileManager) Read(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	b := make([]byte, fm.blockSize)
	n, err := f.Read(b)

	if err != nil {
		return err
	}

	resizedBuf := make([]byte, n)
	copy(resizedBuf, b[:n])

	err = p.WriteBuf(resizedBuf)

	return err
}

func (fm *FileManager) Write(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	_, err = f.Write(p.ReadBuf())

	return err
}

func (fm *FileManager) Append(filename string) error {
	// TODO: これであっているか？
	newBlkNum := len(filename)
	blk := NewBlockID(filename, newBlkNum)

	f, err := fm.getFile(filename)

	if err != nil {
		return err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	b := make([]byte, fm.blockSize)
	_, err = f.Write(b)

	return err
}

func (fm *FileManager) Length(filename string) (int, error) {
	f, err := fm.getFile(filename)

	if err != nil {
		return 0, err
	}

	fs, err := f.Stat()

	if err != nil {
		return 0, err
	}

	return int(fs.Size()) / fm.blockSize, nil
}

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

func (fm *FileManager) BlockSize() int {
	return fm.blockSize
}

func (fm *FileManager) getFile(filename string) (*os.File, error) {
	f, ok := fm.openFiles[filename]

	if ok {
		return f, nil
	}

	newFile, err := os.Create(filepath.Join(fm.dbDirectory, filename))

	if err != nil {
		return nil, err
	}

	fm.openFiles[filename] = newFile
	return newFile, nil
}
