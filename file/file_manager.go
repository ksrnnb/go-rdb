package file

import (
	"errors"
	"fmt"
	"io"
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

	// tempから始まるファイルは削除
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

// 指定したブロック領域をページに読み込む
func (fm *FileManager) Read(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return fmt.Errorf("file: Read() failed to get file from BlockID, %w", err)
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	b := make([]byte, fm.blockSize)
	n, ioErr := f.Read(b)

	// Callers should always process the n > 0 bytes returned before
	// considering the error err.
	// Doing so correctly handles I/O errors that happen after
	// reading some bytes and also both of the allowed EOF behaviors.
	if n > 0 {
		resizedBuf := make([]byte, n)
		copy(resizedBuf, b[:n])

		err = p.WriteBuf(resizedBuf)

		if err != nil {
			return fmt.Errorf("file: Read() failed to write buffer to page, %w", err)
		}
	}

	if ioErr != nil && !errors.Is(ioErr, io.EOF) {
		return fmt.Errorf("file: Read() failed to read file to buffer, %w", err)
	}

	return nil
}

// 指定したブロック位置にページの内容を全て書き込む
func (fm *FileManager) Write(blk *BlockID, p *Page) error {
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	_, err = f.Write(p.ReadBuf())

	return err
}

// Append()は新しく空のブロックを作成して、指定したファイルに割り当てる。
func (fm *FileManager) Append(filename string) (*BlockID, error) {
	newBlkNum, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	blk := NewBlockID(filename, newBlkNum)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	b := make([]byte, fm.blockSize)
	_, err = f.Write(b)
	if err != nil {
		return nil, err
	}

	return blk, nil
}

// Length()は指定したファイルの長さ（=ブロックNo.）を取得する
// 具体的には、指定したファイルのサイズをブロックサイズで割る。
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

// IsNew()は、DBのディレクトリを新規に作成したかどうかを返す
func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

// BlockSize()はFileManagerがもつブロックサイズを返す
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
